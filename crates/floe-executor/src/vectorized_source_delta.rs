use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, Int64Array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use dbsp::circuit::WEIGHT_COLUMN_NAME;

use crate::table_provider::DynamicStateTableProvider;

pub(super) async fn apply_source_delta(
    schema: &SchemaRef,
    primary_key_columns: &[String],
    provider: &DynamicStateTableProvider,
    delta: &RecordBatch,
) -> Result<Vec<RecordBatch>> {
    let weight_idx = delta.schema().index_of(WEIGHT_COLUMN_NAME)?;
    if delta.schema().field(weight_idx).data_type() != &DataType::Int64 {
        bail!("source delta {} column must be Int64", WEIGHT_COLUMN_NAME);
    }
    let expected_delta_schema = crate::delta_consolidation::weighted_snapshot_schema(schema)?;
    if delta.schema().as_ref() != expected_delta_schema.as_ref() {
        bail!("source delta schema does not match source schema");
    }

    let old_snapshot = provider.snapshot();
    let key_indices = source_key_indices(schema, primary_key_columns)?;
    let key_effects = source_delta_key_effects(schema, &key_indices, delta, weight_idx)?;
    let mut next = if key_effects.touched_keys.is_empty() {
        old_snapshot.iter().cloned().collect::<Vec<_>>()
    } else {
        filter_touched_source_rows(
            schema,
            &key_indices,
            &key_effects.touched_keys,
            &old_snapshot,
        )?
    };
    if let Some(positive_batch) =
        final_positive_delta_batch(schema, delta, weight_idx, &key_effects.final_positive_rows)?
    {
        next.push(positive_batch);
    }
    Ok(next)
}

pub(super) fn insert_only_source_delta_batch(
    schema: &SchemaRef,
    delta: &RecordBatch,
) -> Result<Option<RecordBatch>> {
    let weight_idx = delta.schema().index_of(WEIGHT_COLUMN_NAME)?;
    if delta.schema().field(weight_idx).data_type() != &DataType::Int64 {
        bail!("source delta {} column must be Int64", WEIGHT_COLUMN_NAME);
    }
    let expected_delta_schema = crate::delta_consolidation::weighted_snapshot_schema(schema)?;
    if delta.schema().as_ref() != expected_delta_schema.as_ref() {
        bail!("source delta schema does not match source schema");
    }

    let weights = delta
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("source delta weight column must be Int64"))?;
    for row_idx in 0..weights.len() {
        if weights.is_null(row_idx) || weights.value(row_idx) <= 0 {
            return Ok(None);
        }
    }

    let columns = delta
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| (idx != weight_idx).then_some(Arc::clone(column)))
        .collect::<Vec<_>>();
    Ok(Some(RecordBatch::try_new(Arc::clone(schema), columns)?))
}

fn source_key_indices(schema: &SchemaRef, primary_key_columns: &[String]) -> Result<Vec<usize>> {
    if primary_key_columns.is_empty() {
        return Ok((0..schema.fields().len()).collect());
    }
    primary_key_columns
        .iter()
        .map(|column| {
            schema.index_of(column).with_context(|| {
                format!("source primary key column '{column}' missing from schema")
            })
        })
        .collect()
}

struct SourceDeltaKeyEffects {
    touched_keys: HashSet<Vec<u8>>,
    final_positive_rows: HashSet<usize>,
}

fn source_delta_key_effects(
    schema: &SchemaRef,
    key_indices: &[usize],
    delta: &RecordBatch,
    weight_idx: usize,
) -> Result<SourceDeltaKeyEffects> {
    let weights = delta
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("source delta weight column must be Int64"))?;
    let converter = key_row_converter(schema, key_indices)?;
    let rows = converter
        .convert_columns(&project_columns(delta, key_indices))
        .context("encode source delta keys")?;
    let mut touched_keys = HashSet::new();
    let mut final_positive_row_by_key: HashMap<Vec<u8>, Option<usize>> = HashMap::new();
    for row_idx in 0..weights.len() {
        if weights.is_null(row_idx) {
            continue;
        }
        let weight = weights.value(row_idx);
        if weight == 0 {
            continue;
        }
        let key = rows.row(row_idx).data().to_vec();
        touched_keys.insert(key.clone());
        final_positive_row_by_key.insert(key, (weight > 0).then_some(row_idx));
    }
    let final_positive_rows = final_positive_row_by_key
        .into_values()
        .flatten()
        .collect::<HashSet<_>>();
    Ok(SourceDeltaKeyEffects {
        touched_keys,
        final_positive_rows,
    })
}

fn filter_touched_source_rows(
    schema: &SchemaRef,
    key_indices: &[usize],
    touched_keys: &HashSet<Vec<u8>>,
    snapshot: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    let converter = key_row_converter(schema, key_indices)?;
    let mut next = Vec::with_capacity(snapshot.len());
    for batch in snapshot {
        if batch.num_rows() == 0 {
            continue;
        }
        let rows = converter
            .convert_columns(&project_columns(batch, key_indices))
            .context("encode source state keys")?;
        let mut keep = BooleanBuilder::with_capacity(batch.num_rows());
        let mut kept_rows = 0usize;
        for row_idx in 0..batch.num_rows() {
            let keep_row = !touched_keys.contains(rows.row(row_idx).data());
            if keep_row {
                kept_rows = kept_rows.saturating_add(1);
            }
            keep.append_value(keep_row);
        }
        if kept_rows == batch.num_rows() {
            next.push(batch.clone());
        } else if kept_rows > 0 {
            next.push(filter_record_batch(batch, &keep.finish())?);
        }
    }
    Ok(next)
}

fn final_positive_delta_batch(
    schema: &SchemaRef,
    delta: &RecordBatch,
    weight_idx: usize,
    final_positive_rows: &HashSet<usize>,
) -> Result<Option<RecordBatch>> {
    if final_positive_rows.is_empty() {
        return Ok(None);
    }
    let weights = delta
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("source delta weight column must be Int64"))?;
    let mut keep = BooleanBuilder::with_capacity(weights.len());
    let mut kept_rows = 0usize;
    for row_idx in 0..weights.len() {
        let keep_row = final_positive_rows.contains(&row_idx);
        if keep_row {
            kept_rows = kept_rows.saturating_add(1);
        }
        keep.append_value(keep_row);
    }
    if kept_rows == 0 {
        return Ok(None);
    }
    let filtered = filter_record_batch(delta, &keep.finish())?;
    let columns = filtered
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| (idx != weight_idx).then_some(Arc::clone(column)))
        .collect::<Vec<_>>();
    Ok(Some(RecordBatch::try_new(Arc::clone(schema), columns)?))
}

fn key_row_converter(schema: &SchemaRef, key_indices: &[usize]) -> Result<RowConverter> {
    let fields = key_indices
        .iter()
        .map(|idx| SortField::new(schema.field(*idx).data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build Arrow row converter for source keys")
}

fn project_columns(batch: &RecordBatch, indices: &[usize]) -> Vec<ArrayRef> {
    indices
        .iter()
        .map(|idx| Arc::clone(batch.column(*idx)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};

    fn int64_values(batch: &RecordBatch, column_idx: usize) -> Vec<i64> {
        let column = batch
            .column(column_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        (0..column.len()).map(|idx| column.value(idx)).collect()
    }

    #[test]
    fn insert_only_delta_strips_weight_without_rebuilding_source_state() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
        let delta = RecordBatch::try_new(
            weighted_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .expect("weighted delta");

        let batch = insert_only_source_delta_batch(&schema, &delta)
            .expect("detect insert-only")
            .expect("insert batch");

        assert_eq!(batch.schema().as_ref(), schema.as_ref());
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn delete_delta_uses_general_source_state_path() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
        let delta = RecordBatch::try_new(
            weighted_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![-1])),
            ],
        )
        .expect("weighted delta");

        assert!(
            insert_only_source_delta_batch(&schema, &delta)
                .expect("inspect delta")
                .is_none()
        );
    }

    #[tokio::test]
    async fn source_delta_replaces_existing_row_with_final_positive_for_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let provider = DynamicStateTableProvider::new(Arc::clone(&schema));
        let old_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .expect("old source batch");
        provider.set_batches(vec![old_batch]);
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
        let delta = RecordBatch::try_new(
            weighted_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![-1, 1])),
            ],
        )
        .expect("weighted delta");

        let next = apply_source_delta(&schema, &["id".to_string()], &provider, &delta)
            .await
            .expect("apply source delta");

        assert_eq!(next.len(), 1);
        assert_eq!(int64_values(&next[0], 0), vec![1]);
        assert_eq!(int64_values(&next[0], 1), vec![20]);
    }

    #[tokio::test]
    async fn source_delta_drops_same_key_rows_when_final_operation_is_delete() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let provider = DynamicStateTableProvider::new(Arc::clone(&schema));
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
        let delta = RecordBatch::try_new(
            weighted_schema,
            vec![
                Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int64Array::from(vec![10, 10, 20, 20])),
                Arc::new(Int64Array::from(vec![1, -1, 1, -1])),
            ],
        )
        .expect("weighted delta");

        let next = apply_source_delta(&schema, &["id".to_string()], &provider, &delta)
            .await
            .expect("apply source delta");

        assert!(next.is_empty());
    }
}
