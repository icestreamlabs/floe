use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::{BinaryArray, BooleanBuilder, Int64Array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DFResult, internal_err};
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::expr_fn::{min, sum};
use datafusion::prelude::{Expr, SessionContext, col};
use datafusion::scalar::ScalarValue;

use dbsp::circuit::{KEY_COLUMN_NAME, WEIGHT_COLUMN_NAME};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationMode {
    ByAllColumns,
    ByKey,
}

#[derive(Debug, Clone)]
pub struct DeltaConsolidator {
    schema: SchemaRef,
    mode: ConsolidationMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsolidationStats {
    pub input_rows: usize,
    pub grouped_rows: usize,
    pub output_rows: usize,
    pub zero_weight_dropped_rows: usize,
}

#[derive(Debug, Clone)]
pub struct ConsolidationOutput {
    pub batches: Vec<RecordBatch>,
    pub stats: ConsolidationStats,
}

impl DeltaConsolidator {
    pub fn new(schema: SchemaRef) -> DFResult<Self> {
        Self::with_mode(schema, ConsolidationMode::ByAllColumns)
    }

    pub fn with_mode(schema: SchemaRef, mode: ConsolidationMode) -> DFResult<Self> {
        validate_schema(&schema, mode)?;
        Ok(Self { schema, mode })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub async fn consolidate(&self, batches: Vec<RecordBatch>) -> DFResult<Vec<RecordBatch>> {
        Ok(self.consolidate_with_stats(batches).await?.batches)
    }

    pub async fn consolidate_with_stats(
        &self,
        batches: Vec<RecordBatch>,
    ) -> DFResult<ConsolidationOutput> {
        if batches.is_empty() {
            return Ok(ConsolidationOutput {
                batches: vec![RecordBatch::new_empty(Arc::clone(&self.schema))],
                stats: ConsolidationStats::default(),
            });
        }

        for batch in &batches {
            if batch.schema().as_ref() != self.schema.as_ref() {
                return internal_err!("delta batch schema does not match consolidator schema");
            }
        }
        if self.mode == ConsolidationMode::ByKey {
            validate_key_payload_consistency(&batches, &self.schema)?;
        }
        let input_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        ctx.register_table("delta", Arc::new(table))?;

        let df = ctx.table("delta").await?;
        let (group_exprs, aggr_exprs, select_exprs) = build_exprs(&self.schema, self.mode)?;
        let df = df.aggregate(group_exprs, aggr_exprs)?;
        let df = df.select(select_exprs)?;
        let grouped = df.collect().await?;
        let grouped_rows = grouped.iter().map(RecordBatch::num_rows).sum::<usize>();

        let mut batches = filter_zero_weight_rows(&grouped, &self.schema)?;
        let output_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        Ok(ConsolidationOutput {
            batches,
            stats: ConsolidationStats {
                input_rows,
                grouped_rows,
                output_rows,
                zero_weight_dropped_rows: grouped_rows.saturating_sub(output_rows),
            },
        })
    }
}

fn filter_zero_weight_rows(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> DFResult<Vec<RecordBatch>> {
    let weight_idx = match schema.index_of(WEIGHT_COLUMN_NAME) {
        Ok(idx) => idx,
        Err(_) => return internal_err!("missing {} column", WEIGHT_COLUMN_NAME),
    };
    let mut filtered = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let Some(weight_col) = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
        else {
            return internal_err!("{} column must be Int64", WEIGHT_COLUMN_NAME);
        };
        let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            mask_builder.append_value(weight_col.value(row) != 0);
        }
        let mask = mask_builder.finish();
        let batch = filter_record_batch(batch, &mask)?;
        if batch.num_rows() > 0 {
            filtered.push(batch);
        }
    }
    Ok(filtered)
}

fn validate_key_payload_consistency(batches: &[RecordBatch], schema: &SchemaRef) -> DFResult<()> {
    let key_idx = match schema.index_of(KEY_COLUMN_NAME) {
        Ok(idx) => idx,
        Err(_) => return internal_err!("missing {} column", KEY_COLUMN_NAME),
    };
    let payload_indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            (field.name() != KEY_COLUMN_NAME && field.name() != WEIGHT_COLUMN_NAME).then_some(idx)
        })
        .collect::<Vec<_>>();

    let mut payload_by_key: HashMap<Vec<u8>, Vec<ScalarValue>> = HashMap::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let Some(keys) = batch.column(key_idx).as_any().downcast_ref::<BinaryArray>() else {
            return internal_err!("{} column must be Binary", KEY_COLUMN_NAME);
        };
        for row in 0..batch.num_rows() {
            let key = keys.value(row).to_vec();
            let payload = payload_indices
                .iter()
                .map(|idx| ScalarValue::try_from_array(batch.column(*idx), row))
                .collect::<DFResult<Vec<_>>>()?;
            match payload_by_key.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(payload);
                }
                Entry::Occupied(existing) => {
                    if existing.get() != &payload {
                        return internal_err!(
                            "conflicting payloads found for {} while consolidating by key",
                            KEY_COLUMN_NAME
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn validate_schema(schema: &SchemaRef, mode: ConsolidationMode) -> DFResult<()> {
    let weight_idx = match schema.index_of(WEIGHT_COLUMN_NAME) {
        Ok(idx) => idx,
        Err(_) => return internal_err!("missing {} column", WEIGHT_COLUMN_NAME),
    };
    let weight_field = schema.field(weight_idx);
    if weight_field.data_type() != &DataType::Int64 {
        return internal_err!("{} column must be Int64", WEIGHT_COLUMN_NAME);
    }

    if mode == ConsolidationMode::ByKey {
        if schema.index_of(KEY_COLUMN_NAME).is_err() {
            return internal_err!("missing {} column", KEY_COLUMN_NAME);
        }
    }

    Ok(())
}

fn build_exprs(
    schema: &SchemaRef,
    mode: ConsolidationMode,
) -> DFResult<(Vec<Expr>, Vec<Expr>, Vec<Expr>)> {
    validate_schema(schema, mode)?;

    let mut group_exprs = Vec::new();
    let mut aggr_exprs = Vec::new();

    match mode {
        ConsolidationMode::ByAllColumns => {
            for field in schema.fields() {
                if field.name() == WEIGHT_COLUMN_NAME {
                    continue;
                }
                group_exprs.push(col(field.name()));
            }
            aggr_exprs.push(sum(col(WEIGHT_COLUMN_NAME)).alias(WEIGHT_COLUMN_NAME));
        }
        ConsolidationMode::ByKey => {
            group_exprs.push(col(KEY_COLUMN_NAME));
            for field in schema.fields() {
                let name = field.name();
                if name == KEY_COLUMN_NAME || name == WEIGHT_COLUMN_NAME {
                    continue;
                }
                aggr_exprs.push(min(col(name)).alias(name));
            }
            aggr_exprs.push(sum(col(WEIGHT_COLUMN_NAME)).alias(WEIGHT_COLUMN_NAME));
        }
    }

    let select_exprs = schema
        .fields()
        .iter()
        .map(|field| col(field.name()))
        .collect::<Vec<_>>();

    Ok((group_exprs, aggr_exprs, select_exprs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BinaryArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    fn collect_rows(batches: &[RecordBatch]) -> Vec<(i64, String, Vec<u8>, i64)> {
        let mut rows = Vec::new();
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id array");
            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name array");
            let key_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("key array");
            let weight_col = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("weight array");

            for row in 0..batch.num_rows() {
                rows.push((
                    id_col.value(row),
                    name_col.value(row).to_string(),
                    key_col.value(row).to_vec(),
                    weight_col.value(row),
                ));
            }
        }
        rows
    }

    #[tokio::test]
    async fn consolidates_by_all_columns_and_drops_zero_weight() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![1, -1, 2])),
            ],
        )
        .expect("batch");

        let consolidator = DeltaConsolidator::new(Arc::clone(&schema)).expect("consolidator");
        let out = consolidator.consolidate(vec![batch]).await.expect("out");

        let batch = out.iter().find(|b| b.num_rows() > 0).expect("non-empty");
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id array");
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name array");
        let weight_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight array");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(id_col.value(0), 2);
        assert_eq!(name_col.value(0), "b");
        assert_eq!(weight_col.value(0), 2);
    }

    #[tokio::test]
    async fn reports_consolidation_stats() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![1, -1, 2])),
            ],
        )
        .expect("batch");

        let consolidator = DeltaConsolidator::new(Arc::clone(&schema)).expect("consolidator");
        let out = consolidator
            .consolidate_with_stats(vec![batch])
            .await
            .expect("out");

        assert_eq!(out.stats.input_rows, 3);
        assert_eq!(out.stats.grouped_rows, 2);
        assert_eq!(out.stats.output_rows, 1);
        assert_eq!(out.stats.zero_weight_dropped_rows, 1);
    }

    #[tokio::test]
    async fn consolidates_by_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(KEY_COLUMN_NAME, DataType::Binary, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));

        let key_one = vec![1, 0, 0, 0, 0, 0, 0, 0];
        let key_two = vec![2, 0, 0, 0, 0, 0, 0, 0];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(BinaryArray::from(vec![
                    key_one.as_slice(),
                    key_one.as_slice(),
                    key_two.as_slice(),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 1])),
            ],
        )
        .expect("batch");

        let consolidator =
            DeltaConsolidator::with_mode(Arc::clone(&schema), ConsolidationMode::ByKey)
                .expect("consolidator");
        let out = consolidator.consolidate(vec![batch]).await.expect("out");

        let mut rows = collect_rows(&out);
        rows.sort_by_key(|row| row.0);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, "a".to_string(), key_one, 3));
        assert_eq!(rows[1], (2, "b".to_string(), key_two, 1));
    }

    #[test]
    fn fails_fast_when_weight_column_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = DeltaConsolidator::new(schema).expect_err("missing weight should fail");
        assert!(err.to_string().contains("missing __weight column"));
    }

    #[test]
    fn fails_fast_when_key_column_missing_in_key_mode() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));
        let err = DeltaConsolidator::with_mode(schema, ConsolidationMode::ByKey)
            .expect_err("missing key should fail");
        assert!(err.to_string().contains("missing __key column"));
    }

    #[tokio::test]
    async fn rejects_conflicting_payloads_for_the_same_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(KEY_COLUMN_NAME, DataType::Binary, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));
        let key = vec![1u8; 8];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(BinaryArray::from(vec![key.as_slice(), key.as_slice()])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .expect("batch");

        let consolidator =
            DeltaConsolidator::with_mode(schema, ConsolidationMode::ByKey).expect("consolidator");
        let err = consolidator
            .consolidate(vec![batch])
            .await
            .expect_err("conflicting key payloads should fail");
        assert!(
            err.to_string()
                .contains("conflicting payloads found for __key")
        );
    }
}
