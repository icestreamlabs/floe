use super::*;
use crate::encoding::{
    EncodedRowProjectionColumn, EncodedRowProjectionSource, extract_encoded_row_columns,
};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::{BTreeSet, HashMap};

pub(super) fn project_encoded_delta_batch<K>(
    delta_values: &[(K, i64)],
    projector: impl Fn(&K) -> Vec<u8>,
) -> Vec<(Vec<u8>, i64)> {
    let mut projected = HashMap::<Vec<u8>, i64>::new();
    for (key, weight) in delta_values {
        if *weight == 0 {
            continue;
        }
        let encoded = projector(key);
        if encoded.is_empty() {
            continue;
        }
        let entry = projected.entry(encoded.clone()).or_insert(0);
        *entry += *weight;
        if *entry == 0 {
            projected.remove(&encoded);
        }
    }
    projected.into_iter().collect()
}

pub(super) async fn strip_semijoin_precomputed_columns(
    stream: DeltaHandleStream,
    output_width: usize,
    graph_id: &str,
    label: String,
    task_events: &GraphTaskSender,
) -> Result<DeltaHandleStream> {
    let columns = Arc::new((0..output_width).collect::<Vec<_>>());
    let project_graph_id = graph_id.to_string();
    let projector = move |row: &Vec<u8>| -> Vec<u8> {
        match extract_encoded_row_columns(row, columns.as_ref(), false) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => Vec::new(),
            Err(err) => {
                tracing::warn!(
                    graph_id = %project_graph_id,
                    error = %err,
                    "failed to strip semijoin precomputed key columns"
                );
                Vec::new()
            }
        }
    };

    let project_events = task_events.clone();
    let project_error_graph_id = graph_id.to_string();
    let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
        report_graph_task_error(&project_events, &project_error_graph_id, label.clone(), err);
    });
    let transform = move |delta_values: &[(Vec<u8>, i64)]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
        Ok(project_encoded_delta_batch(delta_values, &projector))
    };
    let projected = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
        &stream,
        transform,
        Some(project_error_handler),
    )
    .await
    .context("initialize semijoin output projection")?;
    Ok(projected.stream())
}

pub(super) async fn strip_asof_precomputed_columns(
    stream: DeltaHandleStream,
    left_width: usize,
    left_join_width: usize,
    right_width: usize,
    graph_id: &str,
    label: String,
    task_events: &GraphTaskSender,
) -> Result<DeltaHandleStream> {
    let mut columns = Vec::with_capacity(left_width + right_width);
    columns.extend(0..left_width);
    columns.extend(left_join_width..left_join_width + right_width);
    let columns = Arc::new(columns);
    let project_graph_id = graph_id.to_string();
    let projector = move |row: &Vec<u8>| -> Vec<u8> {
        match extract_encoded_row_columns(row, columns.as_ref(), false) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => Vec::new(),
            Err(err) => {
                tracing::warn!(
                    graph_id = %project_graph_id,
                    error = %err,
                    "failed to strip ASOF precomputed columns"
                );
                Vec::new()
            }
        }
    };

    let project_events = task_events.clone();
    let project_error_graph_id = graph_id.to_string();
    let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
        report_graph_task_error(&project_events, &project_error_graph_id, label.clone(), err);
    });
    let transform = move |delta_values: &[(Vec<u8>, i64)]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
        Ok(project_encoded_delta_batch(delta_values, &projector))
    };
    let projected = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
        &stream,
        transform,
        Some(project_error_handler),
    )
    .await
    .context("initialize ASOF output projection")?;
    Ok(projected.stream())
}

pub(super) fn asof_composite_key(prefix: &[u8], timestamp: i64) -> dbsp::collections::OrderedBytes {
    let mut encoded = Vec::with_capacity(prefix.len() + 8);
    encoded.extend_from_slice(prefix);
    append_desc_ordered_i64(timestamp, &mut encoded);
    dbsp::collections::OrderedBytes::new(encoded)
}

pub(super) fn asof_composite_upper_bound(
    prefix: &[u8],
    _timestamp: i64,
) -> dbsp::collections::OrderedBytes {
    let mut encoded = Vec::with_capacity(prefix.len() + 9);
    encoded.extend_from_slice(prefix);
    encoded.extend_from_slice(&u64::MAX.to_be_bytes());
    encoded.push(0xFF);
    dbsp::collections::OrderedBytes::new(encoded)
}

pub(super) fn append_desc_ordered_i64(value: i64, out: &mut Vec<u8>) {
    let shifted = (value as u64) ^ 0x8000_0000_0000_0000;
    out.extend_from_slice(&(!shifted).to_be_bytes());
}

pub(super) fn asof_candidate_residual_schema(
    left_schema: &RowSchema,
    left_join_schema: &RowSchema,
    right_schema: &RowSchema,
    right_join_schema: &RowSchema,
    output_schema: &RowSchema,
) -> Result<Arc<RowSchema>> {
    let mut fields = Vec::with_capacity(left_join_schema.len() + right_join_schema.len());
    fields.extend(output_schema.fields()[..left_schema.len()].iter().cloned());
    fields.extend(
        left_join_schema.fields()[left_schema.len()..]
            .iter()
            .cloned(),
    );

    let right_output_start = left_schema.len();
    let right_output_end = right_output_start + right_schema.len();
    fields.extend(
        output_schema.fields()[right_output_start..right_output_end]
            .iter()
            .cloned(),
    );
    fields.extend(
        right_join_schema.fields()[right_schema.len()..]
            .iter()
            .cloned(),
    );

    RowSchema::try_new(fields).context("build ASOF candidate residual schema")
}

pub(super) fn build_join_state_projection(
    schema: &RowSchema,
    required_columns: &BTreeSet<usize>,
) -> Result<(Option<dbsp::DbspProjectNode>, HashMap<usize, usize>)> {
    if required_columns
        .iter()
        .any(|column| *column >= schema.len())
    {
        return Err(anyhow!(
            "join state projection requested column outside schema width {}",
            schema.len()
        ));
    }

    let mut remap = HashMap::with_capacity(required_columns.len());
    for (new_index, old_index) in required_columns.iter().copied().enumerate() {
        remap.insert(old_index, new_index);
    }

    let is_identity = required_columns.len() == schema.len()
        && required_columns
            .iter()
            .copied()
            .enumerate()
            .all(|(expected, actual)| expected == actual);
    if is_identity {
        return Ok((None, remap));
    }

    let mut items = Vec::with_capacity(required_columns.len());
    for old_index in required_columns {
        let field = schema
            .field(*old_index)
            .ok_or_else(|| anyhow!("join state projection missing column {old_index}"))?;
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }
    let projection = dbsp::DbspProjectNode::try_new(Arc::new(schema.clone()), items)
        .context("build join state projection node")?;
    Ok((Some(projection), remap))
}

pub(super) fn remap_join_state_indices(
    indices: &[usize],
    remap: &HashMap<usize, usize>,
) -> Result<Vec<usize>> {
    indices
        .iter()
        .copied()
        .map(|index| {
            remap
                .get(&index)
                .copied()
                .ok_or_else(|| anyhow!("join state projection dropped required column {index}"))
        })
        .collect()
}

pub(super) fn remap_join_output_projection(
    columns: &[EncodedRowProjectionColumn],
    left_remap: &HashMap<usize, usize>,
    right_remap: &HashMap<usize, usize>,
) -> Result<Vec<EncodedRowProjectionColumn>> {
    columns
        .iter()
        .copied()
        .map(|column| {
            let remapped_index = match column.source {
                EncodedRowProjectionSource::Left => left_remap.get(&column.index),
                EncodedRowProjectionSource::Right => right_remap.get(&column.index),
            }
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "join state projection dropped output column {:?}:{}",
                    column.source,
                    column.index
                )
            })?;
            Ok(EncodedRowProjectionColumn {
                source: column.source,
                index: remapped_index,
            })
        })
        .collect()
}

pub(super) fn encode_null_row_template(schema: &RowSchema) -> Result<Vec<u8>> {
    let count = u32::try_from(schema.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + schema.len());
    encoded.extend_from_slice(&count.to_le_bytes());
    for field in schema.fields() {
        match field.data_type {
            DbspScalarType::Int64 => encoded.push(0x05),
            DbspScalarType::Utf8 => encoded.push(0x06),
            DbspScalarType::TimestampMillis => encoded.push(0x07),
            DbspScalarType::Bool => encoded.push(0x08),
            DbspScalarType::DateDays => encoded.push(0x0A),
            DbspScalarType::Decimal128 { .. } => encoded.push(0x0C),
        }
    }
    Ok(encoded)
}

pub(super) fn direct_column_index(
    expr: &dbsp::circuit::plan::DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    match expr.expr() {
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        other => direct_column_index_expression(other, schema),
    }
}

pub(super) fn direct_column_index_expression(expr: &Expr, schema: &RowSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => resolve_direct_column(schema, column),
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        _ => None,
    }
}

pub(super) fn resolve_direct_column(schema: &RowSchema, column: &Column) -> Option<usize> {
    let qualified = column.flat_name();
    schema
        .field_index(&qualified)
        .or_else(|| schema.field_index(&column.name))
}
