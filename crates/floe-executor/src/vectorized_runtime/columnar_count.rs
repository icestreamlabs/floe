use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::logical_plan::{Aggregate, Projection};
use datafusion::logical_expr::{Expr, LogicalPlan};
use dbsp::SlateBackedColumnarCountByKeyOp;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarI64ZSet, SlateBackedColumnarI64ZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::weighted_snapshot_schema;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::vectorized_runtime::source_state::resolve_source_table;

use super::{VectorizedMaterializedViewState, VectorizedSourceState};

pub(super) struct ColumnarCountPlan {
    source_name: String,
    source_key_column_idx: usize,
}

pub(super) struct ColumnarCountMaterializedViewState {
    source_name: String,
    source_key_column_idx: usize,
    input_zset: SlateBackedColumnarI64ZSet,
    operator: SlateBackedColumnarCountByKeyOp,
}

pub(super) fn columnar_count_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    output_schema: &SchemaRef,
) -> Result<Option<ColumnarCountPlan>> {
    let Some((aggregate, projection)) = columnar_count_aggregate_for_plan(plan) else {
        return Ok(None);
    };
    if aggregate.group_expr.len() != 1 || aggregate.aggr_expr.len() != 1 {
        return Ok(None);
    }
    if !output_schema_is_supported(output_schema) {
        return Ok(None);
    }

    let Expr::Column(group_column) = strip_alias(&aggregate.group_expr[0]) else {
        return Ok(None);
    };
    if !is_count_star_expr(&aggregate.aggr_expr[0]) {
        return Ok(None);
    }
    if let Some(projection) = projection
        && !projection_preserves_count_order(projection, aggregate)
    {
        return Ok(None);
    }

    let Some(source_name) = aggregate_input_source(aggregate.input.as_ref(), sources) else {
        return Ok(None);
    };
    let source = sources
        .get(&source_name)
        .ok_or_else(|| anyhow::anyhow!("unknown vectorized source '{source_name}'"))?;
    let Ok(source_key_column_idx) = source.schema.index_of(&group_column.name) else {
        return Ok(None);
    };
    let key_field = source.schema.field(source_key_column_idx);
    if key_field.data_type() != &DataType::Int64 || key_field.is_nullable() {
        return Ok(None);
    }

    Ok(Some(ColumnarCountPlan {
        source_name,
        source_key_column_idx,
    }))
}

pub(super) async fn build_columnar_count_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    plan: ColumnarCountPlan,
) -> Result<ColumnarCountMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let operator_namespace = format!("{mv_namespace}/columnar/count_by_key/operator");
    let input_namespace = format!("{mv_namespace}/columnar/count_by_key/input");

    Ok(ColumnarCountMaterializedViewState {
        source_name: plan.source_name,
        source_key_column_idx: plan.source_key_column_idx,
        input_zset: SlateBackedColumnarI64ZSet::new(Arc::clone(&table), input_namespace, &["key"])
            .await
            .context("initialize SlateDB-backed columnar count input zset")?,
        operator: SlateBackedColumnarCountByKeyOp::new(table, operator_namespace)
            .await
            .context("initialize SlateDB-backed columnar count operator")?,
    })
}

pub(super) async fn run_columnar_count_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<()> {
    let super::MaterializedViewOperator::CountByKey(columnar) = &mut mv.operator else {
        unreachable!("count tick dispatched to non-count operator")
    };

    let plan_start = Instant::now();
    let input_delta =
        if let Some(weighted_batches) = weighted_delta_batches.get(columnar.source_name.as_str()) {
            columnar_count_input_delta_from_batches(
                weighted_batches,
                columnar.source_key_column_idx,
                InputBatchWeights::ExistingWeightColumn,
            )?
        } else if let Some(source_batches) = insert_batches.get(columnar.source_name.as_str()) {
            columnar_count_input_delta_from_batches(
                source_batches,
                columnar.source_key_column_idx,
                InputBatchWeights::UnitInsert,
            )?
        } else {
            ColumnarI64ZSet::empty(&["key"])
        };

    let persisted_input_delta = if let Some(handle) = columnar
        .input_zset
        .create_version(&input_delta, None)
        .await?
    {
        columnar.input_zset.read_delta(&handle).await?
    } else {
        input_delta
    };
    columnar
        .operator
        .apply_delta(&persisted_input_delta)
        .await?;
    let output_delta = if let Some(handle) = columnar.operator.last_output_handle().cloned() {
        columnar.operator.read_output_delta(&handle).await?
    } else {
        ColumnarI64ZSet::empty(&["key", "count"])
    };

    let next_snapshot =
        columnar_count_snapshot_batches(columnar.operator.state_snapshot(), &mv.output_schema)?;
    let delta_batches = columnar_count_output_delta_batches(&output_delta, &mv.output_schema)?;

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_count_by_key",
        "SlateDB-backed columnar DBSP materialized view tick completed"
    );
    Ok(())
}

fn columnar_count_aggregate_for_plan(
    plan: &LogicalPlan,
) -> Option<(&Aggregate, Option<&Projection>)> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some((aggregate, None)),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => Some((aggregate, Some(projection))),
            _ => None,
        },
        LogicalPlan::SubqueryAlias(alias) => {
            columnar_count_aggregate_for_plan(alias.input.as_ref())
        }
        _ => None,
    }
}

fn aggregate_input_source(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => aggregate_input_source(alias.input.as_ref(), sources),
        LogicalPlan::TableScan(scan) if scan.filters.is_empty() && scan.fetch.is_none() => {
            resolve_source_table(scan.table_name.to_string(), sources)
        }
        _ => None,
    }
}

fn projection_preserves_count_order(projection: &Projection, aggregate: &Aggregate) -> bool {
    if projection.expr.len() != 2 || aggregate.schema.fields().len() != 2 {
        return false;
    }

    let aggregate_key_name = aggregate.schema.field(0).name();
    let aggregate_count_name = aggregate.schema.field(1).name();
    expr_refers_to_column(strip_alias(&projection.expr[0]), aggregate_key_name)
        && (expr_refers_to_column(strip_alias(&projection.expr[1]), aggregate_count_name)
            || is_count_star_expr(&projection.expr[1]))
}

fn output_schema_is_supported(output_schema: &SchemaRef) -> bool {
    output_schema.fields().len() == 2
        && output_schema
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Int64)
}

fn is_count_star_expr(expr: &Expr) -> bool {
    let Expr::AggregateFunction(aggregate) = strip_alias(expr) else {
        return false;
    };
    let params = &aggregate.params;
    aggregate.func.name().eq_ignore_ascii_case("count")
        && !params.distinct
        && params.filter.is_none()
        && params.order_by.is_empty()
        && params.null_treatment.is_none()
        && matches!(
            params.args.as_slice(),
            [Expr::Literal(ScalarValue::Int64(Some(1)), _)]
        )
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
}

fn expr_refers_to_column(expr: &Expr, name: &str) -> bool {
    matches!(expr, Expr::Column(column) if column.name == name)
}

enum InputBatchWeights {
    UnitInsert,
    ExistingWeightColumn,
}

fn columnar_count_input_delta_from_batches(
    batches: &[RecordBatch],
    source_key_column_idx: usize,
    weights: InputBatchWeights,
) -> Result<ColumnarI64ZSet> {
    let mut delta = ColumnarI64ZSet::empty(&["key"]);
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let keys = batch
            .column(source_key_column_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .with_context(|| format!("source key column {source_key_column_idx} is not Int64"))?;
        if keys.null_count() != 0 {
            bail!("columnar count source key column contains NULL values");
        }

        let weight_array = match weights {
            InputBatchWeights::UnitInsert => Int64Array::from_value(1, batch.num_rows()),
            InputBatchWeights::ExistingWeightColumn => {
                let weight_idx = batch.schema().index_of(WEIGHT_COLUMN_NAME)?;
                let weights = batch
                    .column(weight_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .with_context(|| format!("{WEIGHT_COLUMN_NAME} column is not Int64"))?;
                if weights.null_count() != 0 {
                    bail!("columnar count source weight column contains NULL values");
                }
                weights.clone()
            }
        };

        delta.extend(ColumnarI64ZSet::from_i64_arrays(
            &["key"],
            vec![keys.clone()],
            weight_array,
        )?)?;
    }
    Ok(delta)
}

fn columnar_count_snapshot_batches(
    snapshot: &ColumnarI64ZSet,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for batch in snapshot.batches() {
        if batch.num_rows() == 0 {
            continue;
        }
        batches.push(RecordBatch::try_new(
            Arc::clone(output_schema),
            vec![Arc::clone(batch.column(0)), Arc::clone(batch.column(1))],
        )?);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::clone(output_schema)));
    }
    Ok(batches)
}

fn columnar_count_output_delta_batches(
    output_delta: &ColumnarI64ZSet,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let weighted_schema = weighted_snapshot_schema(output_schema)?;
    let mut batches = Vec::new();
    for batch in output_delta.batches() {
        if batch.num_rows() == 0 {
            continue;
        }
        batches.push(RecordBatch::try_new(
            Arc::clone(&weighted_schema),
            vec![
                Arc::clone(batch.column(0)),
                Arc::clone(batch.column(1)),
                Arc::clone(batch.column(2)) as ArrayRef,
            ],
        )?);
    }
    Ok(batches)
}
