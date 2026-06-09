use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    apply_weighted_snapshot_delta, build_incremental_materialized_view_state,
    collect_incremental_output,
};

pub(super) struct ColumnarStatelessPlan {
    source_name: String,
}

pub(super) struct ColumnarStatelessMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    output_zset: SlateBackedColumnarZSet,
    incremental: IncrementalMaterializedViewState,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarStatelessMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

pub(super) fn columnar_stateless_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<ColumnarStatelessPlan> {
    incremental_source_for_plan(plan, sources)
        .map(|source_name| ColumnarStatelessPlan { source_name })
}

pub(super) async fn build_columnar_stateless_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    query: &str,
    output_schema: &SchemaRef,
    plan: ColumnarStatelessPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarStatelessMaterializedViewState> {
    let source = sources
        .get(&plan.source_name)
        .ok_or_else(|| anyhow::anyhow!("unknown vectorized source '{}'", plan.source_name))?;
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let input_namespace = format!("{mv_namespace}/columnar/stateless/input");
    let output_namespace = format!("{mv_namespace}/columnar/stateless/output");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed stateless output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load stateless output snapshot")?,
    )?;

    Ok(ColumnarStatelessMaterializedViewState {
        source_name: plan.source_name.clone(),
        source_schema: Arc::clone(&source.schema),
        input_zset: SlateBackedColumnarZSet::new(
            Arc::clone(&table),
            input_namespace,
            Arc::clone(&source.schema),
        )
        .await
        .context("initialize SlateDB-backed stateless input zset")?,
        output_zset,
        incremental: build_incremental_materialized_view_state(
            query,
            &plan.source_name,
            sources,
            udfs,
        )
        .await
        .context("build stateless vectorized delta plan")?,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_stateless_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_stateless.as_mut() else {
        return Ok(false);
    };

    let plan_start = Instant::now();
    let input_delta =
        if let Some(weighted_batches) = weighted_delta_batches.get(columnar.source_name.as_str()) {
            ColumnarZSet::try_new_weighted(
                Arc::clone(&columnar.source_schema),
                weighted_batches.clone(),
            )
            .with_context(|| {
                format!(
                    "build weighted stateless input delta for '{}'",
                    columnar.source_name
                )
            })?
        } else if let Some(source_batches) = insert_batches.get(columnar.source_name.as_str()) {
            ColumnarZSet::from_value_batches(
                Arc::clone(&columnar.source_schema),
                source_batches.clone(),
                1,
            )
            .with_context(|| {
                format!(
                    "build insert stateless input delta for '{}'",
                    columnar.source_name
                )
            })?
        } else {
            ColumnarZSet::empty(Arc::clone(&columnar.source_schema))?
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
    let output_delta_batches = stateless_output_delta_batches(
        columnar,
        persisted_input_delta.batches(),
        &mv.output_schema,
    )
    .await?;
    let output_delta =
        ColumnarZSet::try_new_weighted(Arc::clone(&mv.output_schema), output_delta_batches)
            .context("build stateless output zset delta")?;
    let persisted_output_delta = if let Some(handle) = columnar
        .output_zset
        .create_version(
            &output_delta,
            columnar
                .output_zset
                .current_handle()
                .map(|handle| handle.version),
        )
        .await?
    {
        columnar.output_zset.read_delta(&handle).await?
    } else {
        output_delta
    };

    let delta_batches = persisted_output_delta.batches().to_vec();
    let next_snapshot = apply_weighted_snapshot_delta(
        &mv.output_schema,
        &mv.previous_snapshot,
        delta_batches.clone(),
    )
    .await
    .with_context(|| {
        format!(
            "apply Slate-backed stateless columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_stateless",
        "SlateDB-backed stateless columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn stateless_output_delta_batches(
    columnar: &ColumnarStatelessMaterializedViewState,
    input_batches: &[RecordBatch],
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if input_batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut positive_source_batches = Vec::new();
    let mut negative_source_batches = Vec::new();
    for batch in input_batches {
        let unit_delta =
            unit_source_delta_batches(&columnar.source_schema, batch)?.with_context(|| {
                format!(
                    "stateless columnar materialized view received non-unit weighted source deltas for '{}'",
                    columnar.source_name
                )
            })?;
        positive_source_batches.extend(unit_delta.positive);
        negative_source_batches.extend(unit_delta.negative);
    }

    let weighted_schema = weighted_snapshot_schema(output_schema)?;
    let mut output_delta_batches = Vec::new();
    let positive_output = collect_incremental_output(
        &columnar.incremental,
        &positive_source_batches,
        output_schema,
    )
    .await?;
    output_delta_batches.extend(add_weight_column_to_batches(
        &positive_output,
        &weighted_schema,
        1,
    )?);
    let negative_output = collect_incremental_output(
        &columnar.incremental,
        &negative_source_batches,
        output_schema,
    )
    .await?;
    output_delta_batches.extend(add_weight_column_to_batches(
        &negative_output,
        &weighted_schema,
        -1,
    )?);
    Ok(output_delta_batches)
}

fn snapshot_batches_from_zset(zset: &ColumnarZSet) -> Result<Vec<RecordBatch>> {
    let weight_idx = zset.weighted_schema().index_of(WEIGHT_COLUMN_NAME)?;
    let mut batches = zset
        .batches()
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| -> Result<RecordBatch> {
            let weights = batch
                .column(weight_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
            let mut indices = Vec::new();
            for row_idx in 0..weights.len() {
                if weights.is_null(row_idx) {
                    anyhow::bail!("materialized columnar zset weight cannot be NULL");
                }
                let weight = weights.value(row_idx);
                if weight < 0 {
                    anyhow::bail!("materialized columnar zset contains negative weight");
                }
                let row_idx =
                    u32::try_from(row_idx).context("columnar zset batch exceeds u32 rows")?;
                for _ in 0..weight {
                    indices.push(row_idx);
                }
            }
            let indices = UInt32Array::from(indices);
            let columns = batch
                .columns()
                .iter()
                .take(zset.value_column_count())
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
            Ok(RecordBatch::try_new(zset.value_schema(), columns)?)
        })
        .collect::<Result<Vec<_>>>()?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(zset.value_schema()));
    }
    Ok(batches)
}
