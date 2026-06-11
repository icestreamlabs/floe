use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::columnar_snapshot::columnar_zset_weight_sum;
use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::mv::registry::{ColumnarMaterializedViewStorage, MaterializedViewRegistry};
use crate::namespaces;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    build_incremental_materialized_view_state, collect_incremental_output,
};

pub(super) struct ColumnarStatelessPlan {
    source_name: String,
}

pub(super) struct ColumnarStatelessMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    operator_table: Arc<dyn KeyValueTable>,
    output_zset: SlateBackedColumnarZSet,
    incremental: IncrementalMaterializedViewState,
    row_count: i64,
}

impl ColumnarStatelessMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        Vec::new()
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
    let output_namespace = format!("{mv_namespace}/columnar/stateless/output");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace.clone(),
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed stateless output zset")?;
    let row_count = columnar_zset_weight_sum(
        &output_zset
            .materialize_columnar()
            .await
            .context("load stateless output snapshot")?,
    )?;

    Ok(ColumnarStatelessMaterializedViewState {
        source_name: plan.source_name.clone(),
        source_schema: Arc::clone(&source.schema),
        operator_table: Arc::clone(&table),
        output_zset,
        incremental: build_incremental_materialized_view_state(
            query,
            &plan.source_name,
            sources,
            udfs,
        )
        .await
        .context("build stateless vectorized delta plan")?,
        row_count,
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
    let output_delta_batches =
        if let Some(weighted_batches) = weighted_delta_batches.get(columnar.source_name.as_str()) {
            stateless_output_delta_batches(columnar, weighted_batches, &mv.output_schema).await?
        } else if let Some(source_batches) = insert_batches.get(columnar.source_name.as_str()) {
            stateless_append_only_output_delta_batches(columnar, source_batches, &mv.output_schema)
                .await?
        } else {
            Vec::new()
        };
    let output_delta =
        ColumnarZSet::try_new_weighted(Arc::clone(&mv.output_schema), output_delta_batches)
            .context("build stateless output zset delta")?;
    let row_count_delta = columnar_zset_weight_sum(&output_delta)
        .context("compute stateless output row-count delta")?;
    let created_handle = columnar
        .output_zset
        .create_version(
            &output_delta,
            columnar
                .output_zset
                .current_handle()
                .map(|handle| handle.version),
        )
        .await?;
    columnar.row_count = columnar.row_count.saturating_add(row_count_delta);
    if columnar.row_count < 0 {
        anyhow::bail!(
            "stateless columnar materialized view '{}' row count became negative",
            mv.view_name
        );
    }

    let handle = registry.register(mv.view_name.clone());
    let Some(zset_handle) = created_handle.or_else(|| columnar.output_zset.current_handle()) else {
        handle.publish_arrow_version(
            version,
            vec![RecordBatch::new_empty(Arc::clone(&mv.output_schema))],
            output_delta.batches().to_vec(),
        );
        tracing::debug!(
            view = %mv.view_name,
            version,
            total_ms = plan_start.elapsed().as_millis() as u64,
            mode = "columnar_stateless",
            "SlateDB-backed stateless columnar DBSP materialized view empty tick completed"
        );
        return Ok(true);
    };
    handle.publish_columnar_version(
        version,
        zset_handle,
        ColumnarMaterializedViewStorage::new(
            Arc::clone(&columnar.operator_table),
            Arc::clone(&mv.output_schema),
        ),
        usize::try_from(columnar.row_count).context("stateless row count exceeds usize")?,
        output_delta.batches().to_vec(),
    );
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

async fn stateless_append_only_output_delta_batches(
    columnar: &ColumnarStatelessMaterializedViewState,
    source_batches: &[RecordBatch],
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() {
        return Ok(Vec::new());
    }

    let weighted_schema = weighted_snapshot_schema(output_schema)?;
    let positive_output =
        collect_incremental_output(&columnar.incremental, source_batches, output_schema).await?;
    Ok(add_weight_column_to_batches(
        &positive_output,
        &weighted_schema,
        1,
    )?)
}
