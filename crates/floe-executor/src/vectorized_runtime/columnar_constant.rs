use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::collect;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;

use super::{VectorizedMaterializedViewState, apply_weighted_snapshot_delta, normalize_batches};

pub(super) struct ColumnarConstantPlan {
    logical_plan: LogicalPlan,
}

pub(super) struct ColumnarConstantMaterializedViewState {
    state_table: Arc<dyn KeyValueTable>,
    initialized_key: Vec<u8>,
    output_zset: SlateBackedColumnarZSet,
    constant_snapshot: Vec<RecordBatch>,
    initialized: bool,
}

impl ColumnarConstantMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        if self.initialized {
            self.constant_snapshot.clone()
        } else {
            Vec::new()
        }
    }
}

pub(super) fn columnar_constant_plan_for_plan(plan: &LogicalPlan) -> Option<ColumnarConstantPlan> {
    (!plan_contains_table_scan(plan)).then(|| ColumnarConstantPlan {
        logical_plan: plan.clone(),
    })
}

pub(super) async fn build_columnar_constant_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarConstantPlan,
    ctx: &SessionContext,
) -> Result<ColumnarConstantMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let output_namespace = format!("{mv_namespace}/columnar/constant/output");
    let initialized_key =
        format!("{mv_namespace}/columnar/constant/state/initialized").into_bytes();
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed constant output zset")?;
    let initialized = table
        .get_bytes(&initialized_key)
        .await
        .context("read SlateDB-backed constant initialized marker")?
        .is_some()
        || output_zset.current_handle().is_some();
    let constant_snapshot = if initialized {
        crate::columnar_snapshot::columnar_zset_snapshot(
            &output_zset
                .materialize_columnar()
                .await
                .context("load constant output snapshot")?,
        )?
    } else {
        let physical_plan = ctx
            .state()
            .create_physical_plan(&plan.logical_plan)
            .await
            .context("create constant materialized view physical plan")?;
        let mut batches = collect(physical_plan, ctx.task_ctx())
            .await
            .context("evaluate constant materialized view")?;
        batches = normalize_batches(batches, output_schema)?;
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(Arc::clone(output_schema)));
        }
        batches
    };

    Ok(ColumnarConstantMaterializedViewState {
        state_table: table,
        initialized_key,
        output_zset,
        constant_snapshot,
        initialized,
    })
}

pub(super) async fn run_columnar_constant_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<()> {
    let super::MaterializedViewOperator::Constant(columnar) = &mut mv.operator else {
        unreachable!("constant tick dispatched to non-constant operator")
    };

    let plan_start = Instant::now();
    let (next_snapshot, delta_batches) = if columnar.initialized {
        (mv.previous_snapshot.clone(), Vec::new())
    } else {
        let output_delta = ColumnarZSet::from_value_batches(
            Arc::clone(&mv.output_schema),
            columnar.constant_snapshot.clone(),
            1,
        )
        .context("build constant output zset delta")?;
        let persisted_output_delta = if let Some(handle) = columnar
            .output_zset
            .create_version(&output_delta, None)
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
                "apply Slate-backed constant columnar snapshot delta for '{}'",
                mv.view_name
            )
        })?;
        columnar
            .state_table
            .put(&columnar.initialized_key, b"1")
            .await
            .context("persist SlateDB-backed constant initialized marker")?;
        columnar.initialized = true;
        columnar.constant_snapshot = next_snapshot.clone();
        (next_snapshot, delta_batches)
    };

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_constant",
        "SlateDB-backed constant columnar DBSP materialized view tick completed"
    );
    Ok(())
}

fn plan_contains_table_scan(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::TableScan(_)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}
