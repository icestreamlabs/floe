use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::logical_expr::expr::Sort as ExprSort;
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator, WindowFunctionDefinition};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DataFusionError};

use dbsp_circuit::circuit::plan::{
    DbspAggregateNode, DbspDistinctNode, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspProjectNode,
    DbspSelectNode, DbspSourceNode, DbspTopNNode, DbspUnionNode, DbspWindowAggregateNode,
    DbspWindowPolicy, DbspWindowSpec, OrderExpr, ProjectItem,
};
use dbsp_circuit::circuit::schema::{Field, RowSchema};
use dbsp_circuit::circuit::types::DbspScalarType;

use super::asof_extension::FloeAsofJoinNode;
use super::circuit::{CircuitNode, CircuitPlan};
use super::config::PlannerConfig;
use super::error::PlannerError;
use super::expr::{
    combine_filters, extract_alias, extract_asof_join_and_residual,
    extract_asof_join_and_residual_with_logical_schemas, extract_join_keys_and_residual,
    extract_join_keys_and_residual_with_logical_schemas, extract_range_join_and_residual,
    map_aggregate_expr, normalize_expr,
};
use super::logical_optimizer::{OptimizerDiagnostics, optimize_logical_plan};

pub struct CircuitPlanner {
    config: PlannerConfig,
}

impl CircuitPlanner {
    pub fn new(config: PlannerConfig) -> Self {
        Self { config }
    }

    pub fn plan(&self, plan: &LogicalPlan) -> Result<CircuitPlan, PlannerError> {
        let plan = optimize_logical_plan(plan, &self.config)?.plan;
        let mut ctx = PlannerContext::new(&self.config);
        let planned = ctx.plan_node(&plan)?;
        Ok(CircuitPlan {
            root: planned.id,
            nodes: ctx.into_reachable_nodes(planned.id)?,
        })
    }

    pub fn optimize_logical_plan_with_diagnostics(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(LogicalPlan, OptimizerDiagnostics), PlannerError> {
        let optimized = optimize_logical_plan(plan, &self.config)?;
        Ok((optimized.plan, optimized.diagnostics))
    }
}

struct PlannerContext<'cfg> {
    config: &'cfg PlannerConfig,
    nodes: Vec<CircuitNode>,
}

#[derive(Clone)]
struct PlannedNode {
    id: usize,
    schema: Arc<RowSchema>,
}

type RowNumberSpec = (String, Vec<Expr>, Vec<ExprSort>);
const DEFAULT_WINDOW_ALLOWED_LATENESS_MS: i64 = i64::MAX;

mod aggregate_window;
mod core;
mod join_helpers;
mod join_row_number;
mod projection_join;
mod row_number_helpers;
