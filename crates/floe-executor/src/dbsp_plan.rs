use datafusion::logical_expr::LogicalPlan;

pub use dbsp::circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateNode, DbspJoinNode, DbspJoinType,
    DbspNodeKind, DbspProjectNode, DbspSelectNode, DbspSourceNode, DbspUnionNode, Field,
    OrderExpr, PlannerConfig, PlannerError, ProjectItem, RowSchema, ScalarValue, TableDescriptor,
    DbspScalarType, nexmark_auction_table, nexmark_bid_table, nexmark_person_table,
};

/// Thin wrapper around DBSP's [`CircuitPlanner`] that exposes a planning API within Floe.
pub struct DbspPlanBuilder {
    planner: CircuitPlanner,
}

impl DbspPlanBuilder {
    pub fn new(config: PlannerConfig) -> Self {
        Self {
            planner: CircuitPlanner::new(config),
        }
    }

    pub fn build(&self, df_plan: &LogicalPlan) -> Result<CircuitPlan, PlannerError> {
        self.planner.plan(df_plan)
    }
}

/// Returns a [`PlannerConfig`] pre-populated with Nexmark table descriptors.
pub fn nexmark_config() -> PlannerConfig {
    let mut cfg = PlannerConfig::new();
    cfg.register_table(nexmark_person_table());
    cfg.register_table(nexmark_auction_table());
    cfg.register_table(nexmark_bid_table());
    cfg
}
