//! Floe DBSP facade.
//!
//! Root-level exports focus on operational execution types, handle/Z-set
//! streams, and planner/runtime wrappers.

pub mod circuit {
    pub use dbsp_circuit::circuit::*;
    pub use dbsp_planner::{
        CircuitNode, CircuitPlan, CircuitPlanner, FloeAsofJoinNode, OptimizerDiagnostics,
        OptimizerRuleDiagnostics, OptimizerStageDiagnostics, PlannerConfig, PlannerError,
        create_logical_plan_with_asof_preplanner,
    };

    pub mod planner {
        pub use dbsp_planner::{
            CircuitNode, CircuitPlan, CircuitPlanner, FloeAsofJoinNode, OptimizerDiagnostics,
            OptimizerRuleDiagnostics, OptimizerStageDiagnostics, PlannerConfig, PlannerError,
            create_logical_plan_with_asof_preplanner,
        };
    }
}

pub use dbsp_runtime::{
    aggregate, algebra, collections, count_aggregate, distinct, filter_map, handles,
    incremental_aggregate, join, operator_state_registry, operators, range_join, relation_state,
    semijoin, session_window_aggregate, stream, top1, topn, union, window, window_count_aggregate,
    window_count_star_aggregate, window_incremental_aggregate,
};
pub use dbsp_storage::storage;

pub use aggregate::DbspAggregate;
pub use algebra::AbelianGroup;
pub use circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateFunction, DbspAggregateNode,
    DbspAsofJoinSpec, DbspDistinctNode, DbspExpression, DbspJoinNode, DbspJoinType, DbspNodeKind,
    DbspPredicate, DbspProjectNode, DbspRangeJoinSpec, DbspScalarType, DbspSelectNode,
    DbspSinkNode, DbspSourceNode, DbspTopNNode, DbspUnionNode, DbspWindowAggregateNode,
    DbspWindowPolicy, DbspWindowSpec, Field, FieldRef, FloeAsofJoinNode, OptimizerDiagnostics,
    OptimizerRuleDiagnostics, OptimizerStageDiagnostics, OrderExpr, PlannerConfig, PlannerError,
    PrimaryKey, ProjectItem, RowSchema, TableDescriptor, create_logical_plan_with_asof_preplanner,
    nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table, nexmark_bid_table,
    nexmark_person_alias_table, nexmark_person_table,
};
pub use collections::{ZSet, h};
pub use count_aggregate::{
    DbspCountAggregate, DbspTransientCountAggregate, TransientCountAggregateDistinctWeight,
    TransientCountAggregateGroupedState, TransientCountAggregateSnapshot,
};
pub use dbsp_runtime::{DbspRangeJoin, LogicalWorkCollector, LogicalWorkSnapshot};
pub use distinct::DbspDistinct;
pub use filter_map::DbspFilterMap;
pub use handles::{StreamHandle, ZSetHandle, ZSetHandleView};
pub use incremental_aggregate::{
    DbspIncrementalAggregate, DbspTransientIncrementalAggregate,
    TransientIncrementalAggregateDistinctWeight, TransientIncrementalAggregateGroupedState,
    TransientIncrementalAggregateInputWeight, TransientIncrementalAggregateSnapshot,
};
pub use join::DbspJoin;
pub use operator_state_registry::{
    OperatorStateHandle, install_operator_state_restore, install_operator_state_restore_for_graph,
    snapshot_operator_states, snapshot_operator_states_for_graph,
};
pub use operators::count_aggregate::{
    CountAggregateRow, CountAggregateSlotKind, CountAggregateSlotUpdate,
};
pub use operators::distinct::DistinctOp;
pub use operators::incremental_aggregate::{
    AggregateValue, AggregateValueType, IncrementalAggregateRow, IncrementalAggregateSlotKind,
    IncrementalAggregateSlotState, IncrementalAggregateSlotUpdate,
};
pub use operators::join::{JoinInputRetention, JoinOp};
pub use operators::range_join::{RangeJoinOp, RangeLookupMode};
pub use operators::top1::PartitionedTop1Op;
pub use operators::topn::TopNOp;
pub use operators::window::WindowKey;
pub use relation_state::RelationState;
pub use semijoin::DbspSemiJoin;
pub use session_window_aggregate::DbspSessionWindowAggregate;
pub use stream::{
    CompactionSchedulerConfig, DeltaHandleStream, SnapshotHandleStream, StreamRetention, ZSetStream,
};
pub use top1::DbspPartitionedTop1;
pub use topn::DbspTopN;
pub use union::DbspUnion;
pub use window::DbspWindowAggregate;
pub use window_count_aggregate::{DbspWindowCountAggregate, WindowCountInput};
pub use window_count_star_aggregate::DbspWindowCountStarAggregate;
pub use window_incremental_aggregate::{DbspWindowIncrementalAggregate, WindowIncrementalInput};
