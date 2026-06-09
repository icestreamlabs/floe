//! Floe DBSP facade.
//!
//! Root-level exports focus on operational execution types, handle/Z-set
//! streams, planner types, and maintained runtime operator implementations.

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
    algebra, collections, handles, operator_state_registry, operators, relation_state, stream,
};
pub use dbsp_storage::storage;

pub use algebra::AbelianGroup;
pub use circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateFunction, DbspAggregateNode,
    DbspAsofJoinSpec, DbspDistinctNode, DbspExpression, DbspJoinNode, DbspJoinType, DbspNodeKind,
    DbspOneRowNode, DbspPredicate, DbspProjectNode, DbspRangeJoinSpec, DbspScalarType,
    DbspSelectNode, DbspSinkNode, DbspSourceNode, DbspTopNNode, DbspUnionNode,
    DbspWindowAggregateNode, DbspWindowPolicy, DbspWindowSpec, Field, FieldRef, FloeAsofJoinNode,
    OptimizerDiagnostics, OptimizerRuleDiagnostics, OptimizerStageDiagnostics, OrderExpr,
    PlannerConfig, PlannerError, PrimaryKey, ProjectItem, RowSchema, TableDescriptor,
    create_logical_plan_with_asof_preplanner, nexmark_auction_alias_table, nexmark_auction_table,
    nexmark_bid_alias_table, nexmark_bid_table, nexmark_person_alias_table, nexmark_person_table,
};
pub use collections::ZSet;
pub use dbsp_runtime::{LogicalWorkCollector, LogicalWorkSnapshot};
pub use handles::{StreamHandle, ZSetHandle, ZSetHandleView};
pub use operator_state_registry::{
    OperatorStateHandle, install_operator_state_restore, install_operator_state_restore_for_graph,
    snapshot_operator_states, snapshot_operator_states_for_graph,
};
pub use operators::aggregate::{AggregateOp, AggregateSpec};
pub use operators::columnar_count::{ColumnarCountByKeyOp, SlateBackedColumnarCountByKeyOp};
pub use operators::count_aggregate::{
    CountAggregateOp, CountAggregateRow, CountAggregateSlotKind, CountAggregateSlotUpdate,
    GroupedCountState,
};
pub use operators::distinct::DistinctOp;
pub use operators::incremental_aggregate::{
    AggregateValue, AggregateValueType, DistinctGroupKey, GroupedIncrementalAggregateState,
    IncrementalAggregateIndexes, IncrementalAggregateOp, IncrementalAggregateRow,
    IncrementalAggregateSlotKind, IncrementalAggregateSlotState, IncrementalAggregateSlotUpdate,
};
pub use operators::join::JoinOp;
pub use operators::range_join::{RangeJoinBatchConfig, RangeJoinOp, RangeLookupMode};
pub use operators::semijoin::{SemiJoinBatchConfig, SemiJoinMode, SemiJoinOp};
pub use operators::top1::PartitionedTop1Op;
pub use operators::topn::TopNOp;
pub use operators::union::UnionOp;
pub use operators::window::{WindowAggregateBatchConfig, WindowAggregateOp, WindowKey};
pub use relation_state::RelationState;
pub use stream::{
    CompactionSchedulerConfig, DeltaHandleStream, SnapshotHandleStream, StreamRetention, ZSetStream,
};
