//! Floe DBSP facade.
//!
//! Paper-facing semantic streams and circuits live under [`semantic`]. The
//! root-level runtime exports remain focused on operational execution types,
//! handle/Z-set streams, and planner/runtime wrappers.

pub mod circuit {
    pub use dbsp_circuit::circuit::*;
    pub use dbsp_planner::{CircuitNode, CircuitPlan, CircuitPlanner, PlannerConfig, PlannerError};

    pub mod planner {
        pub use dbsp_planner::{
            CircuitNode, CircuitPlan, CircuitPlanner, PlannerConfig, PlannerError,
        };
    }
}

pub mod semantic {
    pub use dbsp_semantic::*;
}

pub use dbsp_runtime::{
    aggregate, algebra, collections, count_aggregate, distinct, filter_map, handles,
    incremental_aggregate, join, operator_state_registry, operators, relation_state, semijoin,
    stream, top1, topn, union, window, window_count_aggregate, window_count_star_aggregate,
};
pub use dbsp_storage::storage;

pub use aggregate::DbspAggregate;
pub use algebra::AbelianGroup;
pub use circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateFunction, DbspAggregateNode,
    DbspDistinctNode, DbspExpression, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspPredicate,
    DbspProjectNode, DbspScalarType, DbspSelectNode, DbspSinkNode, DbspSourceNode, DbspTopNNode,
    DbspUnionNode, DbspWindowAggregateNode, DbspWindowPolicy, DbspWindowSpec, Field, FieldRef,
    OrderExpr, PlannerConfig, PlannerError, PrimaryKey, ProjectItem, RowSchema, TableDescriptor,
    nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table, nexmark_bid_table,
    nexmark_person_alias_table, nexmark_person_table,
};
pub use collections::{ZSet, h};
pub use count_aggregate::{
    DbspCountAggregate, DbspTransientCountAggregate, TransientCountAggregateDistinctWeight,
    TransientCountAggregateGroupedState, TransientCountAggregateSnapshot,
};
pub use dbsp_runtime::{LogicalWorkCollector, LogicalWorkSnapshot};
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
    OperatorStateHandle, install_operator_state_restore, snapshot_operator_states,
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
pub use operators::top1::PartitionedTop1Op;
pub use operators::topn::TopNOp;
pub use operators::window::WindowKey;
pub use relation_state::RelationState;
pub use semijoin::DbspSemiJoin;
pub use stream::{
    CompactionSchedulerConfig, DeltaHandleStream, SnapshotHandleStream, StreamRetention, ZSetStream,
};
pub use top1::DbspPartitionedTop1;
pub use topn::DbspTopN;
pub use union::DbspUnion;
pub use window::DbspWindowAggregate;
pub use window_count_aggregate::{DbspWindowCountAggregate, WindowCountInput};
pub use window_count_star_aggregate::DbspWindowCountStarAggregate;
