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
    aggregate, algebra, collections, count_aggregate, distinct, filter, filter_map, handles,
    incremental_aggregate, join, map, operators, relation_state, semijoin, stream, topk, topn,
    union, window,
};
pub use dbsp_storage::storage;

pub use aggregate::DbspAggregate;
pub use algebra::AbelianGroup;
pub use circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateFunction, DbspAggregateNode,
    DbspDistinctNode, DbspExpression, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspPredicate,
    DbspProjectNode, DbspScalarType, DbspSelectNode, DbspSinkNode, DbspSourceNode, DbspTopNNode,
    DbspUnionNode, DbspWindowAggregateNode, DbspWindowPolicy, DbspWindowSpec, Field, FieldRef,
    KeyEncoder, OrderExpr, PlannerConfig, PlannerError, PrimaryKey, ProjectItem, Row, RowBuilder,
    RowSchema, ScalarValue, TableDescriptor, encode_composite_key, encode_scalar,
    nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table, nexmark_bid_table,
    nexmark_person_alias_table, nexmark_person_table,
};
pub use collections::{ZSet, h};
pub use count_aggregate::DbspCountAggregate;
pub use distinct::DbspDistinct;
pub use filter::DbspFilter;
pub use filter_map::DbspFilterMap;
pub use handles::{StreamHandle, ZSetHandle, ZSetHandleView};
pub use incremental_aggregate::DbspIncrementalAggregate;
pub use join::DbspJoin;
pub use map::DbspMap;
pub use operators::consolidate::ConsolidateOp;
pub use operators::count_aggregate::{
    CountAggregateRow, CountAggregateSlotKind, CountAggregateSlotUpdate,
};
pub use operators::distinct::DistinctOp;
pub use operators::filter::FilterOp;
pub use operators::incremental_aggregate::{
    AggregateValue, AggregateValueType, IncrementalAggregateRow, IncrementalAggregateSlotKind,
    IncrementalAggregateSlotUpdate,
};
pub use operators::join::JoinOp;
pub use operators::map::MapOp;
pub use operators::topk::TopKOp;
pub use operators::topn::TopNOp;
pub use operators::window::WindowKey;
pub use relation_state::RelationState;
pub use semijoin::DbspSemiJoin;
pub use stream::{
    CompactionSchedulerConfig, DeltaHandleStream, SnapshotHandleStream, StreamRetention, ZSetStream,
};
pub use topk::DbspTopK;
pub use topn::DbspTopN;
pub use union::DbspUnion;
pub use window::DbspWindowAggregate;
