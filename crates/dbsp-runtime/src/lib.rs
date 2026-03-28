//! Operational DBSP runtime components.
//!
//! The paper-facing denotational stream and circuit model lives in
//! `dbsp-semantic`. The `dbsp-runtime::stream::Stream<T>` type remains public
//! as an execution and storage abstraction: it exposes current logical time,
//! committed frontiers, semantic horizons, and default-tail state.
//! Those operational observations are intentionally distinct from the paper
//! DBSP stream object.

pub mod aggregate;
pub mod algebra;
pub mod collections;
pub mod count_aggregate;
pub mod distinct;
mod ephemeral_state;
pub mod filter;
pub mod filter_map;
pub mod handles;
pub mod incremental_aggregate;
pub mod join;
pub mod map;
mod metrics;
pub mod operators;
pub mod relation_state;
pub mod semijoin;
pub mod stream;
pub mod top1;
pub mod topk;
pub mod topn;
pub mod union;
pub mod window;

pub use dbsp_storage::storage;

pub use aggregate::DbspAggregate;
pub use algebra::AbelianGroup;
pub use collections::{OrderedBytes, ZSet, h};
pub use count_aggregate::DbspCountAggregate;
pub use distinct::DbspDistinct;
pub use filter::DbspFilter;
pub use filter_map::DbspFilterMap;
pub use handles::{StreamHandle, ZSetHandle, ZSetHandleView};
pub use incremental_aggregate::DbspIncrementalAggregate;
pub use join::DbspJoin;
pub use map::DbspMap;
pub use operators::aggregate::{AggregateOp, AggregateSpec};
pub use operators::asof_join::AsofJoinOp;
pub use operators::consolidate::ConsolidateOp;
pub use operators::count_aggregate::{
    CountAggregateOp, CountAggregateRow, CountAggregateSlotKind, CountAggregateSlotUpdate,
    GroupedCountState,
};
pub use operators::distinct::DistinctOp;
pub use operators::filter::FilterOp;
pub use operators::group_by::GroupByOp;
pub use operators::incremental_aggregate::{
    AggregateValue, AggregateValueType, GroupedIncrementalAggregateState, IncrementalAggregateOp,
    IncrementalAggregateRow, IncrementalAggregateSlotKind, IncrementalAggregateSlotState,
    IncrementalAggregateSlotUpdate,
};
pub use operators::index::ArrangeByKeyOp;
pub use operators::join::JoinOp;
pub use operators::join_range::JoinRangeOp;
pub use operators::map::MapOp;
pub use operators::rolling_aggregate::RollingAggregateOp;
pub use operators::semijoin::{SemiJoinMode, SemiJoinOp};
pub use operators::top1::PartitionedTop1Op;
pub use operators::topk::TopKOp;
pub use operators::topn::TopNOp;
pub use operators::union::UnionOp;
pub use operators::waterline::WaterlineOp;
pub use operators::window::{WindowAggregateOp, WindowKey};
pub use relation_state::RelationState;
pub use semijoin::DbspSemiJoin;
pub use stream::{
    CompactionSchedulerConfig, DeltaHandleStream, SnapshotHandleStream, StreamRetention, ZSetStream,
};
pub use top1::DbspPartitionedTop1;
pub use topk::DbspTopK;
pub use topn::DbspTopN;
pub use union::DbspUnion;
pub use window::DbspWindowAggregate;
