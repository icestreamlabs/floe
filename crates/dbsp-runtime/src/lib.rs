//! Operational DBSP runtime components.
//!
//! The `dbsp-runtime::stream::Stream<T>` type is an execution and storage
//! abstraction: it exposes current logical time, committed frontiers, semantic
//! horizons, and default-tail state.

pub mod aggregate;
pub mod algebra;
pub mod collections;
pub mod count_aggregate;
pub mod distinct;
mod ephemeral_state;
pub mod filter_map;
pub mod handles;
pub mod incremental_aggregate;
pub mod join;
mod metrics;
pub mod operator_state_registry;
pub mod operators;
pub mod range_join;
pub mod relation_state;
pub mod semijoin;
pub mod session_window_aggregate;
pub mod stream;
pub mod top1;
pub mod topn;
pub mod union;
pub mod window;
pub mod window_count_aggregate;
pub mod window_count_star_aggregate;
pub mod window_incremental_aggregate;

pub use dbsp_storage::storage;

pub use aggregate::DbspAggregate;
pub use algebra::AbelianGroup;
pub use collections::{OrderedBytes, ZSet, h};
pub use count_aggregate::{
    DbspCountAggregate, DbspTransientCountAggregate, TransientCountAggregateDistinctWeight,
    TransientCountAggregateGroupedState, TransientCountAggregateSnapshot,
};
pub use distinct::DbspDistinct;
pub use filter_map::DbspFilterMap;
pub use handles::{StreamHandle, ZSetHandle, ZSetHandleView};
pub use incremental_aggregate::{
    DbspIncrementalAggregate, DbspTransientIncrementalAggregate,
    TransientIncrementalAggregateDistinctWeight, TransientIncrementalAggregateGroupedState,
    TransientIncrementalAggregateInputWeight, TransientIncrementalAggregateSnapshot,
};
pub use join::DbspJoin;
pub use metrics::{LogicalWorkCollector, LogicalWorkSnapshot};
pub use operator_state_registry::{
    OperatorStateHandle, install_operator_state_restore, install_operator_state_restore_for_graph,
    snapshot_operator_states, snapshot_operator_states_for_graph,
};
pub use operators::aggregate::{AggregateOp, AggregateSpec};
pub use operators::count_aggregate::{
    CountAggregateOp, CountAggregateRow, CountAggregateSlotKind, CountAggregateSlotUpdate,
    GroupedCountState,
};
pub use operators::distinct::DistinctOp;
pub use operators::incremental_aggregate::{
    AggregateValue, AggregateValueType, GroupedIncrementalAggregateState, IncrementalAggregateOp,
    IncrementalAggregateRow, IncrementalAggregateSlotKind, IncrementalAggregateSlotState,
    IncrementalAggregateSlotUpdate,
};
pub use operators::join::{JoinInputRetention, JoinOp};
pub use operators::range_join::{RangeJoinOp, RangeLookupMode};
pub use operators::semijoin::{SemiJoinMode, SemiJoinOp};
pub use operators::top1::PartitionedTop1Op;
pub use operators::topn::TopNOp;
pub use operators::union::UnionOp;
pub use operators::window::{WindowAggregateOp, WindowKey};
pub use range_join::DbspRangeJoin;
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
