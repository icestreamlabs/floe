//! Operational DBSP runtime components.
//!
//! The `dbsp-runtime::stream::Stream<T>` type is an execution and storage
//! abstraction: it exposes current logical time, committed frontiers, semantic
//! horizons, and default-tail state.

pub mod algebra;
pub mod collections;
pub mod handles;
mod metrics;
pub mod operator_state_registry;
pub mod operators;
pub mod relation_state;
pub mod stream;

pub use dbsp_storage::storage;

pub use algebra::AbelianGroup;
pub use collections::{OrderedBytes, ZSet};
pub use handles::{StreamHandle, ZSetHandle, ZSetHandleView};
pub use metrics::{LogicalWorkCollector, LogicalWorkSnapshot};
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
