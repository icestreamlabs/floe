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
mod profile;
pub mod stream;

pub use dbsp_storage::storage;

pub use algebra::AbelianGroup;
pub use collections::OrderedBytes;
pub use handles::{StreamHandle, ZSetHandle, ZSetHandleView};
pub use metrics::LogicalWorkSnapshot;
pub use operator_state_registry::{
    OperatorStateHandle, install_operator_state_restore, install_operator_state_restore_for_graph,
    snapshot_operator_states, snapshot_operator_states_for_graph,
};
pub use operators::columnar_count::{ColumnarCountByKeyOp, SlateBackedColumnarCountByKeyOp};
pub use profile::{print_runtime_phase_profile, reset_runtime_phase_profile};
pub use stream::{
    CompactionSchedulerConfig, DeltaHandleStream, SnapshotHandleStream, StreamRetention, ZSetStream,
};

pub const KEY_COLUMN_NAME: &str = "__key";
pub const WEIGHT_COLUMN_NAME: &str = "__weight";
