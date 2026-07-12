use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::watch;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

mod accessors;
mod constructors;
mod flush;
mod handles;
mod state;
mod values;

/// Logical-time stream: at time `t`, this holds one value of type `T`.
///
/// Terminology:
/// - Logical time: the in-memory timeline advanced by `send`/`push_value_in_place`.
/// - Committed frontier: the last flushed timestamp that is durable and safe for cross-process reads.
///
/// For Floe SQL:
/// - `Stream<ZSetHandle>` represents the delta (Delta R_t) of a relation `R` at time `t`.
pub type DeltaStream = Stream<ZSetHandle>;

/// DBSP logical-time stream keyed by a logical transaction index.
///
/// Semantically, a stream is a total function from logical time to values in an
/// Abelian group.
///
/// Persisted streams store materialized observations and compact unchanged
/// suffixes internally, but that storage detail is not the semantic contract:
/// - `current_time()` is the committed logical frontier,
/// - `semantic_horizon()` is the last timestamp with an explicitly materialized
///   value,
/// - `default_value()` is the storage compaction value for materialized base
///   streams.
///
/// Derived streams attach an evaluator and compute their value at every logical
/// timestamp from their input streams. Operators must preserve this total-stream
/// semantics instead of using the storage tail as an approximation.
///
/// For each relation `R` in the SQL runtime:
/// - `Stream<ZSetHandle>` represents `Delta R_t`,
/// - `VersionedZSet<K>` represents the integrated `R_t`.
pub struct Stream<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    core: Arc<StreamCore<T>>,
    frontier_rx: watch::Receiver<i64>,
}

struct StreamCore<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    data_prefix: Vec<u8>,
    default_prefix: Vec<u8>,
    state_key: Vec<u8>,
    group: Arc<dyn AbelianGroup<T>>,
    state: RwLock<StreamState<T>>,
    frontier_tx: watch::Sender<i64>,
}

fn read_lock<'a, T>(lock: &'a RwLock<T>, label: &str) -> RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(%label, "stream rwlock read was poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

fn write_lock<'a, T>(lock: &'a RwLock<T>, label: &str) -> RwLockWriteGuard<'a, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(%label, "stream rwlock write was poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

struct StreamState<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    logical_timestamp: i64,
    max_known_timestamp: i64,
    identity: bool,
    default: T,
    pending_data: BTreeMap<i64, T>,
    pending_defaults: BTreeMap<i64, T>,
    pending_state: bool,
    data_cache: HashMap<i64, T>,
    default_changes: BTreeMap<i64, T>,
    last_default_ts: i64,
}

impl<T> Clone for Stream<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            frontier_rx: self.core.frontier_tx.subscribe(),
        }
    }
}

impl<T> Stream<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, StreamState<T>> {
        read_lock(&self.core.state, "stream state")
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, StreamState<T>> {
        write_lock(&self.core.state, "stream state")
    }

    fn notify_committed_frontier(&self, ts: i64) {
        let _ = self.core.frontier_tx.send(ts);
    }
}
