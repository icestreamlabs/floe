use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, RwLock};

use anyhow::Result;
use async_trait::async_trait;
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
    evaluator_key: Vec<u8>,
    group: Arc<dyn AbelianGroup<T>>,
    evaluator: Option<Arc<dyn StreamEvaluator<T>>>,
    state: RwLock<StreamState<T>>,
    frontier_tx: watch::Sender<i64>,
}

#[async_trait]
pub(crate) trait StreamEvaluator<T>: Send + Sync
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
    async fn value_at(&self, timestamp: i64, group: Arc<dyn AbelianGroup<T>>) -> Result<T>;
}

static STREAM_EVALUATOR_REGISTRY: LazyLock<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn register_stream_evaluator<T>(namespace: &str, evaluator: Arc<dyn StreamEvaluator<T>>)
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
    let erased: Arc<dyn Any + Send + Sync> = Arc::new(evaluator);
    STREAM_EVALUATOR_REGISTRY
        .lock()
        .expect("stream evaluator registry lock poisoned")
        .insert(namespace.to_string(), erased);
}

fn registered_stream_evaluator<T>(namespace: &str) -> Option<Arc<dyn StreamEvaluator<T>>>
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
    let erased = STREAM_EVALUATOR_REGISTRY
        .lock()
        .expect("stream evaluator registry lock poisoned")
        .get(namespace)
        .cloned()?;
    let typed = Arc::downcast::<Arc<dyn StreamEvaluator<T>>>(erased).ok()?;
    Some(typed.as_ref().clone())
}

#[cfg(test)]
pub(crate) fn unregister_stream_evaluator_for_test(namespace: &str) {
    STREAM_EVALUATOR_REGISTRY
        .lock()
        .expect("stream evaluator registry lock poisoned")
        .remove(namespace);
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
        self.core.state.read().expect("stream state poisoned")
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, StreamState<T>> {
        self.core.state.write().expect("stream state poisoned")
    }

    fn notify_committed_frontier(&self, ts: i64) {
        let _ = self.core.frontier_tx.send(ts);
    }

    pub(crate) fn derived_value_at(
        &self,
        timestamp: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<T>>> + Send + '_>> {
        Box::pin(async move {
            let Some(evaluator) = self.core.evaluator.clone() else {
                return Ok(None);
            };
            evaluator.value_at(timestamp, self.group()).await.map(Some)
        })
    }
}
