use std::sync::Arc;

use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::watch;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::Stream;

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
    pub fn group(&self) -> Arc<dyn AbelianGroup<T>> {
        self.core.group.clone()
    }

    pub fn namespace(&self) -> &str {
        &self.core.namespace
    }

    pub(crate) fn table(&self) -> Arc<dyn KeyValueTable> {
        self.core.table.clone()
    }

    /// Current logical time (may be ahead of committed frontier).
    pub fn current_time(&self) -> i64 {
        self.read_state().logical_timestamp
    }

    /// Last timestamp with a materialized value or scheduled storage change.
    ///
    /// Derived streams remain semantically defined for all logical times; this
    /// value is a cache/materialization boundary, not a semantic tail boundary.
    pub fn semantic_horizon(&self) -> i64 {
        self.read_state().max_known_timestamp
    }

    /// Last committed frontier persisted to storage.
    pub fn committed_frontier(&self) -> i64 {
        *self.frontier_rx.borrow()
    }

    pub fn is_identity(&self) -> bool {
        self.read_state().identity
    }

    pub fn default_value(&self) -> T {
        self.read_state().default.clone()
    }

    #[cfg(test)]
    pub(crate) fn last_default_ts(&self) -> i64 {
        self.read_state().last_default_ts
    }

    pub fn handle(&self) -> StreamHandle {
        StreamHandle {
            ns: self.core.namespace.clone(),
            frontier: self.committed_frontier(),
        }
    }

    #[cfg(test)]
    pub(crate) fn encode_intent_key(&self) -> Vec<u8> {
        self.core.encode_intent_key()
    }

    /// Subscribe to committed frontier updates.
    pub fn subscribe_frontier(&self) -> watch::Receiver<i64> {
        self.core.frontier_tx.subscribe()
    }

    pub(crate) fn commit_frontier(&self, ts: i64) {
        self.notify_committed_frontier(ts);
    }
}
