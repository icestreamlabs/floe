use std::collections::HashMap;

use anyhow::Result;
use dbsp::ZSetStream;
use dbsp::handles::{ZSetHandle, ZSetHandleView};

use crate::stream_types::Diff;

/// Handle that identifies a persisted operator state table snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct OperatorStateHandle {
    pub table: String,
    pub namespace: String,
    pub version: u64,
}

impl OperatorStateHandle {
    pub fn new(table: impl Into<String>, namespace: impl Into<String>, version: u64) -> Self {
        Self {
            table: table.into(),
            namespace: namespace.into(),
            version,
        }
    }
}

/// Wrapper over a [`ZSetStream`] that tracks pending overlays for an operator's
/// in-memory state and flushes them as versioned snapshots.
pub struct StateTable {
    name: String,
    namespace: String,
    stream: ZSetStream<Vec<u8>>,
    dirty: bool,
}

impl StateTable {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        stream: ZSetStream<Vec<u8>>,
    ) -> Self {
        Self {
            name: name.into(),
            namespace: namespace.into(),
            stream,
            dirty: false,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Adds a serialized delta into the overlay buffer for this table.
    pub fn add_delta(&mut self, key: Vec<u8>, diff: Diff) {
        if diff == 0 {
            return;
        }
        self.stream.add_delta(key, diff);
        self.dirty = true;
    }

    /// Returns the current handle without flushing any pending deltas.
    pub fn current_handle(&self) -> ZSetHandle {
        self.stream.current_handle().clone()
    }

    /// Materializes the latest snapshot of this table by decoding the current handle view.
    pub async fn snapshot(&self) -> Result<HashMap<Vec<u8>, i64>> {
        let view = self.stream.latest_view();
        view.materialize().await
    }

    /// Flushes any staged deltas and returns the persisted handle metadata for checkpoint manifests.
    pub async fn flush(&mut self) -> Result<OperatorStateHandle> {
        let handle = if self.dirty {
            let flushed = self.stream.flush().await?;
            self.dirty = false;
            flushed
        } else {
            self.stream.current_handle().clone()
        };
        Ok(OperatorStateHandle::new(
            self.name.clone(),
            self.namespace.clone(),
            handle.version,
        ))
    }

    /// Builds a [`ZSetHandleView`] referencing the current handle.
    pub fn latest_view(&self) -> ZSetHandleView<Vec<u8>> {
        self.stream.latest_view()
    }

    /// Replaces the dirty flag when reloading persisted state to avoid redundant flushes.
    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbsp_bridge::DbspBridge;
    use dbsp::StreamRetention;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::sync::Arc;

    async fn build_state_table(name: &str) -> StateTable {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open(format!("state-table-{name}"), store)
                .await
                .expect("open db"),
        );
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        bridge
            .new_state_table(
                format!("op/test/{name}"),
                name.to_string(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("state table")
    }

    #[tokio::test]
    async fn flushes_dirty_overlay() {
        let mut table = build_state_table("flush").await;
        table.add_delta(vec![1, 2, 3], 1);
        let handle = table.flush().await.expect("flush");
        assert_eq!(handle.version, 1);

        // Second flush without new deltas should reuse same version.
        let handle2 = table.flush().await.expect("flush");
        assert_eq!(handle2.version, handle.version);
    }

    #[tokio::test]
    async fn materializes_snapshot() {
        let mut table = build_state_table("snapshot").await;
        table.add_delta(vec![9], 2);
        table.flush().await.expect("flush");
        let snapshot = table.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.get(&vec![9]), Some(&2));
    }
}
