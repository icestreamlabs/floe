use anyhow::{Result, anyhow};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::handles::ZSetHandleView;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

use super::registry::MaterializedViewHandle;

#[async_trait]
pub trait MaterializedView: Send + Sync {
    fn latest_version(&self) -> Option<i64>;
    fn next_version_after(&self, version: i64) -> Option<i64>;
    fn subscribe_versions(&self) -> watch::Receiver<Option<i64>>;
    fn handle_for(&self, version: i64) -> Result<ZSetHandleView<Vec<u8>>>;
    fn version_time(&self, version: i64) -> Option<i64>;
    fn arrow_snapshot_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>>;
    fn arrow_delta_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>>;
    async fn snapshot_for(&self, version: i64) -> Result<HashMap<Vec<u8>, i64>>;
    async fn delta_for(&self, version: i64) -> Result<Vec<(Vec<u8>, i64)>>;
}

#[async_trait]
impl MaterializedView for MaterializedViewHandle {
    fn latest_version(&self) -> Option<i64> {
        MaterializedViewHandle::latest_version(self)
    }

    fn next_version_after(&self, version: i64) -> Option<i64> {
        MaterializedViewHandle::next_version_after(self, version)
    }

    fn subscribe_versions(&self) -> watch::Receiver<Option<i64>> {
        self.version_watch()
    }

    fn handle_for(&self, version: i64) -> Result<ZSetHandleView<Vec<u8>>> {
        let handle = self.handle_for_version(version).ok_or_else(|| {
            anyhow!(
                "version {version} not found for materialized view '{}'.",
                self.name()
            )
        })?;
        let state = self.dbsp_state().ok_or_else(|| {
            anyhow!(
                "materialized view '{}' is missing persisted DBSP state",
                self.name()
            )
        })?;
        Ok(ZSetHandleView::new(
            state.dictionary(),
            state.table(),
            handle.ns,
            handle.version,
        ))
    }

    fn version_time(&self, version: i64) -> Option<i64> {
        MaterializedViewHandle::version_time(self, version)
    }

    fn arrow_snapshot_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>> {
        MaterializedViewHandle::arrow_snapshot_for(self, version)
    }

    fn arrow_delta_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>> {
        MaterializedViewHandle::arrow_delta_for(self, version)
    }

    async fn snapshot_for(&self, version: i64) -> Result<HashMap<Vec<u8>, i64>> {
        if let Ok(handle) = self.handle_for(version) {
            return handle.materialize().await;
        }

        let version_u64 = u64::try_from(version).map_err(|_| {
            anyhow!(
                "version {version} is out of range for materialized view '{}'.",
                self.name()
            )
        })?;
        if let Some((base_version, _target_version, overlay)) =
            self.encoded_overlay_merged_delta(Some(version_u64))
        {
            let mut snapshot = if let Some(state) = self.dbsp_state() {
                if let Some(base_dbsp_version) = resolve_dbsp_version(self, &state, base_version) {
                    materialize_dbsp_version(&state, base_dbsp_version).await?
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };
            for (key, diff) in overlay {
                let previous = snapshot.get(&key).copied().unwrap_or(0);
                let next = previous.saturating_add(diff);
                if next <= 0 {
                    snapshot.remove(&key);
                } else {
                    snapshot.insert(key, next);
                }
            }
            return Ok(snapshot);
        }

        if let Some(state) = self.dbsp_state()
            && let Some(dbsp_version) = resolve_dbsp_version(self, &state, version_u64)
        {
            return materialize_dbsp_version(&state, dbsp_version).await;
        }

        Err(anyhow!(
            "version {version} not found for materialized view '{}'.",
            self.name()
        ))
    }

    async fn delta_for(&self, version: i64) -> Result<Vec<(Vec<u8>, i64)>> {
        if let Ok(handle) = self.handle_for(version) {
            return handle.delta_iter().await;
        }

        let version_u64 = u64::try_from(version).map_err(|_| {
            anyhow!(
                "version {version} is out of range for materialized view '{}'.",
                self.name()
            )
        })?;
        if let Some(delta) = self.encoded_overlay_batch(version_u64) {
            return Ok(delta);
        }

        if self.is_version_published(version) {
            return Ok(Vec::new());
        }

        Err(anyhow!(
            "version {version} not found for materialized view '{}'.",
            self.name()
        ))
    }
}

fn resolve_dbsp_version(
    view: &MaterializedViewHandle,
    state: &crate::materialized_view::DbspPersistedState,
    target_version: u64,
) -> Option<u64> {
    let target_version_i64 = i64::try_from(target_version).ok()?;
    if let Some(handle) = view.handle_for_version(target_version_i64) {
        return Some(handle.version);
    }
    if view.is_version_published(target_version_i64) {
        return view
            .handle_at_or_before_version(target_version_i64)
            .map(|handle| handle.version)
            .or_else(|| (target_version == state.logical_version()).then_some(state.version()))
            .or_else(|| (state.version() == 0).then_some(0));
    }
    if target_version <= state.version() {
        Some(target_version)
    } else if target_version == state.logical_version() {
        Some(state.version())
    } else {
        None
    }
}

async fn materialize_dbsp_version(
    state: &crate::materialized_view::DbspPersistedState,
    version: u64,
) -> Result<HashMap<Vec<u8>, i64>> {
    ZSetHandleView::new(
        state.dictionary(),
        state.table(),
        state.namespace().to_string(),
        version,
    )
    .materialize()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::registry::MaterializedViewRegistry;
    use dbsp::handles::ZSetHandle;

    #[tokio::test]
    async fn notifies_version_subscribers() {
        let registry = MaterializedViewRegistry::new();
        let view = registry.register("mv_runtime_test");

        let mut rx =
            <MaterializedViewHandle as MaterializedView>::subscribe_versions(view.as_ref());
        assert_eq!(*rx.borrow(), None);

        view.publish_version(
            1,
            ZSetHandle {
                ns: "mv_runtime_test".into(),
                version: 10,
            },
        );
        rx.changed().await.expect("watch update for v1");
        assert_eq!(*rx.borrow(), Some(1));

        view.publish_version(
            2,
            ZSetHandle {
                ns: "mv_runtime_test".into(),
                version: 11,
            },
        );
        rx.changed().await.expect("watch update for v2");
        assert_eq!(*rx.borrow(), Some(2));
    }
}
