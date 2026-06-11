use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::collections::SlateBackedColumnarZSet;
use dbsp::handles::ZSetHandleView;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

use crate::columnar_snapshot::{columnar_zset_positive_row_count, columnar_zset_to_arrow_snapshot};

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
    async fn columnar_snapshot_for(
        &self,
        schema: SchemaRef,
        version: i64,
    ) -> Result<Option<Arc<Vec<RecordBatch>>>>;
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

    async fn columnar_snapshot_for(
        &self,
        schema: SchemaRef,
        version: i64,
    ) -> Result<Option<Arc<Vec<RecordBatch>>>> {
        let version_u64 = u64::try_from(version).map_err(|_| {
            anyhow!(
                "version {version} is out of range for materialized view '{}'.",
                self.name()
            )
        })?;
        let Some(handle) = self.handle_for_version(version) else {
            return Ok(None);
        };
        let Some(storage) = self.columnar_storage() else {
            return Ok(None);
        };
        if storage.schema().as_ref() != schema.as_ref() {
            bail!(
                "columnar materialized view schema for '{}' does not match requested snapshot schema",
                self.name()
            );
        }
        let zset = SlateBackedColumnarZSet::new(storage.table(), handle.ns, Arc::clone(&schema))
            .await
            .context("initialize columnar materialized view snapshot reader")?;
        let materialized = zset
            .materialize_columnar_version(handle.version)
            .await
            .with_context(|| {
                format!(
                    "materialize columnar snapshot for '{}' at version {version}",
                    self.name()
                )
            })?;
        let row_count = columnar_zset_positive_row_count(&materialized)?;
        self.seed_authoritative_row_count_if_latest(version_u64, row_count);
        let batches = columnar_zset_to_arrow_snapshot(&materialized, schema, None)?;
        Ok(Some(Arc::new(batches)))
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
    state: &crate::mv::registry::DbspPersistedState,
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
    state: &crate::mv::registry::DbspPersistedState,
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
