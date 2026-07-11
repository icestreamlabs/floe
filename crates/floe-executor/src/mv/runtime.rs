use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::collections::SlateBackedColumnarZSet;
use std::sync::Arc;
use tokio::sync::watch;

use crate::columnar_snapshot::{columnar_zset_positive_row_count, columnar_zset_to_arrow_snapshot};

use super::registry::MaterializedViewHandle;

#[async_trait]
pub trait MaterializedView: Send + Sync {
    fn latest_version(&self) -> Option<i64>;
    fn next_version_after(&self, version: i64) -> Option<i64>;
    fn subscribe_versions(&self) -> watch::Receiver<Option<i64>>;
    fn version_time(&self, version: i64) -> Option<i64>;
    fn arrow_snapshot_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>>;
    fn arrow_delta_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>>;
    async fn columnar_snapshot_for(
        &self,
        schema: SchemaRef,
        version: i64,
    ) -> Result<Option<Arc<Vec<RecordBatch>>>>;
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
        let Some((handle, storage)) = self.columnar_storage_for(version) else {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::registry::MaterializedViewRegistry;

    #[tokio::test]
    async fn notifies_version_subscribers() {
        let registry = MaterializedViewRegistry::new();
        let view = registry.register("mv_runtime_test");

        let mut rx =
            <MaterializedViewHandle as MaterializedView>::subscribe_versions(view.as_ref());
        assert_eq!(*rx.borrow(), None);

        view.publish_arrow_version(1, Vec::new(), Vec::new());
        rx.changed().await.expect("watch update for v1");
        assert_eq!(*rx.borrow(), Some(1));

        view.publish_arrow_version(2, Vec::new(), Vec::new());
        rx.changed().await.expect("watch update for v2");
        assert_eq!(*rx.borrow(), Some(2));
    }
}
