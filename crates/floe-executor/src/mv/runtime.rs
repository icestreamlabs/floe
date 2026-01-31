use anyhow::{Result, anyhow};
use dbsp::handles::ZSetHandleView;
use tokio::sync::watch;

use super::registry::MaterializedViewHandle;

pub trait MaterializedView: Send + Sync {
    fn latest_version(&self) -> Option<i64>;
    fn subscribe_versions(&self) -> watch::Receiver<Option<i64>>;
    fn handle_for(&self, version: i64) -> Result<ZSetHandleView<Vec<u8>>>;
    fn version_time(&self, version: i64) -> Option<i64>;
}

impl MaterializedView for MaterializedViewHandle {
    fn latest_version(&self) -> Option<i64> {
        MaterializedViewHandle::latest_version(self)
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
