use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::stream_types::{Diff, Row, Timestamp};
use datafusion::arrow::datatypes::SchemaRef;
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use tokio::sync::watch;
use tracing::field;

#[derive(Debug, Default)]
pub struct MaterializedViewRegistry {
    views: RwLock<HashMap<String, Arc<MaterializedViewHandle>>>,
    schemas: RwLock<HashMap<String, SchemaRef>>,
}

impl MaterializedViewRegistry {
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, name: impl Into<String>) -> Arc<MaterializedViewHandle> {
        let mut guard = self.views.write().expect("mutex poisoned");
        let name = name.into();
        guard
            .entry(name.clone())
            .or_insert_with(|| Arc::new(MaterializedViewHandle::new(name)))
            .clone()
    }

    pub fn get(&self, name: &str) -> Option<Arc<MaterializedViewHandle>> {
        self.views
            .read()
            .expect("mutex poisoned")
            .get(name)
            .cloned()
    }

    pub fn set_schema(&self, name: impl Into<String>, schema: SchemaRef) {
        self.schemas
            .write()
            .expect("mutex poisoned")
            .insert(name.into(), schema);
    }

    pub fn schema(&self, name: &str) -> Option<SchemaRef> {
        self.schemas
            .read()
            .expect("mutex poisoned")
            .get(name)
            .cloned()
    }
}

pub struct MaterializedViewHandle {
    name: String,
    state: RwLock<HashMap<Row, Diff>>,
    watermark: RwLock<Option<Timestamp>>,
    dbsp_state: RwLock<Option<DbspPersistedState>>,
    versions: RwLock<HashMap<i64, ZSetHandle>>,
    latest_version: RwLock<Option<i64>>,
    version_watch: watch::Sender<Option<i64>>,
}

impl MaterializedViewHandle {
    fn new(name: String) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self {
            name,
            state: RwLock::new(HashMap::new()),
            watermark: RwLock::new(None),
            dbsp_state: RwLock::new(None),
            versions: RwLock::new(HashMap::new()),
            latest_version: RwLock::new(None),
            version_watch: tx,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn apply(&self, row: Row, diff: Diff) {
        if diff == 0 {
            return;
        }

        let mut guard = self.state.write().expect("mutex poisoned");
        let entry = guard.entry(row.clone()).or_insert(0);
        *entry += diff;
        if *entry == 0 {
            guard.remove(&row);
        }
    }

    pub fn update_watermark(&self, watermark: Timestamp) {
        *self.watermark.write().expect("mutex poisoned") = Some(watermark);
    }

    pub fn watermark(&self) -> Option<Timestamp> {
        *self.watermark.read().expect("mutex poisoned")
    }

    pub fn snapshot(&self) -> HashMap<Row, Diff> {
        self.state.read().expect("mutex poisoned").clone()
    }

    pub fn set_dbsp_state(&self, state: DbspPersistedState) {
        let span = tracing::debug_span!(
            "materialize",
            view = %self.name,
            namespace = %state.namespace(),
            version = field::Empty
        );
        let _enter = span.enter();
        span.record("version", state.version());
        tracing::debug!("materialized view DBSP state updated");
        *self.dbsp_state.write().expect("mutex poisoned") = Some(state);
    }

    pub fn dbsp_state(&self) -> Option<DbspPersistedState> {
        self.dbsp_state.read().expect("mutex poisoned").clone()
    }

    pub fn publish_version(&self, version: i64, handle: ZSetHandle) {
        let namespace = handle.ns.clone();
        {
            let mut guard = self
                .versions
                .write()
                .expect("materialized view versions lock poisoned");
            guard.insert(version, handle);
        }
        {
            let mut guard = self
                .latest_version
                .write()
                .expect("materialized view version lock poisoned");
            *guard = Some(version);
        }
        tracing::debug!(
            view = %self.name,
            version,
            namespace = %namespace,
            "materialized view version recorded"
        );
        let _ = self.version_watch.send_replace(Some(version));
    }

    pub fn latest_version(&self) -> Option<i64> {
        *self
            .latest_version
            .read()
            .expect("materialized view version lock poisoned")
    }

    pub fn version_watch(&self) -> watch::Receiver<Option<i64>> {
        self.version_watch.subscribe()
    }

    pub fn handle_for_version(&self, version: i64) -> Option<ZSetHandle> {
        self.versions
            .read()
            .expect("materialized view versions lock poisoned")
            .get(&version)
            .cloned()
    }
}

impl fmt::Debug for MaterializedViewHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_len = self.state.read().map(|state| state.len()).unwrap_or(0);
        let latest = *self
            .latest_version
            .read()
            .expect("materialized view version lock poisoned");
        f.debug_struct("MaterializedViewHandle")
            .field("name", &self.name)
            .field("state_len", &state_len)
            .field("latest_version", &latest)
            .finish()
    }
}

#[derive(Clone)]
pub struct DbspPersistedState {
    dictionary: Arc<Dictionary<Vec<u8>>>,
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    version: u64,
}

impl std::fmt::Debug for DbspPersistedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbspPersistedState")
            .field("namespace", &self.namespace)
            .field("version", &self.version)
            .finish()
    }
}

impl DbspPersistedState {
    pub fn new(
        dictionary: Arc<Dictionary<Vec<u8>>>,
        table: Arc<dyn KeyValueTable>,
        namespace: String,
        version: u64,
    ) -> Self {
        Self {
            dictionary,
            table,
            namespace,
            version,
        }
    }

    pub fn dictionary(&self) -> Arc<Dictionary<Vec<u8>>> {
        Arc::clone(&self.dictionary)
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::*;

    #[test]
    fn registers_and_updates_view_state() {
        let registry = MaterializedViewRegistry::new();
        let view = registry.register("mv_test");
        let row = vec![ScalarValue::Int64(Some(1))];
        view.apply(row.clone(), 1);
        assert_eq!(view.snapshot().get(&row), Some(&1));
        view.apply(row.clone(), -1);
        assert!(view.snapshot().is_empty());
        view.update_watermark(42);
        assert_eq!(view.watermark(), Some(42));
    }

    #[test]
    fn registry_returns_same_handle() {
        let registry = MaterializedViewRegistry::new();
        let view_a = registry.register("mv");
        let view_b = registry.get("mv").expect("view registered");
        assert!(Arc::ptr_eq(&view_a, &view_b));
    }
}
