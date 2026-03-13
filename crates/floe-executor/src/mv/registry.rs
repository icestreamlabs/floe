use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

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
    retention_keep_last: Option<usize>,
}

impl MaterializedViewRegistry {
    pub fn new() -> Self {
        Self::new_with_retention(None)
    }

    pub fn new_with_retention(retention_keep_last: Option<usize>) -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            retention_keep_last,
        }
    }

    pub fn register(&self, name: impl Into<String>) -> Arc<MaterializedViewHandle> {
        let mut guard = self.views.write().expect("mutex poisoned");
        let name = name.into();
        guard
            .entry(name.clone())
            .or_insert_with(|| {
                Arc::new(MaterializedViewHandle::new(name, self.retention_keep_last))
            })
            .clone()
    }

    pub fn get(&self, name: &str) -> Option<Arc<MaterializedViewHandle>> {
        self.views
            .read()
            .expect("mutex poisoned")
            .get(name)
            .cloned()
    }

    pub fn handles(&self) -> Vec<Arc<MaterializedViewHandle>> {
        self.views
            .read()
            .expect("mutex poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn update_watermark_all(&self, watermark: Timestamp) {
        let views: Vec<Arc<MaterializedViewHandle>> = self
            .views
            .read()
            .expect("mutex poisoned")
            .values()
            .cloned()
            .collect();
        for view in views {
            view.update_watermark(watermark);
        }
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
    encoded_overlay_state: RwLock<Option<EncodedOverlayState>>,
    published_versions: RwLock<BTreeSet<i64>>,
    versions: RwLock<HashMap<i64, ZSetHandle>>,
    version_times: RwLock<HashMap<i64, i64>>,
    latest_version: RwLock<Option<i64>>,
    version_watch: watch::Sender<Option<i64>>,
    retention_keep_last: Option<usize>,
}

impl MaterializedViewHandle {
    fn new(name: String, retention_keep_last: Option<usize>) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self {
            name,
            state: RwLock::new(HashMap::new()),
            watermark: RwLock::new(None),
            dbsp_state: RwLock::new(None),
            encoded_overlay_state: RwLock::new(None),
            published_versions: RwLock::new(BTreeSet::new()),
            versions: RwLock::new(HashMap::new()),
            version_times: RwLock::new(HashMap::new()),
            latest_version: RwLock::new(None),
            version_watch: tx,
            retention_keep_last,
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

    pub fn has_encoded_overlay(&self) -> bool {
        self.encoded_overlay_state
            .read()
            .expect("mutex poisoned")
            .is_some()
    }

    pub fn append_encoded_overlay_batch<I>(
        &self,
        version: u64,
        deltas: I,
    ) -> EncodedOverlayApplyStats
    where
        I: IntoIterator<Item = (Vec<u8>, i64)>,
    {
        let apply_start = Instant::now();
        let mut batches = Vec::new();
        let mut overlay_rows = 0usize;
        let mut overlay_bytes = 0usize;
        for (key, diff) in deltas {
            if diff == 0 {
                continue;
            }
            overlay_rows = overlay_rows.saturating_add(1);
            overlay_bytes = overlay_bytes.saturating_add(key.len() + std::mem::size_of::<i64>());
            batches.push((key, diff));
        }
        let mut guard = self.encoded_overlay_state.write().expect("mutex poisoned");
        let state = guard.get_or_insert_with(|| EncodedOverlayState {
            base_version: self.dbsp_state().map(|state| state.version()).unwrap_or(0),
            ..Default::default()
        });
        if !batches.is_empty() {
            state.batches.insert(version, batches);
        }
        state.latest_version = state.latest_version.max(version);
        let stats = EncodedOverlayApplyStats {
            overlay_rows,
            overlay_bytes,
            overlay_batches: state.batches.len(),
            apply_ms: apply_start.elapsed().as_millis() as u64,
        };
        drop(guard);
        self.publish_logical_version(version as i64);
        stats
    }

    pub fn encoded_overlay_batches(
        &self,
        as_of_version: Option<u64>,
    ) -> Option<(u64, u64, Vec<(Vec<u8>, i64)>)> {
        let guard = self.encoded_overlay_state.read().expect("mutex poisoned");
        let state = guard.as_ref()?;
        let target_version = as_of_version.unwrap_or(state.latest_version);
        if target_version < state.base_version {
            return None;
        }
        let mut overlay = Vec::new();
        for (version, deltas) in &state.batches {
            if *version > target_version {
                break;
            }
            overlay.extend(deltas.iter().cloned());
        }
        Some((state.base_version, target_version, overlay))
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
        self.record_latest_version(version);
        self.prune_versions();
        tracing::debug!(
            view = %self.name,
            version,
            namespace = %namespace,
            "materialized view version recorded"
        );
    }

    pub fn publish_logical_version(&self, version: i64) {
        self.record_latest_version(version);
        tracing::debug!(
            view = %self.name,
            version,
            "materialized view logical version recorded"
        );
    }

    pub fn latest_version(&self) -> Option<i64> {
        *self
            .latest_version
            .read()
            .expect("materialized view version lock poisoned")
    }

    pub fn next_version_after(&self, version: i64) -> Option<i64> {
        self.published_versions
            .read()
            .expect("materialized view versions lock poisoned")
            .iter()
            .copied()
            .filter(|candidate| *candidate > version)
            .min()
    }

    pub fn version_time(&self, version: i64) -> Option<i64> {
        self.version_times
            .read()
            .expect("materialized view version lock poisoned")
            .get(&version)
            .copied()
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

    fn prune_versions(&self) {
        let Some(keep_last) = self.retention_keep_last else {
            return;
        };
        if keep_last == 0 {
            return;
        }
        let mut guard = self
            .versions
            .write()
            .expect("materialized view versions lock poisoned");
        if guard.len() <= keep_last {
            return;
        }
        let mut versions: Vec<i64> = guard.keys().copied().collect();
        versions.sort_unstable();
        let remove_count = versions.len().saturating_sub(keep_last);
        if remove_count == 0 {
            return;
        }
        let mut times = self
            .version_times
            .write()
            .expect("materialized view versions lock poisoned");
        for version in versions.into_iter().take(remove_count) {
            guard.remove(&version);
            times.remove(&version);
        }
    }

    fn record_latest_version(&self, version: i64) {
        let version_time = self
            .watermark()
            .map(watermark_to_micros)
            .unwrap_or_else(current_time_micros);
        {
            let mut guard = self
                .published_versions
                .write()
                .expect("materialized view versions lock poisoned");
            guard.insert(version);
        }
        {
            let mut guard = self
                .version_times
                .write()
                .expect("materialized view versions lock poisoned");
            guard.insert(version, version_time);
        }
        {
            let mut guard = self
                .latest_version
                .write()
                .expect("materialized view version lock poisoned");
            *guard = Some(version);
        }
        let _ = self.version_watch.send_replace(Some(version));
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EncodedOverlayApplyStats {
    pub overlay_rows: usize,
    pub overlay_bytes: usize,
    pub overlay_batches: usize,
    pub apply_ms: u64,
}

#[derive(Clone, Debug, Default)]
struct EncodedOverlayState {
    base_version: u64,
    latest_version: u64,
    batches: BTreeMap<u64, Vec<(Vec<u8>, i64)>>,
}

fn current_time_micros() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros().try_into().unwrap_or(0),
        Err(_) => 0,
    }
}

fn watermark_to_micros(watermark: Timestamp) -> i64 {
    let micros = watermark.saturating_mul(1_000);
    i64::try_from(micros).unwrap_or(i64::MAX)
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
    use dbsp::handles::ZSetHandle;

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

    #[test]
    fn retention_prunes_old_versions() {
        let registry = MaterializedViewRegistry::new_with_retention(Some(2));
        let view = registry.register("mv_retained");

        view.publish_version(
            1,
            ZSetHandle {
                ns: "mv_retained".to_string(),
                version: 1,
            },
        );
        view.publish_version(
            2,
            ZSetHandle {
                ns: "mv_retained".to_string(),
                version: 2,
            },
        );
        view.publish_version(
            3,
            ZSetHandle {
                ns: "mv_retained".to_string(),
                version: 3,
            },
        );

        assert!(view.handle_for_version(1).is_none());
        assert!(view.handle_for_version(2).is_some());
        assert!(view.handle_for_version(3).is_some());
    }
}
