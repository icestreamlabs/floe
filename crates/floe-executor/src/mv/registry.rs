use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::stream_types::Timestamp;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::LogicalWorkSnapshot;
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use tokio::sync::watch;
use tracing::field;

pub use super::dbsp_state::DbspPersistedState;

fn read_lock<'a, T>(lock: &'a RwLock<T>, label: &str) -> RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(%label, "rwlock read was poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

fn write_lock<'a, T>(lock: &'a RwLock<T>, label: &str) -> RwLockWriteGuard<'a, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(%label, "rwlock write was poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

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
        let mut guard = write_lock(&self.views, "materialized view registry views");
        let name = name.into();
        guard
            .entry(name.clone())
            .or_insert_with(|| {
                Arc::new(MaterializedViewHandle::new(name, self.retention_keep_last))
            })
            .clone()
    }

    pub fn get(&self, name: &str) -> Option<Arc<MaterializedViewHandle>> {
        read_lock(&self.views, "materialized view registry views")
            .get(name)
            .cloned()
    }

    pub fn handles(&self) -> Vec<Arc<MaterializedViewHandle>> {
        read_lock(&self.views, "materialized view registry views")
            .values()
            .cloned()
            .collect()
    }

    pub fn update_watermark_all(&self, watermark: Timestamp) {
        let views: Vec<Arc<MaterializedViewHandle>> =
            read_lock(&self.views, "materialized view registry views")
                .values()
                .cloned()
                .collect();
        for view in views {
            view.update_watermark(watermark);
        }
    }

    pub fn set_schema(&self, name: impl Into<String>, schema: SchemaRef) {
        write_lock(&self.schemas, "materialized view registry schemas").insert(name.into(), schema);
    }

    pub fn schema(&self, name: &str) -> Option<SchemaRef> {
        read_lock(&self.schemas, "materialized view registry schemas")
            .get(name)
            .cloned()
    }
}

pub struct MaterializedViewHandle {
    name: String,
    state_row_count: RwLock<i64>,
    published_row_count: RwLock<i64>,
    state_row_count_version: RwLock<Option<i64>>,
    state_authoritative: RwLock<bool>,
    watermark: RwLock<Option<Timestamp>>,
    arrow_snapshots: RwLock<BTreeMap<i64, Arc<Vec<RecordBatch>>>>,
    arrow_deltas: RwLock<BTreeMap<i64, Arc<Vec<RecordBatch>>>>,
    dbsp_state: RwLock<Option<DbspPersistedState>>,
    columnar_storage: RwLock<Option<ColumnarMaterializedViewStorage>>,
    published_versions: PublishedVersionIndex,
    versions: RwLock<HashMap<i64, ZSetHandle>>,
    logical_work: RwLock<BTreeMap<i64, LogicalWorkSnapshot>>,
    commit_visibility_barrier: RwLock<bool>,
    retention_keep_last: Option<usize>,
}

#[derive(Clone)]
pub struct ColumnarMaterializedViewStorage {
    table: Arc<dyn KeyValueTable>,
    schema: SchemaRef,
}

impl fmt::Debug for ColumnarMaterializedViewStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColumnarMaterializedViewStorage")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ColumnarMaterializedViewStorage {
    pub fn new(table: Arc<dyn KeyValueTable>, schema: SchemaRef) -> Self {
        Self { table, schema }
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        Arc::clone(&self.table)
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[derive(Debug)]
struct PublishedVersionIndex {
    versions: RwLock<BTreeSet<i64>>,
    version_times: RwLock<HashMap<i64, i64>>,
    latest_version: RwLock<Option<i64>>,
    version_watch: watch::Sender<Option<i64>>,
}

impl PublishedVersionIndex {
    fn new(version_watch: watch::Sender<Option<i64>>) -> Self {
        Self {
            versions: RwLock::new(BTreeSet::new()),
            version_times: RwLock::new(HashMap::new()),
            latest_version: RwLock::new(None),
            version_watch,
        }
    }

    fn record(&self, version: i64, version_time: i64) {
        write_lock(&self.versions, "materialized view published versions").insert(version);
        write_lock(&self.version_times, "materialized view version times")
            .insert(version, version_time);
        *write_lock(&self.latest_version, "materialized view latest version") = Some(version);
        let _ = self.version_watch.send_replace(Some(version));
    }

    fn latest(&self) -> Option<i64> {
        *read_lock(&self.latest_version, "materialized view latest version")
    }

    fn next_after(&self, version: i64) -> Option<i64> {
        read_lock(&self.versions, "materialized view published versions")
            .range((Excluded(version), Unbounded))
            .next()
            .copied()
    }

    fn contains(&self, version: i64) -> bool {
        read_lock(&self.versions, "materialized view published versions").contains(&version)
    }

    fn version_time(&self, version: i64) -> Option<i64> {
        read_lock(&self.version_times, "materialized view version times")
            .get(&version)
            .copied()
    }

    fn subscribe(&self) -> watch::Receiver<Option<i64>> {
        self.version_watch.subscribe()
    }

    fn prune_to_last(&self, keep_last: usize) -> Vec<i64> {
        let mut versions = write_lock(&self.versions, "materialized view published versions");
        if versions.len() <= keep_last {
            return Vec::new();
        }
        let remove_count = versions.len().saturating_sub(keep_last);
        let remove_versions = versions
            .iter()
            .copied()
            .take(remove_count)
            .collect::<Vec<_>>();
        for version in &remove_versions {
            versions.remove(version);
        }
        let mut times = write_lock(&self.version_times, "materialized view version times");
        for version in &remove_versions {
            times.remove(version);
        }
        remove_versions
    }
}

impl MaterializedViewHandle {
    fn new(name: String, retention_keep_last: Option<usize>) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self {
            name,
            state_row_count: RwLock::new(0),
            published_row_count: RwLock::new(0),
            state_row_count_version: RwLock::new(None),
            state_authoritative: RwLock::new(false),
            watermark: RwLock::new(None),
            arrow_snapshots: RwLock::new(BTreeMap::new()),
            arrow_deltas: RwLock::new(BTreeMap::new()),
            dbsp_state: RwLock::new(None),
            columnar_storage: RwLock::new(None),
            published_versions: PublishedVersionIndex::new(tx),
            versions: RwLock::new(HashMap::new()),
            logical_work: RwLock::new(BTreeMap::new()),
            commit_visibility_barrier: RwLock::new(true),
            retention_keep_last,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn update_watermark(&self, watermark: Timestamp) {
        *write_lock(&self.watermark, "materialized view watermark") = Some(watermark);
    }

    pub fn watermark(&self) -> Option<Timestamp> {
        *read_lock(&self.watermark, "materialized view watermark")
    }

    pub fn seed_authoritative_row_count_if_latest(&self, version: u64, row_count: usize) -> bool {
        let Ok(version) = i64::try_from(version) else {
            return false;
        };
        if self.latest_version() != Some(version) {
            return false;
        }
        let row_count = i64::try_from(row_count).unwrap_or(i64::MAX);
        *write_lock(&self.state_row_count, "materialized view row count") = row_count;
        *write_lock(
            &self.published_row_count,
            "materialized view published row count",
        ) = row_count;
        *write_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        ) = Some(version);
        *write_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) = true;
        true
    }

    pub fn authoritative_row_count(&self) -> Option<usize> {
        if !*read_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) {
            return None;
        }
        Some(
            usize::try_from(*read_lock(
                &self.state_row_count,
                "materialized view row count",
            ))
            .unwrap_or(0),
        )
    }

    pub fn authoritative_row_count_for(&self, version: u64) -> Option<usize> {
        let Ok(version) = i64::try_from(version) else {
            return None;
        };
        if read_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        )
        .as_ref()
            != Some(&version)
        {
            return None;
        }
        Some(
            usize::try_from(*read_lock(
                &self.published_row_count,
                "materialized view published row count",
            ))
            .unwrap_or(0),
        )
    }

    pub fn publish_arrow_version(
        &self,
        version: i64,
        snapshot: Vec<RecordBatch>,
        delta: Vec<RecordBatch>,
    ) {
        let row_count = snapshot
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
            .try_into()
            .unwrap_or(i64::MAX);
        {
            let mut snapshots =
                write_lock(&self.arrow_snapshots, "materialized view arrow snapshots");
            snapshots.insert(version, Arc::new(snapshot));
        }
        {
            let mut deltas = write_lock(&self.arrow_deltas, "materialized view arrow deltas");
            deltas.insert(version, Arc::new(delta));
        }
        *write_lock(&self.state_row_count, "materialized view row count") = row_count;
        *write_lock(
            &self.published_row_count,
            "materialized view published row count",
        ) = row_count;
        *write_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        ) = Some(version);
        self.record_latest_version(version);
        self.prune_retained_versions();
        tracing::debug!(
            view = %self.name,
            version,
            rows = row_count,
            "materialized view Arrow version recorded"
        );
    }

    pub fn arrow_snapshot_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>> {
        read_lock(&self.arrow_snapshots, "materialized view arrow snapshots")
            .get(&version)
            .cloned()
    }

    pub fn arrow_delta_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>> {
        read_lock(&self.arrow_deltas, "materialized view arrow deltas")
            .get(&version)
            .cloned()
    }

    pub fn arrow_row_count_for(&self, version: i64) -> Option<usize> {
        self.arrow_snapshot_for(version)
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
    }

    pub fn set_columnar_storage(&self, storage: ColumnarMaterializedViewStorage) {
        *write_lock(&self.columnar_storage, "materialized view columnar storage") = Some(storage);
    }

    pub fn columnar_storage(&self) -> Option<ColumnarMaterializedViewStorage> {
        read_lock(&self.columnar_storage, "materialized view columnar storage").clone()
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
        *write_lock(&self.dbsp_state, "materialized view DBSP state") = Some(state);
    }

    pub fn dbsp_state(&self) -> Option<DbspPersistedState> {
        read_lock(&self.dbsp_state, "materialized view DBSP state").clone()
    }

    pub fn publish_version(&self, version: i64, handle: ZSetHandle) {
        let namespace = handle.ns.clone();
        {
            let mut guard = write_lock(&self.versions, "materialized view versions");
            guard.insert(version, handle);
        }
        self.record_latest_version(version);
        self.prune_retained_versions();
        tracing::debug!(
            view = %self.name,
            version,
            namespace = %namespace,
            "materialized view version recorded"
        );
    }

    pub fn publish_columnar_version(
        &self,
        version: i64,
        handle: ZSetHandle,
        storage: ColumnarMaterializedViewStorage,
        row_count: usize,
        delta: Vec<RecordBatch>,
    ) {
        self.set_columnar_storage(storage);
        {
            let mut guard = write_lock(&self.versions, "materialized view versions");
            guard.insert(version, handle.clone());
        }
        {
            let mut deltas = write_lock(&self.arrow_deltas, "materialized view arrow deltas");
            deltas.insert(version, Arc::new(delta));
        }
        let row_count = i64::try_from(row_count).unwrap_or(i64::MAX);
        *write_lock(&self.state_row_count, "materialized view row count") = row_count;
        *write_lock(
            &self.published_row_count,
            "materialized view published row count",
        ) = row_count;
        *write_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        ) = Some(version);
        *write_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) = true;
        self.record_latest_version(version);
        self.prune_retained_versions();
        tracing::debug!(
            view = %self.name,
            version,
            namespace = %handle.ns,
            rows = row_count,
            "materialized view columnar version recorded"
        );
    }

    pub fn publish_logical_version(&self, version: i64) {
        self.record_latest_version(version);
        self.prune_retained_versions();
        tracing::debug!(
            view = %self.name,
            version,
            "materialized view logical version recorded"
        );
    }

    pub fn latest_version(&self) -> Option<i64> {
        self.published_versions.latest()
    }

    pub fn next_version_after(&self, version: i64) -> Option<i64> {
        self.published_versions.next_after(version)
    }

    pub fn is_version_published(&self, version: i64) -> bool {
        self.published_versions.contains(version)
    }

    pub fn version_time(&self, version: i64) -> Option<i64> {
        self.published_versions.version_time(version)
    }

    pub fn record_logical_work(&self, version: i64, work: LogicalWorkSnapshot) {
        let mut guard = write_lock(&self.logical_work, "materialized view logical work");
        guard.insert(version, work);
        if let Some(keep_last) = self.retention_keep_last
            && keep_last > 0
            && guard.len() > keep_last
        {
            let remove_count = guard.len().saturating_sub(keep_last);
            let remove_versions = guard.keys().copied().take(remove_count).collect::<Vec<_>>();
            for version in remove_versions {
                guard.remove(&version);
            }
        }
    }

    pub fn logical_work_for(&self, version: i64) -> Option<LogicalWorkSnapshot> {
        read_lock(&self.logical_work, "materialized view logical work")
            .get(&version)
            .copied()
    }

    pub fn latest_logical_work(&self) -> Option<(i64, LogicalWorkSnapshot)> {
        read_lock(&self.logical_work, "materialized view logical work")
            .iter()
            .next_back()
            .map(|(version, work)| (*version, *work))
    }

    pub fn version_watch(&self) -> watch::Receiver<Option<i64>> {
        self.published_versions.subscribe()
    }

    pub fn set_commit_visibility_barrier_enabled(&self, enabled: bool) {
        *write_lock(
            &self.commit_visibility_barrier,
            "materialized view visibility barrier",
        ) = enabled;
    }

    pub fn commit_visibility_barrier_enabled(&self) -> bool {
        *read_lock(
            &self.commit_visibility_barrier,
            "materialized view visibility barrier",
        )
    }

    pub fn handle_for_version(&self, version: i64) -> Option<ZSetHandle> {
        read_lock(&self.versions, "materialized view versions")
            .get(&version)
            .cloned()
    }

    pub fn handle_at_or_before_version(&self, version: i64) -> Option<ZSetHandle> {
        read_lock(&self.versions, "materialized view versions")
            .iter()
            .filter(|(candidate, _)| **candidate <= version)
            .max_by_key(|(candidate, _)| *candidate)
            .map(|(_, handle)| handle.clone())
    }

    fn prune_retained_versions(&self) {
        let Some(keep_last) = self.retention_keep_last else {
            return;
        };
        if keep_last == 0 {
            return;
        }
        let removed_versions = self.published_versions.prune_to_last(keep_last);
        if removed_versions.is_empty() {
            return;
        }
        let mut handles = write_lock(&self.versions, "materialized view versions");
        let mut logical_work = write_lock(&self.logical_work, "materialized view logical work");
        let mut snapshots = write_lock(&self.arrow_snapshots, "materialized view arrow snapshots");
        let mut deltas = write_lock(&self.arrow_deltas, "materialized view arrow deltas");
        for version in removed_versions {
            handles.remove(&version);
            logical_work.remove(&version);
            snapshots.remove(&version);
            deltas.remove(&version);
        }
    }

    fn record_latest_version(&self, version: i64) {
        let version_time = self
            .watermark()
            .map(watermark_to_micros)
            .unwrap_or_else(current_time_micros);
        self.published_versions.record(version, version_time);
    }
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
        let latest = self.latest_version();
        f.debug_struct("MaterializedViewHandle")
            .field("name", &self.name)
            .field("latest_version", &latest)
            .finish()
    }
}

#[cfg(test)]
mod tests;
