use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::stream_types::{Diff, EncodedDeltaBatch, EncodedRow, Timestamp};
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::LogicalWorkSnapshot;
use dbsp::handles::ZSetHandle;
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
    state: RwLock<EncodedStateMap>,
    state_row_count: RwLock<i64>,
    published_row_count: RwLock<i64>,
    state_row_count_version: RwLock<Option<i64>>,
    staged_row_count_versions: RwLock<BTreeMap<i64, i64>>,
    state_authoritative: RwLock<bool>,
    watermark: RwLock<Option<Timestamp>>,
    arrow_snapshots: RwLock<BTreeMap<i64, Arc<Vec<RecordBatch>>>>,
    arrow_deltas: RwLock<BTreeMap<i64, Arc<Vec<RecordBatch>>>>,
    dbsp_state: RwLock<Option<DbspPersistedState>>,
    encoded_overlay_state: RwLock<Option<EncodedOverlayState>>,
    published_versions: RwLock<BTreeSet<i64>>,
    versions: RwLock<HashMap<i64, ZSetHandle>>,
    version_times: RwLock<HashMap<i64, i64>>,
    logical_work: RwLock<BTreeMap<i64, LogicalWorkSnapshot>>,
    latest_version: RwLock<Option<i64>>,
    version_watch: watch::Sender<Option<i64>>,
    commit_visibility_barrier: RwLock<bool>,
    retention_keep_last: Option<usize>,
}

pub type EncodedStateMap = AHashMap<EncodedRow, Diff>;
type EncodedOverlayRows = Vec<(Vec<u8>, i64)>;

impl MaterializedViewHandle {
    fn new(name: String, retention_keep_last: Option<usize>) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self {
            name,
            state: RwLock::new(EncodedStateMap::default()),
            state_row_count: RwLock::new(0),
            published_row_count: RwLock::new(0),
            state_row_count_version: RwLock::new(None),
            staged_row_count_versions: RwLock::new(BTreeMap::new()),
            state_authoritative: RwLock::new(false),
            watermark: RwLock::new(None),
            arrow_snapshots: RwLock::new(BTreeMap::new()),
            arrow_deltas: RwLock::new(BTreeMap::new()),
            dbsp_state: RwLock::new(None),
            encoded_overlay_state: RwLock::new(None),
            published_versions: RwLock::new(BTreeSet::new()),
            versions: RwLock::new(HashMap::new()),
            version_times: RwLock::new(HashMap::new()),
            logical_work: RwLock::new(BTreeMap::new()),
            latest_version: RwLock::new(None),
            version_watch: tx,
            commit_visibility_barrier: RwLock::new(true),
            retention_keep_last,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn apply_encoded_row(&self, row: &[u8], diff: Diff) {
        self.apply_encoded(row, diff);
    }

    fn apply_encoded(&self, key: &[u8], diff: Diff) {
        if diff == 0 {
            return;
        }

        let mut guard = write_lock(&self.state, "materialized view encoded state");
        let mut row_count = write_lock(&self.state_row_count, "materialized view row count");
        Self::apply_encoded_locked(&mut guard, &mut row_count, key, diff);
    }

    fn apply_encoded_locked(
        state: &mut EncodedStateMap,
        row_count: &mut i64,
        key: &[u8],
        diff: Diff,
    ) {
        if diff == 0 {
            return;
        }

        let previous = state.get(key).copied().unwrap_or(0);
        let next = previous.saturating_add(diff);
        if next == 0 {
            state.remove(key);
        } else if let Some(current) = state.get_mut(key) {
            *current = next;
        } else {
            state.insert(key.to_vec(), next);
        }
        let previous_rows = previous.max(0);
        let next_rows = next.max(0);
        *row_count = row_count
            .saturating_add(next_rows)
            .saturating_sub(previous_rows)
            .max(0);
    }

    pub fn update_watermark(&self, watermark: Timestamp) {
        *write_lock(&self.watermark, "materialized view watermark") = Some(watermark);
    }

    pub fn watermark(&self) -> Option<Timestamp> {
        *read_lock(&self.watermark, "materialized view watermark")
    }

    pub fn snapshot_encoded(&self) -> EncodedStateMap {
        read_lock(&self.state, "materialized view encoded state").clone()
    }

    pub fn mark_state_authoritative(&self) {
        *write_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) = true;
    }

    pub fn mark_state_non_authoritative(&self) {
        *write_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) = false;
        write_lock(
            &self.staged_row_count_versions,
            "materialized view staged row count versions",
        )
        .clear();
        *write_lock(
            &self.published_row_count,
            "materialized view published row count",
        ) = 0;
        *write_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        ) = None;
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
        write_lock(
            &self.staged_row_count_versions,
            "materialized view staged row count versions",
        )
        .retain(|candidate, _| *candidate > version);
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

    pub fn seed_cached_row_count_if_latest(&self, version: u64, row_count: usize) -> bool {
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
        write_lock(
            &self.staged_row_count_versions,
            "materialized view staged row count versions",
        )
        .retain(|candidate, _| *candidate > version);
        *write_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        ) = Some(version);
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

    pub fn advance_authoritative_row_count_version(&self, version: u64) {
        if !*read_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) {
            return;
        }
        let Ok(version) = i64::try_from(version) else {
            return;
        };
        let row_count = *read_lock(&self.state_row_count, "materialized view row count");
        *write_lock(
            &self.published_row_count,
            "materialized view published row count",
        ) = row_count;
        *write_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        ) = Some(version);
        write_lock(
            &self.staged_row_count_versions,
            "materialized view staged row count versions",
        )
        .retain(|candidate, _| *candidate > version);
    }

    pub fn apply_encoded_state_batch(&self, version: u64, deltas: &[(Vec<u8>, i64)]) -> Result<()> {
        self.apply_encoded_state_batch_inner(version, deltas, false)
    }

    pub fn apply_consolidated_encoded_state_batch(
        &self,
        version: u64,
        deltas: &[(Vec<u8>, i64)],
    ) -> Result<()> {
        self.apply_encoded_state_batch_inner(version, deltas, true)
    }

    fn apply_encoded_state_batch_inner(
        &self,
        version: u64,
        deltas: &[(Vec<u8>, i64)],
        deltas_consolidated: bool,
    ) -> Result<()> {
        if !*read_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) {
            return Ok(());
        }
        {
            let mut state = write_lock(&self.state, "materialized view encoded state");
            let mut row_count = write_lock(&self.state_row_count, "materialized view row count");
            if deltas_consolidated {
                for (key, diff) in deltas {
                    Self::apply_encoded_locked(&mut state, &mut row_count, key, *diff);
                }
            } else {
                let mut merged = HashMap::<&[u8], i64>::with_capacity(deltas.len());
                for (key, diff) in deltas {
                    if *diff != 0 {
                        *merged.entry(key.as_slice()).or_insert(0) += *diff;
                    }
                }
                for (key, diff) in merged {
                    Self::apply_encoded_locked(&mut state, &mut row_count, key, diff);
                }
            }
        }
        self.stage_authoritative_row_count_version(version);
        Ok(())
    }

    pub fn stage_authoritative_row_count_version(&self, version: u64) {
        if !*read_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) {
            return;
        }
        let Ok(version) = i64::try_from(version) else {
            return;
        };
        let row_count = *read_lock(&self.state_row_count, "materialized view row count");
        write_lock(
            &self.staged_row_count_versions,
            "materialized view staged row count versions",
        )
        .insert(version, row_count);
        self.promote_staged_row_count_if_visible(version);
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
        self.prune_versions();
        self.prune_arrow_versions();
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

    pub fn latest_arrow_snapshot(&self) -> Option<(i64, Arc<Vec<RecordBatch>>)> {
        read_lock(&self.arrow_snapshots, "materialized view arrow snapshots")
            .iter()
            .next_back()
            .map(|(version, batches)| (*version, Arc::clone(batches)))
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

    pub fn has_encoded_overlay(&self) -> bool {
        read_lock(
            &self.encoded_overlay_state,
            "materialized view encoded overlay state",
        )
        .is_some()
    }

    pub fn append_shared_encoded_overlay_batch(
        &self,
        version: u64,
        deltas: EncodedDeltaBatch,
    ) -> EncodedOverlayApplyStats {
        let apply_start = Instant::now();
        let overlay_rows = deltas.iter().filter(|(_, diff)| *diff != 0).count();
        let overlay_bytes = deltas
            .iter()
            .filter(|(_, diff)| *diff != 0)
            .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
            .sum();
        let mut guard = write_lock(
            &self.encoded_overlay_state,
            "materialized view encoded overlay state",
        );
        let state = guard.get_or_insert_with(|| EncodedOverlayState {
            base_version: self
                .dbsp_state()
                .map(|state| state.logical_version())
                .unwrap_or(0),
            ..Default::default()
        });
        if overlay_rows > 0 {
            state.batches.insert(version, deltas);
        }
        state.latest_version = state.latest_version.max(version);
        let stats = EncodedOverlayApplyStats {
            overlay_rows,
            overlay_bytes,
            overlay_batches: state.batches.len(),
            apply_ms: apply_start.elapsed().as_millis() as u64,
        };
        drop(guard);
        stats
    }

    pub fn append_encoded_overlay_batch<I>(
        &self,
        version: u64,
        deltas: I,
    ) -> EncodedOverlayApplyStats
    where
        I: IntoIterator<Item = (Vec<u8>, i64)>,
    {
        let stats = self
            .append_shared_encoded_overlay_batch(version, Arc::new(deltas.into_iter().collect()));
        self.publish_logical_version(version as i64);
        stats
    }

    pub fn encoded_overlay_batches(
        &self,
        as_of_version: Option<u64>,
    ) -> Option<(u64, u64, EncodedOverlayRows)> {
        let guard = read_lock(
            &self.encoded_overlay_state,
            "materialized view encoded overlay state",
        );
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

    pub fn encoded_overlay_merged_delta(
        &self,
        as_of_version: Option<u64>,
    ) -> Option<(u64, u64, HashMap<Vec<u8>, i64>)> {
        let guard = read_lock(
            &self.encoded_overlay_state,
            "materialized view encoded overlay state",
        );
        let state = guard.as_ref()?;
        let target_version = as_of_version.unwrap_or(state.latest_version);
        if target_version < state.base_version {
            return None;
        }
        let mut overlay = HashMap::new();
        for (version, deltas) in &state.batches {
            if *version > target_version {
                break;
            }
            for (key, diff) in deltas.iter() {
                if *diff == 0 {
                    continue;
                }
                let entry = overlay.entry(key.clone()).or_insert(0);
                *entry += *diff;
                if *entry == 0 {
                    overlay.remove(key);
                }
            }
        }
        Some((state.base_version, target_version, overlay))
    }

    pub fn encoded_overlay_batch(&self, version: u64) -> Option<Vec<(Vec<u8>, i64)>> {
        let guard = read_lock(
            &self.encoded_overlay_state,
            "materialized view encoded overlay state",
        );
        let state = guard.as_ref()?;
        state
            .batches
            .get(&version)
            .map(|deltas| deltas.iter().cloned().collect())
    }

    pub fn compact_encoded_overlay_up_to(
        &self,
        base_version: u64,
    ) -> EncodedOverlayCompactionStats {
        let mut guard = write_lock(
            &self.encoded_overlay_state,
            "materialized view encoded overlay state",
        );
        let Some(state) = guard.as_mut() else {
            return EncodedOverlayCompactionStats::default();
        };
        let removed_batches = state
            .batches
            .keys()
            .take_while(|version| **version <= base_version)
            .count();
        let removed_versions: Vec<u64> = state
            .batches
            .keys()
            .copied()
            .take_while(|version| *version <= base_version)
            .collect();
        for version in removed_versions {
            state.batches.remove(&version);
        }
        state.base_version = state.base_version.max(base_version);
        let remaining_rows = state.batches.values().map(|deltas| deltas.len()).sum();
        EncodedOverlayCompactionStats {
            removed_batches,
            remaining_batches: state.batches.len(),
            remaining_rows,
        }
    }

    pub fn publish_version(&self, version: i64, handle: ZSetHandle) {
        let namespace = handle.ns.clone();
        {
            let mut guard = write_lock(&self.versions, "materialized view versions");
            guard.insert(version, handle);
        }
        self.record_latest_version(version);
        self.promote_staged_row_count_if_visible(version);
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
        self.promote_staged_row_count_if_visible(version);
        tracing::debug!(
            view = %self.name,
            version,
            "materialized view logical version recorded"
        );
    }

    pub fn latest_version(&self) -> Option<i64> {
        *read_lock(&self.latest_version, "materialized view latest version")
    }

    pub fn next_version_after(&self, version: i64) -> Option<i64> {
        read_lock(
            &self.published_versions,
            "materialized view published versions",
        )
        .iter()
        .copied()
        .filter(|candidate| *candidate > version)
        .min()
    }

    pub fn is_version_published(&self, version: i64) -> bool {
        read_lock(
            &self.published_versions,
            "materialized view published versions",
        )
        .contains(&version)
    }

    pub fn version_time(&self, version: i64) -> Option<i64> {
        read_lock(&self.version_times, "materialized view version times")
            .get(&version)
            .copied()
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
        self.version_watch.subscribe()
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

    fn prune_versions(&self) {
        let Some(keep_last) = self.retention_keep_last else {
            return;
        };
        if keep_last == 0 {
            return;
        }
        let mut guard = write_lock(&self.versions, "materialized view versions");
        if guard.len() <= keep_last {
            return;
        }
        let mut versions: Vec<i64> = guard.keys().copied().collect();
        versions.sort_unstable();
        let remove_count = versions.len().saturating_sub(keep_last);
        if remove_count == 0 {
            return;
        }
        let mut times = write_lock(&self.version_times, "materialized view version times");
        for version in versions.into_iter().take(remove_count) {
            guard.remove(&version);
            times.remove(&version);
            write_lock(&self.logical_work, "materialized view logical work").remove(&version);
        }
    }

    fn prune_arrow_versions(&self) {
        let Some(keep_last) = self.retention_keep_last else {
            return;
        };
        if keep_last == 0 {
            return;
        }
        prune_btree_to_last_n(
            &mut write_lock(&self.arrow_snapshots, "materialized view arrow snapshots"),
            keep_last,
        );
        prune_btree_to_last_n(
            &mut write_lock(&self.arrow_deltas, "materialized view arrow deltas"),
            keep_last,
        );
    }

    fn record_latest_version(&self, version: i64) {
        let version_time = self
            .watermark()
            .map(watermark_to_micros)
            .unwrap_or_else(current_time_micros);
        {
            let mut guard = write_lock(
                &self.published_versions,
                "materialized view published versions",
            );
            guard.insert(version);
        }
        {
            let mut guard = write_lock(&self.version_times, "materialized view version times");
            guard.insert(version, version_time);
        }
        {
            let mut guard = write_lock(&self.latest_version, "materialized view latest version");
            *guard = Some(version);
        }
        let _ = self.version_watch.send_replace(Some(version));
    }

    fn promote_staged_row_count_if_visible(&self, version: i64) {
        if !*read_lock(
            &self.state_authoritative,
            "materialized view authoritative state flag",
        ) {
            return;
        }
        if self.latest_version() != Some(version) {
            return;
        }
        let row_count = {
            let mut staged = write_lock(
                &self.staged_row_count_versions,
                "materialized view staged row count versions",
            );
            let Some(row_count) = staged.get(&version).copied() else {
                return;
            };
            staged.retain(|candidate, _| *candidate > version);
            row_count
        };
        *write_lock(
            &self.published_row_count,
            "materialized view published row count",
        ) = row_count;
        *write_lock(
            &self.state_row_count_version,
            "materialized view state row count version",
        ) = Some(version);
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EncodedOverlayApplyStats {
    pub overlay_rows: usize,
    pub overlay_bytes: usize,
    pub overlay_batches: usize,
    pub apply_ms: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EncodedOverlayCompactionStats {
    pub removed_batches: usize,
    pub remaining_batches: usize,
    pub remaining_rows: usize,
}

#[derive(Clone, Debug, Default)]
struct EncodedOverlayState {
    base_version: u64,
    latest_version: u64,
    batches: BTreeMap<u64, EncodedDeltaBatch>,
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

fn prune_btree_to_last_n<T>(map: &mut BTreeMap<i64, T>, keep_last: usize) {
    if map.len() <= keep_last {
        return;
    }
    let remove_count = map.len().saturating_sub(keep_last);
    let remove_versions = map.keys().copied().take(remove_count).collect::<Vec<_>>();
    for version in remove_versions {
        map.remove(&version);
    }
}

impl fmt::Debug for MaterializedViewHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_len = read_lock(&self.state, "materialized view encoded state").len();
        let latest = *read_lock(&self.latest_version, "materialized view latest version");
        f.debug_struct("MaterializedViewHandle")
            .field("name", &self.name)
            .field("state_len", &state_len)
            .field("latest_version", &latest)
            .finish()
    }
}

#[cfg(test)]
mod tests;
