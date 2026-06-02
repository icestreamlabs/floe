use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::stream_types::{Diff, EncodedDeltaBatch, EncodedRow, Timestamp};
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::LogicalWorkSnapshot;
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

        let mut guard = self.state.write().expect("mutex poisoned");
        let mut row_count = self.state_row_count.write().expect("mutex poisoned");
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
        *self.watermark.write().expect("mutex poisoned") = Some(watermark);
    }

    pub fn watermark(&self) -> Option<Timestamp> {
        *self.watermark.read().expect("mutex poisoned")
    }

    pub fn snapshot_encoded(&self) -> EncodedStateMap {
        self.state.read().expect("mutex poisoned").clone()
    }

    pub fn mark_state_authoritative(&self) {
        *self.state_authoritative.write().expect("mutex poisoned") = true;
    }

    pub fn mark_state_non_authoritative(&self) {
        *self.state_authoritative.write().expect("mutex poisoned") = false;
        self.staged_row_count_versions
            .write()
            .expect("mutex poisoned")
            .clear();
        *self.published_row_count.write().expect("mutex poisoned") = 0;
        *self
            .state_row_count_version
            .write()
            .expect("mutex poisoned") = None;
    }

    pub fn seed_authoritative_row_count_if_latest(&self, version: u64, row_count: usize) -> bool {
        let Ok(version) = i64::try_from(version) else {
            return false;
        };
        if self.latest_version() != Some(version) {
            return false;
        }
        let row_count = i64::try_from(row_count).unwrap_or(i64::MAX);
        *self.state_row_count.write().expect("mutex poisoned") = row_count;
        *self.published_row_count.write().expect("mutex poisoned") = row_count;
        self.staged_row_count_versions
            .write()
            .expect("mutex poisoned")
            .retain(|candidate, _| *candidate > version);
        *self
            .state_row_count_version
            .write()
            .expect("mutex poisoned") = Some(version);
        *self.state_authoritative.write().expect("mutex poisoned") = true;
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
        *self.state_row_count.write().expect("mutex poisoned") = row_count;
        *self.published_row_count.write().expect("mutex poisoned") = row_count;
        self.staged_row_count_versions
            .write()
            .expect("mutex poisoned")
            .retain(|candidate, _| *candidate > version);
        *self
            .state_row_count_version
            .write()
            .expect("mutex poisoned") = Some(version);
        true
    }

    pub fn authoritative_row_count(&self) -> Option<usize> {
        if !*self.state_authoritative.read().expect("mutex poisoned") {
            return None;
        }
        Some(usize::try_from(*self.state_row_count.read().expect("mutex poisoned")).unwrap_or(0))
    }

    pub fn authoritative_row_count_for(&self, version: u64) -> Option<usize> {
        let Ok(version) = i64::try_from(version) else {
            return None;
        };
        if self
            .state_row_count_version
            .read()
            .expect("mutex poisoned")
            .as_ref()
            != Some(&version)
        {
            return None;
        }
        Some(
            usize::try_from(*self.published_row_count.read().expect("mutex poisoned")).unwrap_or(0),
        )
    }

    pub fn advance_authoritative_row_count_version(&self, version: u64) {
        if !*self.state_authoritative.read().expect("mutex poisoned") {
            return;
        }
        let Ok(version) = i64::try_from(version) else {
            return;
        };
        let row_count = *self.state_row_count.read().expect("mutex poisoned");
        *self.published_row_count.write().expect("mutex poisoned") = row_count;
        *self
            .state_row_count_version
            .write()
            .expect("mutex poisoned") = Some(version);
        self.staged_row_count_versions
            .write()
            .expect("mutex poisoned")
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
        if !*self.state_authoritative.read().expect("mutex poisoned") {
            return Ok(());
        }
        {
            let mut state = self.state.write().expect("mutex poisoned");
            let mut row_count = self.state_row_count.write().expect("mutex poisoned");
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
        if !*self.state_authoritative.read().expect("mutex poisoned") {
            return;
        }
        let Ok(version) = i64::try_from(version) else {
            return;
        };
        let row_count = *self.state_row_count.read().expect("mutex poisoned");
        self.staged_row_count_versions
            .write()
            .expect("mutex poisoned")
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
            let mut snapshots = self.arrow_snapshots.write().expect("mutex poisoned");
            snapshots.insert(version, Arc::new(snapshot));
        }
        {
            let mut deltas = self.arrow_deltas.write().expect("mutex poisoned");
            deltas.insert(version, Arc::new(delta));
        }
        *self.state_row_count.write().expect("mutex poisoned") = row_count;
        *self.published_row_count.write().expect("mutex poisoned") = row_count;
        *self
            .state_row_count_version
            .write()
            .expect("mutex poisoned") = Some(version);
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
        self.arrow_snapshots
            .read()
            .expect("mutex poisoned")
            .get(&version)
            .cloned()
    }

    pub fn latest_arrow_snapshot(&self) -> Option<(i64, Arc<Vec<RecordBatch>>)> {
        self.arrow_snapshots
            .read()
            .expect("mutex poisoned")
            .iter()
            .next_back()
            .map(|(version, batches)| (*version, Arc::clone(batches)))
    }

    pub fn arrow_delta_for(&self, version: i64) -> Option<Arc<Vec<RecordBatch>>> {
        self.arrow_deltas
            .read()
            .expect("mutex poisoned")
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
        let mut guard = self.encoded_overlay_state.write().expect("mutex poisoned");
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

    pub fn encoded_overlay_merged_delta(
        &self,
        as_of_version: Option<u64>,
    ) -> Option<(u64, u64, HashMap<Vec<u8>, i64>)> {
        let guard = self.encoded_overlay_state.read().expect("mutex poisoned");
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
        let guard = self.encoded_overlay_state.read().expect("mutex poisoned");
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
        let mut guard = self.encoded_overlay_state.write().expect("mutex poisoned");
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
            let mut guard = self
                .versions
                .write()
                .expect("materialized view versions lock poisoned");
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

    pub fn is_version_published(&self, version: i64) -> bool {
        self.published_versions
            .read()
            .expect("materialized view versions lock poisoned")
            .contains(&version)
    }

    pub fn version_time(&self, version: i64) -> Option<i64> {
        self.version_times
            .read()
            .expect("materialized view version lock poisoned")
            .get(&version)
            .copied()
    }

    pub fn record_logical_work(&self, version: i64, work: LogicalWorkSnapshot) {
        let mut guard = self
            .logical_work
            .write()
            .expect("materialized view logical work lock poisoned");
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
        self.logical_work
            .read()
            .expect("materialized view logical work lock poisoned")
            .get(&version)
            .copied()
    }

    pub fn latest_logical_work(&self) -> Option<(i64, LogicalWorkSnapshot)> {
        self.logical_work
            .read()
            .expect("materialized view logical work lock poisoned")
            .iter()
            .next_back()
            .map(|(version, work)| (*version, *work))
    }

    pub fn version_watch(&self) -> watch::Receiver<Option<i64>> {
        self.version_watch.subscribe()
    }

    pub fn set_commit_visibility_barrier_enabled(&self, enabled: bool) {
        *self
            .commit_visibility_barrier
            .write()
            .expect("materialized view visibility barrier lock poisoned") = enabled;
    }

    pub fn commit_visibility_barrier_enabled(&self) -> bool {
        *self
            .commit_visibility_barrier
            .read()
            .expect("materialized view visibility barrier lock poisoned")
    }

    pub fn handle_for_version(&self, version: i64) -> Option<ZSetHandle> {
        self.versions
            .read()
            .expect("materialized view versions lock poisoned")
            .get(&version)
            .cloned()
    }

    pub fn handle_at_or_before_version(&self, version: i64) -> Option<ZSetHandle> {
        self.versions
            .read()
            .expect("materialized view versions lock poisoned")
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
            self.logical_work
                .write()
                .expect("materialized view logical work lock poisoned")
                .remove(&version);
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
            &mut self.arrow_snapshots.write().expect("mutex poisoned"),
            keep_last,
        );
        prune_btree_to_last_n(
            &mut self.arrow_deltas.write().expect("mutex poisoned"),
            keep_last,
        );
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

    fn promote_staged_row_count_if_visible(&self, version: i64) {
        if !*self.state_authoritative.read().expect("mutex poisoned") {
            return;
        }
        if self.latest_version() != Some(version) {
            return;
        }
        let row_count = {
            let mut staged = self
                .staged_row_count_versions
                .write()
                .expect("mutex poisoned");
            let Some(row_count) = staged.get(&version).copied() else {
                return;
            };
            staged.retain(|candidate, _| *candidate > version);
            row_count
        };
        *self.published_row_count.write().expect("mutex poisoned") = row_count;
        *self
            .state_row_count_version
            .write()
            .expect("mutex poisoned") = Some(version);
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
    logical_version: u64,
}

impl std::fmt::Debug for DbspPersistedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbspPersistedState")
            .field("namespace", &self.namespace)
            .field("version", &self.version)
            .field("logical_version", &self.logical_version)
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
            logical_version: version,
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

    pub fn with_logical_version(mut self, logical_version: u64) -> Self {
        self.logical_version = logical_version;
        self
    }

    pub fn logical_version(&self) -> u64 {
        self.logical_version
    }
}

#[cfg(test)]
mod tests;
