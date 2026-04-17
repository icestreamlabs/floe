use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::SchemaRef;
use dbsp::RowSchema;
use dbsp::StreamRetention;
use dbsp::collections::CompactionPolicy;
use dbsp::handles::ZSetHandle;
use dbsp::stream::util::DeltaZSetHandleReader;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, DbspView};
use crate::delta_consolidation::ConsolidationMode;
use crate::materialized_view::{
    DbspPersistedState, MaterializedViewHandle, MaterializedViewRegistry,
};
use crate::metrics;
use crate::outer_stream::{TransientSourceBatch, TransientSourceHandleStream};
use crate::stream_types::EncodedDeltaBatch;
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::{DbspGraphBuilder, MvFlushCoalescingConfig, OverlaySnapshotConfig};

static MV_UPDATE_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_UPDATE_LOG_SAMPLE_EVERY: u64 = 128;
static MV_OVERLAY_APPLY_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_OVERLAY_APPLY_LOG_SAMPLE_EVERY: u64 = 16;
static MV_OVERLAY_SNAPSHOT_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_OVERLAY_SNAPSHOT_LOG_SAMPLE_EVERY: u64 = 8;
static MV_OPTIMIZATION_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_OPTIMIZATION_LOG_SAMPLE_EVERY: u64 = 64;
const MV_OPTIMIZATION_LOG_MIN_TOTAL_MS: u64 = 250;

pub(super) type DeltaTransformFn =
    dyn Fn(&[(Vec<u8>, i64)]) -> Result<Vec<(Vec<u8>, i64)>> + Send + Sync;

#[derive(Debug, Clone)]
pub(crate) struct TransientMaterializeBatch {
    pub version: i64,
    pub deltas: EncodedDeltaBatch,
}

impl From<TransientSourceBatch> for TransientMaterializeBatch {
    fn from(batch: TransientSourceBatch) -> Self {
        Self {
            version: batch.version,
            deltas: batch.deltas,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DbspGraphBuilder;

    #[test]
    fn bootstraps_authoritative_zero_only_for_zero_frontier_zero_logical_version() {
        assert!(DbspGraphBuilder::should_bootstrap_authoritative_zero(
            0, None
        ));
        assert!(DbspGraphBuilder::should_bootstrap_authoritative_zero(
            0,
            Some(0)
        ));
        assert!(!DbspGraphBuilder::should_bootstrap_authoritative_zero(
            1, None
        ));
        assert!(!DbspGraphBuilder::should_bootstrap_authoritative_zero(
            0,
            Some(1)
        ));
        assert!(!DbspGraphBuilder::should_bootstrap_authoritative_zero(
            2,
            Some(0)
        ));
    }
}

#[derive(Debug, Clone, Copy)]
enum FlushTrigger {
    MaxPendingDeltas,
    MaxPendingVersions,
    MaxPendingRows,
    MaxPendingBytes,
    MaxDelay,
    CatchupBoundary,
    Shutdown,
}

impl FlushTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::MaxPendingDeltas => "max_pending_deltas",
            Self::MaxPendingVersions => "max_pending_versions",
            Self::MaxPendingRows => "max_pending_rows",
            Self::MaxPendingBytes => "max_pending_bytes",
            Self::MaxDelay => "max_delay",
            Self::CatchupBoundary => "catchup_boundary",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Debug, Default)]
struct DeltaApplyStats {
    delta_rows: usize,
    delta_bytes: usize,
    load_ms: u64,
    transform_ms: u64,
    merge_ms: u64,
}

#[derive(Debug, Default)]
struct PendingMvFlush {
    pending_deltas: usize,
    pending_versions: usize,
    pending_rows: usize,
    pending_bytes: usize,
    total_load_ms: u64,
    total_transform_ms: u64,
    total_merge_ms: u64,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    first_enqueue_at: Option<Instant>,
}

impl PendingMvFlush {
    fn record(&mut self, ts: i64, apply: &DeltaApplyStats) {
        self.pending_deltas = self.pending_deltas.saturating_add(1);
        self.pending_versions = self.pending_versions.saturating_add(1);
        self.pending_rows = self.pending_rows.saturating_add(apply.delta_rows);
        self.pending_bytes = self.pending_bytes.saturating_add(apply.delta_bytes);
        self.total_load_ms = self.total_load_ms.saturating_add(apply.load_ms);
        self.total_transform_ms = self.total_transform_ms.saturating_add(apply.transform_ms);
        self.total_merge_ms = self.total_merge_ms.saturating_add(apply.merge_ms);
        if self.first_ts.is_none() {
            self.first_ts = Some(ts);
        }
        self.last_ts = Some(ts);
        if self.first_enqueue_at.is_none() {
            self.first_enqueue_at = Some(Instant::now());
        }
    }

    fn has_pending(&self) -> bool {
        self.pending_versions > 0
    }

    fn trigger(&self, cfg: MvFlushCoalescingConfig, now: Instant) -> Option<FlushTrigger> {
        if !self.has_pending() {
            return None;
        }
        if self.pending_deltas >= cfg.max_pending_deltas {
            return Some(FlushTrigger::MaxPendingDeltas);
        }
        if let Some(limit) = cfg.max_pending_versions
            && self.pending_versions >= limit
        {
            return Some(FlushTrigger::MaxPendingVersions);
        }
        if let Some(limit) = cfg.max_pending_rows
            && self.pending_rows >= limit
        {
            return Some(FlushTrigger::MaxPendingRows);
        }
        if let Some(limit) = cfg.max_pending_bytes
            && self.pending_bytes >= limit
        {
            return Some(FlushTrigger::MaxPendingBytes);
        }
        if let Some(delay_ms) = cfg.max_delay_ms
            && let Some(first_enqueue_at) = self.first_enqueue_at
            && now.duration_since(first_enqueue_at) >= Duration::from_millis(delay_ms)
        {
            return Some(FlushTrigger::MaxDelay);
        }
        None
    }

    fn delay_remaining(&self, cfg: MvFlushCoalescingConfig, now: Instant) -> Option<Duration> {
        if !self.has_pending() {
            return None;
        }
        let delay_ms = cfg.max_delay_ms?;
        let first_enqueue_at = self.first_enqueue_at?;
        let elapsed = now.duration_since(first_enqueue_at);
        let max_delay = Duration::from_millis(delay_ms);
        Some(max_delay.saturating_sub(elapsed))
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

struct FlushedBatch {
    published_ts: i64,
    handle: ZSetHandle,
    latency_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct HotspotSummary {
    phase: &'static str,
    phase_ms: u64,
    phase_share: f64,
}

fn summarize_hotspot(phases: &[(&'static str, u64)], total_ms: u64) -> Option<HotspotSummary> {
    if total_ms == 0 {
        return None;
    }
    let (phase, phase_ms) = phases.iter().max_by_key(|(_, ms)| *ms).copied()?;
    if phase_ms == 0 {
        return None;
    }
    Some(HotspotSummary {
        phase,
        phase_ms,
        phase_share: phase_ms as f64 / total_ms as f64,
    })
}

fn should_log_optimization_hotspot(total_ms: u64) -> bool {
    if total_ms >= MV_OPTIMIZATION_LOG_MIN_TOTAL_MS {
        return true;
    }
    MV_OPTIMIZATION_LOG_COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(MV_OPTIMIZATION_LOG_SAMPLE_EVERY)
}

#[derive(Debug, Default)]
struct PendingOverlaySnapshot {
    batches: usize,
    rows: usize,
    bytes: usize,
    first_version: Option<i64>,
    last_version: Option<i64>,
    first_enqueue_at: Option<Instant>,
    delta_batches: Vec<EncodedDeltaBatch>,
}

impl PendingOverlaySnapshot {
    fn record(&mut self, version: i64, deltas: EncodedDeltaBatch) {
        if deltas.is_empty() {
            return;
        }
        self.batches = self.batches.saturating_add(1);
        self.rows = self.rows.saturating_add(deltas.len());
        self.bytes = self.bytes.saturating_add(
            deltas
                .iter()
                .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
                .sum::<usize>(),
        );
        if self.first_version.is_none() {
            self.first_version = Some(version);
        }
        self.last_version = Some(version);
        if self.first_enqueue_at.is_none() {
            self.first_enqueue_at = Some(Instant::now());
        }
        self.delta_batches.push(deltas);
    }

    fn has_pending(&self) -> bool {
        !self.delta_batches.is_empty()
    }

    fn should_flush(&self, config: OverlaySnapshotConfig, now: Instant) -> bool {
        if !self.has_pending() {
            return false;
        }
        if self.batches >= config.max_pending_batches || self.rows >= config.max_pending_rows {
            return true;
        }
        self.first_enqueue_at.is_some_and(|started| {
            now.duration_since(started) >= Duration::from_millis(config.max_delay_ms)
        })
    }

    fn delay_remaining(&self, config: OverlaySnapshotConfig, now: Instant) -> Option<Duration> {
        if !self.has_pending() {
            return None;
        }
        let first_enqueue_at = self.first_enqueue_at?;
        let elapsed = now.duration_since(first_enqueue_at);
        let max_delay = Duration::from_millis(config.max_delay_ms);
        Some(max_delay.saturating_sub(elapsed))
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn take_request(&mut self, reason: &'static str) -> Option<OverlaySnapshotFlushRequest> {
        if !self.has_pending() {
            return None;
        }
        let request = OverlaySnapshotFlushRequest {
            reason,
            batches: self.batches,
            rows: self.rows,
            bytes: self.bytes,
            first_version: self.first_version.unwrap_or(-1),
            last_version: self.last_version.unwrap_or(-1),
            delta_batches: std::mem::take(&mut self.delta_batches),
        };
        self.clear();
        Some(request)
    }
}

struct OverlaySnapshotFlushRequest {
    reason: &'static str,
    batches: usize,
    rows: usize,
    bytes: usize,
    first_version: i64,
    last_version: i64,
    delta_batches: Vec<EncodedDeltaBatch>,
}

fn into_owned_deltas(deltas: EncodedDeltaBatch) -> Vec<(Vec<u8>, i64)> {
    match Arc::try_unwrap(deltas) {
        Ok(deltas) => deltas,
        Err(deltas) => deltas.as_ref().clone(),
    }
}

impl DbspGraphBuilder {
    fn should_bootstrap_authoritative_zero(
        view_frontier: i64,
        logical_version: Option<u64>,
    ) -> bool {
        if view_frontier != 0 {
            return false;
        }
        logical_version.unwrap_or(0) == 0
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn materialize_view(
        &mut self,
        view_name: &str,
        schema: Arc<RowSchema>,
        upstream: DeltaHandleStream,
        delta_transform: Option<Arc<DeltaTransformFn>>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
        retention: StreamRetention,
        consolidation_mode: ConsolidationMode,
    ) -> Result<DeltaHandleStream> {
        let handle_stream = upstream.clone();
        let registry_handle = mv_registry.register(view_name.to_string());
        let arrow_schema = schema.to_arrow_schema();
        mv_registry.set_schema(view_name.to_string(), Arc::clone(&arrow_schema));
        {
            let bridge = self.bridge.lock().await;
            bridge
                .save_mv_schema(view_name, Arc::clone(&arrow_schema))
                .await
                .with_context(|| format!("persist schema metadata for '{view_name}'"))?;
        }

        let mut view = {
            let mut bridge = self.bridge.lock().await;
            bridge
                .new_view(view_name, retention)
                .await
                .with_context(|| format!("provision materialized view '{view_name}'"))?
        };
        let mut view_handle_stream = view.handle_stream();
        let view_frontier = view_handle_stream.committed_frontier();
        if view_frontier >= 0 {
            let handle = view_handle_stream.get(view_frontier).await?;
            let state = self.state_from_handle(&handle).await?;
            registry_handle.set_dbsp_state(state);
            registry_handle.publish_version(view_frontier, handle.clone());
            if Self::should_bootstrap_authoritative_zero(view_frontier, None) {
                let _ = registry_handle.seed_authoritative_row_count_if_latest(0, 0);
            } else {
                registry_handle.mark_state_non_authoritative();
            }
            mv_latest.insert(view_name.to_string(), (view_frontier, handle));
        } else {
            // Fresh non-overlay materializations can keep an exact in-memory count
            // cache by applying visible deltas incrementally until the first flush.
            registry_handle.mark_state_authoritative();
        }

        let registry_clone = registry_handle.clone();
        let table = {
            let bridge = self.bridge.lock().await;
            bridge.table()
        };
        let cursor = StreamCursor::new(upstream.stream());
        let upstream_frontier = cursor.observed();
        let mut upstream_stream = handle_stream.stream();
        let mut delta_reader = DeltaZSetHandleReader::<Vec<u8>>::new(table.clone());
        let graph_id = self.graph_id().to_string();
        let view_namespace = crate::namespaces::materialized_view(view_name)
            .unwrap_or_else(|_| format!("materialized_view/{view_name}"));
        let flush_cfg = self.mv_flush_coalescing;
        tracing::info!(
            view = %view_name,
            enabled = flush_cfg.enabled,
            max_pending_deltas = flush_cfg.max_pending_deltas,
            max_pending_versions = ?flush_cfg.max_pending_versions,
            max_pending_rows = ?flush_cfg.max_pending_rows,
            max_pending_bytes = ?flush_cfg.max_pending_bytes,
            max_delay_ms = ?flush_cfg.max_delay_ms,
            flush_on_catchup_boundary = flush_cfg.flush_on_catchup_boundary,
            flush_on_shutdown = flush_cfg.flush_on_shutdown,
            "materialized view flush coalescing config"
        );
        let mut pending = PendingMvFlush::default();
        if view_frontier < upstream_frontier {
            for ts in (view_frontier + 1)..=upstream_frontier {
                let update_span = tracing::info_span!(
                    "dbsp_write",
                    graph_id = %graph_id,
                    view = %view_name,
                    namespace = %view_namespace,
                    version = ts,
                );
                let _enter = update_span.enter();
                let delta_handle = upstream_stream
                    .get(ts)
                    .await
                    .with_context(|| format!("load delta handle for view '{view_name}' at {ts}"))?;
                let apply = Self::queue_delta_handle_for_view(
                    &mut view,
                    &mut delta_reader,
                    Arc::clone(&arrow_schema),
                    consolidation_mode,
                    delta_transform.as_ref(),
                    &delta_handle,
                    Some((
                        &registry_handle,
                        u64::try_from(ts.max(0)).unwrap_or(u64::MAX),
                    )),
                )
                .await
                .with_context(|| format!("apply delta for view '{view_name}' at {ts}"))?;
                pending.record(ts, &apply);
                if let Some(trigger) = pending.trigger(flush_cfg, Instant::now()) {
                    if let Some(flush) = Self::flush_pending_view(
                        &mut view,
                        &graph_id,
                        &view_namespace,
                        &mut pending,
                        trigger,
                    )
                    .await
                    .context("flush pending materialized view updates (catchup)")?
                    {
                        let logical_version =
                            u64::try_from(flush.published_ts.max(0)).unwrap_or(u64::MAX);
                        let state = self
                            .state_from_handle(&flush.handle)
                            .await?
                            .with_logical_version(logical_version);
                        registry_handle.set_dbsp_state(state);
                        registry_handle.publish_version(flush.published_ts, flush.handle.clone());
                        mv_latest.insert(view_name.to_string(), (flush.published_ts, flush.handle));
                        metrics::observe_mv_update_latency_ms(flush.latency_ms);
                        metrics::inc_mv_updates();
                    }
                }
            }
            if flush_cfg.flush_on_catchup_boundary {
                if let Some(flush) = Self::flush_pending_view(
                    &mut view,
                    &graph_id,
                    &view_namespace,
                    &mut pending,
                    FlushTrigger::CatchupBoundary,
                )
                .await
                .context("flush pending materialized view updates at catchup boundary")?
                {
                    let logical_version =
                        u64::try_from(flush.published_ts.max(0)).unwrap_or(u64::MAX);
                    let state = self
                        .state_from_handle(&flush.handle)
                        .await?
                        .with_logical_version(logical_version);
                    registry_handle.set_dbsp_state(state);
                    registry_handle.publish_version(flush.published_ts, flush.handle.clone());
                    mv_latest.insert(view_name.to_string(), (flush.published_ts, flush.handle));
                    metrics::observe_mv_update_latency_ms(flush.latency_ms);
                    metrics::inc_mv_updates();
                }
            }
        }

        let bridge_clone = Arc::clone(&self.bridge);
        let view_label = view_name.to_string();
        let view_namespace_label = view_namespace.clone();
        let task_label = format!("materialize-view:{view_label}");
        let task_events = task_events.clone();
        let cancel = cancel.clone();
        let delta_transform = delta_transform.clone();
        tokio::spawn(async move {
            let mut cursor = cursor;
            let mut view = view;
            let mut delta_reader = delta_reader;
            let mut pending = pending;
            loop {
                let delay_remaining = pending.delay_remaining(flush_cfg, Instant::now());
                if let Some(delay_remaining) = delay_remaining {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            if flush_cfg.flush_on_shutdown {
                                if let Err(err) = Self::publish_pending_view(
                                    &mut view,
                                    &bridge_clone,
                                    &registry_clone,
                                    &graph_id,
                                    &view_label,
                                    &view_namespace_label,
                                    &mut pending,
                                    FlushTrigger::Shutdown,
                                )
                                .await
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        err,
                                    );
                                }
                            }
                            break;
                        }
                        _ = tokio::time::sleep(delay_remaining) => {
                            if let Err(err) = Self::publish_pending_view(
                                &mut view,
                                &bridge_clone,
                                &registry_clone,
                                &graph_id,
                                &view_label,
                                &view_namespace_label,
                                &mut pending,
                                FlushTrigger::MaxDelay,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        result = cursor.next() => {
                            if let Err(err) = Self::process_materialize_delta(
                                result,
                                &mut view,
                                &mut delta_reader,
                                Arc::clone(&arrow_schema),
                                consolidation_mode,
                                delta_transform.as_ref(),
                                flush_cfg,
                                &bridge_clone,
                                &registry_clone,
                                &graph_id,
                                &view_label,
                                &view_namespace_label,
                                &mut pending,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            if flush_cfg.flush_on_shutdown {
                                if let Err(err) = Self::publish_pending_view(
                                    &mut view,
                                    &bridge_clone,
                                    &registry_clone,
                                    &graph_id,
                                    &view_label,
                                    &view_namespace_label,
                                    &mut pending,
                                    FlushTrigger::Shutdown,
                                )
                                .await
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        err,
                                    );
                                }
                            }
                            break;
                        }
                        result = cursor.next() => {
                            if let Err(err) = Self::process_materialize_delta(
                                result,
                                &mut view,
                                &mut delta_reader,
                                Arc::clone(&arrow_schema),
                                consolidation_mode,
                                delta_transform.as_ref(),
                                flush_cfg,
                                &bridge_clone,
                                &registry_clone,
                                &graph_id,
                                &view_label,
                                &view_namespace_label,
                                &mut pending,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(handle_stream)
    }

    pub(super) async fn materialize_view_from_transient_source_overlay(
        &mut self,
        view_name: &str,
        schema: Arc<RowSchema>,
        upstream: TransientSourceHandleStream,
        delta_transform: Arc<DeltaTransformFn>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        mv_registry: &Arc<MaterializedViewRegistry>,
    ) -> Result<()> {
        let registry_handle = mv_registry.register(view_name.to_string());
        let arrow_schema = schema.to_arrow_schema();
        mv_registry.set_schema(view_name.to_string(), Arc::clone(&arrow_schema));
        {
            let bridge = self.bridge.lock().await;
            bridge
                .save_mv_schema(view_name, Arc::clone(&arrow_schema))
                .await
                .with_context(|| format!("persist schema metadata for '{view_name}'"))?;
        }

        let mut view = {
            let mut bridge = self.bridge.lock().await;
            bridge
                .new_view(view_name, StreamRetention::KeepLast { keep_last: 1 })
                .await
                .with_context(|| format!("provision materialized view '{view_name}'"))?
        };
        // Periodic snapshots should remain stable recovery anchors until we
        // explicitly flush them from the overlay path.
        view.set_compaction_policy(CompactionPolicy::disabled());
        let replay_floor = {
            let logical_version = {
                let bridge = self.bridge.lock().await;
                bridge
                    .load_mv_logical_version(view_name)
                    .await
                    .with_context(|| {
                        format!("load logical version metadata for materialized view '{view_name}'")
                    })?
            };
            let mut view_handle_stream = view.handle_stream();
            let view_frontier = view_handle_stream.committed_frontier();
            if let Some(logical_version) = logical_version
                && view_frontier >= 0
            {
                let handle = view_handle_stream.get(view_frontier).await?;
                let mut state = self.state_from_handle(&handle).await?;
                state = state.with_logical_version(logical_version);
                registry_handle.set_dbsp_state(state);
                registry_handle
                    .publish_version(i64::try_from(logical_version).unwrap_or(i64::MAX), handle);
                if Self::should_bootstrap_authoritative_zero(view_frontier, Some(logical_version)) {
                    let _ = registry_handle.seed_authoritative_row_count_if_latest(0, 0);
                } else {
                    registry_handle.mark_state_non_authoritative();
                }
                Some(logical_version)
            } else {
                registry_handle.mark_state_authoritative();
                None
            }
        };

        let graph_id = self.graph_id().to_string();
        let view_label = view_name.to_string();
        let view_namespace = crate::namespaces::materialized_view(view_name)
            .unwrap_or_else(|_| format!("materialized_view/{view_name}"));
        let task_label = format!("materialize-view:{view_label}");
        let task_events = task_events.clone();
        let cancel = cancel.clone();
        let bridge_clone = Arc::clone(&self.bridge);
        let snapshot_cfg = self.mv_overlay_snapshot;
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<OverlaySnapshotFlushRequest>();
        let flush_registry = Arc::clone(&registry_handle);
        let flush_graph_id = graph_id.clone();
        let flush_view_label = view_label.clone();
        let flush_view_namespace = view_namespace.clone();
        let flush_task_events = task_events.clone();
        let flush_task_label = task_label.clone();
        tokio::spawn(async move {
            while let Some(request) = flush_rx.recv().await {
                if let Err(err) = Self::flush_overlay_snapshot_request(
                    &mut view,
                    &bridge_clone,
                    &flush_registry,
                    &flush_graph_id,
                    &flush_view_label,
                    &flush_view_namespace,
                    request,
                )
                .await
                {
                    report_graph_task_error(
                        &flush_task_events,
                        &flush_graph_id,
                        flush_task_label.clone(),
                        err,
                    );
                    break;
                }
            }
        });
        let mut rx = upstream.subscribe();
        tokio::spawn(async move {
            let mut pending_snapshot = PendingOverlaySnapshot::default();
            loop {
                if let Some(delay_remaining) =
                    pending_snapshot.delay_remaining(snapshot_cfg, Instant::now())
                {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            if let Some(request) = pending_snapshot.take_request("shutdown")
                                && flush_tx.send(request).is_err()
                            {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                );
                            }
                            break;
                        },
                        _ = tokio::time::sleep(delay_remaining) => {
                            if let Some(request) = pending_snapshot.take_request("max_delay")
                                && flush_tx.send(request).is_err()
                            {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                );
                                break;
                            }
                        },
                        maybe_batch = rx.recv() => {
                            let Some(batch) = maybe_batch else {
                                if let Some(request) = pending_snapshot.take_request("channel_closed")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                }
                                break;
                            };
                            if let Err(err) = Self::process_transient_materialize_batch_overlay(
                                batch.into(),
                                Some(&delta_transform),
                                &registry_handle,
                                replay_floor,
                                &mut pending_snapshot,
                                &graph_id,
                                &view_label,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now()) {
                                if let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        maybe_batch = rx.recv() => {
                            let Some(batch) = maybe_batch else {
                                break;
                            };
                            if let Err(err) = Self::process_transient_materialize_batch_overlay(
                                batch.into(),
                                Some(&delta_transform),
                                &registry_handle,
                                replay_floor,
                                &mut pending_snapshot,
                                &graph_id,
                                &view_label,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now()) {
                                if let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(())
    }

    pub(super) async fn materialize_view_from_transient_overlay_receiver(
        &mut self,
        view_name: &str,
        schema: Arc<RowSchema>,
        mut upstream: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
        delta_transform: Option<Arc<DeltaTransformFn>>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        mv_registry: &Arc<MaterializedViewRegistry>,
    ) -> Result<()> {
        let registry_handle = mv_registry.register(view_name.to_string());
        let arrow_schema = schema.to_arrow_schema();
        mv_registry.set_schema(view_name.to_string(), Arc::clone(&arrow_schema));
        {
            let bridge = self.bridge.lock().await;
            bridge
                .save_mv_schema(view_name, Arc::clone(&arrow_schema))
                .await
                .with_context(|| format!("persist schema metadata for '{view_name}'"))?;
        }

        let mut view = {
            let mut bridge = self.bridge.lock().await;
            bridge
                .new_view(view_name, StreamRetention::KeepLast { keep_last: 1 })
                .await
                .with_context(|| format!("provision materialized view '{view_name}'"))?
        };
        view.set_compaction_policy(CompactionPolicy::disabled());
        let replay_floor = {
            let logical_version = {
                let bridge = self.bridge.lock().await;
                bridge
                    .load_mv_logical_version(view_name)
                    .await
                    .with_context(|| {
                        format!("load logical version metadata for materialized view '{view_name}'")
                    })?
            };
            let mut view_handle_stream = view.handle_stream();
            let view_frontier = view_handle_stream.committed_frontier();
            if let Some(logical_version) = logical_version
                && view_frontier >= 0
            {
                let handle = view_handle_stream.get(view_frontier).await?;
                let mut state = self.state_from_handle(&handle).await?;
                state = state.with_logical_version(logical_version);
                registry_handle.set_dbsp_state(state);
                registry_handle
                    .publish_version(i64::try_from(logical_version).unwrap_or(i64::MAX), handle);
                if Self::should_bootstrap_authoritative_zero(view_frontier, Some(logical_version)) {
                    let _ = registry_handle.seed_authoritative_row_count_if_latest(0, 0);
                } else {
                    registry_handle.mark_state_non_authoritative();
                }
                Some(logical_version)
            } else {
                registry_handle.mark_state_authoritative();
                None
            }
        };

        let graph_id = self.graph_id().to_string();
        let view_label = view_name.to_string();
        let view_namespace = crate::namespaces::materialized_view(view_name)
            .unwrap_or_else(|_| format!("materialized_view/{view_name}"));
        let task_label = format!("materialize-view:{view_label}");
        let task_events = task_events.clone();
        let cancel = cancel.clone();
        let bridge_clone = Arc::clone(&self.bridge);
        let snapshot_cfg = self.mv_overlay_snapshot;
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<OverlaySnapshotFlushRequest>();
        let flush_registry = Arc::clone(&registry_handle);
        let flush_graph_id = graph_id.clone();
        let flush_view_label = view_label.clone();
        let flush_view_namespace = view_namespace.clone();
        let flush_task_events = task_events.clone();
        let flush_task_label = task_label.clone();
        tokio::spawn(async move {
            while let Some(request) = flush_rx.recv().await {
                if let Err(err) = Self::flush_overlay_snapshot_request(
                    &mut view,
                    &bridge_clone,
                    &flush_registry,
                    &flush_graph_id,
                    &flush_view_label,
                    &flush_view_namespace,
                    request,
                )
                .await
                {
                    report_graph_task_error(
                        &flush_task_events,
                        &flush_graph_id,
                        flush_task_label.clone(),
                        err,
                    );
                    break;
                }
            }
        });
        tokio::spawn(async move {
            let mut pending_snapshot = PendingOverlaySnapshot::default();
            loop {
                if let Some(delay_remaining) =
                    pending_snapshot.delay_remaining(snapshot_cfg, Instant::now())
                {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            if let Some(request) = pending_snapshot.take_request("shutdown")
                                && flush_tx.send(request).is_err()
                            {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                );
                            }
                            break;
                        },
                        _ = tokio::time::sleep(delay_remaining) => {
                            if let Some(request) = pending_snapshot.take_request("max_delay")
                                && flush_tx.send(request).is_err()
                            {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                );
                                break;
                            }
                        },
                        maybe_batch = upstream.recv() => {
                            let Some(batch) = maybe_batch else {
                                if let Some(request) = pending_snapshot.take_request("channel_closed")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                }
                                break;
                            };
                            if let Err(err) = Self::process_transient_materialize_batch_overlay(
                                batch,
                                delta_transform.as_ref(),
                                &registry_handle,
                                replay_floor,
                                &mut pending_snapshot,
                                &graph_id,
                                &view_label,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now()) {
                                if let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        maybe_batch = upstream.recv() => {
                            let Some(batch) = maybe_batch else {
                                break;
                            };
                            if let Err(err) = Self::process_transient_materialize_batch_overlay(
                                batch,
                                delta_transform.as_ref(),
                                &registry_handle,
                                replay_floor,
                                &mut pending_snapshot,
                                &graph_id,
                                &view_label,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now()) {
                                if let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(())
    }

    pub(super) async fn materialize_view_from_delta_overlay(
        &mut self,
        view_name: &str,
        schema: Arc<RowSchema>,
        upstream: DeltaHandleStream,
        delta_transform: Option<Arc<DeltaTransformFn>>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        mv_registry: &Arc<MaterializedViewRegistry>,
    ) -> Result<()> {
        let registry_handle = mv_registry.register(view_name.to_string());
        let arrow_schema = schema.to_arrow_schema();
        mv_registry.set_schema(view_name.to_string(), Arc::clone(&arrow_schema));
        {
            let bridge = self.bridge.lock().await;
            bridge
                .save_mv_schema(view_name, Arc::clone(&arrow_schema))
                .await
                .with_context(|| format!("persist schema metadata for '{view_name}'"))?;
        }

        let mut view = {
            let mut bridge = self.bridge.lock().await;
            bridge
                .new_view(view_name, StreamRetention::KeepLast { keep_last: 1 })
                .await
                .with_context(|| format!("provision materialized view '{view_name}'"))?
        };
        view.set_compaction_policy(CompactionPolicy::disabled());
        let replay_floor = {
            let logical_version = {
                let bridge = self.bridge.lock().await;
                bridge
                    .load_mv_logical_version(view_name)
                    .await
                    .with_context(|| {
                        format!("load logical version metadata for materialized view '{view_name}'")
                    })?
            };
            let mut view_handle_stream = view.handle_stream();
            let view_frontier = view_handle_stream.committed_frontier();
            if let Some(logical_version) = logical_version
                && view_frontier >= 0
            {
                let handle = view_handle_stream.get(view_frontier).await?;
                let mut state = self.state_from_handle(&handle).await?;
                state = state.with_logical_version(logical_version);
                registry_handle.set_dbsp_state(state);
                registry_handle
                    .publish_version(i64::try_from(logical_version).unwrap_or(i64::MAX), handle);
                if Self::should_bootstrap_authoritative_zero(view_frontier, Some(logical_version)) {
                    let _ = registry_handle.seed_authoritative_row_count_if_latest(0, 0);
                } else {
                    registry_handle.mark_state_non_authoritative();
                }
                Some(logical_version)
            } else {
                registry_handle.mark_state_authoritative();
                None
            }
        };

        let table = {
            let bridge = self.bridge.lock().await;
            bridge.table()
        };
        let mut cursor = StreamCursor::new(upstream.stream());
        let upstream_frontier = cursor.observed();
        let mut upstream_stream = upstream.stream();
        let mut delta_reader = DeltaZSetHandleReader::<Vec<u8>>::new(table);
        let graph_id = self.graph_id().to_string();
        let view_label = view_name.to_string();
        let view_namespace = crate::namespaces::materialized_view(view_name)
            .unwrap_or_else(|_| format!("materialized_view/{view_name}"));
        let task_label = format!("materialize-view:{view_label}");
        let task_events = task_events.clone();
        let cancel = cancel.clone();
        let bridge_clone = Arc::clone(&self.bridge);
        let snapshot_cfg = self.mv_overlay_snapshot;
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<OverlaySnapshotFlushRequest>();
        let flush_registry = Arc::clone(&registry_handle);
        let flush_graph_id = graph_id.clone();
        let flush_view_label = view_label.clone();
        let flush_view_namespace = view_namespace.clone();
        let flush_task_events = task_events.clone();
        let flush_task_label = task_label.clone();
        tokio::spawn(async move {
            while let Some(request) = flush_rx.recv().await {
                if let Err(err) = Self::flush_overlay_snapshot_request(
                    &mut view,
                    &bridge_clone,
                    &flush_registry,
                    &flush_graph_id,
                    &flush_view_label,
                    &flush_view_namespace,
                    request,
                )
                .await
                {
                    report_graph_task_error(
                        &flush_task_events,
                        &flush_graph_id,
                        flush_task_label.clone(),
                        err,
                    );
                    break;
                }
            }
        });

        let mut pending_snapshot = PendingOverlaySnapshot::default();
        let replay_floor_ts = replay_floor
            .and_then(|version| i64::try_from(version).ok())
            .unwrap_or(-1);
        if replay_floor_ts < upstream_frontier {
            for ts in (replay_floor_ts + 1)..=upstream_frontier {
                let delta_handle = upstream_stream.get(ts).await.with_context(|| {
                    format!("load delta handle for materialized view '{view_name}' at {ts}")
                })?;
                Self::process_delta_handle_overlay(
                    Ok((ts, delta_handle)),
                    &mut delta_reader,
                    delta_transform.as_ref(),
                    &registry_handle,
                    replay_floor,
                    &mut pending_snapshot,
                    &graph_id,
                    &view_label,
                )
                .await?;
                if pending_snapshot.should_flush(snapshot_cfg, Instant::now()) {
                    if let Some(request) = pending_snapshot.take_request("background_threshold")
                        && flush_tx.send(request).is_err()
                    {
                        return Err(anyhow::anyhow!(
                            "overlay snapshot flush task unexpectedly stopped"
                        ));
                    }
                }
            }
        }

        tokio::spawn(async move {
            let mut pending_snapshot = pending_snapshot;
            loop {
                if let Some(delay_remaining) =
                    pending_snapshot.delay_remaining(snapshot_cfg, Instant::now())
                {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            if let Some(request) = pending_snapshot.take_request("shutdown")
                                && flush_tx.send(request).is_err()
                            {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                );
                            }
                            break;
                        },
                        _ = tokio::time::sleep(delay_remaining) => {
                            if let Some(request) = pending_snapshot.take_request("max_delay")
                                && flush_tx.send(request).is_err()
                            {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                );
                                break;
                            }
                        },
                        result = cursor.next() => {
                            if let Err(err) = Self::process_delta_handle_overlay(
                                result,
                                &mut delta_reader,
                                delta_transform.as_ref(),
                                &registry_handle,
                                replay_floor,
                                &mut pending_snapshot,
                                &graph_id,
                                &view_label,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now()) {
                                if let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        result = cursor.next() => {
                            if let Err(err) = Self::process_delta_handle_overlay(
                                result,
                                &mut delta_reader,
                                delta_transform.as_ref(),
                                &registry_handle,
                                replay_floor,
                                &mut pending_snapshot,
                                &graph_id,
                                &view_label,
                            )
                            .await
                            {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now()) {
                                if let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).is_err()
                                {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow::anyhow!("overlay snapshot flush task unexpectedly stopped"),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn process_materialize_delta(
        result: Result<(i64, ZSetHandle)>,
        view: &mut DbspView,
        delta_reader: &mut DeltaZSetHandleReader<Vec<u8>>,
        row_schema: SchemaRef,
        consolidation_mode: ConsolidationMode,
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        flush_cfg: MvFlushCoalescingConfig,
        bridge: &Arc<Mutex<DbspBridge>>,
        registry: &Arc<MaterializedViewHandle>,
        graph_id: &str,
        view_label: &str,
        view_namespace: &str,
        pending: &mut PendingMvFlush,
    ) -> Result<()> {
        let (ts, delta_handle) = result.with_context(|| {
            format!("stream for materialized view '{view_label}' closed unexpectedly")
        })?;
        let update_span = tracing::info_span!(
            "dbsp_write",
            graph_id = %graph_id,
            view = %view_label,
            namespace = %view_namespace,
            version = ts,
        );
        let _enter = update_span.enter();
        let apply = Self::queue_delta_handle_for_view(
            view,
            delta_reader,
            row_schema,
            consolidation_mode,
            delta_transform,
            &delta_handle,
            Some((registry, u64::try_from(ts.max(0)).unwrap_or(u64::MAX))),
        )
        .await
        .with_context(|| format!("apply delta for materialized view '{view_label}' at {ts}"))?;
        pending.record(ts, &apply);
        if let Some(trigger) = pending.trigger(flush_cfg, Instant::now()) {
            Self::publish_pending_view(
                view,
                bridge,
                registry,
                graph_id,
                view_label,
                view_namespace,
                pending,
                trigger,
            )
            .await?;
        }
        Ok(())
    }

    async fn process_delta_handle_overlay(
        result: Result<(i64, ZSetHandle)>,
        delta_reader: &mut DeltaZSetHandleReader<Vec<u8>>,
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        registry: &Arc<MaterializedViewHandle>,
        replay_floor: Option<u64>,
        pending_snapshot: &mut PendingOverlaySnapshot,
        graph_id: &str,
        view_label: &str,
    ) -> Result<()> {
        let (ts, delta_handle) = result.with_context(|| {
            format!("stream for materialized view '{view_label}' closed unexpectedly")
        })?;
        let ts_u64 = u64::try_from(ts.max(0)).unwrap_or(u64::MAX);
        if replay_floor.is_some_and(|floor| ts_u64 <= floor) {
            return Ok(());
        }
        let update_span = tracing::info_span!(
            "dbsp_write",
            graph_id = %graph_id,
            view = %view_label,
            namespace = "overlay",
            version = ts,
        );
        let _enter = update_span.enter();
        let apply_start = Instant::now();
        let (apply, deltas) =
            Self::load_transformed_delta_handle(delta_reader, delta_transform, &delta_handle)
                .await
                .with_context(|| {
                    format!("apply delta handle for materialized view '{view_label}' at {ts}")
                })?;
        Self::apply_encoded_overlay_batch(
            apply_start,
            ts,
            apply,
            deltas,
            registry,
            pending_snapshot,
            view_label,
        )
        .context("apply encoded overlay batch for transformed delta handle")
    }

    async fn process_transient_materialize_batch_overlay(
        batch: TransientMaterializeBatch,
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        registry: &Arc<MaterializedViewHandle>,
        replay_floor: Option<u64>,
        pending_snapshot: &mut PendingOverlaySnapshot,
        graph_id: &str,
        view_label: &str,
    ) -> Result<()> {
        let ts = batch.version;
        let ts_u64 = u64::try_from(ts.max(0)).unwrap_or(u64::MAX);
        if replay_floor.is_some_and(|floor| ts_u64 <= floor) {
            return Ok(());
        }
        let update_span = tracing::info_span!(
            "dbsp_write",
            graph_id = %graph_id,
            view = %view_label,
            namespace = "overlay",
            version = ts,
        );
        let _enter = update_span.enter();
        let apply_start = Instant::now();
        let (apply, merged) = Self::transform_transient_batch(delta_transform, batch)
            .await
            .with_context(|| {
                format!("apply transient source batch for materialized view '{view_label}' at {ts}")
            })?;
        Self::apply_encoded_overlay_batch(
            apply_start,
            ts,
            apply,
            merged,
            registry,
            pending_snapshot,
            view_label,
        )
        .context("apply encoded overlay batch for transient materialization")
    }

    fn apply_encoded_overlay_batch(
        apply_start: Instant,
        ts: i64,
        apply: DeltaApplyStats,
        merged: EncodedDeltaBatch,
        registry: &Arc<MaterializedViewHandle>,
        pending_snapshot: &mut PendingOverlaySnapshot,
        view_label: &str,
    ) -> Result<()> {
        let ts_u64 = u64::try_from(ts.max(0)).unwrap_or(u64::MAX);
        if merged.is_empty() {
            registry.publish_logical_version(ts);
            registry.advance_authoritative_row_count_version(ts_u64);
            return Ok(());
        }
        let apply_stats = registry.append_shared_encoded_overlay_batch(ts_u64, Arc::clone(&merged));
        registry
            .apply_encoded_state_batch(ts_u64, merged.as_slice())
            .with_context(|| {
                format!("update overlay state cache for materialized view '{view_label}' at {ts}")
            })?;
        registry.publish_logical_version(ts);
        pending_snapshot.record(ts, merged);
        let latency_ms = apply_start.elapsed().as_millis() as u64;
        metrics::observe_mv_update_latency_ms(latency_ms);
        metrics::inc_mv_updates();
        let hotspot = summarize_hotspot(
            &[
                ("load", apply.load_ms),
                ("transform", apply.transform_ms),
                ("state_apply", apply_stats.apply_ms),
            ],
            latency_ms,
        );
        if let Some(hotspot) = hotspot {
            metrics::observe_mv_optimization_hotspot(
                "overlay_apply",
                hotspot.phase,
                hotspot.phase_share,
                latency_ms,
            );
            if should_log_optimization_hotspot(latency_ms) {
                tracing::info!(
                    view = %view_label,
                    namespace = "overlay",
                    version = ts,
                    path = "overlay_apply",
                    delta_rows = apply.delta_rows,
                    delta_bytes = apply.delta_bytes,
                    total_ms = latency_ms,
                    hotspot_phase = hotspot.phase,
                    hotspot_phase_ms = hotspot.phase_ms,
                    hotspot_phase_share = hotspot.phase_share,
                    "materialized view optimization hotspot"
                );
            }
        }
        if MV_OVERLAY_APPLY_LOG_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(MV_OVERLAY_APPLY_LOG_SAMPLE_EVERY)
        {
            tracing::info!(
                view = %view_label,
                namespace = "overlay",
                version = ts,
                delta_rows = apply.delta_rows,
                delta_bytes = apply.delta_bytes,
                load_ms = apply.load_ms,
                transform_ms = apply.transform_ms,
                merge_ms = apply.merge_ms,
                overlay_rows = apply_stats.overlay_rows,
                overlay_bytes = apply_stats.overlay_bytes,
                state_apply_ms = apply_stats.apply_ms,
                overlay_batches = apply_stats.overlay_batches,
                latency_ms,
                "materialized view overlay apply breakdown"
            );
        }
        Ok(())
    }

    async fn flush_overlay_snapshot_request(
        view: &mut DbspView,
        bridge: &Arc<Mutex<DbspBridge>>,
        registry: &Arc<MaterializedViewHandle>,
        graph_id: &str,
        view_label: &str,
        view_namespace: &str,
        request: OverlaySnapshotFlushRequest,
    ) -> Result<()> {
        let flush_start = Instant::now();
        let mut deltas = Vec::with_capacity(request.rows);
        for batch in request.delta_batches {
            deltas.extend(into_owned_deltas(batch));
        }
        view.add_deltas(deltas);
        let handle = view
            .flush()
            .await
            .with_context(|| format!("flush hybrid overlay snapshot for '{view_label}'"))?;
        let flush_ms = flush_start.elapsed().as_millis() as u64;
        let published_version = request.last_version;
        bridge
            .lock()
            .await
            .save_mv_logical_version(
                view_label,
                u64::try_from(published_version.max(0)).unwrap_or(0),
            )
            .await
            .with_context(|| {
                format!("persist logical version metadata for materialized view '{view_label}'")
            })?;
        let state = Self::state_from_handle_with_bridge(bridge, &handle)
            .await?
            .with_logical_version(u64::try_from(published_version.max(0)).unwrap_or(0));
        registry.set_dbsp_state(state);
        registry.publish_version(published_version, handle);
        let compaction = registry.compact_encoded_overlay_up_to(
            u64::try_from(published_version.max(0)).unwrap_or(u64::MAX),
        );
        if MV_OVERLAY_SNAPSHOT_LOG_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(MV_OVERLAY_SNAPSHOT_LOG_SAMPLE_EVERY)
        {
            tracing::info!(
                graph_id = %graph_id,
                view = %view_label,
                namespace = %view_namespace,
                reason = request.reason,
                first_version = request.first_version,
                last_version = published_version,
                pending_batches = request.batches,
                pending_rows = request.rows,
                pending_bytes = request.bytes,
                flush_ms,
                removed_overlay_batches = compaction.removed_batches,
                remaining_overlay_batches = compaction.remaining_batches,
                remaining_overlay_rows = compaction.remaining_rows,
                "materialized view hybrid snapshot flushed"
            );
        }
        Ok(())
    }

    async fn publish_pending_view(
        view: &mut DbspView,
        bridge: &Arc<Mutex<DbspBridge>>,
        registry: &Arc<MaterializedViewHandle>,
        graph_id: &str,
        view_label: &str,
        view_namespace: &str,
        pending: &mut PendingMvFlush,
        trigger: FlushTrigger,
    ) -> Result<()> {
        if let Some(flush) =
            Self::flush_pending_view(view, graph_id, view_namespace, pending, trigger)
                .await
                .context("flush pending materialized view updates")?
        {
            let logical_version = u64::try_from(flush.published_ts.max(0)).unwrap_or(u64::MAX);
            let state = Self::state_from_handle_with_bridge(bridge, &flush.handle)
                .await
                .with_context(|| format!("update materialized view '{view_label}' state"))?
                .with_logical_version(logical_version);
            registry.set_dbsp_state(state);
            registry.publish_version(flush.published_ts, flush.handle);
            metrics::observe_mv_update_latency_ms(flush.latency_ms);
            metrics::inc_mv_updates();
            if MV_UPDATE_LOG_COUNTER
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(MV_UPDATE_LOG_SAMPLE_EVERY)
            {
                tracing::info!(
                    view = %view_label,
                    namespace = %view_namespace,
                    version = flush.published_ts,
                    trigger = trigger.as_str(),
                    "materialized view advanced"
                );
            }
        }
        Ok(())
    }

    async fn load_transformed_delta_handle(
        delta_reader: &mut DeltaZSetHandleReader<Vec<u8>>,
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        delta_handle: &ZSetHandle,
    ) -> Result<(DeltaApplyStats, EncodedDeltaBatch)> {
        let load_start = Instant::now();
        let mut deltas = delta_reader
            .read(delta_handle)
            .await
            .context("materialize delta handle for materialized view")?;
        let load_ms = load_start.elapsed().as_millis() as u64;

        let raw_delta_rows = deltas.len();
        let transform_start = Instant::now();
        if let Some(transform) = delta_transform {
            deltas =
                transform(&deltas).context("apply transient transform before materialized view")?;
        }
        let transform_ms = transform_start.elapsed().as_millis() as u64;

        let delta_rows = deltas.len();
        let delta_bytes = deltas
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
            .sum();
        tracing::debug!(
            delta_handle_version = delta_handle.version,
            raw_delta_rows,
            delta_rows,
            delta_bytes,
            load_ms,
            transform_ms,
            "materialized view delta transformed"
        );
        Ok((
            DeltaApplyStats {
                delta_rows,
                delta_bytes,
                load_ms,
                transform_ms,
                merge_ms: 0,
            },
            Arc::new(deltas),
        ))
    }

    async fn queue_delta_handle_for_view(
        view: &mut DbspView,
        delta_reader: &mut DeltaZSetHandleReader<Vec<u8>>,
        _row_schema: SchemaRef,
        _consolidation_mode: ConsolidationMode,
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        delta_handle: &ZSetHandle,
        authoritative_state: Option<(&Arc<MaterializedViewHandle>, u64)>,
    ) -> Result<DeltaApplyStats> {
        let (mut apply, deltas) =
            Self::load_transformed_delta_handle(delta_reader, delta_transform, delta_handle)
                .await?;
        let merge_start = Instant::now();
        if let Some((registry, version)) = authoritative_state {
            if deltas.is_empty() {
                registry.stage_authoritative_row_count_version(version);
            } else {
                registry
                    .apply_encoded_state_batch(version, deltas.as_ref())
                    .context("update authoritative materialized view state cache")?;
            }
        }
        if !deltas.is_empty() {
            // Transient segment outputs are already batch-transformed; feed rows straight
            // into MV overlay and let ZSetStream overlay consolidation handle duplicates.
            view.add_deltas(into_owned_deltas(deltas));
        }
        apply.merge_ms = merge_start.elapsed().as_millis() as u64;
        tracing::debug!(
            delta_handle_version = delta_handle.version,
            delta_rows = apply.delta_rows,
            delta_bytes = apply.delta_bytes,
            load_ms = apply.load_ms,
            transform_ms = apply.transform_ms,
            merge_ms = apply.merge_ms,
            "materialized view delta queued"
        );
        Ok(apply)
    }

    async fn transform_transient_batch(
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        batch: TransientMaterializeBatch,
    ) -> Result<(DeltaApplyStats, EncodedDeltaBatch)> {
        let input_rows = batch.deltas.len();
        let input_bytes = batch
            .deltas
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
            .sum();
        let (merged, transform_ms) = if let Some(transform) = delta_transform {
            let transform_start = Instant::now();
            let merged = transform(batch.deltas.as_ref())
                .context("apply transient transform before materialized view")?;
            (
                Arc::new(merged),
                transform_start.elapsed().as_millis() as u64,
            )
        } else {
            (batch.deltas, 0)
        };
        Ok((
            DeltaApplyStats {
                delta_rows: input_rows,
                delta_bytes: input_bytes,
                load_ms: 0,
                transform_ms,
                merge_ms: 0,
            },
            merged,
        ))
    }

    async fn flush_pending_view(
        view: &mut DbspView,
        graph_id: &str,
        view_namespace: &str,
        pending: &mut PendingMvFlush,
        trigger: FlushTrigger,
    ) -> Result<Option<FlushedBatch>> {
        if !pending.has_pending() {
            return Ok(None);
        }
        let flush_start = Instant::now();
        let handle = view
            .flush()
            .await
            .context("flush materialized view updates")?;
        let flush_ms = flush_start.elapsed().as_millis() as u64;
        let total_ms =
            pending.total_load_ms + pending.total_transform_ms + pending.total_merge_ms + flush_ms;
        let first_ts = pending.first_ts.unwrap_or(-1);
        let last_ts = pending.last_ts.unwrap_or(-1);
        let latency_ms = pending
            .first_enqueue_at
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or(total_ms);
        let hotspot = summarize_hotspot(
            &[
                ("load", pending.total_load_ms),
                ("transform", pending.total_transform_ms),
                ("merge", pending.total_merge_ms),
                ("flush", flush_ms),
            ],
            total_ms,
        );
        if let Some(hotspot) = hotspot {
            metrics::observe_mv_optimization_hotspot(
                "flush_apply",
                hotspot.phase,
                hotspot.phase_share,
                total_ms,
            );
            if should_log_optimization_hotspot(total_ms) {
                tracing::info!(
                    graph_id = %graph_id,
                    namespace = %view_namespace,
                    trigger = trigger.as_str(),
                    first_version = first_ts,
                    last_version = last_ts,
                    pending_versions = pending.pending_versions,
                    pending_deltas = pending.pending_deltas,
                    pending_rows = pending.pending_rows,
                    pending_bytes = pending.pending_bytes,
                    path = "flush_apply",
                    total_ms,
                    latency_ms,
                    hotspot_phase = hotspot.phase,
                    hotspot_phase_ms = hotspot.phase_ms,
                    hotspot_phase_share = hotspot.phase_share,
                    "materialized view optimization hotspot"
                );
            }
        }
        if total_ms >= 1_000 {
            tracing::info!(
                graph_id = %graph_id,
                namespace = %view_namespace,
                trigger = trigger.as_str(),
                first_version = first_ts,
                last_version = last_ts,
                pending_versions = pending.pending_versions,
                pending_deltas = pending.pending_deltas,
                pending_rows = pending.pending_rows,
                pending_bytes = pending.pending_bytes,
                load_ms = pending.total_load_ms,
                transform_ms = pending.total_transform_ms,
                merge_ms = pending.total_merge_ms,
                flush_ms,
                total_ms,
                latency_ms,
                "materialized view delta apply breakdown"
            );
        } else {
            tracing::debug!(
                graph_id = %graph_id,
                namespace = %view_namespace,
                trigger = trigger.as_str(),
                first_version = first_ts,
                last_version = last_ts,
                pending_versions = pending.pending_versions,
                pending_deltas = pending.pending_deltas,
                pending_rows = pending.pending_rows,
                pending_bytes = pending.pending_bytes,
                load_ms = pending.total_load_ms,
                transform_ms = pending.total_transform_ms,
                merge_ms = pending.total_merge_ms,
                flush_ms,
                total_ms,
                latency_ms,
                "materialized view delta apply breakdown"
            );
        }
        pending.clear();
        Ok(Some(FlushedBatch {
            published_ts: last_ts,
            handle,
            latency_ms,
        }))
    }

    async fn state_from_handle(&self, handle: &ZSetHandle) -> Result<DbspPersistedState> {
        Self::state_from_handle_with_bridge(&self.bridge, handle).await
    }

    async fn state_from_handle_with_bridge(
        bridge: &Arc<Mutex<DbspBridge>>,
        handle: &ZSetHandle,
    ) -> Result<DbspPersistedState> {
        let mut guard = bridge.lock().await;
        let handle_view = guard
            .handle_view_for(&handle.ns, handle.version)
            .await
            .context("open handle view for materialized view state")?;
        let (dict, table, namespace, version) = handle_view.into_parts();
        Ok(DbspPersistedState::new(dict, table, namespace, version))
    }
}

#[cfg(test)]
mod helper_tests {
    use super::*;

    fn delta_batch(rows: Vec<(Vec<u8>, i64)>) -> EncodedDeltaBatch {
        Arc::new(rows)
    }

    #[test]
    fn flush_trigger_string_labels_are_stable() {
        assert_eq!(
            FlushTrigger::MaxPendingDeltas.as_str(),
            "max_pending_deltas"
        );
        assert_eq!(
            FlushTrigger::MaxPendingVersions.as_str(),
            "max_pending_versions"
        );
        assert_eq!(FlushTrigger::MaxPendingRows.as_str(), "max_pending_rows");
        assert_eq!(FlushTrigger::MaxPendingBytes.as_str(), "max_pending_bytes");
        assert_eq!(FlushTrigger::MaxDelay.as_str(), "max_delay");
        assert_eq!(FlushTrigger::CatchupBoundary.as_str(), "catchup_boundary");
        assert_eq!(FlushTrigger::Shutdown.as_str(), "shutdown");
    }

    #[test]
    fn pending_mv_flush_tracks_stats_triggers_and_reset() {
        let mut pending = PendingMvFlush::default();
        let cfg = MvFlushCoalescingConfig {
            enabled: true,
            max_pending_deltas: 2,
            max_pending_versions: Some(8),
            max_pending_rows: Some(100),
            max_pending_bytes: Some(1000),
            max_delay_ms: Some(1_000),
            flush_on_catchup_boundary: true,
            flush_on_shutdown: true,
        };

        assert!(pending.trigger(cfg, Instant::now()).is_none());
        assert!(pending.delay_remaining(cfg, Instant::now()).is_none());

        pending.record(
            10,
            &DeltaApplyStats {
                delta_rows: 2,
                delta_bytes: 11,
                load_ms: 1,
                transform_ms: 2,
                merge_ms: 3,
            },
        );
        assert!(pending.has_pending());
        assert_eq!(pending.first_ts, Some(10));
        assert_eq!(pending.last_ts, Some(10));
        assert!(pending.delay_remaining(cfg, Instant::now()).is_some());

        pending.record(
            11,
            &DeltaApplyStats {
                delta_rows: 4,
                delta_bytes: 13,
                load_ms: 5,
                transform_ms: 7,
                merge_ms: 11,
            },
        );
        assert!(matches!(
            pending.trigger(cfg, Instant::now()),
            Some(FlushTrigger::MaxPendingDeltas)
        ));

        let mut delayed = PendingMvFlush::default();
        delayed.record(22, &DeltaApplyStats::default());
        delayed.first_enqueue_at = Some(Instant::now() - Duration::from_millis(25));
        let delay_cfg = MvFlushCoalescingConfig {
            max_pending_deltas: usize::MAX,
            max_pending_versions: None,
            max_pending_rows: None,
            max_pending_bytes: None,
            max_delay_ms: Some(5),
            ..MvFlushCoalescingConfig::default()
        };
        assert!(matches!(
            delayed.trigger(delay_cfg, Instant::now()),
            Some(FlushTrigger::MaxDelay)
        ));

        delayed.clear();
        assert!(!delayed.has_pending());
    }

    #[test]
    fn hotspot_summary_and_logging_gate_behave_as_expected() {
        assert!(summarize_hotspot(&[], 100).is_none());
        assert!(summarize_hotspot(&[("load", 0)], 100).is_none());
        assert!(summarize_hotspot(&[("load", 10)], 0).is_none());

        let hotspot = summarize_hotspot(&[("load", 15), ("merge", 35)], 50).expect("hotspot");
        assert_eq!(hotspot.phase, "merge");
        assert_eq!(hotspot.phase_ms, 35);
        assert!((hotspot.phase_share - 0.7).abs() < f64::EPSILON);

        assert!(should_log_optimization_hotspot(
            MV_OPTIMIZATION_LOG_MIN_TOTAL_MS
        ));
        assert!(should_log_optimization_hotspot(
            MV_OPTIMIZATION_LOG_MIN_TOTAL_MS + 1
        ));
    }

    #[test]
    fn pending_overlay_snapshot_tracks_batches_and_flush_request() {
        let mut pending = PendingOverlaySnapshot::default();
        let cfg = OverlaySnapshotConfig {
            max_pending_batches: 2,
            max_pending_rows: 10,
            max_delay_ms: 1_000,
        };

        pending.record(1, delta_batch(vec![]));
        assert!(!pending.has_pending());

        pending.record(5, delta_batch(vec![(vec![1], 1), (vec![2, 3], -1)]));
        assert!(pending.has_pending());
        assert_eq!(pending.batches, 1);
        assert_eq!(pending.rows, 2);
        assert_eq!(pending.first_version, Some(5));
        assert_eq!(pending.last_version, Some(5));
        assert!(!pending.should_flush(cfg, Instant::now()));

        pending.record(6, delta_batch(vec![(vec![9], 1)]));
        assert!(pending.should_flush(cfg, Instant::now()));

        let request = pending.take_request("test_reason").expect("flush request");
        assert_eq!(request.reason, "test_reason");
        assert_eq!(request.batches, 2);
        assert_eq!(request.rows, 3);
        assert_eq!(request.first_version, 5);
        assert_eq!(request.last_version, 6);
        assert_eq!(request.delta_batches.len(), 2);
        assert!(!pending.has_pending());
    }

    #[test]
    fn pending_overlay_snapshot_delay_and_clear_are_consistent() {
        let mut pending = PendingOverlaySnapshot::default();
        pending.record(42, delta_batch(vec![(vec![7], 1)]));
        pending.first_enqueue_at = Some(Instant::now() - Duration::from_millis(50));

        let cfg = OverlaySnapshotConfig {
            max_pending_batches: usize::MAX,
            max_pending_rows: usize::MAX,
            max_delay_ms: 10,
        };

        assert!(pending.should_flush(cfg, Instant::now()));
        assert_eq!(
            pending.delay_remaining(cfg, Instant::now()),
            Some(Duration::from_millis(0))
        );

        pending.clear();
        assert!(!pending.has_pending());
        assert!(pending.take_request("after_clear").is_none());
    }

    #[test]
    fn into_owned_deltas_covers_unique_and_shared_arcs() {
        let unique = delta_batch(vec![(vec![1, 2], 1)]);
        assert_eq!(into_owned_deltas(unique), vec![(vec![1, 2], 1)]);

        let shared = delta_batch(vec![(vec![9], -2)]);
        let _keep_alive = Arc::clone(&shared);
        assert_eq!(into_owned_deltas(shared), vec![(vec![9], -2)]);
    }
}
