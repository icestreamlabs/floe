use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::SchemaRef;
use dbsp::RowSchema;
use dbsp::StreamRetention;
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::util::delta_zset_handle;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, DbspView};
use crate::delta_consolidation::ConsolidationMode;
use crate::materialized_view::{
    DbspPersistedState, MaterializedViewHandle, MaterializedViewRegistry,
};
use crate::metrics;
use crate::outer_stream::{TransientSourceBatch, TransientSourceHandleStream};
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::{DbspGraphBuilder, MvFlushCoalescingConfig};

static MV_UPDATE_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_UPDATE_LOG_SAMPLE_EVERY: u64 = 128;
static MV_OVERLAY_APPLY_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_OVERLAY_APPLY_LOG_SAMPLE_EVERY: u64 = 16;

pub(super) type DeltaTransformFn =
    dyn Fn(Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> + Send + Sync;

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

impl DbspGraphBuilder {
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
            mv_latest.insert(view_name.to_string(), (view_frontier, handle));
        }

        let registry_clone = registry_handle.clone();
        let table = {
            let bridge = self.bridge.lock().await;
            bridge.table()
        };
        let cursor = StreamCursor::new(upstream.stream());
        let upstream_frontier = cursor.observed();
        let mut upstream_stream = handle_stream.stream();
        let mut dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>> = HashMap::new();
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
                    table.clone(),
                    &mut dict_cache,
                    Arc::clone(&arrow_schema),
                    consolidation_mode,
                    delta_transform.as_ref(),
                    &delta_handle,
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
                        let state = self.state_from_handle(&flush.handle).await?;
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
                    let state = self.state_from_handle(&flush.handle).await?;
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
            let mut dict_cache = dict_cache;
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
                                table.clone(),
                                &mut dict_cache,
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
                                table.clone(),
                                &mut dict_cache,
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

        let existing_handle = {
            let mut bridge = self.bridge.lock().await;
            let view = bridge
                .new_view(view_name, StreamRetention::KeepLast { keep_last: 1 })
                .await
                .with_context(|| format!("open materialized view '{view_name}' for recovery"))?;
            let mut view_handle_stream = view.handle_stream();
            let view_frontier = view_handle_stream.committed_frontier();
            if view_frontier >= 0 {
                Some((view_frontier, view_handle_stream.get(view_frontier).await?))
            } else {
                None
            }
        };
        let mut existing_frontier = None;
        if let Some((view_frontier, handle)) = existing_handle {
            let state = self.state_from_handle(&handle).await?;
            registry_handle.set_dbsp_state(state);
            registry_handle.publish_version(view_frontier, handle);
            existing_frontier = Some(view_frontier);
        }

        let graph_id = self.graph_id().to_string();
        let view_label = view_name.to_string();
        let task_label = format!("materialize-view:{view_label}");
        let task_events = task_events.clone();
        let cancel = cancel.clone();
        let mut rx = upstream.subscribe();
        tokio::spawn(async move {
            if let Some(frontier) = existing_frontier {
                tracing::info!(
                    graph_id = %graph_id,
                    view = %view_label,
                    version = frontier,
                    "seeded source-journal materialized view overlay from persisted base"
                );
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        if let Err(err) = Self::process_transient_materialize_batch_overlay(
                            batch,
                            Arc::clone(&delta_transform),
                            &registry_handle,
                            &graph_id,
                            &view_label,
                        )
                        .await
                        {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
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
        table: Arc<dyn KeyValueTable>,
        dict_cache: &mut HashMap<String, Arc<Dictionary<Vec<u8>>>>,
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
            table,
            dict_cache,
            row_schema,
            consolidation_mode,
            delta_transform,
            &delta_handle,
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

    async fn process_transient_materialize_batch_overlay(
        batch: TransientSourceBatch,
        delta_transform: Arc<DeltaTransformFn>,
        registry: &Arc<MaterializedViewHandle>,
        graph_id: &str,
        view_label: &str,
    ) -> Result<()> {
        let ts = batch.version;
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
        if merged.is_empty() {
            registry.publish_logical_version(ts);
            return Ok(());
        }
        let apply_stats = registry
            .append_encoded_overlay_batch(u64::try_from(ts.max(0)).unwrap_or(u64::MAX), merged);
        let latency_ms = apply_start.elapsed().as_millis() as u64;
        metrics::observe_mv_update_latency_ms(latency_ms);
        metrics::inc_mv_updates();
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
            let state = Self::state_from_handle_with_bridge(bridge, &flush.handle)
                .await
                .with_context(|| format!("update materialized view '{view_label}' state"))?;
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

    async fn queue_delta_handle_for_view(
        view: &mut DbspView,
        table: Arc<dyn KeyValueTable>,
        dict_cache: &mut HashMap<String, Arc<Dictionary<Vec<u8>>>>,
        _row_schema: SchemaRef,
        _consolidation_mode: ConsolidationMode,
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        delta_handle: &ZSetHandle,
    ) -> Result<DeltaApplyStats> {
        let load_start = Instant::now();
        let mut deltas = delta_zset_handle::<Vec<u8>>(table, dict_cache, delta_handle)
            .await
            .context("materialize delta handle for materialized view")?;
        let load_ms = load_start.elapsed().as_millis() as u64;

        let raw_delta_rows = deltas.len();
        let transform_start = Instant::now();
        if let Some(transform) = delta_transform {
            deltas =
                transform(deltas).context("apply transient transform before materialized view")?;
        }
        let transform_ms = transform_start.elapsed().as_millis() as u64;

        let delta_rows = deltas.len();
        let delta_bytes = deltas
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
            .sum();
        let merge_start = Instant::now();
        if !deltas.is_empty() {
            // Transient segment outputs are already batch-transformed; feed rows straight
            // into MV overlay and let ZSetStream overlay consolidation handle duplicates.
            view.add_deltas(deltas);
        }
        let merge_ms = merge_start.elapsed().as_millis() as u64;
        tracing::debug!(
            delta_handle_version = delta_handle.version,
            raw_delta_rows,
            delta_rows,
            delta_bytes,
            load_ms,
            transform_ms,
            merge_ms,
            "materialized view delta queued"
        );
        Ok(DeltaApplyStats {
            delta_rows,
            delta_bytes,
            load_ms,
            transform_ms,
            merge_ms,
        })
    }

    async fn transform_transient_batch(
        delta_transform: Arc<DeltaTransformFn>,
        batch: TransientSourceBatch,
    ) -> Result<(DeltaApplyStats, Vec<(Vec<u8>, i64)>)> {
        let transform_start = Instant::now();
        let input_rows = batch.deltas.len();
        let input_bytes = batch
            .deltas
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
            .sum();
        let merged = delta_transform(batch.deltas)
            .context("apply transient transform before materialized view")?;
        let transform_ms = transform_start.elapsed().as_millis() as u64;
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
