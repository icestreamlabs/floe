use super::*;

impl DbspGraphBuilder {
    pub(super) async fn process_materialize_delta(
        result: Result<(i64, ZSetHandle)>,
        view: &mut DbspView,
        delta_reader: &mut DeltaZSetHandleReader<Vec<u8>>,
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

    pub(super) async fn process_delta_handle_overlay(
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
            false,
            registry,
            pending_snapshot,
            view_label,
        )
        .context("apply encoded overlay batch for transformed delta handle")
    }

    pub(super) async fn process_transient_materialize_batch_overlay(
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
        let (apply, merged, deltas_consolidated) =
            Self::transform_transient_batch(delta_transform, batch)
                .await
                .with_context(|| {
                    format!(
                        "apply transient source batch for materialized view '{view_label}' at {ts}"
                    )
                })?;
        Self::apply_encoded_overlay_batch(
            apply_start,
            ts,
            apply,
            merged,
            deltas_consolidated,
            registry,
            pending_snapshot,
            view_label,
        )
        .context("apply encoded overlay batch for transient materialization")
    }

    pub(super) fn apply_encoded_overlay_batch(
        apply_start: Instant,
        ts: i64,
        apply: DeltaApplyStats,
        merged: EncodedDeltaBatch,
        deltas_consolidated: bool,
        registry: &Arc<MaterializedViewHandle>,
        pending_snapshot: &mut PendingOverlaySnapshot,
        view_label: &str,
    ) -> Result<()> {
        let ts_u64 = u64::try_from(ts.max(0)).unwrap_or(u64::MAX);
        if merged.is_empty() {
            registry.record_logical_work(
                ts,
                LogicalWorkSnapshot::from_input_delta_rows(apply.delta_rows),
            );
            registry.publish_logical_version(ts);
            registry.advance_authoritative_row_count_version(ts_u64);
            return Ok(());
        }
        let mut work = LogicalWorkSnapshot::from_input_delta_rows(apply.delta_rows);
        work.record_output_delta_rows(merged.len());
        registry.record_logical_work(ts, work);
        let apply_stats = registry.append_shared_encoded_overlay_batch(ts_u64, Arc::clone(&merged));
        if deltas_consolidated {
            registry
                .apply_consolidated_encoded_state_batch(ts_u64, merged.as_slice())
                .with_context(|| {
                    format!(
                        "update overlay state cache for materialized view '{view_label}' at {ts}"
                    )
                })?;
        } else {
            registry
                .apply_encoded_state_batch(ts_u64, merged.as_slice())
                .with_context(|| {
                    format!(
                        "update overlay state cache for materialized view '{view_label}' at {ts}"
                    )
                })?;
        }
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

    pub(super) async fn flush_overlay_snapshot_request(
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

    pub(super) async fn publish_pending_view(
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

    pub(super) async fn load_transformed_delta_handle(
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
            deltas = transform(Arc::new(deltas))
                .await
                .context("apply transient transform before materialized view")?;
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

    pub(super) async fn queue_delta_handle_for_view(
        view: &mut DbspView,
        delta_reader: &mut DeltaZSetHandleReader<Vec<u8>>,
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
            let mut work = LogicalWorkSnapshot::from_input_delta_rows(apply.delta_rows);
            work.record_output_delta_rows(deltas.len());
            registry.record_logical_work(i64::try_from(version).unwrap_or(i64::MAX), work);
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

    pub(super) async fn transform_transient_batch(
        delta_transform: Option<&Arc<DeltaTransformFn>>,
        batch: TransientMaterializeBatch,
    ) -> Result<(DeltaApplyStats, EncodedDeltaBatch, bool)> {
        let input_rows = batch.deltas.len();
        let input_bytes = batch
            .deltas
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
            .sum();
        let (merged, transform_ms) = if let Some(transform) = delta_transform {
            let transform_start = Instant::now();
            let merged = transform(Arc::clone(&batch.deltas))
                .await
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
            delta_transform.is_none() && batch.deltas_consolidated,
        ))
    }

    pub(super) async fn flush_pending_view(
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

    pub(super) async fn state_from_handle(
        &self,
        handle: &ZSetHandle,
    ) -> Result<DbspPersistedState> {
        Self::state_from_handle_with_bridge(&self.bridge, handle).await
    }

    pub(super) async fn state_from_handle_with_bridge(
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
