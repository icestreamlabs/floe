use super::*;

impl LegacyGraphHarness {
    pub(super) fn should_bootstrap_authoritative_zero(
        view_frontier: i64,
        logical_version: Option<u64>,
    ) -> bool {
        if view_frontier != 0 {
            return false;
        }
        logical_version.unwrap_or(0) == 0
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::dbsp_graph_builder) async fn materialize_view(
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
        registry_handle.set_commit_visibility_barrier_enabled(flush_cfg.max_pending_deltas <= 1);
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
                if let Some(trigger) = pending.trigger(flush_cfg, Instant::now())
                    && let Some(flush) = Self::flush_pending_view(
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
            if flush_cfg.flush_on_catchup_boundary
                && let Some(flush) = Self::flush_pending_view(
                    &mut view,
                    &graph_id,
                    &view_namespace,
                    &mut pending,
                    FlushTrigger::CatchupBoundary,
                )
                .await
                .context("flush pending materialized view updates at catchup boundary")?
            {
                let logical_version = u64::try_from(flush.published_ts.max(0)).unwrap_or(u64::MAX);
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
                            if flush_cfg.flush_on_shutdown
                                && let Err(err) = Self::publish_pending_view(
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
                            if flush_cfg.flush_on_shutdown
                                && let Err(err) = Self::publish_pending_view(
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
                            break;
                        }
                        result = cursor.next() => {
                            if let Err(err) = Self::process_materialize_delta(
                                result,
                                &mut view,
                                &mut delta_reader,
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
}
