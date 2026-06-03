use super::*;

impl LegacyGraphHarness {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::dbsp_graph_builder) async fn materialize_view_from_transient_overlay_receiver(
        &mut self,
        view_name: &str,
        schema: Arc<RowSchema>,
        mut upstream: TransientMaterializeReceiver,
        delta_transform: Option<Arc<DeltaTransformFn>>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        mv_registry: &Arc<MaterializedViewRegistry>,
    ) -> Result<()> {
        let registry_handle = mv_registry.register(view_name.to_string());
        registry_handle.set_commit_visibility_barrier_enabled(true);
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
        let (flush_tx, mut flush_rx) =
            mpsc::channel::<OverlaySnapshotFlushRequest>(OVERLAY_SNAPSHOT_FLUSH_CHANNEL_CAPACITY);
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
                                && flush_tx.send(request).await.is_err()
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
                                && flush_tx.send(request).await.is_err()
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
                                    && flush_tx.send(request).await.is_err()
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
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now())
                                && let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).await.is_err()
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
                            if pending_snapshot.should_flush(snapshot_cfg, Instant::now())
                                && let Some(request) = pending_snapshot.take_request("background_threshold")
                                    && flush_tx.send(request).await.is_err()
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
        });
        Ok(())
    }
}
