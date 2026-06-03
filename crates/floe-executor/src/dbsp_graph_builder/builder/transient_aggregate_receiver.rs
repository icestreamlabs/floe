use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_build_transient_source_window_aggregate_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceWindowAggregateRootMaterialization>> {
    let Some(shape) = try_build_transient_source_window_aggregate_root_shape(plan, root_idx)?
    else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = build_transient_window_incremental_receiver(
        graph_id,
        &shape.window,
        upstream,
        Arc::clone(&shape.source_root.transform),
        Arc::clone(&shape.transform),
        watermark,
        cancel,
        task_events,
        state_table,
        "source_window_aggregate",
    )
    .await?;
    Ok(Some(TransientSourceWindowAggregateRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
    }))
}

pub(super) async fn try_build_transient_source_aggregate_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceAggregateRootMaterialization>> {
    let Some(shape) = try_build_transient_source_aggregate_root_shape(plan, root_idx)? else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = build_transient_aggregate_receiver(
        graph_id,
        &shape.aggregate,
        upstream,
        Arc::clone(&shape.source_root.transform),
        Arc::clone(&shape.transform),
        cancel,
        task_events,
        state_table,
        "source_aggregate",
    )
    .await?;
    Ok(Some(TransientSourceAggregateRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
    }))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_transient_aggregate_receiver(
    graph_id: &str,
    aggregate: &DbspAggregateNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<TransientMaterializeReceiver> {
    let state_label = state_label.into();
    let compact_source_state =
        should_compact_transient_helper_state(&upstream, state_table.as_ref());
    tracing::info!(
        graph_id,
        state_label = %state_label,
        recoverable = upstream.recoverable(),
        helper_state_persistent = state_table.is_some(),
        compact_source_state,
        "configured transient aggregate helper state"
    );
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-aggregate-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_aggregate_receiver_from_batches(
        graph_id,
        aggregate,
        upstream_rx,
        output_transform,
        // Source-journal deltas are signed ZSet updates. Do not enable
        // append-only aggregate shortcuts without explicit source metadata.
        false,
        compact_source_state,
        cancel,
        task_events,
        state_table,
        state_label,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_transient_aggregate_receiver_from_batches(
    graph_id: &str,
    aggregate: &DbspAggregateNode,
    mut upstream_rx: TransientMaterializeReceiver,
    output_transform: Arc<DeltaTransformFn>,
    append_only_input: bool,
    compact_source_state: bool,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<TransientMaterializeReceiver> {
    let (tx, rx) =
        mpsc::channel::<TransientMaterializeBatch>(TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY);
    let (precompute_evaluator, aggregate_input_schema, aggregate_expression_columns) =
        build_transient_aggregate_precompute(aggregate)?;
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-aggregate:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let debug_transient_join = tracing::enabled!(tracing::Level::DEBUG);
    if aggregate
        .aggregates()
        .iter()
        .all(|agg| agg.function() == &dbsp::DbspAggregateFunction::Count)
    {
        let slot_kinds = build_count_aggregate_slot_kinds(aggregate.aggregates());
        let row_evaluator = build_count_batch_row_evaluator(
            Arc::clone(&aggregate_input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&aggregate_expression_columns),
            graph_id.clone(),
            "transient_count_aggregate",
        );
        let aggregate_processor = Arc::new(
            dbsp::DbspTransientCountAggregate::<Vec<u8>, Vec<u8>, Vec<u8>>::new_batch(
                row_evaluator,
                slot_kinds,
            )
            .await
            .context("initialize transient count aggregate")?,
        );
        let count_state_label = if compact_source_state {
            format!("{state_label}_count_state")
        } else {
            state_label.clone()
        };
        let mut persistent_state =
            PersistentTransientInputState::load(state_table.clone(), &graph_id, count_state_label)
                .await?;
        let restored_deltas = persistent_state.snapshot_deltas();
        if !restored_deltas.is_empty() {
            if compact_source_state {
                let snapshot = decode_transient_count_aggregate_snapshot(restored_deltas)
                    .context("decode transient count aggregate state snapshot")?;
                aggregate_processor.restore_state(snapshot).await;
            } else {
                aggregate_processor
                    .apply_deltas(restored_deltas)
                    .await
                    .context("restore transient count aggregate input state")?;
            }
        }
        let precompute_evaluator = precompute_evaluator.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_source_state
                            && let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        let aggregate_deltas = match aggregate_processor.apply_deltas(input_deltas).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_source_state {
                            let snapshot = aggregate_processor.snapshot_state().await;
                            let encoded_snapshot = match encode_transient_count_aggregate_snapshot(snapshot) {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            if let Err(err) = persistent_state.replace_with_snapshot(encoded_snapshot).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let encoded_output = match encode_count_aggregate_output_deltas(aggregate_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let final_deltas = match output_transform(Arc::new(encoded_output)).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if debug_transient_join {
                            tracing::debug!(
                                graph_id = %graph_id,
                                version = batch.version,
                                rows = final_deltas.len(),
                                "transient aggregate output"
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(final_deltas),
                            deltas_consolidated: false,
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    } else {
        let slot_kinds = build_incremental_aggregate_slot_kinds(aggregate.aggregates())
            .ok_or_else(|| {
                anyhow!("aggregate is not eligible for transient incremental aggregation")
            })?;
        let row_evaluator = build_incremental_aggregate_batch_row_evaluator(
            Arc::clone(&aggregate_input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&aggregate_expression_columns),
            graph_id.clone(),
            "transient_aggregate",
        );
        let aggregate_processor = Arc::new(
            dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, Vec<u8>>::new_batch(
                row_evaluator,
                slot_kinds,
            )
            .await
            .context("initialize transient incremental aggregate")?,
        );
        if append_only_input {
            aggregate_processor.enable_append_only_input().await;
        }
        let incremental_state_label = if compact_source_state {
            format!("{state_label}_incremental_state")
        } else {
            state_label.clone()
        };
        let mut persistent_state = PersistentTransientInputState::load(
            state_table.clone(),
            &graph_id,
            incremental_state_label,
        )
        .await?;
        let restored_deltas = persistent_state.snapshot_deltas();
        if !restored_deltas.is_empty() {
            if compact_source_state {
                let snapshot = decode_transient_incremental_aggregate_snapshot(restored_deltas)
                    .context("decode transient incremental aggregate state snapshot")?;
                aggregate_processor
                    .restore_state(snapshot)
                    .await
                    .context("restore transient incremental aggregate state snapshot")?;
            } else {
                aggregate_processor
                    .apply_deltas(restored_deltas)
                    .await
                    .context("restore transient incremental aggregate input state")?;
            }
        }
        let precompute_evaluator = precompute_evaluator.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_source_state && let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let aggregate_deltas = match aggregate_processor.apply_deltas(input_deltas).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_source_state {
                            let snapshot = match aggregate_processor.snapshot_state().await {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            let encoded_snapshot = match encode_transient_incremental_aggregate_snapshot(snapshot) {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            if let Err(err) = persistent_state.replace_with_snapshot(encoded_snapshot).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let encoded_output = match encode_incremental_aggregate_output_deltas(aggregate_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let final_deltas = match output_transform(Arc::new(encoded_output)).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if debug_transient_join {
                            tracing::debug!(
                                graph_id = %graph_id,
                                version = batch.version,
                                rows = final_deltas.len(),
                                "transient aggregate output"
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(final_deltas),
                            deltas_consolidated: false,
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    Ok(rx)
}
