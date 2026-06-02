use super::*;

pub(super) async fn build_transient_window_count_star_receiver(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_transform: Option<Arc<DeltaTransformFn>>,
    output_projection: Option<TransientWindowCountOutputProjection>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<TransientMaterializeReceiver> {
    let state_label = state_label.into();
    let compact_count_state =
        should_compact_transient_helper_state(&upstream, state_table.as_ref());
    tracing::info!(
        graph_id,
        state_label = %state_label,
        recoverable = upstream.recoverable(),
        helper_state_persistent = state_table.is_some(),
        compact_count_state,
        "configured transient window count-star helper state"
    );
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-window-count-star-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_window_count_star_receiver_from_batches(
        graph_id,
        window,
        upstream_rx,
        output_transform,
        output_projection,
        watermark,
        compact_count_state,
        cancel,
        task_events,
        state_table,
        state_label,
    )
    .await
}

pub(super) async fn build_transient_window_count_star_receiver_from_batches(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    mut upstream_rx: TransientMaterializeReceiver,
    output_transform: Option<Arc<DeltaTransformFn>>,
    output_projection: Option<TransientWindowCountOutputProjection>,
    watermark: Arc<AtomicI64>,
    compact_count_state: bool,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<TransientMaterializeReceiver> {
    let (tx, rx) =
        mpsc::channel::<TransientMaterializeBatch>(TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY);
    let (precompute_evaluator, eval_schema, expression_columns) =
        build_transient_window_count_star_precompute(window)?;
    let group_key_columns = transient_window_direct_group_key_columns(
        window.aggregate.group_keys(),
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window count-star group key columns"))?;
    let time_column = transient_window_resolved_expression_column_index(
        &window.window.time_expression,
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window count-star time column"))?;
    let (window_size, window_slide) = match &window.window.policy {
        dbsp::DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
        dbsp::DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
        dbsp::DbspWindowPolicy::Session { .. } => {
            bail!("SESSION windows are not supported by the transient fixed-window receiver")
        }
    };
    let allowed_lateness_ms = window.window.allowed_lateness_ms;
    let track_evictions = allowed_lateness_ms != i64::MAX;
    let group_key_columns = Arc::new(group_key_columns);
    let window_key_extractor = Arc::new(
        VectorizedEncodedKeyExtractor::new(
            eval_schema.to_arrow_schema(),
            Arc::clone(&group_key_columns),
        )
        .context("build vectorized transient window count-star key extractor")?,
    );
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-window-count-star:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let state_label = if compact_count_state {
        format!("{state_label}_counts")
    } else {
        state_label
    };
    let mut persistent_state =
        PersistentTransientInputState::load(state_table, &graph_id, state_label).await?;
    let restored_deltas = persistent_state.snapshot_deltas();
    tokio::spawn(async move {
        let mut counts: AHashMap<TransientWindowCountKey, i64> = AHashMap::new();
        let mut eviction_schedule: BTreeMap<i64, Vec<TransientWindowCountKey>> = BTreeMap::new();
        let restore_result = if compact_count_state {
            restore_transient_window_count_state(
                restored_deltas,
                &mut counts,
                &mut eviction_schedule,
                track_evictions,
            )
        } else {
            apply_transient_window_count_star_deltas(
                restored_deltas,
                window_key_extractor.as_ref(),
                time_column,
                window_size,
                window_slide,
                transient_window_watermark_cutoff(&watermark, allowed_lateness_ms),
                None,
                &mut counts,
                &mut eviction_schedule,
                track_evictions,
            )
            .map(|_| ())
        };
        if let Err(err) = restore_result {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return;
        }
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
                    if !compact_count_state {
                        if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }
                    let updates = match apply_transient_window_count_star_deltas(
                        input_deltas,
                        window_key_extractor.as_ref(),
                        time_column,
                        window_size,
                        window_slide,
                        transient_window_watermark_cutoff(&watermark, allowed_lateness_ms),
                        output_projection,
                        &mut counts,
                        &mut eviction_schedule,
                        track_evictions,
                    ) {
                        Ok(updates) => updates,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if compact_count_state {
                        let snapshot = match encode_transient_window_count_state(&counts) {
                            Ok(snapshot) => snapshot,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if let Err(err) = persistent_state.replace_with_snapshot(snapshot).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }

                    let encoded_output = match encode_transient_window_count_output_deltas(updates) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    let final_deltas = if let Some(output_transform) = output_transform.as_ref() {
                        match output_transform(Arc::new(encoded_output)).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    } else {
                        encoded_output
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(final_deltas),
                        deltas_consolidated: output_transform.is_none(),
                    }).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    Ok(rx)
}

pub(super) type PrecomputedWindowAggregateRows = VecDeque<dbsp::IncrementalAggregateRow<Vec<u8>>>;

pub(super) fn build_transient_window_incremental_batches(
    keyed_time_batch: VectorizedKeyedTimeBatch,
    row_evaluator: &PrekeyedIncrementalAggregateBatchEvaluator,
    has_group_key: bool,
    window_size: i64,
    window_slide: i64,
    cutoff: Option<i64>,
    persist_inputs: bool,
) -> Result<(
    Vec<((Vec<u8>, Vec<u8>), i64)>,
    PrecomputedWindowAggregateRows,
    Vec<(Vec<u8>, i64)>,
)> {
    let mut windowed_deltas = Vec::new();
    let mut precomputed_rows = PrecomputedWindowAggregateRows::new();
    let mut persisted_window_rows = Vec::new();
    let mut encoded_window_cache: HashMap<(i64, i64), Vec<u8>> = HashMap::new();

    for delta in keyed_time_batch.deltas {
        if delta.diff == 0 || delta.event_ts < 0 {
            continue;
        }
        if let Some(cutoff) = cutoff
            && delta.event_ts < cutoff
        {
            continue;
        }

        let group_key = has_group_key.then_some(delta.key);
        let mut encoded_keys = Vec::new();
        let mut build_error: Option<anyhow::Error> = None;
        transient_window_for_each_window(
            delta.event_ts,
            window_size,
            window_slide,
            |window_start, window_end| {
                if build_error.is_some() {
                    return;
                }
                let encoded_window = match encoded_window_cache.get(&(window_start, window_end)) {
                    Some(encoded) => encoded.clone(),
                    None => match encode_transient_window_bounds(window_start, window_end) {
                        Ok(encoded) => {
                            encoded_window_cache
                                .insert((window_start, window_end), encoded.clone());
                            encoded
                        }
                        Err(err) => {
                            build_error = Some(err);
                            return;
                        }
                    },
                };
                let encoded_key = if let Some(group_key) = group_key.as_ref() {
                    match concat_encoded_rows(&encoded_window, group_key) {
                        Ok(encoded) => encoded,
                        Err(err) => {
                            build_error = Some(err);
                            return;
                        }
                    }
                } else {
                    encoded_window
                };
                encoded_keys.push(encoded_key);
            },
        );
        if let Some(err) = build_error {
            return Err(err);
        }
        if encoded_keys.is_empty() {
            continue;
        }

        let slots = row_evaluator.evaluate_batch_row(
            &keyed_time_batch.batch,
            &keyed_time_batch.input_positions,
            delta.batch_row,
        );
        let last_idx = encoded_keys.len() - 1;
        let mut row = Some(delta.row);
        for (idx, encoded_key) in encoded_keys.into_iter().enumerate() {
            let row_value = if idx == last_idx {
                row.take().expect("transient window row already moved")
            } else {
                row.as_ref().expect("transient window row missing").clone()
            };
            let slot_values = slots.clone();
            let pair = (encoded_key.clone(), row_value);
            if persist_inputs {
                let encoded = encode_transient_window_aggregate_input_pair(&pair.0, &pair.1)?;
                persisted_window_rows.push((encoded, delta.diff));
            }
            precomputed_rows.push_back(dbsp::IncrementalAggregateRow {
                key: encoded_key,
                slots: slot_values,
            });
            windowed_deltas.push((pair, delta.diff));
        }
    }

    Ok((windowed_deltas, precomputed_rows, persisted_window_rows))
}

pub(super) async fn build_transient_window_incremental_receiver(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_transform: Arc<DeltaTransformFn>,
    watermark: Arc<AtomicI64>,
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
        "configured transient window aggregate helper state"
    );
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-window-aggregate-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_window_incremental_receiver_from_batches(
        graph_id,
        window,
        upstream_rx,
        output_transform,
        watermark,
        compact_source_state,
        cancel,
        task_events,
        state_table,
        state_label,
    )
    .await
}

pub(super) async fn build_transient_window_incremental_receiver_from_batches(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    mut upstream_rx: TransientMaterializeReceiver,
    output_transform: Arc<DeltaTransformFn>,
    watermark: Arc<AtomicI64>,
    compact_source_state: bool,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<TransientMaterializeReceiver> {
    let (tx, rx) =
        mpsc::channel::<TransientMaterializeBatch>(TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY);
    let (precompute_evaluator, eval_schema, expression_columns) =
        build_transient_window_aggregate_precompute(window)?;
    let group_key_columns = transient_window_direct_group_key_columns(
        window.aggregate.group_keys(),
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window aggregate group key columns"))?;
    let time_column = transient_window_resolved_expression_column_index(
        &window.window.time_expression,
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window aggregate time column"))?;
    let (window_size, window_slide) = match &window.window.policy {
        dbsp::DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
        dbsp::DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
        dbsp::DbspWindowPolicy::Session { .. } => {
            bail!("SESSION windows are not supported by the transient fixed-window receiver")
        }
    };
    let allowed_lateness_ms = window.window.allowed_lateness_ms;
    let slot_kinds = build_incremental_aggregate_slot_kinds(window.aggregate.aggregates())
        .ok_or_else(|| {
            anyhow!("window aggregate is not eligible for transient incremental aggregation")
        })?;
    let group_key_columns = Arc::new(group_key_columns);
    let window_key_extractor = Arc::new(
        VectorizedEncodedKeyExtractor::new(
            eval_schema.to_arrow_schema(),
            Arc::clone(&group_key_columns),
        )
        .context("build vectorized transient window key extractor")?,
    );
    let prekeyed_evaluator = Arc::new(build_prekeyed_incremental_aggregate_batch_evaluator(
        Arc::clone(&eval_schema),
        window.aggregate.aggregates().to_vec(),
        Arc::clone(&expression_columns),
        graph_id.to_string(),
        "transient_window_aggregate",
    ));
    let precomputed_rows = Arc::new(StdMutex::new(PrecomputedWindowAggregateRows::new()));
    let aggregate_processor = Arc::new(
        dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, (Vec<u8>, Vec<u8>)>::new_batch(
            {
                let prekeyed_evaluator = Arc::clone(&prekeyed_evaluator);
                let precomputed_rows = Arc::clone(&precomputed_rows);
                move |delta_values: &[((Vec<u8>, Vec<u8>), i64)]| {
                    let mut evaluated = Vec::with_capacity(delta_values.len());
                    let mut misses = Vec::new();
                    match precomputed_rows.lock() {
                        Ok(mut precomputed) if precomputed.len() >= delta_values.len() => {
                            for (pair, weight) in delta_values {
                                if let Some(row) = precomputed.pop_front() {
                                    evaluated.push((pair.clone(), row, *weight));
                                } else {
                                    misses.push((pair.clone(), *weight));
                                }
                            }
                        }
                        Ok(_) | Err(_) => {
                            misses.extend(delta_values.iter().cloned());
                        }
                    }
                    if !misses.is_empty() {
                        evaluated.extend(prekeyed_evaluator.evaluate_deltas(&misses));
                    }
                    evaluated
                }
            },
            slot_kinds,
        )
        .await
        .context("initialize transient window incremental aggregate")?,
    );
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-window-aggregate:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let state_label = if compact_source_state {
        format!("{state_label}_incremental_state")
    } else {
        state_label
    };
    let mut persistent_state =
        PersistentTransientInputState::load(state_table, &graph_id, state_label).await?;
    let restored_state = persistent_state.snapshot_deltas();
    if !restored_state.is_empty() {
        if compact_source_state {
            let snapshot = decode_transient_window_incremental_aggregate_snapshot(restored_state)
                .context("decode transient window aggregate state snapshot")?;
            aggregate_processor
                .restore_state(snapshot)
                .await
                .context("restore transient window aggregate state snapshot")?;
        } else {
            let restored_deltas = restored_state
                .into_iter()
                .filter_map(|(row, weight)| {
                    match decode_transient_window_aggregate_input_pair(&row) {
                        Ok(pair) => Some((pair, weight)),
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %graph_id,
                                error = %err,
                                "skipping malformed transient window aggregate input state row"
                            );
                            None
                        }
                    }
                })
                .collect::<Vec<_>>();
            aggregate_processor
                .apply_deltas(restored_deltas)
                .await
                .context("restore transient window aggregate input state")?;
        }
    }
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
                    let cutoff = transient_window_watermark_cutoff(&watermark, allowed_lateness_ms);
                    let keyed_time_batch = match window_key_extractor
                        .extract_keyed_time_batch_with_columns(
                            &input_deltas,
                            time_column,
                            prekeyed_evaluator.required_input_columns(),
                        ) {
                        Ok(batch) => batch,
                        Err(err) => {
                            report_graph_task_error(
                                &task_events,
                                &graph_id,
                                task_label.clone(),
                                err.context("extract vectorized transient window aggregate keys"),
                            );
                            break;
                        }
                    };
                    let (windowed_deltas, evaluated_rows, persisted_window_rows) =
                        match keyed_time_batch {
                            Some(batch) => match build_transient_window_incremental_batches(
                                batch,
                                prekeyed_evaluator.as_ref(),
                                !group_key_columns.is_empty(),
                                window_size,
                                window_slide,
                                cutoff,
                                !compact_source_state,
                            ) {
                                Ok(batches) => batches,
                                Err(err) => {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        err,
                                    );
                                    break;
                                }
                            },
                            None => (Vec::new(), PrecomputedWindowAggregateRows::new(), Vec::new()),
                        };
                    if !compact_source_state {
                        if let Err(err) = persistent_state.apply_deltas(&persisted_window_rows).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }
                    let use_precomputed_rows =
                        windowed_deltas.iter().all(|(_, weight)| *weight >= 0);
                    if let Ok(mut precomputed) = precomputed_rows.lock() {
                        *precomputed = if use_precomputed_rows {
                            evaluated_rows
                        } else {
                            PrecomputedWindowAggregateRows::new()
                        };
                    }
                    let mut aggregate_deltas = match aggregate_processor.apply_deltas(windowed_deltas).await {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            if let Ok(mut precomputed) = precomputed_rows.lock() {
                                precomputed.clear();
                            }
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if let Ok(mut precomputed) = precomputed_rows.lock() {
                        precomputed.clear();
                    }
                    if let Some(cutoff) = cutoff {
                        let evicted = match aggregate_processor
                            .evict_keys_where(|key| match transient_window_encoded_key_end(key) {
                                Ok(end) => end <= cutoff,
                                Err(err) => {
                                    tracing::warn!(
                                        graph_id = %graph_id,
                                        error = %err,
                                        "skipping malformed transient window aggregate key during eviction"
                                    );
                                    false
                                }
                            })
                            .await
                        {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        merge_incremental_aggregate_output_deltas(&mut aggregate_deltas, evicted);
                    }
                    if compact_source_state {
                        let snapshot = match aggregate_processor.snapshot_state().await {
                            Ok(snapshot) => snapshot,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let encoded_snapshot = match encode_transient_window_incremental_aggregate_snapshot(snapshot) {
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
    Ok(rx)
}
