use super::*;

mod buffers;
mod checkpoint;
mod context;

use buffers::ExecutorTickBuffers;
use checkpoint::{
    CdcOnlyTickCommit, ExecutorCheckpointState, FinalCheckpoint, IngestMetrics,
    PersistTickCheckpoint, apply_decoded_source_batches, build_kafka_metadata_journal_batches,
    build_source_journal_batches, notify_kafka_commit_senders, persist_cdc_only_tick_commit,
    persist_final_checkpoint_unless_failed, persist_tick_checkpoint, publish_watermark_debug_state,
    record_fatal_source_batch_failure, record_fatal_tick_failure, record_ingest_queue_metrics,
    update_checkpoint_source_offsets, wait_for_tick_materialized_views,
};
pub(super) use context::{
    ExecutorBatchLimits, ExecutorCdcContext, ExecutorCheckpointContext, ExecutorIngestContext,
    ExecutorRuntimeContext, ExecutorSourceContext, ExecutorTaskContext,
};

pub(super) fn spawn_executor_task(context: ExecutorTaskContext) -> JoinHandle<()> {
    let ExecutorTaskContext {
        runtime,
        sources,
        cdc,
        ingest,
        checkpoint,
        limits,
    } = context;
    let ExecutorRuntimeContext {
        outer_registry,
        event_watermark,
        mv_registry,
        vectorized_runtime,
        runtime_cancel,
        executor_running,
        runtime_failure,
    } = runtime;
    let ExecutorSourceContext {
        active_source_definitions_by_id,
        materialized_source_ids,
        source_names_by_id,
        source_id_by_name,
        definitions,
        kafka_metadata_journal_source_ids,
        source_journal_required_sources,
    } = sources;
    let ExecutorCdcContext {
        cdc_table_store,
        cdc_schemas_by_source_id,
        cdc_stateful_table_ids_by_source_id,
        cdc_transaction_receiver,
        cdc_replication_debug,
        postgres_cdc_commit_senders,
        storage,
        replication_pipeline_runtime,
    } = cdc;
    let ExecutorIngestContext {
        connector_receiver,
        connector_queues,
        kafka_commit_senders,
        pending_event_counter,
    } = ingest;
    let ExecutorCheckpointContext {
        sink_checkpoint_rx,
        checkpoint_manager,
        tracked_mv_names,
        watermark_debug,
        watermark_idle_source_ms,
        pre_tick_commit_delay_ms,
    } = checkpoint;
    let ExecutorBatchLimits {
        max_batch,
        max_batch_per_source,
        max_batch_per_connector,
    } = limits;

    let outer_for_task = Arc::clone(&outer_registry);
    let cdc_table_store_for_task = cdc_table_store.clone();
    let cdc_schemas_by_source_id_for_task = Arc::clone(&cdc_schemas_by_source_id);
    let cdc_stateful_table_ids_by_source_id_for_task =
        Arc::clone(&cdc_stateful_table_ids_by_source_id);
    let active_source_definitions_by_id_for_task = Arc::clone(&active_source_definitions_by_id);
    let materialized_source_ids_for_task = Arc::clone(&materialized_source_ids);
    let source_names_by_id_for_task = Arc::clone(&source_names_by_id);
    let watermark_for_task = Arc::clone(&event_watermark);
    let mv_for_task = Arc::clone(&mv_registry);
    let kafka_commit_senders_for_task = kafka_commit_senders;
    let postgres_cdc_commit_senders_for_task = postgres_cdc_commit_senders;
    let mut sink_checkpoint_rx_for_task = sink_checkpoint_rx;
    const MAX_SINK_CURSOR_UPDATES_PER_ITER: usize = 4096;
    let watermark_debug_for_task = Arc::clone(&watermark_debug);
    let cdc_replication_debug_for_task = Arc::clone(&cdc_replication_debug);
    let executor_running_for_task = Arc::clone(&executor_running);
    let failure_for_executor = Arc::clone(&runtime_failure);
    let kafka_metadata_journal_source_ids_for_task = Arc::clone(&kafka_metadata_journal_source_ids);
    let source_journal_required_sources_for_task = Arc::clone(&source_journal_required_sources);
    let source_id_by_name_for_task = source_id_by_name;
    let storage_for_replication_task = storage.clone();
    let replication_pipeline_runtime_for_task = Arc::clone(&replication_pipeline_runtime);
    let mut connector_receiver_for_task = connector_receiver;
    let mut cdc_transaction_receiver_for_task = cdc_transaction_receiver;
    let mut vectorized_runtime_for_task = vectorized_runtime;
    let executor_cancel = runtime_cancel.clone();
    let executor_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut connector_queues = connector_queues;
        let mut cdc_transaction_queue = VecDeque::new();
        let mut checkpoint_manager = checkpoint_manager;
        let mut next_connector = 0usize;
        let mut checkpoint_state = ExecutorCheckpointState::new(&tracked_mv_names);
        let mut source_watermarks: HashMap<String, i64> = HashMap::new();
        let mut source_last_seen_at: HashMap<String, Instant> = HashMap::new();
        let watermark_idle_timeout = Duration::from_millis(watermark_idle_source_ms);
        let executor_loop_started = Instant::now();
        let mut first_nonempty_decode_logged = false;
        let mut first_tick_commit_logged = false;
        let mut tick_buffers = ExecutorTickBuffers::new(
            active_source_definitions_by_id_for_task.as_slice(),
            max_batch_per_source,
        );
        checkpoint_state.restore_latest_commit(&checkpoint_manager, &watermark_for_task);
        'executor: loop {
            for _ in 0..MAX_SINK_CURSOR_UPDATES_PER_ITER {
                match sink_checkpoint_rx_for_task.try_recv() {
                    Ok(cursor) => {
                        checkpoint_manager.update_sink_cursor(
                            &cursor.sink,
                            &cursor.mv_name,
                            cursor.last_emitted_mv_version,
                            cursor.row_index,
                        );
                    }
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                }
            }
            checkpoint_state.record_periodic_metrics();
            if executor_cancel.is_cancelled() {
                break;
            }
            if connector_queues.is_empty() && cdc_transaction_queue.is_empty() {
                break;
            }
            if connector_queues
                .iter()
                .all(|queue| queue.pending.is_empty())
                && cdc_transaction_queue.is_empty()
            {
                let has_events = loop {
                    let connector_receiver_active = !connector_receiver_for_task.is_closed();
                    let cdc_receiver_active = !cdc_schemas_by_source_id_for_task.is_empty()
                        && !cdc_transaction_receiver_for_task.is_closed();
                    match (connector_receiver_active, cdc_receiver_active) {
                        (false, false) => break false,
                        (true, false) => {
                            break tokio::select! {
                                _ = executor_cancel.cancelled() => false,
                                has_events = recv_from_ready(&mut connector_receiver_for_task, &mut connector_queues) => has_events,
                            };
                        }
                        (false, true) => {
                            break tokio::select! {
                                _ = executor_cancel.cancelled() => false,
                                has_events = recv_cdc_from_ready(
                                    &mut cdc_transaction_receiver_for_task,
                                    &mut cdc_transaction_queue,
                                ) => has_events,
                            };
                        }
                        (true, true) => {
                            let has_events = tokio::select! {
                                _ = executor_cancel.cancelled() => false,
                                has_events = recv_cdc_from_ready(
                                    &mut cdc_transaction_receiver_for_task,
                                    &mut cdc_transaction_queue,
                                ) => has_events,
                                has_events = recv_from_ready(&mut connector_receiver_for_task, &mut connector_queues) => has_events,
                            };
                            if has_events {
                                break true;
                            }
                        }
                    }
                };
                if !has_events {
                    break;
                }
            }
            drain_ready(&mut connector_receiver_for_task, &mut connector_queues);
            if !cdc_schemas_by_source_id_for_task.is_empty() {
                drain_cdc_ready(
                    &mut cdc_transaction_receiver_for_task,
                    &mut cdc_transaction_queue,
                );
            }

            let pending_epoch = checkpoint_state.epoch.saturating_add(1);
            let source_count = source_names_by_id_for_task.len();
            let decode_start = Instant::now();
            let mut tick_commit_acks = Vec::new();
            tick_buffers.reset_for_tick();
            let decoded_counts = &mut tick_buffers.decoded_counts;
            let tick_source_offsets = &mut tick_buffers.tick_source_offsets;
            let tick_kafka_offsets = &mut tick_buffers.tick_kafka_offsets;
            let tick_kafka_source_ranges = &mut tick_buffers.tick_kafka_source_ranges;
            let tick_postgres_lsns = &mut tick_buffers.tick_postgres_lsns;
            let tick_postgres_sources = &mut tick_buffers.tick_postgres_sources;
            let tick_postgres_table_lsns = &mut tick_buffers.tick_postgres_table_lsns;
            let tick_source_max_event_ts = &mut tick_buffers.tick_source_max_event_ts;
            let arrow_batches_by_source = &mut tick_buffers.arrow_batches_by_source;
            let weighted_arrow_batches_by_source =
                &mut tick_buffers.weighted_arrow_batches_by_source;
            let vectorized_source_journal_batches =
                &mut tick_buffers.vectorized_source_journal_batches;
            let arrow_builders_by_source = &mut tick_buffers.arrow_builders_by_source;
            let commit_acks_by_source = &mut tick_buffers.commit_acks_by_source;
            let mut cdc_staged_writes = None::<WriteBatch>;
            let mut per_connector_counts = vec![0usize; connector_queues.len()];
            let batch_len: usize;
            let mut decoded_rows_len = 0usize;

            if let Some(cdc_transaction) = cdc_transaction_queue.pop_front() {
                batch_len = 1;
                tracing::debug!(
                    source = %cdc_transaction.source_id.as_str(),
                    slot = %cdc_transaction.slot,
                    change_batches = cdc_transaction.transaction.change_batches().len(),
                    changes = cdc_transaction
                        .transaction
                        .change_batches()
                        .iter()
                        .map(ChangeBatch::change_count)
                        .sum::<usize>(),
                    commit_position = ?cdc_transaction.transaction.commit_position(),
                    "executor applying native CDC transaction"
                );
                let Some(schemas) =
                    cdc_schemas_by_source_id_for_task.get(&cdc_transaction.source_id)
                else {
                    let message = format!(
                        "received native CDC transaction for unknown source '{}'",
                        cdc_transaction.source_id.as_str()
                    );
                    tracing::error!("{message}");
                    record_runtime_failure(&failure_for_executor, message);
                    executor_cancel.cancel();
                    break 'executor;
                };
                let cdc_transaction_batch = match cdc_table_store_for_task
                    .complete_unchanged_toast(schemas, &cdc_transaction.transaction)
                    .await
                {
                    Ok(transaction) => transaction,
                    Err(err) => {
                        let message = format!(
                            "failed to complete native CDC unchanged TOAST values for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                };
                let stateful_table_ids = cdc_stateful_table_ids_by_source_id_for_task
                    .get(&cdc_transaction.source_id)
                    .cloned()
                    .unwrap_or_default();
                let stateful_transaction = match materialized_transaction(
                    &cdc_transaction.source_id,
                    &stateful_table_ids,
                    &cdc_transaction_batch,
                ) {
                    Ok(transaction) => transaction,
                    Err(err) => {
                        let message = format!(
                            "failed to split native CDC state transaction for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                };
                let mut staged_writes = WriteBatch::new();
                let mut apply_result = None;
                if let Some(transaction) = stateful_transaction.as_ref() {
                    apply_result = match cdc_table_store_for_task
                        .stage_transaction(schemas, transaction, &mut staged_writes)
                        .await
                    {
                        Ok(result) => Some(result),
                        Err(err) => {
                            let message = format!(
                                "failed to stage native CDC transaction for source '{}': {err}",
                                cdc_transaction.source_id.as_str()
                            );
                            tracing::error!(error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    };
                }
                let pipeline_records = if replication_pipeline_runtime_for_task
                    .has_pipelines_for_source(&cdc_transaction.source_id)
                {
                    match replication_pipeline_runtime_for_task
                        .run_transaction(
                            &cdc_transaction.source_id,
                            schemas,
                            &cdc_transaction_batch,
                            Some(&storage_for_replication_task),
                        )
                        .await
                    {
                        Ok(records) => records,
                        Err(err) => {
                            let message = format!(
                                "failed to run replication pipelines for source '{}': {err}",
                                cdc_transaction.source_id.as_str()
                            );
                            tracing::error!(error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    }
                } else {
                    0
                };
                let feedback_position = apply_result
                    .as_ref()
                    .map(|result| result.checkpoint().position())
                    .unwrap_or_else(|| cdc_transaction_batch.commit_position());
                let feedback_lsn = match PostgresLsn::from_source_position(feedback_position) {
                    Ok(lsn) => lsn,
                    Err(err) => {
                        let message = format!(
                            "failed to derive native CDC feedback LSN for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                };
                if stateful_transaction.is_none() && pipeline_records > 0 {
                    let checkpoint = pipeline_checkpoint_from_transaction(&cdc_transaction_batch);
                    if let Err(err) = cdc_table_store_for_task
                        .commit_checkpoint(&checkpoint)
                        .await
                    {
                        let message = format!(
                            "failed to commit replication-only CDC checkpoint for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                }
                tick_postgres_lsns.insert(
                    cdc_transaction.slot.clone(),
                    (feedback_lsn.as_u64(), feedback_lsn.to_pg_string()),
                );
                tick_postgres_sources.insert(
                    cdc_transaction.slot.clone(),
                    cdc_transaction.source_id.as_str().to_string(),
                );
                for change_batch in cdc_transaction_batch.change_batches() {
                    tick_postgres_table_lsns.push((
                        cdc_transaction.source_id.as_str().to_string(),
                        cdc_transaction.slot.clone(),
                        change_batch.table_id().as_str().to_string(),
                        feedback_lsn.as_u64(),
                    ));
                }
                if stateful_transaction.is_none() {
                    if pipeline_records > 0 {
                        record_postgres_cdc_lsn_progress(
                            &mut checkpoint_state.committed_postgres_lsns,
                            tick_postgres_lsns,
                            tick_postgres_sources,
                            tick_postgres_table_lsns,
                            &cdc_replication_debug_for_task,
                        );
                        notify_postgres_cdc_commit_senders(
                            checkpoint_state.epoch,
                            &checkpoint_state.committed_postgres_lsns,
                            tick_postgres_lsns,
                            &postgres_cdc_commit_senders_for_task,
                        );
                        metrics::record_checkpoint_age_seconds(0);
                        checkpoint_state.last_checkpoint_commit_at = Instant::now();
                    }
                    continue;
                }
                let apply_result = apply_result
                    .as_ref()
                    .expect("materialized transaction should produce apply result");
                for table_deltas in apply_result.table_deltas() {
                    let source_name = table_deltas.table_id().as_str();
                    let Some(source_id) = source_id_by_name_for_task.get(source_name).copied()
                    else {
                        tracing::debug!(
                            source = %source_name,
                            "dropping native CDC state delta for table outside DBSP source registry"
                        );
                        continue;
                    };
                    if !materialized_source_ids_for_task
                        .get(source_id)
                        .copied()
                        .unwrap_or(false)
                    {
                        tracing::debug!(
                            source = %source_name,
                            "dropping native CDC deltas for source outside active materialization set"
                        );
                        continue;
                    }
                    let Some(definition) = definitions.get(source_id) else {
                        let message =
                            format!("received CDC deltas for unknown source '{source_name}'");
                        tracing::error!(source = %source_name, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    };
                    match CdcArrowDeltaBatch::from_table_deltas(definition, table_deltas).and_then(
                        |arrow_delta| {
                            let weighted_schema =
                                floe_executor::delta_consolidation::weighted_snapshot_schema(
                                    &definition.to_arrow_schema(),
                                )?;
                            weighted_batch_from_diffs(
                                arrow_delta.record_batch(),
                                &weighted_schema,
                                arrow_delta.diffs(),
                            )
                            .map(|batch| (arrow_delta.len(), batch))
                        },
                    ) {
                        Ok((row_count, batch)) => {
                            decoded_counts[source_id] =
                                decoded_counts[source_id].saturating_add(row_count);
                            decoded_rows_len = decoded_rows_len.saturating_add(row_count);
                            weighted_arrow_batches_by_source[source_id].push(batch);
                        }
                        Err(err) => {
                            let message = format!(
                                "failed to build native CDC Arrow deltas for source '{source_name}': {err}"
                            );
                            tracing::error!(source = %source_name, error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    }
                }
                if !apply_result.already_committed() {
                    cdc_staged_writes = Some(staged_writes);
                }
            } else {
                let selection = build_batch(
                    &mut connector_queues,
                    &source_id_by_name_for_task,
                    source_count,
                    next_connector,
                    max_batch,
                    max_batch_per_source,
                    max_batch_per_connector,
                    &pending_event_counter,
                );
                let BatchSelection {
                    batch,
                    per_connector_counts: selected_per_connector_counts,
                } = selection;
                per_connector_counts = selected_per_connector_counts;

                if batch.is_empty() {
                    continue;
                }

                next_connector = if connector_queues.is_empty() {
                    0
                } else {
                    (next_connector + 1) % connector_queues.len()
                };

                batch_len = batch.len();
                let decode_span = tracing::debug_span!(
                    "ingest_decode",
                    epoch = pending_epoch,
                    raw_batch_size = batch_len
                );
                let _decode_guard = decode_span.enter();
                for SelectedAppendIngestEvent {
                    source_id,
                    event,
                    commit_ack,
                } in batch
                {
                    let Some(source_id) = source_id else {
                        let source_name = event.source().to_string();
                        tracing::debug!(
                            source = %source_name,
                            "dropping event for unknown source"
                        );
                        if let Some(ack) = commit_ack {
                            ack.record_failed(format!("unknown source '{source_name}'"))
                                .await;
                        }
                        continue;
                    };
                    let source_name = source_names_by_id_for_task[source_id].as_str();
                    let kafka_position = event_fast_kafka_offset(&event)
                        .or_else(|| event_kafka_offset(event.resume_token()));
                    if let Some((partition, offset)) = event_fast_resume_offset(&event)
                        .or_else(|| event_resume_offset(event.resume_token()))
                    {
                        let entry = tick_source_offsets[source_id]
                            .get_or_insert_with(HashMap::new)
                            .entry(partition)
                            .or_insert(0);
                        *entry = (*entry).max(offset);
                    }
                    if let Some((topic, partition, offset)) = kafka_position.clone() {
                        let entry = tick_kafka_offsets.entry((topic, partition)).or_insert(0);
                        *entry = (*entry).max(offset);
                    }
                    if !materialized_source_ids_for_task
                        .get(source_id)
                        .copied()
                        .unwrap_or(false)
                    {
                        tracing::debug!(
                            source = %source_name,
                            "dropping event for source outside active materialization set"
                        );
                        if let Some(ack) = commit_ack {
                            ack.record_failed(format!(
                                "source '{source_name}' is outside the active materialization set"
                            ))
                            .await;
                        }
                        continue;
                    }
                    let Some(builder) = arrow_builders_by_source
                        .get_mut(source_id)
                        .and_then(Option::as_mut)
                    else {
                        let message = format!("received event for unknown source '{source_name}'");
                        tracing::error!(source = %source_name, "{message}");
                        if let Some(ack) = commit_ack {
                            ack.record_failed(message.clone()).await;
                        }
                        record_fatal_source_batch_failure(
                            commit_acks_by_source,
                            &failure_for_executor,
                            &executor_cancel,
                            message,
                        )
                        .await;
                        break 'executor;
                    };
                    let event_ts = match builder.append_event(&event) {
                        Ok(event_ts) => event_ts,
                        Err(err) => {
                            tracing::warn!(
                                source = %source_name,
                                error = %err,
                                "failed to decode append ingest event into Arrow"
                            );
                            if let Some(ack) = commit_ack {
                                ack.record_failed(format!(
                                    "failed to decode append ingest event for '{source_name}': {err}"
                                ))
                                .await;
                            }
                            continue;
                        }
                    };
                    if kafka_metadata_journal_source_ids_for_task.contains(&source_id)
                        && let Some((topic, partition, offset)) = kafka_position.clone()
                    {
                        observe_kafka_source_journal_event(
                            &mut tick_kafka_source_ranges[source_id],
                            topic,
                            partition,
                            offset,
                            &event,
                        );
                    }
                    let event_ts = event_ts.or(event.event_time_ms());
                    if let Some(ts) = event_ts {
                        let ts_i64 = i64::try_from(ts).unwrap_or(i64::MAX);
                        let entry = tick_source_max_event_ts[source_id].get_or_insert(i64::MIN);
                        *entry = (*entry).max(ts_i64);
                    }
                    decoded_counts[source_id] = decoded_counts[source_id].saturating_add(1);
                    if let Some(ack) = commit_ack {
                        commit_acks_by_source[source_id].push(ack);
                    }
                }
                for (source_id, builder) in arrow_builders_by_source.iter_mut().enumerate() {
                    let Some(builder) = builder.as_mut() else {
                        continue;
                    };
                    match builder.finish() {
                        Ok(Some(batch)) => {
                            decoded_rows_len = decoded_rows_len.saturating_add(batch.num_rows());
                            arrow_batches_by_source[source_id].push(batch);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            let source_name = source_names_by_id_for_task[source_id].as_str();
                            tracing::error!(
                                source = %source_name,
                                error = %err,
                                "failed to finish Arrow ingest batch"
                            );
                            record_fatal_source_batch_failure(
                                commit_acks_by_source,
                                &failure_for_executor,
                                &executor_cancel,
                                format!(
                                    "failed to finish Arrow ingest batch for '{source_name}': {err}"
                                ),
                            )
                            .await;
                            break 'executor;
                        }
                    }
                }
                if decoded_rows_len == 0 {
                    continue;
                }
            }
            let decode_latency_ms = decode_start.elapsed().as_millis() as u64;
            metrics::observe_decode_latency_ms(decode_latency_ms);
            metrics::observe_tick_phase_latency_ms("decode", decode_latency_ms);
            if !first_nonempty_decode_logged {
                first_nonempty_decode_logged = true;
                tracing::info!(
                    epoch = pending_epoch,
                    batch_size = batch_len,
                    decoded_rows = decoded_rows_len,
                    decode_latency_ms,
                    time_to_first_nonempty_decode_ms =
                        executor_loop_started.elapsed().as_millis() as u64,
                    "executor decoded first non-empty ingest batch"
                );
            }
            tracing::debug!(
                decoded_rows = decoded_rows_len,
                latency_ms = decode_latency_ms,
                "decoded ingest batch"
            );
            if decoded_rows_len == 0 {
                if (!tick_postgres_lsns.is_empty() || cdc_staged_writes.is_some())
                    && let Err(err) = persist_cdc_only_tick_commit(CdcOnlyTickCommit {
                        checkpoint_manager: &mut checkpoint_manager,
                        state: &mut checkpoint_state,
                        pending_epoch,
                        mv_registry: &mv_for_task,
                        watermark: &watermark_for_task,
                        cdc_staged_writes,
                        tick_postgres_lsns,
                        tick_postgres_sources,
                        tick_postgres_table_lsns,
                        cdc_replication_debug: &cdc_replication_debug_for_task,
                        postgres_cdc_commit_senders: &postgres_cdc_commit_senders_for_task,
                    })
                    .await
                {
                    tracing::error!(
                        epoch = pending_epoch,
                        error = %err,
                        "failed to persist CDC-only tick commit"
                    );
                    record_runtime_failure(
                        &failure_for_executor,
                        format!("failed to persist CDC-only tick commit {pending_epoch}: {err}"),
                    );
                    executor_cancel.cancel();
                    break 'executor;
                }
                continue;
            }
            let changed = match apply_decoded_source_batches(
                &mut vectorized_runtime_for_task,
                &source_names_by_id_for_task,
                arrow_batches_by_source,
                weighted_arrow_batches_by_source,
                commit_acks_by_source,
            )
            .await
            {
                Ok(changed) => changed,
                Err(err) => {
                    let message = format!("failed to apply decoded source batches: {err}");
                    tracing::error!(epoch = pending_epoch, error = %err, "{message}");
                    record_fatal_source_batch_failure(
                        commit_acks_by_source,
                        &failure_for_executor,
                        &executor_cancel,
                        message,
                    )
                    .await;
                    break 'executor;
                }
            };
            if !changed {
                continue;
            }
            if let Err(err) = build_source_journal_batches(
                &source_names_by_id_for_task,
                &definitions,
                &source_journal_required_sources_for_task,
                arrow_batches_by_source,
                weighted_arrow_batches_by_source,
                tick_source_max_event_ts,
                vectorized_source_journal_batches,
            ) {
                let message = err.to_string();
                tracing::error!(error = %err, "{message}");
                record_fatal_source_batch_failure(
                    commit_acks_by_source,
                    &failure_for_executor,
                    &executor_cancel,
                    message,
                )
                .await;
                break 'executor;
            }
            for acks in commit_acks_by_source.iter_mut() {
                tick_commit_acks.append(acks);
            }
            let kafka_metadata_journal_batches = build_kafka_metadata_journal_batches(
                &source_names_by_id_for_task,
                &kafka_metadata_journal_source_ids_for_task,
                tick_source_max_event_ts,
                tick_kafka_source_ranges,
            );
            checkpoint_state.epoch = pending_epoch;
            let epoch = checkpoint_state.epoch;
            let now_instant = Instant::now();
            for (source_id, max_event_ts) in tick_source_max_event_ts.iter().enumerate() {
                let Some(max_event_ts) = *max_event_ts else {
                    continue;
                };
                let source = source_names_by_id_for_task[source_id].clone();
                let watermark_entry = source_watermarks.entry(source.clone()).or_insert(i64::MIN);
                *watermark_entry = (*watermark_entry).max(max_event_ts);
                metrics::record_source_watermark_ms(&source, *watermark_entry);
                source_last_seen_at.insert(source, now_instant);
            }
            let prev_watermark = watermark_for_task.load(Ordering::Relaxed);
            let global_candidate = compute_global_watermark(
                &source_watermarks,
                &source_last_seen_at,
                now_instant,
                watermark_idle_timeout,
            );
            let next_watermark = advance_global_watermark(prev_watermark, global_candidate);
            let tick_start = Instant::now();
            let tick_span = tracing::info_span!(
                "connector_tick",
                epoch,
                watermark = watermark_for_task.load(Ordering::Relaxed),
            );
            let _tick_guard = tick_span.enter();
            if epoch <= 8 || epoch.is_multiple_of(128) {
                tracing::info!(
                    epoch,
                    batch_size = batch_len,
                    decoded_rows = decoded_rows_len,
                    "tick begin"
                );
            }
            if pre_tick_commit_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(pre_tick_commit_delay_ms)).await;
            }
            let tick_all_start = Instant::now();
            if let Err(err) = vectorized_runtime_for_task
                .run_tick(i64::try_from(epoch).unwrap_or(i64::MAX))
                .await
            {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                tracing::error!(epoch, error = %err, "failed to run vectorized materialization tick");
                record_fatal_tick_failure(
                    tick_commit_acks,
                    &failure_for_executor,
                    &executor_cancel,
                    format!("failed to run vectorized materialization tick {epoch}: {err}"),
                )
                .await;
                metrics::inc_ingest_tick("error");
                break 'executor;
            } else if should_sample(&TICK_LOG_COUNTER, TICK_LOG_SAMPLE_EVERY) {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                tracing::debug!(epoch, "completed vectorized materialization tick");
                metrics::inc_ingest_tick("ok");
            } else {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                metrics::inc_ingest_tick("ok");
            }
            let state_write_latency_ms = tick_all_start.elapsed().as_millis() as u64;
            if epoch <= 8 || epoch.is_multiple_of(128) {
                tracing::info!(epoch, state_write_latency_ms, "tick state_write completed");
            }

            tick_commit_acks = match wait_for_tick_materialized_views(
                &mv_for_task,
                epoch,
                &executor_cancel,
                tick_commit_acks,
                &failure_for_executor,
            )
            .await
            {
                Some(acks) => acks,
                None => break 'executor,
            };

            checkpoint_state
                .record_latest_source_offset_lag(&source_names_by_id_for_task, tick_source_offsets);
            update_checkpoint_source_offsets(
                &mut checkpoint_manager,
                &source_names_by_id_for_task,
                tick_source_offsets,
            );
            let frontier = next_watermark.max(0).try_into().unwrap_or(0_u64);
            let mv_versions = collect_mv_versions_for_commit(
                &mv_for_task,
                &mut checkpoint_state.last_mv_versions,
            );
            let mut next_committed_kafka_offsets = checkpoint_state.committed_kafka_offsets.clone();
            advance_kafka_offset_commit_state(
                &mut next_committed_kafka_offsets,
                tick_kafka_offsets,
            );
            let persisted_checkpoint = match persist_tick_checkpoint(PersistTickCheckpoint {
                checkpoint_manager: &mut checkpoint_manager,
                epoch,
                frontier,
                mv_versions: &mv_versions,
                next_committed_kafka_offsets: &next_committed_kafka_offsets,
                source_names_by_id: &source_names_by_id_for_task,
                vectorized_source_journal_batches: vectorized_source_journal_batches.as_slice(),
                kafka_metadata_journal_batches: &kafka_metadata_journal_batches,
                cdc_staged_writes,
            })
            .await
            {
                Ok(result) => result,
                Err(err) => {
                    tracing::error!(epoch, error = %err, "failed to persist tick commit");
                    for ack in tick_commit_acks {
                        ack.record_failed(format!("failed to persist tick commit {epoch}: {err}"))
                            .await;
                    }
                    record_runtime_failure(
                        &failure_for_executor,
                        format!("failed to persist tick commit {epoch}: {err}"),
                    );
                    executor_cancel.cancel();
                    break;
                }
            };
            let checkpoint_write_latency_ms = persisted_checkpoint.checkpoint_write_latency_ms;
            if epoch <= 8 || epoch.is_multiple_of(128) {
                tracing::info!(
                    epoch,
                    checkpoint_write_latency_ms,
                    "tick checkpoint_write completed"
                );
            }
            if !first_tick_commit_logged {
                first_tick_commit_logged = true;
                tracing::info!(
                    epoch,
                    batch_size = batch_len,
                    decoded_rows = decoded_rows_len,
                    state_write_latency_ms,
                    checkpoint_write_latency_ms,
                    time_to_first_tick_commit_ms =
                        executor_loop_started.elapsed().as_millis() as u64,
                    "executor committed first tick"
                );
            }
            for ack in tick_commit_acks {
                ack.record_committed().await;
            }
            checkpoint_state.record_committed_source_offset_lag(
                &source_names_by_id_for_task,
                tick_source_offsets,
            );
            checkpoint_state
                .record_mv_versions_committed(&mv_versions, persisted_checkpoint.committed_at_ms);
            advance_kafka_offset_commit_state(
                &mut checkpoint_state.committed_kafka_offsets,
                tick_kafka_offsets,
            );
            record_postgres_cdc_lsn_progress(
                &mut checkpoint_state.committed_postgres_lsns,
                tick_postgres_lsns,
                tick_postgres_sources,
                tick_postgres_table_lsns,
                &cdc_replication_debug_for_task,
            );
            record_mv_freshness_metrics(
                &checkpoint_state.mv_last_update_at_ms,
                current_unix_time_ms(),
            );
            checkpoint_state.record_checkpoint_committed();
            if next_watermark != prev_watermark {
                watermark_for_task.store(next_watermark, Ordering::Relaxed);
            }
            if next_watermark >= 0 {
                metrics::record_global_watermark_ms(next_watermark);
                mv_for_task.update_watermark_all(next_watermark as u64);
                let now_ms = current_unix_time_ms();
                let watermark_ms = u64::try_from(next_watermark).unwrap_or(u64::MAX);
                metrics::record_watermark_lag_ms(now_ms.saturating_sub(watermark_ms));
            }
            publish_watermark_debug_state(
                &watermark_debug_for_task,
                next_watermark,
                now_instant,
                &source_watermarks,
                &source_last_seen_at,
                watermark_idle_timeout,
            )
            .await;
            notify_kafka_commit_senders(
                epoch,
                tick_kafka_offsets,
                &checkpoint_state.committed_kafka_offsets,
                &kafka_commit_senders_for_task,
            );
            notify_postgres_cdc_commit_senders(
                epoch,
                &checkpoint_state.committed_postgres_lsns,
                tick_postgres_lsns,
                &postgres_cdc_commit_senders_for_task,
            );
            let tick_latency_ms = tick_start.elapsed().as_millis() as u64;
            metrics::observe_tick_latency_ms(tick_latency_ms);
            tracing::debug!(tick_latency_ms, "connector tick completed");

            record_ingest_queue_metrics(IngestMetrics {
                connector_queues: &connector_queues,
                connector_receiver_len: connector_receiver_for_task.len(),
                decoded_counts: decoded_counts.as_slice(),
                source_names_by_id: &source_names_by_id_for_task,
                per_connector_counts: &per_connector_counts,
                epoch,
                batch_len,
                decoded_rows_len,
                max_batch,
                max_batch_per_source,
                max_batch_per_connector,
                decode_latency_ms,
                state_write_latency_ms,
                checkpoint_write_latency_ms,
                tick_latency_ms,
            });
        }
        let final_frontier = watermark_for_task
            .load(Ordering::Relaxed)
            .max(0)
            .try_into()
            .unwrap_or(0_u64);
        persist_final_checkpoint_unless_failed(FinalCheckpoint {
            checkpoint_manager: &mut checkpoint_manager,
            final_frontier,
            mv_registry: &mv_for_task,
            outer_registry: &outer_for_task,
            runtime_failure: &failure_for_executor,
        })
        .await;
        executor_running_for_task.store(false, Ordering::Relaxed);
    });

    executor_handle
}
