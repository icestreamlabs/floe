use super::*;

pub(super) type KafkaMetadataJournalBatch = (String, Option<i64>, Vec<KafkaSourceJournalRange>);
pub(super) type KafkaSourceJournalRangeMap =
    HashMap<(Arc<str>, i32), KafkaSourceJournalRangeAccumulator>;

pub(super) struct ExecutorCheckpointState {
    pub(super) epoch: u64,
    pub(super) last_mv_versions: HashMap<String, u64>,
    pub(super) committed_source_offsets: HashMap<(String, u32), u64>,
    pub(super) latest_source_offsets: HashMap<(String, u32), u64>,
    pub(super) committed_kafka_offsets: HashMap<(Arc<str>, i32), i64>,
    pub(super) committed_postgres_lsns: HashMap<String, (u64, String)>,
    pub(super) mv_last_update_at_ms: HashMap<String, u64>,
    pub(super) last_checkpoint_commit_at: Instant,
}

const MAX_SINK_CURSOR_UPDATES_PER_ITER: usize = 4096;

pub(super) fn drain_sink_checkpoint_updates(
    receiver: &mut mpsc::Receiver<SinkCursor>,
    checkpoint_manager: &mut CheckpointManager,
) {
    for _ in 0..MAX_SINK_CURSOR_UPDATES_PER_ITER {
        match receiver.try_recv() {
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
}

impl ExecutorCheckpointState {
    pub(super) fn new(tracked_mv_names: &[String]) -> Self {
        Self {
            epoch: 0,
            last_mv_versions: HashMap::new(),
            committed_source_offsets: HashMap::new(),
            latest_source_offsets: HashMap::new(),
            committed_kafka_offsets: HashMap::new(),
            committed_postgres_lsns: HashMap::new(),
            mv_last_update_at_ms: tracked_mv_names
                .iter()
                .map(|view| (view.clone(), current_unix_time_ms()))
                .collect(),
            last_checkpoint_commit_at: Instant::now(),
        }
    }

    pub(super) fn restore_latest_commit(
        &mut self,
        checkpoint_manager: &CheckpointManager,
        watermark: &AtomicI64,
    ) {
        let Some(existing_commit) = checkpoint_manager.latest_tick_commit() else {
            return;
        };

        metrics::record_last_committed_tick(existing_commit.tick_id);
        self.epoch = existing_commit.tick_id;
        let restored_watermark = i64::try_from(existing_commit.frontier).unwrap_or(i64::MAX);
        watermark.store(restored_watermark.max(0), Ordering::Relaxed);
        for mv_version in &existing_commit.mv_versions {
            self.last_mv_versions
                .insert(mv_version.view.clone(), mv_version.version);
            self.mv_last_update_at_ms.insert(
                mv_version.view.clone(),
                existing_commit.committed_at_unix_ms,
            );
        }
        for offset in &existing_commit.source_offsets {
            let key = (offset.source.clone(), offset.partition);
            self.committed_source_offsets
                .insert(key.clone(), offset.offset);
            self.latest_source_offsets.insert(key, offset.offset);
            metrics::record_source_offset_lag(&offset.source, offset.partition, 0);
        }
        for offset in &existing_commit.kafka_offsets {
            self.committed_kafka_offsets.insert(
                (Arc::<str>::from(offset.topic.as_str()), offset.partition),
                offset.offset,
            );
        }
        let now_ms = current_unix_time_ms();
        let age_secs = now_ms.saturating_sub(existing_commit.committed_at_unix_ms) / 1_000;
        metrics::record_checkpoint_age_seconds(age_secs);
        metrics::record_watermark_lag_ms(now_ms.saturating_sub(existing_commit.frontier));
        metrics::record_global_watermark_ms(
            i64::try_from(existing_commit.frontier).unwrap_or(i64::MAX),
        );
        record_mv_freshness_metrics(&self.mv_last_update_at_ms, now_ms);
    }

    pub(super) fn record_periodic_metrics(&self) {
        metrics::record_checkpoint_age_seconds(self.last_checkpoint_commit_at.elapsed().as_secs());
        record_mv_freshness_metrics(&self.mv_last_update_at_ms, current_unix_time_ms());
    }

    pub(super) fn record_latest_source_offset_lag(
        &mut self,
        source_names_by_id: &[String],
        tick_source_offsets: &[Option<HashMap<u32, u64>>],
    ) {
        for (source_id, offsets) in tick_source_offsets.iter().enumerate() {
            let Some(offsets) = offsets.as_ref() else {
                continue;
            };
            let source = source_names_by_id[source_id].as_str();
            for (&partition, &offset) in offsets {
                let key = (source.to_string(), partition);
                let latest_entry = self.latest_source_offsets.entry(key.clone()).or_insert(0);
                *latest_entry = (*latest_entry).max(offset);
                let committed_offset = self
                    .committed_source_offsets
                    .get(&key)
                    .copied()
                    .unwrap_or(0);
                metrics::record_source_offset_lag(
                    source,
                    partition,
                    latest_entry.saturating_sub(committed_offset),
                );
            }
        }
    }

    pub(super) fn record_committed_source_offset_lag(
        &mut self,
        source_names_by_id: &[String],
        tick_source_offsets: &[Option<HashMap<u32, u64>>],
    ) {
        for (source_id, offsets) in tick_source_offsets.iter().enumerate() {
            let Some(offsets) = offsets.as_ref() else {
                continue;
            };
            let source = source_names_by_id[source_id].as_str();
            for (&partition, &offset) in offsets {
                let key = (source.to_string(), partition);
                let committed_entry = self
                    .committed_source_offsets
                    .entry(key.clone())
                    .or_insert(0);
                *committed_entry = (*committed_entry).max(offset);
                let latest_offset = self
                    .latest_source_offsets
                    .get(&key)
                    .copied()
                    .unwrap_or(offset);
                metrics::record_source_offset_lag(
                    source,
                    partition,
                    latest_offset.saturating_sub(*committed_entry),
                );
            }
        }
    }

    pub(super) fn record_mv_versions_committed(
        &mut self,
        mv_versions: &[MaterializedViewTickVersion],
        committed_at_ms: u64,
    ) {
        for mv_version in mv_versions {
            self.mv_last_update_at_ms
                .insert(mv_version.view.clone(), committed_at_ms);
        }
    }

    pub(super) fn record_checkpoint_committed(&mut self) {
        metrics::record_last_committed_tick(self.epoch);
        metrics::record_checkpoint_age_seconds(0);
        self.last_checkpoint_commit_at = Instant::now();
    }
}

pub(super) fn update_checkpoint_source_offsets(
    checkpoint_manager: &mut CheckpointManager,
    source_names_by_id: &[String],
    tick_source_offsets: &[Option<HashMap<u32, u64>>],
) {
    for (source_id, offsets) in tick_source_offsets.iter().enumerate() {
        let Some(offsets) = offsets.as_ref() else {
            continue;
        };
        let source = source_names_by_id[source_id].as_str();
        for (&partition, &offset) in offsets {
            checkpoint_manager.update_partition_offset(source, partition, offset);
        }
    }
}

pub(super) struct CdcOnlyTickCommit<'a> {
    pub(super) checkpoint_manager: &'a mut CheckpointManager,
    pub(super) state: &'a mut ExecutorCheckpointState,
    pub(super) pending_epoch: u64,
    pub(super) mv_registry: &'a Arc<MaterializedViewRegistry>,
    pub(super) watermark: &'a AtomicI64,
    pub(super) cdc_staged_writes: Option<WriteBatch>,
    pub(super) tick_postgres_lsns: &'a HashMap<String, (u64, String)>,
    pub(super) tick_postgres_sources: &'a HashMap<String, String>,
    pub(super) tick_postgres_table_lsns: &'a [(String, String, String, u64)],
    pub(super) cdc_replication_debug:
        &'a Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    pub(super) postgres_cdc_commit_senders: &'a [watch::Sender<PostgresCdcCommit>],
}

pub(super) async fn persist_cdc_only_tick_commit(
    args: CdcOnlyTickCommit<'_>,
) -> anyhow::Result<()> {
    args.state.epoch = args.pending_epoch;
    let mv_versions =
        collect_mv_versions_for_commit(args.mv_registry, &mut args.state.last_mv_versions);
    let frontier = args
        .watermark
        .load(Ordering::Relaxed)
        .max(0)
        .try_into()
        .unwrap_or(0_u64);
    let tick_commit = build_tick_commit_for_checkpoint(
        args.state.epoch,
        frontier,
        args.checkpoint_manager,
        &mv_versions,
        &args.state.committed_kafka_offsets,
    );
    let checkpoint_write_start = Instant::now();
    let checkpoint_result = if let Some(staged_writes) = args.cdc_staged_writes {
        args.checkpoint_manager
            .persist_tick_commit_with_staged_writes(tick_commit, staged_writes)
            .await
    } else {
        args.checkpoint_manager
            .persist_tick_commit(tick_commit)
            .await
    };
    if let Err(err) = checkpoint_result {
        metrics::observe_tick_phase_latency_ms(
            "checkpoint_write",
            checkpoint_write_start.elapsed().as_millis() as u64,
        );
        return Err(err);
    }
    metrics::observe_tick_phase_latency_ms(
        "checkpoint_write",
        checkpoint_write_start.elapsed().as_millis() as u64,
    );
    record_postgres_cdc_lsn_progress(
        &mut args.state.committed_postgres_lsns,
        args.tick_postgres_lsns,
        args.tick_postgres_sources,
        args.tick_postgres_table_lsns,
        args.cdc_replication_debug,
    );
    notify_postgres_cdc_commit_senders(
        args.state.epoch,
        &args.state.committed_postgres_lsns,
        args.tick_postgres_lsns,
        args.postgres_cdc_commit_senders,
    );
    args.state
        .record_mv_versions_committed(&mv_versions, current_unix_time_ms());
    args.state.record_checkpoint_committed();
    Ok(())
}

pub(super) struct SourceJournalBatchBuildInput<'a> {
    pub(super) source_names_by_id: &'a [String],
    pub(super) definitions: &'a [SourceDefinition],
    pub(super) required_sources: &'a BTreeSet<String>,
    pub(super) execution_arrow_batches_by_source: &'a [Vec<RecordBatch>],
    pub(super) arrow_batches_by_source: &'a [Vec<RecordBatch>],
    pub(super) weighted_arrow_batches_by_source: &'a [Vec<RecordBatch>],
    pub(super) tick_source_max_event_ts: &'a [Option<i64>],
}

pub(super) fn build_source_journal_batches(
    input: SourceJournalBatchBuildInput<'_>,
    output: &mut Vec<VectorizedSourceJournalTransientBatch>,
) -> anyhow::Result<()> {
    for source_id in 0..input.source_names_by_id.len() {
        let source_name = input.source_names_by_id[source_id].as_str();
        if !input.required_sources.contains(source_name) {
            continue;
        }
        let Some(definition) = input.definitions.get(source_id) else {
            continue;
        };
        let source_schema = definition.to_arrow_schema();
        let weighted_schema =
            floe_executor::delta_consolidation::weighted_snapshot_schema(&source_schema)
                .with_context(|| {
                    format!("failed to build vectorized source journal schema for '{source_name}'")
                })?;
        let append_batches = if input.arrow_batches_by_source[source_id].is_empty() {
            input.execution_arrow_batches_by_source[source_id].as_slice()
        } else {
            input.arrow_batches_by_source[source_id].as_slice()
        };
        let mut journal_batches = Vec::with_capacity(
            append_batches.len() + input.weighted_arrow_batches_by_source[source_id].len(),
        );
        for batch in append_batches {
            if batch.schema().as_ref() != source_schema.as_ref() {
                return Err(anyhow!(
                    "source journal batch schema does not match source '{source_name}'"
                ));
            }
            let weighted =
                floe_executor::delta_consolidation::add_weight_column(batch, &weighted_schema, 1)
                    .with_context(|| {
                    format!("failed to build vectorized source journal batch for '{source_name}'")
                })?;
            journal_batches.push(weighted);
        }
        journal_batches.extend(
            input.weighted_arrow_batches_by_source[source_id]
                .iter()
                .cloned(),
        );
        if !journal_batches.is_empty() {
            output.push((
                source_id,
                input.tick_source_max_event_ts[source_id],
                journal_batches,
            ));
        }
    }
    Ok(())
}

pub(super) async fn apply_decoded_source_batches(
    runtime: &mut VectorizedExecutionRuntime,
    source_names_by_id: &[String],
    execution_arrow_batches_by_source: &[Vec<RecordBatch>],
    arrow_batches_by_source: &[Vec<RecordBatch>],
    weighted_arrow_batches_by_source: &[Vec<RecordBatch>],
    commit_acks_by_source: &mut [Vec<core_source::CommitAck>],
) -> anyhow::Result<bool> {
    let mut changed = false;
    for (source_id, batches) in execution_arrow_batches_by_source.iter().enumerate() {
        let source_name = source_names_by_id[source_id].as_str();
        if batches.is_empty() {
            continue;
        }
        let query_batches = arrow_batches_by_source
            .get(source_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Err(err) = runtime
            .append_source_batches_for_execution_and_query(
                source_name,
                batches.clone(),
                query_batches.to_vec(),
            )
            .await
        {
            let message =
                format!("failed to append Arrow source batches for '{source_name}': {err}");
            tracing::error!(
                source = %source_name,
                error = %err,
                "failed to append Arrow source batches"
            );
            for ack in commit_acks_by_source[source_id].drain(..) {
                ack.record_failed(message.clone()).await;
            }
            return Err(anyhow!(message));
        }
        changed = true;
    }
    for (source_id, batches) in weighted_arrow_batches_by_source.iter().enumerate() {
        let source_name = source_names_by_id[source_id].as_str();
        for batch in batches {
            if let Err(err) = runtime
                .apply_weighted_source_delta(source_name, batch.clone())
                .await
            {
                let message = format!(
                    "failed to apply weighted Arrow source delta for '{source_name}': {err}"
                );
                tracing::error!(
                    source = %source_name,
                    error = %err,
                    "failed to apply weighted Arrow source delta"
                );
                for ack in commit_acks_by_source[source_id].drain(..) {
                    ack.record_failed(message.clone()).await;
                }
                return Err(anyhow!(message));
            }
            changed = true;
        }
    }
    Ok(changed)
}

pub(super) async fn fail_commit_acks_by_source(
    commit_acks_by_source: &mut [Vec<core_source::CommitAck>],
    message: &str,
) {
    for acks in commit_acks_by_source {
        fail_commit_acks(acks.drain(..), message).await;
    }
}

pub(super) async fn fail_commit_acks(
    acks: impl IntoIterator<Item = core_source::CommitAck>,
    message: &str,
) {
    for ack in acks {
        ack.record_failed(message.to_string()).await;
    }
}

pub(super) async fn record_fatal_source_batch_failure(
    commit_acks_by_source: &mut [Vec<core_source::CommitAck>],
    runtime_failure: &Arc<StdMutex<Option<String>>>,
    cancel: &CancellationToken,
    message: String,
) {
    fail_commit_acks_by_source(commit_acks_by_source, &message).await;
    record_runtime_failure(runtime_failure, message);
    cancel.cancel();
}

pub(super) async fn record_fatal_tick_failure(
    tick_commit_acks: Vec<core_source::CommitAck>,
    runtime_failure: &Arc<StdMutex<Option<String>>>,
    cancel: &CancellationToken,
    message: String,
) {
    fail_commit_acks(tick_commit_acks, &message).await;
    record_runtime_failure(runtime_failure, message);
    cancel.cancel();
}

pub(super) async fn wait_for_tick_materialized_views(
    mv_registry: &Arc<MaterializedViewRegistry>,
    epoch: u64,
    cancel: &CancellationToken,
    tick_commit_acks: Vec<core_source::CommitAck>,
    runtime_failure: &Arc<StdMutex<Option<String>>>,
) -> Option<Vec<core_source::CommitAck>> {
    let visibility_start = Instant::now();
    let target_mv_version = i64::try_from(epoch).unwrap_or(i64::MAX);
    match wait_for_materialized_views_visible(mv_registry, target_mv_version, cancel).await {
        Ok(waited_views) => {
            let visibility_latency_ms = visibility_start.elapsed().as_millis() as u64;
            metrics::observe_tick_phase_latency_ms("mv_visibility", visibility_latency_ms);
            if waited_views > 0 && (epoch <= 8 || epoch.is_multiple_of(128)) {
                tracing::info!(
                    epoch,
                    waited_views,
                    visibility_latency_ms,
                    "tick materialized views visible"
                );
            }
            Some(tick_commit_acks)
        }
        Err(err) => {
            metrics::observe_tick_phase_latency_ms(
                "mv_visibility",
                visibility_start.elapsed().as_millis() as u64,
            );
            tracing::error!(
                epoch,
                error = %err,
                "failed while waiting for materialized view visibility"
            );
            record_fatal_tick_failure(
                tick_commit_acks,
                runtime_failure,
                cancel,
                format!("failed waiting for materialized view visibility at tick {epoch}: {err}"),
            )
            .await;
            None
        }
    }
}

pub(super) struct FinalCheckpoint<'a> {
    pub(super) checkpoint_manager: &'a mut CheckpointManager,
    pub(super) final_frontier: u64,
    pub(super) mv_registry: &'a Arc<MaterializedViewRegistry>,
    pub(super) runtime_failure: &'a Arc<StdMutex<Option<String>>>,
}

pub(super) struct PersistExecutorFinalCheckpoint<'a> {
    pub(super) checkpoint_manager: &'a mut CheckpointManager,
    pub(super) watermark: &'a AtomicI64,
    pub(super) mv_registry: &'a Arc<MaterializedViewRegistry>,
    pub(super) runtime_failure: &'a Arc<StdMutex<Option<String>>>,
}

pub(super) async fn persist_executor_final_checkpoint(args: PersistExecutorFinalCheckpoint<'_>) {
    let final_frontier = args
        .watermark
        .load(Ordering::Relaxed)
        .max(0)
        .try_into()
        .unwrap_or(0_u64);
    persist_final_checkpoint_unless_failed(FinalCheckpoint {
        checkpoint_manager: args.checkpoint_manager,
        final_frontier,
        mv_registry: args.mv_registry,
        runtime_failure: args.runtime_failure,
    })
    .await;
}

pub(super) async fn persist_final_checkpoint_unless_failed(args: FinalCheckpoint<'_>) {
    let has_runtime_failure = match args.runtime_failure.lock() {
        Ok(guard) => guard.is_some(),
        Err(poisoned) => {
            tracing::warn!(
                "runtime failure lock was poisoned while deciding final checkpoint behavior"
            );
            poisoned.into_inner().is_some()
        }
    };
    if has_runtime_failure {
        tracing::warn!("skipping final checkpoint persistence after runtime failure");
        return;
    }

    if let Err(err) = args
        .checkpoint_manager
        .persist_snapshot(args.final_frontier, args.mv_registry.as_ref())
        .await
    {
        tracing::warn!(error = %err, "final checkpoint persistence failed");
    }
}

pub(super) fn build_kafka_metadata_journal_batches(
    source_names_by_id: &[String],
    source_ids: &[usize],
    tick_source_max_event_ts: &[Option<i64>],
    tick_kafka_source_ranges: &mut [Option<KafkaSourceJournalRangeMap>],
) -> Vec<KafkaMetadataJournalBatch> {
    let mut batches = Vec::new();
    for &source_id in source_ids {
        let Some(ranges_by_partition) = tick_kafka_source_ranges[source_id].take() else {
            continue;
        };
        let mut ranges = ranges_by_partition
            .into_values()
            .map(KafkaSourceJournalRangeAccumulator::into_range)
            .collect::<Vec<_>>();
        ranges.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.partition.cmp(&right.partition))
        });
        if ranges.is_empty() {
            continue;
        }
        batches.push((
            source_names_by_id[source_id].clone(),
            tick_source_max_event_ts[source_id],
            ranges,
        ));
    }
    batches
}

pub(super) struct PersistTickCheckpoint<'a> {
    pub(super) checkpoint_manager: &'a mut CheckpointManager,
    pub(super) epoch: u64,
    pub(super) frontier: u64,
    pub(super) mv_versions: &'a [MaterializedViewTickVersion],
    pub(super) next_committed_kafka_offsets: &'a HashMap<(Arc<str>, i32), i64>,
    pub(super) source_names_by_id: &'a [String],
    pub(super) vectorized_source_journal_batches: &'a [VectorizedSourceJournalTransientBatch],
    pub(super) kafka_metadata_journal_batches: &'a [KafkaMetadataJournalBatch],
    pub(super) cdc_staged_writes: Option<WriteBatch>,
}

pub(super) struct PersistedTickCheckpoint {
    pub(super) committed_at_ms: u64,
    pub(super) checkpoint_write_latency_ms: u64,
}

pub(super) async fn persist_tick_checkpoint(
    args: PersistTickCheckpoint<'_>,
) -> anyhow::Result<PersistedTickCheckpoint> {
    let tick_commit = build_tick_commit_for_checkpoint(
        args.epoch,
        args.frontier,
        args.checkpoint_manager,
        args.mv_versions,
        args.next_committed_kafka_offsets,
    );
    let committed_at_ms = tick_commit.committed_at_unix_ms;
    let vectorized_source_journal_commit_batches = build_vectorized_source_journal_commit_batches(
        args.source_names_by_id,
        args.vectorized_source_journal_batches,
    );
    let mut staged_writes_for_checkpoint = args.cdc_staged_writes;
    let mut vectorized_journal_stage_error = None;
    if !vectorized_source_journal_commit_batches.is_empty() {
        let staged_writes = staged_writes_for_checkpoint.get_or_insert_with(WriteBatch::new);
        for (source, max_event_time_ms, batches) in &vectorized_source_journal_commit_batches {
            if let Err(err) = append_vectorized_entry_to_batch(
                staged_writes,
                source,
                args.epoch,
                *max_event_time_ms,
                batches,
            ) {
                vectorized_journal_stage_error = Some(err);
                break;
            }
        }
    }
    let checkpoint_write_start = Instant::now();
    let checkpoint_result = if let Some(err) = vectorized_journal_stage_error {
        Err(err)
    } else if let Some(staged_writes) = staged_writes_for_checkpoint {
        args.checkpoint_manager
            .persist_tick_commit_with_kafka_metadata_and_staged_writes(
                tick_commit,
                args.kafka_metadata_journal_batches,
                staged_writes,
            )
            .await
    } else {
        args.checkpoint_manager
            .persist_tick_commit_with_kafka_metadata(
                tick_commit,
                args.kafka_metadata_journal_batches,
            )
            .await
    };
    if let Err(err) = checkpoint_result {
        metrics::observe_tick_phase_latency_ms(
            "checkpoint_write",
            checkpoint_write_start.elapsed().as_millis() as u64,
        );
        return Err(err);
    }
    metrics::observe_tick_phase_latency_ms(
        "checkpoint_write",
        checkpoint_write_start.elapsed().as_millis() as u64,
    );
    Ok(PersistedTickCheckpoint {
        committed_at_ms,
        checkpoint_write_latency_ms: checkpoint_write_start.elapsed().as_millis() as u64,
    })
}

pub(super) async fn publish_watermark_debug_state(
    watermark_debug: &Arc<tokio::sync::RwLock<http_ingest::WatermarkDebugState>>,
    next_watermark: i64,
    now_instant: Instant,
    source_watermarks: &HashMap<String, i64>,
    source_last_seen_at: &HashMap<String, Instant>,
    watermark_idle_timeout: Duration,
) {
    let mut debug_state = watermark_debug.write().await;
    debug_state.updated_at_unix_ms = current_unix_time_ms();
    debug_state.global_watermark_ms = (next_watermark >= 0).then_some(next_watermark);
    let mut sources = Vec::with_capacity(source_watermarks.len());
    for (source, watermark) in source_watermarks {
        let idle = source_last_seen_at
            .get(source)
            .map(|last| now_instant.duration_since(*last) >= watermark_idle_timeout)
            .unwrap_or(true);
        sources.push(http_ingest::WatermarkDebugSourceState {
            source: source.clone(),
            watermark_ms: *watermark,
            idle,
        });
    }
    sources.sort_by(|left, right| left.source.cmp(&right.source));
    debug_state.sources = sources;
}

pub(super) fn notify_kafka_commit_senders(
    epoch: u64,
    tick_kafka_offsets: &HashMap<(Arc<str>, i32), i64>,
    committed_kafka_offsets: &HashMap<(Arc<str>, i32), i64>,
    senders: &[watch::Sender<KafkaOffsetCommit>],
) {
    if tick_kafka_offsets.is_empty() || senders.is_empty() {
        return;
    }
    let kafka_commit_start = Instant::now();
    let commit = build_kafka_offset_commit(epoch, committed_kafka_offsets);
    for sender in senders {
        let _ = sender.send(commit.clone());
    }
    metrics::observe_tick_phase_latency_ms(
        "kafka_commit_notify",
        kafka_commit_start.elapsed().as_millis() as u64,
    );
}

pub(super) struct IngestMetrics<'a> {
    pub(super) connector_queues: &'a [ConnectorQueue],
    pub(super) connector_receiver_len: usize,
    pub(super) decoded_counts: &'a [usize],
    pub(super) source_names_by_id: &'a [String],
    pub(super) per_connector_counts: &'a [usize],
    pub(super) epoch: u64,
    pub(super) batch_len: usize,
    pub(super) decoded_rows_len: usize,
    pub(super) max_batch: usize,
    pub(super) max_batch_per_source: usize,
    pub(super) max_batch_per_connector: usize,
    pub(super) decode_latency_ms: u64,
    pub(super) state_write_latency_ms: u64,
    pub(super) checkpoint_write_latency_ms: u64,
    pub(super) tick_latency_ms: u64,
}

pub(super) fn record_ingest_queue_metrics(metrics_input: IngestMetrics<'_>) {
    let queue_depth: usize = metrics_input
        .connector_queues
        .iter()
        .map(|queue| queue.pending.len())
        .sum();
    let total_queue_depth = queue_depth.saturating_add(metrics_input.connector_receiver_len);
    metrics::record_ingest_queue_depth(total_queue_depth);
    if !should_sample(&INGEST_METRICS_COUNTER, INGEST_METRICS_SAMPLE_EVERY) {
        return;
    }
    let per_source: Vec<_> = metrics_input
        .decoded_counts
        .iter()
        .enumerate()
        .filter_map(|(source_id, count)| {
            (*count > 0).then_some((metrics_input.source_names_by_id[source_id].as_str(), *count))
        })
        .collect();
    let per_connector: Vec<_> = metrics_input
        .connector_queues
        .iter()
        .map(|queue| {
            (
                queue.name.as_str(),
                metrics_input.per_connector_counts[queue.id],
            )
        })
        .filter(|(_, count)| *count > 0)
        .collect();
    tracing::info!(
        epoch = metrics_input.epoch,
        queue_depth = total_queue_depth,
        batch_size = metrics_input.batch_len,
        pending = total_queue_depth,
        decoded_rows = metrics_input.decoded_rows_len,
        max_batch = metrics_input.max_batch,
        max_batch_per_source = metrics_input.max_batch_per_source,
        max_batch_per_connector = metrics_input.max_batch_per_connector,
        decode_latency_ms = metrics_input.decode_latency_ms,
        state_write_latency_ms = metrics_input.state_write_latency_ms,
        checkpoint_write_latency_ms = metrics_input.checkpoint_write_latency_ms,
        tick_latency_ms = metrics_input.tick_latency_ms,
        per_source = ?per_source,
        per_connector = ?per_connector,
        "ingest batch metrics"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;

    use super::*;

    #[test]
    fn source_journal_batches_use_execution_batches_when_query_batches_are_disabled() {
        let definition = SourceDefinition::new(
            "orders",
            vec![SourceColumn::new("id", SourceDataType::Int64)],
        )
        .expect("source definition");
        let schema = definition.to_arrow_schema();
        let execution_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
                .expect("record batch");
        let mut required_sources = BTreeSet::new();
        required_sources.insert("orders".to_string());
        let mut output = Vec::new();

        build_source_journal_batches(
            SourceJournalBatchBuildInput {
                source_names_by_id: &["orders".to_string()],
                definitions: &[definition],
                required_sources: &required_sources,
                execution_arrow_batches_by_source: &[vec![execution_batch]],
                arrow_batches_by_source: &[Vec::new()],
                weighted_arrow_batches_by_source: &[Vec::new()],
                tick_source_max_event_ts: &[Some(123)],
            },
            &mut output,
        )
        .expect("source journal batches");

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0, 0);
        assert_eq!(output[0].1, Some(123));
        assert_eq!(output[0].2.len(), 1);
        assert_eq!(output[0].2[0].num_rows(), 1);
    }
}
