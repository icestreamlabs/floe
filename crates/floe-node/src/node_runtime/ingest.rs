use super::*;

pub(super) fn should_sample(counter: &AtomicU64, every: u64) -> bool {
    if every == 0 {
        return true;
    }
    counter
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
}

pub(super) fn record_runtime_failure(state: &Arc<StdMutex<Option<String>>>, message: String) {
    crate::runtime_failure::record_runtime_failure("runtime", state, message);
}

pub(super) fn collect_mv_versions_for_commit(
    registry: &Arc<MaterializedViewRegistry>,
    last_versions: &mut HashMap<String, u64>,
) -> Vec<MaterializedViewTickVersion> {
    let mut committed = Vec::new();
    for handle in registry.handles() {
        let Some(frontier) = handle.latest_version() else {
            continue;
        };
        if frontier < 0 {
            continue;
        }
        let view = handle.name().to_string();
        let version = u64::try_from(frontier).unwrap_or(u64::MAX);
        let entry = last_versions.entry(view.clone()).or_insert(0);
        if version > *entry {
            committed.push(MaterializedViewTickVersion { view, version });
            *entry = version;
        }
    }
    committed.sort_by(|left, right| left.view.cmp(&right.view));
    committed
}

pub(super) fn compute_global_watermark(
    source_watermarks: &HashMap<String, i64>,
    source_last_seen_at: &HashMap<String, Instant>,
    now: Instant,
    idle_timeout: Duration,
) -> Option<i64> {
    let mut global: Option<i64> = None;
    for (source, watermark) in source_watermarks {
        let Some(last_seen) = source_last_seen_at.get(source) else {
            continue;
        };
        if now.duration_since(*last_seen) > idle_timeout {
            continue;
        }
        global = Some(global.map_or(*watermark, |current| current.min(*watermark)));
    }
    global
}

pub(super) fn advance_global_watermark(previous: i64, candidate: Option<i64>) -> i64 {
    candidate.map_or(previous, |value| previous.max(value))
}

pub(super) fn record_mv_freshness_metrics(last_update_at_ms: &HashMap<String, u64>, now_ms: u64) {
    for (view, last_update_ms) in last_update_at_ms {
        let age_seconds = now_ms.saturating_sub(*last_update_ms) / 1_000;
        metrics::record_mv_freshness_seconds(view, age_seconds);
    }
}

pub(super) async fn wait_for_materialized_views_visible(
    registry: &Arc<MaterializedViewRegistry>,
    target_version: i64,
    cancel: &CancellationToken,
) -> anyhow::Result<usize> {
    if target_version < 0 {
        return Ok(0);
    }

    let mut waited_views = 0usize;
    for view in registry.handles() {
        if !view.commit_visibility_barrier_enabled() {
            continue;
        }
        if view
            .latest_version()
            .is_some_and(|version| version >= target_version)
        {
            continue;
        }

        waited_views = waited_views.saturating_add(1);
        wait_for_materialized_view_visible(&view, target_version, cancel).await?;
    }

    Ok(waited_views)
}

pub(super) async fn wait_for_materialized_view_visible(
    view: &floe_executor::MaterializedViewHandle,
    target_version: i64,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    if target_version < 0
        || view
            .latest_version()
            .is_some_and(|version| version >= target_version)
    {
        return Ok(());
    }

    let mut version_rx = view.version_watch();
    loop {
        if version_rx
            .borrow()
            .is_some_and(|version| version >= target_version)
        {
            return Ok(());
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(anyhow!(
                    "runtime cancelled while waiting for materialized view '{}' to publish version {target_version}",
                    view.name()
                ));
            }
            changed = version_rx.changed() => {
                changed.with_context(|| {
                    format!(
                        "wait for materialized view '{}' to publish version {target_version}",
                        view.name()
                    )
                })?;
            }
        }
    }
}

pub(super) fn event_resume_offset(
    token: Option<&core_source::AppendIngestResumeToken>,
) -> Option<(u32, u64)> {
    match token? {
        core_source::AppendIngestResumeToken::Kafka {
            partition, offset, ..
        } => {
            let partition = u32::try_from(*partition).ok()?;
            let offset = u64::try_from(*offset).ok()?;
            Some((partition, offset))
        }
        core_source::AppendIngestResumeToken::File { cursor }
        | core_source::AppendIngestResumeToken::Generator { position: cursor }
        | core_source::AppendIngestResumeToken::ObjectStore { cursor } => Some((0, *cursor)),
    }
}

pub(super) fn event_fast_resume_offset(
    event: &core_source::AppendIngestEvent,
) -> Option<(u32, u64)> {
    let (_, partition, offset) = event.kafka_position()?;
    let partition = u32::try_from(partition).ok()?;
    let offset = u64::try_from(offset).ok()?;
    Some((partition, offset))
}

pub(super) fn event_fast_kafka_offset(
    event: &core_source::AppendIngestEvent,
) -> Option<(Arc<str>, i32, i64)> {
    let (topic, partition, offset) = event.kafka_position()?;
    Some((Arc::clone(topic), partition, offset))
}

pub(super) fn event_kafka_offset(
    token: Option<&core_source::AppendIngestResumeToken>,
) -> Option<(Arc<str>, i32, i64)> {
    match token? {
        core_source::AppendIngestResumeToken::Kafka {
            topic,
            partition,
            offset,
        } => Some((Arc::<str>::from(topic.as_str()), *partition, *offset)),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub(super) struct KafkaSourceJournalRangeAccumulator {
    topic: Arc<str>,
    partition: i32,
    start_offset: i64,
    end_offset: i64,
    row_count: u64,
    checksum: u64,
}

impl KafkaSourceJournalRangeAccumulator {
    fn new(topic: Arc<str>, partition: i32, offset: i64) -> Self {
        Self {
            topic,
            partition,
            start_offset: offset,
            end_offset: offset,
            row_count: 0,
            checksum: kafka_source_journal_initial_checksum(),
        }
    }

    fn observe_event(&mut self, offset: i64, event: &core_source::AppendIngestEvent) {
        self.start_offset = self.start_offset.min(offset);
        self.end_offset = self.end_offset.max(offset);
        self.row_count = self.row_count.saturating_add(1);
        let checksum_bytes = kafka_source_journal_event_checksum_bytes(event);
        update_kafka_source_journal_checksum(&mut self.checksum, offset, &checksum_bytes);
    }

    fn observe_raw_payload(&mut self, offset: i64, source: &str, payload: &[u8]) {
        self.start_offset = self.start_offset.min(offset);
        self.end_offset = self.end_offset.max(offset);
        self.row_count = self.row_count.saturating_add(1);
        update_kafka_source_journal_checksum_parts(
            &mut self.checksum,
            offset,
            &[source.as_bytes(), &[0], payload],
        );
    }

    pub(super) fn into_range(self) -> KafkaSourceJournalRange {
        KafkaSourceJournalRange {
            topic: self.topic.to_string(),
            partition: self.partition,
            start_offset: self.start_offset,
            end_offset: self.end_offset,
            row_count: self.row_count,
            checksum: self.checksum,
        }
    }
}

pub(super) fn observe_kafka_source_journal_event(
    ranges: &mut Option<HashMap<(Arc<str>, i32), KafkaSourceJournalRangeAccumulator>>,
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    event: &core_source::AppendIngestEvent,
) {
    let entry = ranges
        .get_or_insert_with(HashMap::new)
        .entry((Arc::clone(&topic), partition))
        .or_insert_with(|| KafkaSourceJournalRangeAccumulator::new(topic, partition, offset));
    entry.observe_event(offset, event);
}

pub(super) fn observe_kafka_source_journal_raw_payload(
    ranges: &mut Option<HashMap<(Arc<str>, i32), KafkaSourceJournalRangeAccumulator>>,
    source: &str,
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    payload: &[u8],
) {
    let entry = ranges
        .get_or_insert_with(HashMap::new)
        .entry((Arc::clone(&topic), partition))
        .or_insert_with(|| KafkaSourceJournalRangeAccumulator::new(topic, partition, offset));
    entry.observe_raw_payload(offset, source, payload);
}

pub(super) fn kafka_source_journal_event_checksum_bytes(
    event: &core_source::AppendIngestEvent,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(event.source().as_bytes());
    bytes.push(0);
    if let Some(payload) = event.payload() {
        match serde_json::to_vec(payload) {
            Ok(payload) => bytes.extend_from_slice(&payload),
            Err(_) => bytes.extend_from_slice(b"<unserializable-json-payload>"),
        }
    }
    bytes
}

pub(super) fn advance_kafka_offset_commit_state(
    committed_offsets: &mut HashMap<(Arc<str>, i32), i64>,
    tick_offsets: &HashMap<(Arc<str>, i32), i64>,
) {
    for (key, &offset) in tick_offsets {
        let entry = committed_offsets.entry(key.clone()).or_insert(offset);
        *entry = (*entry).max(offset);
    }
}

pub(super) fn build_kafka_offset_commit(
    tick_id: u64,
    offsets: &HashMap<(Arc<str>, i32), i64>,
) -> KafkaOffsetCommit {
    let mut entries: Vec<KafkaTopicPartitionOffset> = offsets
        .iter()
        .map(|((topic, partition), offset)| KafkaTopicPartitionOffset {
            topic: topic.to_string(),
            partition: *partition,
            offset: *offset,
        })
        .collect();
    entries.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    KafkaOffsetCommit {
        tick_id,
        offsets: entries,
    }
}

pub(super) fn build_postgres_cdc_commit(
    tick_id: u64,
    slots: &HashMap<String, (u64, String)>,
) -> PostgresCdcCommit {
    let mut entries: Vec<PostgresSlotCommit> = slots
        .iter()
        .map(|(slot, (_, lsn))| PostgresSlotCommit {
            slot: slot.clone(),
            lsn: lsn.clone(),
        })
        .collect();
    entries.sort_by(|left, right| left.slot.cmp(&right.slot));
    PostgresCdcCommit {
        tick_id,
        slots: entries,
    }
}

pub(super) fn advance_postgres_cdc_commit_state(
    committed_slots: &mut HashMap<String, (u64, String)>,
    tick_slots: &HashMap<String, (u64, String)>,
) {
    for (slot, (lsn_value, lsn_text)) in tick_slots {
        let entry = committed_slots
            .entry(slot.clone())
            .or_insert_with(|| (*lsn_value, lsn_text.clone()));
        if *lsn_value > entry.0 {
            *entry = (*lsn_value, lsn_text.clone());
        }
    }
}

pub(super) fn current_unix_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

pub(super) async fn recv_from_ready(
    receiver: &mut core_source::RoutedAppendIngestEventReceiver,
    queues: &mut [ConnectorQueue],
) -> bool {
    let Some(batch) = receiver.recv().await else {
        return false;
    };
    enqueue_routed_batch(batch, queues);
    true
}

pub(super) async fn recv_cdc_from_ready(
    receiver: &mut mpsc::Receiver<QueuedCdcTransaction>,
    queue: &mut VecDeque<QueuedCdcTransaction>,
) -> bool {
    let Some(transaction) = receiver.recv().await else {
        return false;
    };
    queue.push_back(transaction);
    true
}

pub(super) fn drain_cdc_ready(
    receiver: &mut mpsc::Receiver<QueuedCdcTransaction>,
    queue: &mut VecDeque<QueuedCdcTransaction>,
) {
    while let Ok(transaction) = receiver.try_recv() {
        queue.push_back(transaction);
    }
}

pub(super) fn drain_ready(
    receiver: &mut core_source::RoutedAppendIngestEventReceiver,
    queues: &mut [ConnectorQueue],
) {
    while let Ok(batch) = receiver.try_recv() {
        enqueue_routed_batch(batch, queues);
    }
}

fn enqueue_routed_batch(
    batch: core_source::RoutedAppendIngestEventBatch,
    queues: &mut [ConnectorQueue],
) {
    let Some(queue) = queues.get_mut(batch.connector_id) else {
        return;
    };
    let commit_ack = batch.commit_ack;
    match batch.payload {
        core_source::RoutedIngestPayload::Events(events) => {
            queue.pending.extend(events.into_iter().map(|event| {
                QueuedAppendIngestItem::Event(QueuedAppendIngestEvent {
                    event,
                    commit_ack: commit_ack.clone(),
                })
            }));
        }
        core_source::RoutedIngestPayload::KafkaRaw(batch) => {
            queue.pending.push_back(QueuedAppendIngestItem::KafkaRaw(
                QueuedKafkaRawIngestBatch { batch, commit_ack },
            ));
        }
        core_source::RoutedIngestPayload::KafkaArrow(batch) => {
            queue.pending.push_back(QueuedAppendIngestItem::KafkaArrow(
                QueuedKafkaArrowIngestBatch { batch, commit_ack },
            ));
        }
    }
}

pub(super) struct BuildBatchRequest<'a> {
    pub(super) queues: &'a mut [ConnectorQueue],
    pub(super) source_id_by_name: &'a HashMap<String, usize>,
    pub(super) source_count: usize,
    pub(super) start_index: usize,
    pub(super) max_batch: usize,
    pub(super) max_per_source: usize,
    pub(super) max_per_connector: usize,
    pub(super) pending_events: &'a core_source::PendingAppendIngestEventCounter,
}

pub(super) fn build_batch(request: BuildBatchRequest<'_>) -> BatchSelection {
    let BuildBatchRequest {
        queues,
        source_id_by_name,
        source_count,
        start_index,
        max_batch,
        max_per_source,
        max_per_connector,
        pending_events,
    } = request;
    let mut batch = Vec::with_capacity(max_batch);
    let mut kafka_raw_batches = Vec::new();
    let mut kafka_arrow_batches = Vec::new();
    let mut selected_rows = 0usize;
    let mut per_source_counts = vec![0usize; source_count];
    let mut unknown_source_counts: HashMap<String, usize> = HashMap::new();
    let per_connector_count_len = queues
        .iter()
        .map(|queue| queue.id)
        .max()
        .map_or(0, |id| id + 1);
    let mut per_connector_counts = vec![0usize; per_connector_count_len];
    let mut deferred: Vec<VecDeque<QueuedAppendIngestItem>> =
        (0..queues.len()).map(|_| VecDeque::new()).collect();
    let connector_count = queues.len();
    for step in 0..connector_count {
        let idx = (start_index + step) % connector_count;
        let queue = &mut queues[idx];
        let deferred_queue = &mut deferred[idx];
        let per_connector = &mut per_connector_counts[queue.id];
        while *per_connector < max_per_connector && selected_rows < max_batch {
            let Some(queued) = queue.pending.pop_front() else {
                break;
            };
            match queued {
                QueuedAppendIngestItem::Event(queued) => {
                    let source_id = source_id_by_name.get(queued.event.source()).copied();
                    let count = if let Some(source_id) = source_id {
                        &mut per_source_counts[source_id]
                    } else {
                        unknown_source_counts
                            .entry(queued.event.source().to_string())
                            .or_insert(0)
                    };
                    if *count >= max_per_source {
                        deferred_queue.push_back(QueuedAppendIngestItem::Event(queued));
                        continue;
                    }
                    *count += 1;
                    *per_connector += 1;
                    selected_rows += 1;
                    batch.push(SelectedAppendIngestEvent {
                        source_id,
                        event: queued.event,
                        commit_ack: queued.commit_ack,
                    });
                }
                QueuedAppendIngestItem::KafkaRaw(mut queued) => {
                    let source_id = source_id_by_name.get(queued.batch.source.as_str()).copied();
                    let count = if let Some(source_id) = source_id {
                        &mut per_source_counts[source_id]
                    } else {
                        unknown_source_counts
                            .entry(queued.batch.source.clone())
                            .or_insert(0)
                    };
                    let remaining_source = max_per_source.saturating_sub(*count);
                    let remaining_connector = max_per_connector.saturating_sub(*per_connector);
                    let remaining_batch = max_batch.saturating_sub(selected_rows);
                    let allowed = remaining_source
                        .min(remaining_connector)
                        .min(remaining_batch)
                        .min(queued.batch.len());
                    if allowed == 0 {
                        deferred_queue.push_back(QueuedAppendIngestItem::KafkaRaw(queued));
                        continue;
                    }
                    let selected_batch = if allowed == queued.batch.len() {
                        queued.batch
                    } else {
                        let remaining = queued.batch.records.split_off(allowed);
                        let source = queued.batch.source.clone();
                        let selected_batch = queued.batch;
                        deferred_queue.push_back(QueuedAppendIngestItem::KafkaRaw(
                            QueuedKafkaRawIngestBatch {
                                batch: core_source::KafkaRawIngestBatch {
                                    source,
                                    records: remaining,
                                },
                                commit_ack: queued.commit_ack.clone(),
                            },
                        ));
                        selected_batch
                    };
                    *count += allowed;
                    *per_connector += allowed;
                    selected_rows += allowed;
                    kafka_raw_batches.push(SelectedKafkaRawIngestBatch {
                        source_id,
                        batch: selected_batch,
                        commit_ack: queued.commit_ack,
                    });
                }
                QueuedAppendIngestItem::KafkaArrow(mut queued) => {
                    let source_id = source_id_by_name.get(queued.batch.source.as_str()).copied();
                    let count = if let Some(source_id) = source_id {
                        &mut per_source_counts[source_id]
                    } else {
                        unknown_source_counts
                            .entry(queued.batch.source.clone())
                            .or_insert(0)
                    };
                    let remaining_source = max_per_source.saturating_sub(*count);
                    let remaining_connector = max_per_connector.saturating_sub(*per_connector);
                    let remaining_batch = max_batch.saturating_sub(selected_rows);
                    let allowed = remaining_source
                        .min(remaining_connector)
                        .min(remaining_batch)
                        .min(queued.batch.len());
                    if allowed == 0
                        || (allowed < queued.batch.len()
                            && !queued.batch.kafka_metadata_ranges.is_empty())
                    {
                        deferred_queue.push_back(QueuedAppendIngestItem::KafkaArrow(queued));
                        continue;
                    }
                    let commit_ack = queued.commit_ack;
                    let selected_batch = if allowed == queued.batch.len() {
                        queued.batch
                    } else {
                        let remaining = queued.batch.split_off(allowed);
                        deferred_queue.push_back(QueuedAppendIngestItem::KafkaArrow(
                            QueuedKafkaArrowIngestBatch {
                                batch: remaining,
                                commit_ack: commit_ack.clone(),
                            },
                        ));
                        queued.batch
                    };
                    *count += allowed;
                    *per_connector += allowed;
                    selected_rows += allowed;
                    kafka_arrow_batches.push(SelectedKafkaArrowIngestBatch {
                        source_id,
                        batch: selected_batch,
                        commit_ack,
                    });
                }
            }
        }
    }
    for (queue, mut deferred_queue) in queues.iter_mut().zip(deferred) {
        if !deferred_queue.is_empty() {
            deferred_queue.append(&mut queue.pending);
            queue.pending = deferred_queue;
        }
    }

    pending_events.record_dequeue(selected_rows);

    BatchSelection {
        batch,
        kafka_raw_batches,
        kafka_arrow_batches,
        per_connector_counts,
        selected_rows,
    }
}
