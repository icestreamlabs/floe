use super::*;

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn lookup_decoder_for_source<'a>(
    decoders: &'a HashMap<String, SourceRowDecoder>,
    source_name: &str,
) -> anyhow::Result<&'a SourceRowDecoder> {
    decoders
        .get(source_name)
        .ok_or_else(|| anyhow!("received event for unknown source '{source_name}'"))
}

pub(super) fn should_sample(counter: &AtomicU64, every: u64) -> bool {
    if every == 0 {
        return true;
    }
    counter
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
}

pub(super) fn record_runtime_failure(state: &Arc<StdMutex<Option<String>>>, message: String) {
    metrics::inc_runtime_error("runtime");
    let mut guard = state.lock().expect("runtime failure lock poisoned");
    if guard.is_none() {
        *guard = Some(message);
    }
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

pub(super) fn event_resume_offset(
    token: Option<&core_source::SourceResumeToken>,
) -> Option<(u32, u64)> {
    match token? {
        core_source::SourceResumeToken::Kafka {
            partition, offset, ..
        } => {
            let partition = u32::try_from(*partition).ok()?;
            let offset = u64::try_from(*offset).ok()?;
            Some((partition, offset))
        }
        core_source::SourceResumeToken::File { cursor }
        | core_source::SourceResumeToken::Generator { position: cursor }
        | core_source::SourceResumeToken::ObjectStore { cursor } => Some((0, *cursor)),
    }
}

pub(super) fn event_fast_resume_offset(event: &core_source::SourceEvent) -> Option<(u32, u64)> {
    let (_, partition, offset) = event.kafka_position()?;
    let partition = u32::try_from(partition).ok()?;
    let offset = u64::try_from(offset).ok()?;
    Some((partition, offset))
}

pub(super) fn event_fast_kafka_offset(
    event: &core_source::SourceEvent,
) -> Option<(Arc<str>, i32, i64)> {
    let (topic, partition, offset) = event.kafka_position()?;
    Some((Arc::clone(topic), partition, offset))
}

pub(super) fn event_kafka_offset(
    token: Option<&core_source::SourceResumeToken>,
) -> Option<(Arc<str>, i32, i64)> {
    match token? {
        core_source::SourceResumeToken::Kafka {
            topic,
            partition,
            offset,
        } => Some((Arc::<str>::from(topic.as_str()), *partition, *offset)),
        _ => None,
    }
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
    receiver: &mut core_source::RoutedSourceEventReceiver,
    queues: &mut [ConnectorQueue],
) -> bool {
    let Some(batch) = receiver.recv().await else {
        return false;
    };
    if let Some(queue) = queues.get_mut(batch.connector_id) {
        queue
            .pending
            .extend(batch.events.into_iter().map(|event| QueuedSourceEvent {
                event,
                commit_ack: batch.commit_ack.clone(),
            }));
    }
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
    loop {
        match receiver.try_recv() {
            Ok(transaction) => queue.push_back(transaction),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
}

pub(super) fn drain_ready(
    receiver: &mut core_source::RoutedSourceEventReceiver,
    queues: &mut [ConnectorQueue],
) {
    loop {
        match receiver.try_recv() {
            Ok(batch) => {
                if let Some(queue) = queues.get_mut(batch.connector_id) {
                    queue
                        .pending
                        .extend(batch.events.into_iter().map(|event| QueuedSourceEvent {
                            event,
                            commit_ack: batch.commit_ack.clone(),
                        }));
                }
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
}

pub(super) fn build_batch(
    queues: &mut [ConnectorQueue],
    source_id_by_name: &HashMap<String, usize>,
    source_count: usize,
    start_index: usize,
    max_batch: usize,
    max_per_source: usize,
    max_per_connector: usize,
    pending_events: &core_source::PendingEventCounter,
) -> BatchSelection {
    let mut batch = Vec::with_capacity(max_batch);
    let mut per_source_counts = vec![0usize; source_count];
    let mut unknown_source_counts: HashMap<String, usize> = HashMap::new();
    let per_connector_count_len = queues
        .iter()
        .map(|queue| queue.id)
        .max()
        .map_or(0, |id| id + 1);
    let mut per_connector_counts = vec![0usize; per_connector_count_len];
    let mut deferred: Vec<VecDeque<QueuedSourceEvent>> =
        (0..queues.len()).map(|_| VecDeque::new()).collect();
    let connector_count = queues.len();
    for step in 0..connector_count {
        let idx = (start_index + step) % connector_count;
        let queue = &mut queues[idx];
        let deferred_queue = &mut deferred[idx];
        let per_connector = &mut per_connector_counts[queue.id];
        while *per_connector < max_per_connector && batch.len() < max_batch {
            let Some(queued) = queue.pending.pop_front() else {
                break;
            };
            let source_id = queued
                .event
                .source_id()
                .or_else(|| source_id_by_name.get(queued.event.source()).copied());
            let count = if let Some(source_id) = source_id {
                &mut per_source_counts[source_id]
            } else {
                unknown_source_counts
                    .entry(queued.event.source().to_string())
                    .or_insert(0)
            };
            if *count >= max_per_source {
                deferred_queue.push_back(queued);
                continue;
            }
            *count += 1;
            *per_connector += 1;
            batch.push(SelectedSourceEvent {
                source_id,
                event: queued.event,
                commit_ack: queued.commit_ack,
            });
        }
    }
    for (queue, mut deferred_queue) in queues.iter_mut().zip(deferred) {
        if !deferred_queue.is_empty() {
            deferred_queue.append(&mut queue.pending);
            queue.pending = deferred_queue;
        }
    }

    pending_events.record_dequeue(batch.len());

    BatchSelection {
        batch,
        per_connector_counts,
    }
}
