use super::*;

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
        let Some(zset_handle) = handle.handle_for_version(frontier) else {
            continue;
        };
        let view = handle.name().to_string();
        let version = zset_handle.version;
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
        core_source::SourceResumeToken::PostgresCdc { lsn, .. } => {
            parse_postgres_lsn(lsn).map(|offset| (0, offset))
        }
        core_source::SourceResumeToken::File { cursor }
        | core_source::SourceResumeToken::Generator { position: cursor }
        | core_source::SourceResumeToken::ObjectStore { cursor } => Some((0, *cursor)),
    }
}

pub(super) fn event_kafka_offset(
    token: Option<&core_source::SourceResumeToken>,
) -> Option<(String, i32, i64)> {
    match token? {
        core_source::SourceResumeToken::Kafka {
            topic,
            partition,
            offset,
        } => Some((topic.clone(), *partition, *offset)),
        _ => None,
    }
}

pub(super) fn event_postgres_lsn(
    token: Option<&core_source::SourceResumeToken>,
) -> Option<(String, u64, String)> {
    match token? {
        core_source::SourceResumeToken::PostgresCdc { slot, lsn, .. } => {
            let slot = slot.clone().unwrap_or_else(|| "default".to_string());
            let value = parse_postgres_lsn(lsn)?;
            Some((slot, value, lsn.clone()))
        }
        _ => None,
    }
}

pub(super) fn build_kafka_offset_commit(
    tick_id: u64,
    offsets: &HashMap<(String, i32), i64>,
) -> KafkaOffsetCommit {
    let mut entries: Vec<KafkaTopicPartitionOffset> = offsets
        .iter()
        .map(|((topic, partition), offset)| KafkaTopicPartitionOffset {
            topic: topic.clone(),
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

pub(super) fn parse_postgres_lsn(lsn: &str) -> Option<u64> {
    let (left, right) = lsn.trim().split_once('/')?;
    let high = u64::from_str_radix(left, 16).ok()?;
    let low = u64::from_str_radix(right, 16).ok()?;
    Some((high << 32) | low)
}

pub(super) fn current_unix_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

pub(super) async fn recv_from_any(queues: &mut Vec<ConnectorQueue>) -> bool {
    if queues.is_empty() {
        return false;
    }
    let (event, index) = {
        let futures: Vec<_> = queues
            .iter_mut()
            .map(|queue| Box::pin(queue.receiver.recv()))
            .collect();
        let (event, index, _remaining) = select_all(futures).await;
        (event, index)
    };
    match event {
        Some(event) => {
            queues[index].pending.push_back(event);
        }
        None => {
            queues[index].closed = true;
        }
    }
    queues.retain(|queue| !(queue.closed && queue.pending.is_empty()));
    !queues.is_empty()
}

pub(super) fn drain_connectors(queues: &mut [ConnectorQueue], capacity: usize) {
    for queue in queues.iter_mut() {
        while queue.pending.len() < capacity {
            match queue.receiver.try_recv() {
                Ok(event) => queue.pending.push_back(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    queue.closed = true;
                    break;
                }
            }
        }
    }
}

pub(super) fn build_batch(
    queues: &mut [ConnectorQueue],
    start_index: usize,
    max_batch: usize,
    max_per_source: usize,
    max_per_connector: usize,
) -> BatchSelection {
    let mut batch = Vec::with_capacity(max_batch);
    let mut per_source_counts: HashMap<String, usize> = HashMap::new();
    let mut per_connector_counts: HashMap<String, usize> = HashMap::new();
    let mut deferred: Vec<VecDeque<core_source::SourceEvent>> = vec![VecDeque::new(); queues.len()];
    let connector_count = queues.len();
    for step in 0..connector_count {
        let idx = (start_index + step) % connector_count;
        let queue = &mut queues[idx];
        let deferred_queue = &mut deferred[idx];
        let per_connector = per_connector_counts.entry(queue.name.clone()).or_insert(0);
        while *per_connector < max_per_connector && batch.len() < max_batch {
            let Some(event) = queue.pending.pop_front() else {
                break;
            };
            let source = event.source();
            let count = per_source_counts.entry(source.to_string()).or_insert(0);
            if *count >= max_per_source {
                deferred_queue.push_back(event);
                continue;
            }
            *count += 1;
            *per_connector += 1;
            batch.push(event);
        }
    }
    for (queue, mut deferred_queue) in queues.iter_mut().zip(deferred) {
        if !deferred_queue.is_empty() {
            deferred_queue.append(&mut queue.pending);
            queue.pending = deferred_queue;
        }
    }

    BatchSelection {
        batch,
        per_connector_counts,
    }
}
