use super::super::*;

pub(super) fn build_tick_commit_for_checkpoint(
    epoch: u64,
    frontier: u64,
    checkpoint_manager: &CheckpointManager,
    mv_versions: &[MaterializedViewTickVersion],
    committed_kafka_offsets: &HashMap<(Arc<str>, i32), i64>,
) -> TickCommit {
    TickCommit::new(
        epoch,
        frontier,
        checkpoint_manager.snapshot_offsets(),
        mv_versions.to_vec(),
        checkpoint_manager.snapshot_sink_cursors(),
    )
    .with_kafka_offsets(checkpoint_kafka_offsets(committed_kafka_offsets))
    .with_operator_states(checkpoint_operator_states())
}

pub(super) type VectorizedSourceJournalTransientBatch = (usize, Option<i64>, Vec<RecordBatch>);
pub(super) type VectorizedSourceJournalCommitBatch = (String, Option<i64>, Vec<RecordBatch>);

pub(super) fn build_vectorized_source_journal_commit_batches(
    source_names_by_id: &[String],
    source_journal_batches: &[VectorizedSourceJournalTransientBatch],
) -> Vec<VectorizedSourceJournalCommitBatch> {
    source_journal_batches
        .iter()
        .map(|(source_id, max_event_time_ms, batches)| {
            (
                source_names_by_id[*source_id].clone(),
                *max_event_time_ms,
                batches.clone(),
            )
        })
        .collect()
}

fn checkpoint_kafka_offsets(
    committed_kafka_offsets: &HashMap<(Arc<str>, i32), i64>,
) -> Vec<KafkaCheckpointOffset> {
    let mut offsets = committed_kafka_offsets
        .iter()
        .map(|((topic, partition), offset)| KafkaCheckpointOffset {
            topic: topic.to_string(),
            partition: *partition,
            offset: *offset,
        })
        .collect::<Vec<_>>();
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    offsets
}

fn checkpoint_operator_states() -> Vec<floe_executor::checkpoint::DbspHandleRecord> {
    dbsp::snapshot_operator_states()
        .into_iter()
        .map(|handle| {
            floe_executor::checkpoint::DbspHandleRecord::operator_state(
                handle.name,
                handle.namespace,
                handle.version,
            )
        })
        .collect()
}

pub(super) fn record_postgres_cdc_lsn_progress(
    committed_postgres_lsns: &mut HashMap<String, (u64, String)>,
    tick_postgres_lsns: &HashMap<String, (u64, String)>,
    tick_postgres_sources: &HashMap<String, String>,
    tick_postgres_table_lsns: &[(String, String, String, u64)],
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
) {
    advance_postgres_cdc_commit_state(committed_postgres_lsns, tick_postgres_lsns);
    for (slot, (lsn_value, _)) in tick_postgres_lsns {
        if let Some(source) = tick_postgres_sources.get(slot) {
            metrics::record_postgres_cdc_durable_lsn(source, slot, *lsn_value);
            record_postgres_cdc_debug_lsn(
                cdc_replication_debug,
                source,
                slot,
                None,
                Some(*lsn_value),
            );
        }
    }
    for (source, slot, table, lsn_value) in tick_postgres_table_lsns {
        metrics::record_postgres_cdc_table_applied_lsn(source, slot, table, *lsn_value);
    }
}

pub(super) fn notify_postgres_cdc_commit_senders(
    epoch: u64,
    committed_postgres_lsns: &HashMap<String, (u64, String)>,
    tick_postgres_lsns: &HashMap<String, (u64, String)>,
    senders: &[watch::Sender<PostgresCdcCommit>],
) {
    if tick_postgres_lsns.is_empty() || senders.is_empty() {
        return;
    }
    let postgres_commit_start = Instant::now();
    let commit = build_postgres_cdc_commit(epoch, committed_postgres_lsns);
    for sender in senders {
        let _ = sender.send(commit.clone());
    }
    metrics::observe_tick_phase_latency_ms(
        "postgres_cdc_commit_notify",
        postgres_commit_start.elapsed().as_millis() as u64,
    );
}
