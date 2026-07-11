use super::*;
use dbsp::storage::SlateTable;
use object_store::memory::InMemory;
use slatedb::Db;

async fn checkpoint_manager(graph_id: &str) -> CheckpointManager {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open(format!("checkpoint-{graph_id}"), store)
            .await
            .expect("db"),
    );
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    CheckpointManager::new(graph_id.to_string(), table)
        .await
        .expect("checkpoint manager")
}

#[tokio::test]
async fn tick_commit_roundtrips_via_store() {
    let mut manager = checkpoint_manager("tick-roundtrip").await;
    manager.update_partition_offset("nexmark_bid", 0, 42);
    let commit = TickCommit::new(
        7,
        123,
        manager.snapshot_offsets(),
        vec![MaterializedViewTickVersion {
            view: "mv_bid".to_string(),
            version: 9,
        }],
        vec![SinkCursor {
            sink: "kafka_out".to_string(),
            mv_name: "mv_bid".to_string(),
            last_emitted_mv_version: 9,
            row_index: None,
        }],
    );
    manager
        .persist_tick_commit(commit.clone())
        .await
        .expect("persist tick commit");

    let reloaded = CheckpointManager::new("tick-roundtrip", manager.store().table())
        .await
        .expect("reload checkpoint manager");
    assert_eq!(reloaded.latest_tick_commit(), Some(&commit));
    assert_eq!(reloaded.snapshot_sink_cursors(), commit.sink_cursors);
    assert_eq!(reloaded.snapshot_offsets(), commit.source_offsets);
}

#[tokio::test]
async fn restored_offsets_are_retained_by_the_next_tick() {
    let mut manager = checkpoint_manager("tick-offset-retention").await;
    manager.update_partition_offset("topic_a", 0, 10);
    manager.update_partition_offset("topic_b", 0, 20);
    let first = TickCommit::new(1, 100, manager.snapshot_offsets(), Vec::new(), Vec::new());
    manager
        .persist_tick_commit(first)
        .await
        .expect("persist first tick");

    let mut reloaded = CheckpointManager::new("tick-offset-retention", manager.store().table())
        .await
        .expect("reload checkpoint manager");
    reloaded.update_partition_offset("topic_a", 0, 11);
    let second = TickCommit::new(2, 200, reloaded.snapshot_offsets(), Vec::new(), Vec::new());
    reloaded
        .persist_tick_commit(second.clone())
        .await
        .expect("persist second tick");

    assert!(second.source_offsets.iter().any(|offset| {
        offset.source == "topic_a" && offset.partition == 0 && offset.offset == 11
    }));
    assert!(second.source_offsets.iter().any(|offset| {
        offset.source == "topic_b" && offset.partition == 0 && offset.offset == 20
    }));
}

#[tokio::test]
async fn snapshot_offsets_tracks_partitions() {
    let mut manager = checkpoint_manager("partition-offsets").await;
    manager.update_partition_offset("topic_a", 1, 11);
    manager.update_partition_offset("topic_a", 0, 7);
    manager.update_partition_offset("topic_a", 1, 9);
    manager.update_partition_offset("topic_a", 1, 15);

    let offsets = manager.snapshot_offsets();
    assert_eq!(offsets.len(), 2);
    assert!(
        offsets.iter().any(|entry| {
            entry.source == "topic_a" && entry.partition == 0 && entry.offset == 7
        })
    );
    assert!(
        offsets.iter().any(|entry| {
            entry.source == "topic_a" && entry.partition == 1 && entry.offset == 15
        })
    );
}

#[tokio::test]
async fn sink_cursor_state_roundtrips_and_is_monotonic() {
    let mut manager = checkpoint_manager("sink-cursors").await;
    manager.update_sink_cursor("sink_a", "mv_bid", 7, Some(3));
    manager.update_sink_cursor("sink_a", "mv_bid", 7, Some(1));
    manager.update_sink_cursor("sink_a", "mv_bid", 8, None);
    manager.update_sink_cursor("sink_b", "mv_bid", 2, None);

    let commit = TickCommit::new(
        2,
        100,
        manager.snapshot_offsets(),
        Vec::new(),
        manager.snapshot_sink_cursors(),
    );
    manager
        .persist_tick_commit(commit.clone())
        .await
        .expect("persist sink cursor commit");

    let reloaded = CheckpointManager::new("sink-cursors", manager.store().table())
        .await
        .expect("reload checkpoint manager");
    let cursors = reloaded.snapshot_sink_cursors();
    assert_eq!(cursors, commit.sink_cursors);
    assert!(cursors.iter().any(|cursor| {
        cursor.sink == "sink_a" && cursor.last_emitted_mv_version == 8 && cursor.row_index.is_none()
    }));
}

#[tokio::test]
async fn tick_commit_roundtrips_kafka_offsets() {
    let mut manager = checkpoint_manager("tick-kafka-offsets").await;
    let commit =
        TickCommit::new(7, 123, Vec::new(), Vec::new(), Vec::new()).with_kafka_offsets(vec![
            KafkaCheckpointOffset {
                topic: "topic_a".to_string(),
                partition: 2,
                offset: 42,
            },
        ]);

    manager
        .persist_tick_commit(commit.clone())
        .await
        .expect("persist tick commit");

    let reloaded = CheckpointManager::new("tick-kafka-offsets", manager.store().table())
        .await
        .expect("reload checkpoint manager");
    assert_eq!(reloaded.latest_tick_commit(), Some(&commit));
}

#[tokio::test]
async fn tick_commit_roundtrips_operator_state_handles() {
    let mut manager = checkpoint_manager("tick-operator-state").await;
    let commit =
        TickCommit::new(9, 321, Vec::new(), Vec::new(), Vec::new()).with_operator_states(vec![
            DbspHandleRecord::operator_state("aggregate_state_0", "aggregate_state_0", 7),
        ]);

    manager
        .persist_tick_commit(commit.clone())
        .await
        .expect("persist tick commit");

    let reloaded = CheckpointManager::new("tick-operator-state", manager.store().table())
        .await
        .expect("reload checkpoint manager");
    assert_eq!(reloaded.latest_tick_commit(), Some(&commit));
}
