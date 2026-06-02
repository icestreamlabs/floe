use super::*;
use crate::dbsp_bridge::DbspBridge;
use dbsp::storage::SlateTable;
use object_store::memory::InMemory;
use slatedb::Db;
fn encoded_i64_row(value: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + 1 + 8);
    encoded.extend_from_slice(&(1_u32).to_le_bytes());
    encoded.push(0x01);
    encoded.extend_from_slice(&value.to_le_bytes());
    encoded
}

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

#[tokio::test]
async fn checkpoint_manifest_keeps_materialized_view_with_noop_latest_tick() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_checkpoint_noop");

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open("checkpoint-mv-noop", store).await.expect("db"));
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_checkpoint_noop",
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");

    dbsp_view.add_delta(encoded_i64_row(5), 1);
    let handle = dbsp_view.flush().await.expect("flush base version");
    let latest_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = latest_view.into_parts();
    view.set_dbsp_state(
        DbspPersistedState::new(dict, table, namespace.clone(), version).with_logical_version(2),
    );
    view.publish_version(1, handle.clone());
    view.publish_logical_version(2);

    let entries = materialized_view_entries(&registry);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].view, "mv_checkpoint_noop");
    assert_eq!(entries[0].namespace, namespace);
    assert_eq!(entries[0].version, handle.version);
    assert_eq!(entries[0].frontier, 2);
}

#[tokio::test]
async fn recover_materialized_view_restores_frontier_ahead_of_handle_version() {
    let registry = MaterializedViewRegistry::new();

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("checkpoint-mv-recover-noop", store)
            .await
            .expect("db"),
    );
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_checkpoint_recover_noop",
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");

    dbsp_view.add_delta(encoded_i64_row(11), 1);
    let handle = dbsp_view.flush().await.expect("flush base version");

    let manifest = CheckpointManifest {
        id: 1,
        watermark: 0,
        format: ManifestFormat::V2,
        dbsp_handles: vec![DbspHandleRecord::materialized_view(
            "mv_checkpoint_recover_noop",
            handle.ns.clone(),
            handle.version,
        )],
        source_offsets: Vec::new(),
        operator_states: Vec::new(),
        materialized_views: vec![MaterializedViewCheckpointEntry {
            view: "mv_checkpoint_recover_noop".to_string(),
            namespace: handle.ns.clone(),
            version: handle.version,
            frontier: 2,
        }],
        outer_streams: Vec::new(),
        sink_cursors: Vec::new(),
    };

    recover_materialized_views(&manifest, &registry, &mut bridge)
        .await
        .expect("recover materialized views");

    let view = registry
        .get("mv_checkpoint_recover_noop")
        .expect("recovered view");
    assert_eq!(view.latest_version(), Some(2));
    let state = view.dbsp_state().expect("recovered dbsp state");
    assert_eq!(state.version(), handle.version);
    assert_eq!(state.logical_version(), 2);
    let recovered_handle = view
        .handle_for_version(2)
        .expect("recovered logical version handle");
    assert_eq!(recovered_handle.version, handle.version);
    assert_eq!(recovered_handle.ns, handle.ns);
}
