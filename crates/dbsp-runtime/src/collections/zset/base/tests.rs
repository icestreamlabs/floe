use super::super::{SegmentRecord, VersionedZSet, h, prefix_bounds};
use super::*;
use std::collections::HashMap;
use std::sync::Arc;

use crate::handles::ZSetHandleView;
use object_store::memory::InMemory;
use slatedb::WriteBatch;

async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("test", store).await.expect("open SlateDB"))
}

#[tokio::test]
async fn creates_and_persists_weights() {
    let db = build_db().await;
    let mut zset = ZSet::new(db.clone(), "weights").await.expect("create zset");

    zset.set_weight("a".to_string(), 1);
    zset.set_weight("b".to_string(), 2);
    zset.flush().await.unwrap();

    let mut reload = ZSet::new(db, "weights").await.expect("reload zset");
    assert_eq!(reload.get_weight(&"a".to_string()).await.unwrap(), 1);
    assert_eq!(reload.get_weight(&"b".to_string()).await.unwrap(), 2);
}

#[tokio::test]
async fn logical_merge_of_pending_before_flush() {
    let db = build_db().await;
    let mut zset = ZSet::new(db, "merge").await.expect("create zset");

    zset.set_weight("item".to_string(), 3);
    assert!(zset.contains(&"item".to_string()).await.unwrap());
    zset.set_weight("item".to_string(), 0);
    assert!(!zset.contains(&"item".to_string()).await.unwrap());
    zset.flush().await.unwrap();
    assert!(zset.items().await.unwrap().is_empty());
}

#[tokio::test]
async fn add_weight_accumulates() {
    let db = build_db().await;
    let mut zset = ZSet::new(db.clone(), "acc").await.expect("create zset");

    zset.add_weight("key".to_string(), 3).await.unwrap();
    zset.flush().await.unwrap();

    let mut reload = ZSet::new(db, "acc").await.expect("reload zset");
    assert_eq!(reload.get_weight(&"key".to_string()).await.unwrap(), 3);

    reload.add_weight("key".to_string(), -3).await.unwrap();
    reload.flush().await.unwrap();
    assert!(reload.is_identity().await.unwrap());
}

#[tokio::test]
async fn insert_then_negates_to_zero_removes_entry() {
    let db = build_db().await;
    let mut zset = ZSet::new(db, "zero_remove").await.expect("create zset");

    zset.add_weight("gone".to_string(), 1).await.unwrap();
    zset.flush().await.unwrap();
    zset.add_weight("gone".to_string(), -1).await.unwrap();
    zset.flush().await.unwrap();

    assert!(
        !zset
            .contains(&"gone".to_string())
            .await
            .expect("contains check")
    );
    assert!(zset.items().await.expect("items after cancel").is_empty());
}

#[tokio::test]
async fn sequential_deltas_equivalent_to_aggregated_delta() {
    let db = build_db().await;
    let mut seq = ZSet::new(db.clone(), "seq").await.expect("seq zset");

    let deltas = vec![
        vec![("a".to_string(), 1), ("b".to_string(), 2)],
        vec![("a".to_string(), -1), ("b".to_string(), 3)],
    ];

    for batch in &deltas {
        for (key, delta) in batch {
            seq.add_weight(key.clone(), *delta).await.unwrap();
        }
        seq.flush().await.unwrap();
    }
    let seq_items: HashMap<_, _> = seq.items().await.expect("seq items").into_iter().collect();

    let mut aggregate_map: HashMap<String, i64> = HashMap::new();
    for batch in &deltas {
        for (key, delta) in batch {
            let entry = aggregate_map.entry(key.clone()).or_insert(0);
            *entry += *delta;
            if *entry == 0 {
                aggregate_map.remove(key);
            }
        }
    }

    let mut agg = ZSet::new(db, "agg").await.expect("agg zset");
    for (key, weight) in &aggregate_map {
        agg.set_weight(key.clone(), *weight);
    }
    agg.flush().await.unwrap();
    let agg_items: HashMap<_, _> = agg.items().await.expect("agg items").into_iter().collect();

    assert_eq!(seq_items, agg_items);
    assert_eq!(agg_items.get("a"), None);
    assert_eq!(agg_items.get("b"), Some(&5));
}

#[tokio::test]
async fn h_distincts_differences() {
    let db = build_db().await;
    let mut diff = ZSet::new(db.clone(), "h_diff")
        .await
        .expect("create diff zset");
    let mut state = ZSet::new(db.clone(), "h_state")
        .await
        .expect("create state zset");

    diff.set_weight("enter".to_string(), 2);
    diff.set_weight("leave".to_string(), -3);
    diff.set_weight("stay".to_string(), -1);

    state.set_weight("leave".to_string(), 3);
    state.set_weight("stay".to_string(), 1);

    let mut result = h(&diff, &state).await.expect("compute h");
    let mut entries = result.items().await.expect("materialize result");
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    assert_eq!(
        entries,
        vec![
            ("enter".to_string(), 1),
            ("leave".to_string(), -1),
            ("stay".to_string(), -1)
        ]
    );
}

#[tokio::test]
async fn recovers_after_partial_flush() {
    let db = build_db().await;
    let mut zset = ZSet::new(db.clone(), "recover").await.expect("create zset");

    zset.set_weight("stay".to_string(), 5);
    zset.flush().await.expect("flush zset");

    let dict = zset.dict.clone();
    let stay_id = dict
        .intern(&"stay".to_string())
        .await
        .expect("intern stay key");
    let remove_id = stay_id + 1;

    let mut batch = WriteBatch::new();
    batch.put(zset.encode_id(remove_id), encode_weight(10));
    batch.put(zset.encode_id(stay_id), encode_weight(5));
    zset.table
        .write_batch(batch)
        .await
        .expect("write partial state");

    let mut reopened = ZSet::new(db, "recover").await.expect("reopen zset");
    assert_eq!(reopened.get_weight(&"stay".to_string()).await.unwrap(), 5);
    assert_eq!(reopened.get_weight(&"remove".to_string()).await.unwrap(), 0);
}

#[tokio::test]
async fn reuses_interned_id_after_reopen() {
    let db = build_db().await;
    let mut zset = ZSet::new(db.clone(), "reuse").await.expect("create zset");

    zset.set_weight("shared".to_string(), 4);
    zset.flush().await.expect("flush zset");

    let id_before = zset
        .dict
        .lookup(&"shared".to_string())
        .await
        .expect("lookup shared key")
        .expect("id present");

    let mut reopen = ZSet::new(db, "reuse").await.expect("reopen zset");
    let id_after = reopen
        .dict
        .lookup(&"shared".to_string())
        .await
        .expect("lookup after reopen")
        .expect("id present after reopen");

    assert_eq!(id_before, id_after);
    assert_eq!(reopen.get_weight(&"shared".to_string()).await.unwrap(), 4);
}

#[tokio::test]
async fn versioned_zset_materializes_view() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "vz", None)
            .await
            .expect("build dictionary"),
    );

    let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), "vz".to_string())
        .await
        .expect("create versioned zset");

    let key_id = dict
        .intern(&"item".to_string())
        .await
        .expect("intern item key");
    let segment = SegmentRecord {
        id: 1,
        bucket: 0,
        deltas: vec![(key_id, 7)],
    };
    versioned
        .create_version(vec![segment])
        .await
        .expect("create version");

    let view = versioned.materialize().await.expect("materialize view");
    assert_eq!(view.get("item"), Some(&7));

    let reopened = VersionedZSet::new(dict.clone(), table.clone(), "vz".to_string())
        .await
        .expect("reopen versioned zset");
    let view = reopened
        .materialize()
        .await
        .expect("materialize reopened view");
    assert_eq!(view.get("item"), Some(&7));
}

#[tokio::test]
async fn compacts_versioned_zset() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "vz_compact", None)
            .await
            .expect("build dictionary"),
    );

    let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), "vz_compact".to_string())
        .await
        .expect("create versioned zset");

    let id_a = dict.intern(&"a".to_string()).await.expect("intern a");
    let id_b = dict.intern(&"b".to_string()).await.expect("intern b");

    let segments = vec![
        SegmentRecord {
            id: 1,
            bucket: 0,
            deltas: vec![(id_a, 4)],
        },
        SegmentRecord {
            id: 2,
            bucket: 1,
            deltas: vec![(id_b, 6)],
        },
    ];
    versioned
        .create_version(segments)
        .await
        .expect("create multi-segment version");

    let view_before = versioned.materialize().await.expect("materialize");
    assert_eq!(view_before.get("a"), Some(&4));
    assert_eq!(view_before.get("b"), Some(&6));

    versioned.compact_current().await.expect("compact version");

    let manifests_after = table
        .scan_range(
            prefix_bounds(versioned.manifest_prefix_bytes()),
            &ScanOptions::default(),
        )
        .await
        .expect("scan manifests after compaction");
    assert_eq!(manifests_after.len(), 1);

    let segments_after = table
        .scan_range(
            prefix_bounds(versioned.segment_prefix_bytes()),
            &ScanOptions::default(),
        )
        .await
        .expect("scan segments after compaction");
    assert_eq!(segments_after.len(), 2);

    let view_after = versioned
        .materialize()
        .await
        .expect("materialize after compact");
    assert_eq!(view_after.get("a"), Some(&4));
    assert_eq!(view_after.get("b"), Some(&6));

    let reopened = VersionedZSet::new(dict.clone(), table.clone(), "vz_compact".to_string())
        .await
        .expect("reopen");
    let manifest = reopened.manifest().expect("manifest present");
    assert_eq!(manifest.buckets.len(), 2);
    let total_segments: usize = manifest.buckets.values().map(|v| v.len()).sum();
    assert_eq!(total_segments, 2);
    let view_reopen = reopened.materialize().await.expect("materialize reopened");
    assert_eq!(view_reopen.get("a"), Some(&4));
    assert_eq!(view_reopen.get("b"), Some(&6));
}

#[tokio::test]
async fn release_version_removes_segments() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "vz_release", None)
            .await
            .expect("build dictionary"),
    );

    let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), "vz_release".to_string())
        .await
        .expect("create versioned zset");

    let id = dict.intern(&"x".to_string()).await.expect("intern key");
    let version = versioned
        .create_version(vec![SegmentRecord {
            id: 1,
            bucket: 0,
            deltas: vec![(id, 9)],
        }])
        .await
        .expect("create version");

    versioned
        .release_version(version)
        .await
        .expect("release version");

    let segments = table
        .scan_range(
            prefix_bounds(versioned.segment_prefix_bytes()),
            &ScanOptions::default(),
        )
        .await
        .expect("scan segments");
    assert!(segments.is_empty());

    let manifests = table
        .scan_range(
            prefix_bounds(versioned.manifest_prefix_bytes()),
            &ScanOptions::default(),
        )
        .await
        .expect("scan manifests");
    assert!(manifests.is_empty());

    let view = versioned.materialize().await.expect("materialize");
    assert!(view.is_empty());
}

#[tokio::test]
async fn release_base_keeps_manifest_while_referenced() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "vz_refs", None)
            .await
            .expect("build dictionary"),
    );

    let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), "vz_refs".to_string())
        .await
        .expect("create versioned zset");

    let base_id = dict
        .intern(&"base".to_string())
        .await
        .expect("intern base key");
    let v1 = versioned
        .create_version(vec![SegmentRecord {
            id: 1,
            bucket: 0,
            deltas: vec![(base_id, 2)],
        }])
        .await
        .expect("create base version");

    let child_id = dict
        .intern(&"child".to_string())
        .await
        .expect("intern child key");
    let v2 = versioned
        .create_version(vec![SegmentRecord {
            id: 2,
            bucket: 1,
            deltas: vec![(child_id, 3)],
        }])
        .await
        .expect("create child version");
    assert_eq!(v2, v1 + 1);

    versioned
        .release_version(v1)
        .await
        .expect("release base while child exists");

    let manifests = table
        .scan_range(
            prefix_bounds(versioned.manifest_prefix_bytes()),
            &ScanOptions::default(),
        )
        .await
        .expect("scan manifests after base release");
    assert_eq!(manifests.len(), 2);

    versioned
        .release_version(v2)
        .await
        .expect("release child version");

    let manifests = table
        .scan_range(
            prefix_bounds(versioned.manifest_prefix_bytes()),
            &ScanOptions::default(),
        )
        .await
        .expect("scan manifests after releasing child");
    assert!(manifests.is_empty());
}

#[tokio::test]
async fn recovers_version_intent_on_reopen() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "vz_intent", None)
            .await
            .expect("build dictionary"),
    );

    let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), "vz_intent".to_string())
        .await
        .expect("create versioned zset");

    let id = dict.intern(&"y".to_string()).await.expect("intern key");
    versioned
        .create_version(vec![SegmentRecord {
            id: 1,
            bucket: 0,
            deltas: vec![(id, 5)],
        }])
        .await
        .expect("create version");

    let versioned_intent = versioned.intent_key_bytes().to_vec();
    let mut batch = WriteBatch::new();
    batch.put(versioned_intent.clone(), vec![1]);
    table
        .write_batch(batch)
        .await
        .expect("write lingering intent");

    let reopened = VersionedZSet::new(dict, table, "vz_intent".to_string())
        .await
        .expect("reopen versioned zset");
    assert!(reopened.manifest().is_some());
    assert!(
        reopened
            .table()
            .get(&versioned_intent)
            .await
            .expect("get lingering intent after reopen")
            .is_none(),
        "intent key should be cleared during reopen"
    );
    let materialized = reopened.materialize().await.expect("materialize reopened");
    assert_eq!(materialized.get("y"), Some(&5));
}

#[tokio::test]
async fn orphan_segment_write_is_not_visible_after_reopen() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "vz_orphan_segment", None)
            .await
            .expect("build dictionary"),
    );

    let mut versioned =
        VersionedZSet::new(dict.clone(), table.clone(), "vz_orphan_segment".to_string())
            .await
            .expect("create versioned zset");

    let id = dict
        .intern(&"base".to_string())
        .await
        .expect("intern base key");
    versioned
        .create_version(vec![SegmentRecord {
            id: 1,
            bucket: 0,
            deltas: vec![(id, 3)],
        }])
        .await
        .expect("create base version");

    let segment_prefix = versioned.segment_prefix_bytes().to_vec();
    let segments = table
        .scan_range(prefix_bounds(&segment_prefix), &ScanOptions::default())
        .await
        .expect("scan persisted segments");
    assert_eq!(segments.len(), 1, "expected one persisted segment");
    let (existing_segment_key, existing_segment_payload) = &segments[0];
    let bucket_offset = segment_prefix.len();
    let bucket = u16::from_be_bytes(
        existing_segment_key[bucket_offset..bucket_offset + 2]
            .try_into()
            .expect("bucket bytes"),
    );
    let mut orphan_segment_key = segment_prefix.clone();
    orphan_segment_key.extend_from_slice(&bucket.to_be_bytes());
    orphan_segment_key.push(b'/');
    orphan_segment_key.extend_from_slice(&9_999_u64.to_be_bytes());
    table
        .put(&orphan_segment_key, existing_segment_payload)
        .await
        .expect("write orphan segment payload");

    let reopened = VersionedZSet::new(dict, table.clone(), "vz_orphan_segment".to_string())
        .await
        .expect("reopen versioned zset");
    let materialized = reopened.materialize().await.expect("materialize reopened");
    assert_eq!(materialized.get("base"), Some(&3));
    assert!(
        materialized.get("orphan").is_none(),
        "orphan segment must not become visible without a manifest reference"
    );

    let stats = reopened.chain_stats().await.expect("version chain stats");
    assert_eq!(stats.version_count, 1);
    assert_eq!(stats.segment_count, 1);
    assert!(
        table
            .get(&orphan_segment_key)
            .await
            .expect("get orphan segment key")
            .is_some(),
        "orphan segment bytes may exist physically but must stay unreachable"
    );
}

#[tokio::test]
async fn replayable_head_reads_from_transient_batch_without_persisting() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "vz_replayable", None)
            .await
            .expect("build dictionary"),
    );

    let mut versioned =
        VersionedZSet::new(dict.clone(), table.clone(), "vz_replayable".to_string())
            .await
            .expect("create versioned zset");

    let persisted_id = dict
        .intern(&"persisted".to_string())
        .await
        .expect("intern persisted key");
    versioned
        .create_version(vec![SegmentRecord {
            id: 1,
            bucket: 0,
            deltas: vec![(persisted_id, 1)],
        }])
        .await
        .expect("create persisted version");

    versioned.enable_replayable_persistence();
    let live_handle = versioned.publish_replayable_batch(Arc::new(vec![
        ("live".to_string(), 2),
        ("removed".to_string(), 1),
        ("removed".to_string(), -1),
    ]));

    let live_delta = ZSetHandleView::new(
        dict.clone(),
        table.clone(),
        live_handle.ns.clone(),
        live_handle.version,
    )
    .delta_iter()
    .await
    .expect("read replayable delta through handle view");
    assert_eq!(
        live_delta,
        vec![
            ("live".to_string(), 2),
            ("removed".to_string(), 1),
            ("removed".to_string(), -1)
        ]
    );

    let live_materialized = versioned
        .load_existing_version(live_handle.version)
        .await
        .expect("load replayable version");
    assert_eq!(live_materialized.get("live"), Some(&2));
    assert!(live_materialized.get("removed").is_none());

    let reopened = VersionedZSet::new(dict, table, "vz_replayable".to_string())
        .await
        .expect("reopen replayable zset");
    let reopened_view = reopened.materialize().await.expect("materialize reopened");
    assert_eq!(reopened_view.get("persisted"), Some(&1));
    assert!(reopened_view.get("live").is_none());
}
