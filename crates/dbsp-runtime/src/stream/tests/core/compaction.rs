use std::sync::Arc;

use crate::collections::CompactionPolicy;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::tests::common::build_db;
use crate::stream::{StreamRetention, ZSetStream};
#[tokio::test(flavor = "multi_thread")]
async fn zset_stream_compacts_and_releases_versions() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "zset_compact_stream", None)
            .await
            .expect("build dictionary"),
    );

    let mut zset = ZSetStream::new(
        dict,
        table.clone(),
        "zset_compact_stream".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("create zset stream");
    zset.set_compaction_policy(CompactionPolicy {
        max_chain_len: 2,
        max_segments: 4,
        max_bucket_segments: 3,
    });

    zset.add_delta(b"a".to_vec(), 1);
    zset.flush().await.expect("flush t1");
    zset.add_delta(b"b".to_vec(), 1);
    zset.flush().await.expect("flush t2");
    zset.add_delta(b"c".to_vec(), 1);
    zset.flush().await.expect("flush t3");

    loop {
        let stats = zset.versioned().chain_stats().await.expect("chain stats");
        if stats.version_count == 1 {
            break;
        }
        let _ = zset
            .wait_for_background_compaction()
            .await
            .expect("wait for background compaction");
        zset.flush().await.expect("drive background compaction");
    }

    let stats = zset.versioned().chain_stats().await.expect("chain stats");
    assert_eq!(stats.version_count, 1);

    let view = zset
        .latest_view()
        .materialize()
        .await
        .expect("materialize compacted view");
    assert_eq!(view.get(b"a".as_ref()), Some(&1));
    assert_eq!(view.get(b"b".as_ref()), Some(&1));
    assert_eq!(view.get(b"c".as_ref()), Some(&1));
}

#[tokio::test(flavor = "multi_thread")]
async fn zset_stream_compaction_preserves_retractions_and_zero_weight_consolidation() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "zset_compact_retractions", None)
            .await
            .expect("build dictionary"),
    );

    let mut zset = ZSetStream::new(
        dict,
        table.clone(),
        "zset_compact_retractions".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("create zset stream");
    zset.set_compaction_policy(CompactionPolicy {
        max_chain_len: 2,
        max_segments: 4,
        max_bucket_segments: 3,
    });

    zset.add_delta(b"a".to_vec(), 1);
    zset.add_delta(b"b".to_vec(), 1);
    zset.flush().await.expect("flush initial rows");

    zset.add_delta(b"a".to_vec(), -1);
    zset.add_delta(b"c".to_vec(), 1);
    zset.add_delta(b"d".to_vec(), 1);
    zset.add_delta(b"d".to_vec(), -1);
    zset.flush().await.expect("flush retractions");

    zset.add_delta(b"b".to_vec(), -1);
    zset.add_delta(b"e".to_vec(), 1);
    zset.flush().await.expect("flush replacement");

    loop {
        let stats = zset.versioned().chain_stats().await.expect("chain stats");
        if stats.version_count == 1 {
            break;
        }
        let _ = zset
            .wait_for_background_compaction()
            .await
            .expect("wait for background compaction");
        zset.flush().await.expect("drive background compaction");
    }

    let view = zset
        .latest_view()
        .materialize()
        .await
        .expect("materialize compacted view");
    assert_eq!(view.get(b"a".as_ref()), None);
    assert_eq!(view.get(b"b".as_ref()), None);
    assert_eq!(view.get(b"c".as_ref()), Some(&1));
    assert_eq!(view.get(b"d".as_ref()), None);
    assert_eq!(view.get(b"e".as_ref()), Some(&1));

    drop(zset);
    let reopened_dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "zset_compact_retractions", None)
            .await
            .expect("reopen dictionary"),
    );
    let reopened = ZSetStream::new(
        reopened_dict,
        table,
        "zset_compact_retractions".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("reopen compacted stream");
    let reopened_view = reopened
        .latest_view()
        .materialize()
        .await
        .expect("materialize reopened view");
    assert_eq!(reopened_view.get(b"c".as_ref()), Some(&1));
    assert_eq!(reopened_view.get(b"e".as_ref()), Some(&1));
    assert_eq!(reopened_view.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn zset_stream_batch_boundaries_preserve_equivalent_logical_state() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));

    let one_batch_dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "zset_one_batch", None)
            .await
            .expect("build one-batch dictionary"),
    );
    let mut one_batch = ZSetStream::new(
        one_batch_dict,
        table.clone(),
        "zset_one_batch".to_string(),
        StreamRetention::KeepLast { keep_last: 2 },
    )
    .await
    .expect("create one-batch stream");

    let split_batch_dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "zset_split_batch", None)
            .await
            .expect("build split-batch dictionary"),
    );
    let mut split_batch = ZSetStream::new(
        split_batch_dict,
        table,
        "zset_split_batch".to_string(),
        StreamRetention::KeepLast { keep_last: 4 },
    )
    .await
    .expect("create split-batch stream");

    one_batch.add_delta(b"a".to_vec(), 1);
    one_batch.add_delta(b"b".to_vec(), 1);
    one_batch.add_delta(b"a".to_vec(), -1);
    one_batch.add_delta(b"c".to_vec(), 1);
    one_batch.add_delta(b"d".to_vec(), 1);
    one_batch.add_delta(b"d".to_vec(), -1);
    one_batch.flush().await.expect("flush one logical batch");

    split_batch.add_delta(b"a".to_vec(), 1);
    split_batch.add_delta(b"b".to_vec(), 1);
    split_batch.flush().await.expect("flush split batch 1");
    split_batch.add_delta(b"a".to_vec(), -1);
    split_batch.add_delta(b"c".to_vec(), 1);
    split_batch.flush().await.expect("flush split batch 2");
    split_batch.add_delta(b"d".to_vec(), 1);
    split_batch.add_delta(b"d".to_vec(), -1);
    split_batch
        .flush()
        .await
        .expect("flush canceling split batch");

    let one_view = one_batch
        .latest_view()
        .materialize()
        .await
        .expect("materialize one-batch view");
    let split_view = split_batch
        .latest_view()
        .materialize()
        .await
        .expect("materialize split-batch view");

    assert_eq!(one_view, split_view);
    assert_eq!(one_view.get(b"a".as_ref()), None);
    assert_eq!(one_view.get(b"b".as_ref()), Some(&1));
    assert_eq!(one_view.get(b"c".as_ref()), Some(&1));
    assert_eq!(one_view.get(b"d".as_ref()), None);
}
