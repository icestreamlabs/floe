use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::collections::CompactionPolicy;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::cursor::StreamCursor;
use crate::stream::operations::basic::delay;
use crate::stream::operations::basic::differentiate_zset_stream_live;
use crate::stream::tests::common::build_db;
use crate::stream::util::{collect_values, materialize_zset_handle};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};
use tokio::time::timeout;

#[tokio::test]
async fn live_diff_emits_empty_delta_for_noop_ticks() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "live_diff_input", None)
            .await
            .expect("build dictionary"),
    );

    let mut zset = ZSetStream::new(
        dict,
        table.clone(),
        "live_diff_input".to_string(),
        StreamRetention::KeepLast { keep_last: 4 },
    )
    .await
    .expect("create zset stream");

    // t1: +1 on "a"
    zset.add_delta(b"a".to_vec(), 1);
    zset.flush().await.expect("flush t1");
    // t2: +1 on "b"
    zset.add_delta(b"b".to_vec(), 1);
    zset.flush().await.expect("flush t2");
    // t3: no change
    zset.flush().await.expect("flush t3 noop");

    let diff_stream = zset.delta_handle_stream();
    let diff_handles = collect_values(&diff_stream, diff_stream.current_time())
        .await
        .expect("collect diff handles");

    assert!(
        diff_handles.len() >= 3,
        "expected at least one handle per tick"
    );
    // Use the last three emitted handles to align with the three ticks we produced.
    let start = diff_handles.len() - 3;

    let mut cache = HashMap::new();
    let h1 = materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &diff_handles[start])
        .await
        .expect("mat diff t1");
    let h2 =
        materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &diff_handles[start + 1])
            .await
            .expect("mat diff t2");
    let h3 =
        materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &diff_handles[start + 2])
            .await
            .expect("mat diff t3");

    assert_eq!(h1, HashMap::from([(b"a".to_vec(), 1)]));
    assert_eq!(h2, HashMap::from([(b"b".to_vec(), 1)]));
    assert!(h3.is_empty(), "noop tick should emit empty delta");
}

#[tokio::test]
async fn live_differentiate_stream_emits_empty_on_compaction_noop_tick() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "live_diff_operator_input", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "live_diff_operator_input".to_string(),
        StreamRetention::KeepLast { keep_last: 4 },
    )
    .await
    .expect("create zset stream");
    source.set_compaction_policy(CompactionPolicy {
        max_chain_len: 1,
        max_segments: 1,
        max_bucket_segments: 1,
    });

    let derived = differentiate_zset_stream_live::<Vec<u8>>(&source.handle_stream())
        .await
        .expect("build live differentiate stream");
    let mut cursor = StreamCursor::new(derived);
    let _ = cursor.snapshot().await.expect("initial snapshot");

    source.add_delta(b"a".to_vec(), 1);
    source.flush().await.expect("flush t1");
    let (_ts1, h1) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("wait for t1")
        .expect("t1 handle");

    let _ = source
        .wait_for_background_compaction()
        .await
        .expect("wait for compaction");
    source.flush().await.expect("flush t2 noop");
    let (_ts2, h2) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("wait for t2")
        .expect("t2 handle");

    let mut cache = HashMap::new();
    let t1 = materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &h1)
        .await
        .expect("materialize t1");
    let t2 = materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &h2)
        .await
        .expect("materialize t2");

    assert_eq!(t1, HashMap::from([(b"a".to_vec(), 1)]));
    assert!(t2.is_empty(), "noop tick must emit empty delta");
}

#[tokio::test]
async fn live_differentiate_preserves_future_scheduled_delta() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "live_diff_scheduled_input", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "live_diff_scheduled_input".to_string(),
        StreamRetention::KeepLast { keep_last: 4 },
    )
    .await
    .expect("create zset stream");
    source.add_delta(b"a".to_vec(), 1);
    source.flush().await.expect("flush t1");
    source.add_delta(b"b".to_vec(), 1);
    source.flush().await.expect("flush t2");

    let delayed = delay(&source.handle_stream())
        .await
        .expect("delay handle stream");
    let mut derived = differentiate_zset_stream_live::<Vec<u8>>(&delayed)
        .await
        .expect("build live differentiate stream");
    derived.flush().await.expect("flush derived stream");

    let mut cache = HashMap::new();
    let t3 = materialize_zset_handle::<Vec<u8>>(
        table.clone(),
        &mut cache,
        &derived.get(3).await.expect("derived t3"),
    )
    .await
    .expect("materialize t3");
    let t4 = materialize_zset_handle::<Vec<u8>>(
        table.clone(),
        &mut cache,
        &derived.get(4).await.expect("derived t4"),
    )
    .await
    .expect("materialize t4");

    assert_eq!(t3, HashMap::from([(b"b".to_vec(), 1)]));
    assert!(t4.is_empty(), "tail delta should settle to empty");
}
