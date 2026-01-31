use std::sync::Arc;

use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::tests::common::build_db;
use crate::stream::{StreamCursor, StreamRetention, ZSetStream};

#[tokio::test]
async fn stream_cursor_tracks_new_versions() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "cursor_stream", None)
            .await
            .expect("dictionary"),
    );
    let mut zset = ZSetStream::new(
        dict,
        table,
        "cursor_stream".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("create zset stream");
    let stream = zset.handle_stream();
    let mut cursor = StreamCursor::new(stream.stream());

    let (ts0, handle0) = cursor.snapshot().await.expect("snapshot ts0");
    assert_eq!(ts0, 0);
    assert_eq!(handle0.version, 0);

    zset.add_delta(vec![1], 1);
    let h1 = zset.flush().await.expect("flush first version");
    assert_eq!(h1.version, 1);
    let (ts1, handle1) = cursor.next().await.expect("cursor ts1");
    assert_eq!(ts1, 1);
    assert_eq!(handle1.version, 1);

    zset.add_delta(vec![2], 1);
    zset.flush().await.expect("flush second version");
    let (ts2, handle2) = cursor.next().await.expect("cursor ts2");
    assert_eq!(ts2, 2);
    assert_eq!(handle2.version, 2);
}

#[tokio::test]
async fn handle_stream_clones_observe_frontier_advances() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "handle_clone_stream", None)
            .await
            .expect("dictionary"),
    );
    let mut zset = ZSetStream::new(
        dict,
        table,
        "handle_clone_stream".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("create stream");
    let stream = zset.handle_stream();
    let mut clone_a = stream.clone();
    let mut clone_b = stream.clone();

    let (ts0, handle0) = clone_a
        .latest_with_ts()
        .await
        .expect("initial latest handle");
    assert_eq!(ts0, 0);
    assert_eq!(handle0.version, 0);

    zset.add_delta(vec![1], 1);
    zset.flush().await.expect("flush first version");

    let (ts1, handle1) = clone_b
        .latest_with_ts()
        .await
        .expect("latest after first flush");
    assert_eq!(ts1, 1);
    assert_eq!(handle1.version, 1);

    zset.add_delta(vec![2], 1);
    zset.flush().await.expect("flush second version");

    let (ts2, handle2) = clone_a
        .latest_with_ts()
        .await
        .expect("latest after second flush");
    assert_eq!(ts2, 2);
    assert_eq!(handle2.version, 2);

    let (ts3, handle3) = clone_b.latest_with_ts().await.expect("latest repeat check");
    assert_eq!(ts3, 2);
    assert_eq!(handle3.version, 2);
}

#[tokio::test]
async fn stream_reopens_at_persisted_frontier() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let namespace = "stream_restart_frontier";
    {
        let dict = Arc::new(
            Dictionary::<Vec<u8>>::with_table(table.clone(), namespace, None)
                .await
                .expect("dictionary"),
        );
        let mut zset = ZSetStream::new(
            dict,
            table.clone(),
            namespace.to_string(),
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("create stream");
        zset.add_delta(vec![1], 1);
        zset.flush().await.expect("flush v1");
        zset.add_delta(vec![2], 1);
        zset.flush().await.expect("flush v2");
    }

    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), namespace, None)
            .await
            .expect("dictionary"),
    );
    let reopened = ZSetStream::new(
        dict,
        table.clone(),
        namespace.to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("reopen stream");
    let mut handle_stream = reopened.handle_stream();
    assert_eq!(handle_stream.current_time(), 2);
    let (ts, handle) = handle_stream
        .latest_with_ts()
        .await
        .expect("latest after reopen");
    assert_eq!(ts, 2);
    assert_eq!(handle.version, 2);
}

#[tokio::test]
async fn concurrent_latest_and_get_observe_consistent_handles() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "stream_concurrent_latest", None)
            .await
            .expect("dictionary"),
    );
    let mut zset = ZSetStream::new(
        dict,
        table.clone(),
        "stream_concurrent_latest".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("create stream");
    zset.add_delta(vec![1], 1);
    zset.flush().await.expect("flush first");
    zset.add_delta(vec![2], 1);
    zset.flush().await.expect("flush second");

    let mut latest_reader = zset.handle_stream();
    let mut snapshot_reader = latest_reader.clone();
    let (latest, snapshot) = tokio::join!(
        async { latest_reader.latest_with_ts().await.expect("latest handle") },
        async { snapshot_reader.get(1).await.expect("get handle at ts=1") }
    );
    assert_eq!(latest.0, 2);
    assert_eq!(latest.1.version, 2);
    assert_eq!(snapshot.version, 1);
}
