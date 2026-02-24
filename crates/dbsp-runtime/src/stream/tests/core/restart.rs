use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::tests::common::{IntegerGroup, build_db};
use crate::stream::{StreamRetention, ZSetStream};
use slatedb::WriteBatch;

#[tokio::test]
async fn stream_restart_persists_frontier_and_defaults() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    {
        let mut stream = Stream::new(db.clone(), "restart_stream", group.clone())
            .await
            .expect("create stream");
        stream.send(1).await.expect("send t1");
        stream.set_default(5).await.expect("set default");
        stream.flush().await.expect("flush after default");
        stream.send(5).await.expect("send t2");
        stream.flush().await.expect("flush t2");
        assert_eq!(stream.committed_frontier(), 2);
    }

    let mut reopened = Stream::new(db, "restart_stream", group)
        .await
        .expect("reopen stream");
    assert_eq!(reopened.current_time(), 2);
    assert_eq!(reopened.committed_frontier(), 2);
    assert_eq!(reopened.get(0).await.expect("get t0"), 0);
    assert_eq!(reopened.get(1).await.expect("get t1"), 1);
    assert_eq!(reopened.get(2).await.expect("get t2"), 5);
}

#[tokio::test]
async fn stream_restart_recovers_committed_flush_with_lingering_intent() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "restart_stream_lingering_intent", group.clone())
        .await
        .expect("create stream");

    stream.send(1).await.expect("send t1");
    stream.flush().await.expect("flush t1");
    stream.send(7).await.expect("send t2");

    let intent_key = stream.encode_intent_key();
    let mut batch = WriteBatch::new();
    assert!(
        stream
            .flush_data_into(&mut batch)
            .expect("flush data into staged batch"),
        "expected staged data for t2"
    );
    let committed_ts = stream
        .flush_state_into(&mut batch)
        .expect("flush state into staged batch")
        .expect("staged committed timestamp");
    assert_eq!(committed_ts, 2);
    batch.put(intent_key.clone(), vec![1]);
    stream
        .table()
        .write_batch(batch)
        .await
        .expect("persist staged stream flush without cleanup");

    let mut reopened = Stream::new(db, "restart_stream_lingering_intent", group)
        .await
        .expect("reopen stream");
    assert_eq!(reopened.current_time(), 2);
    assert_eq!(reopened.committed_frontier(), 2);
    assert_eq!(reopened.get(1).await.expect("get t1"), 1);
    assert_eq!(reopened.get(2).await.expect("get t2"), 7);
    assert!(
        reopened
            .table()
            .get(&intent_key)
            .await
            .expect("get lingering intent after reopen")
            .is_none(),
        "intent key should be cleared on reopen"
    );
}

#[tokio::test]
async fn zset_stream_restart_persists_frontier_and_defaults() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let namespace = "restart_zset_stream";

    let (snapshot_version, delta_version) = {
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
        .expect("create zset stream");
        zset.add_delta(vec![1], 1);
        zset.flush_with_delta().await.expect("flush v1");
        zset.add_delta(vec![2], 1);
        let (snapshot, delta) = zset.flush_with_delta().await.expect("flush v2");
        (snapshot.version, delta.version)
    };

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
    .expect("reopen zset stream");

    let mut snapshot_stream = reopened.handle_stream();
    assert_eq!(snapshot_stream.current_time(), 2);
    assert_eq!(snapshot_stream.committed_frontier(), 2);
    let (ts, latest) = snapshot_stream
        .latest_with_ts()
        .await
        .expect("latest snapshot handle");
    assert_eq!(ts, 2);
    assert_eq!(latest.version, snapshot_version);
    assert_eq!(snapshot_stream.default_value(), latest);

    let mut delta_stream = reopened.delta_handle_stream();
    let (ts_delta, latest_delta) = delta_stream
        .latest_with_ts()
        .await
        .expect("latest delta handle");
    assert_eq!(ts_delta, 2);
    assert_eq!(latest_delta.version, delta_version);
    assert_eq!(delta_stream.default_value().version, 0);
}

#[tokio::test]
async fn zset_stream_clears_intent_keys_on_restart() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let namespace = "restart_zset_intents";

    let (stream_intent, delta_stream_intent, versioned_intent, delta_versioned_intent) = {
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
        .expect("create zset stream");
        zset.add_delta(vec![1], 1);
        zset.flush().await.expect("flush");

        let stream_intent = zset.handle_stream().encode_intent_key();
        let delta_stream_intent = zset.delta_handle_stream().encode_intent_key();
        let versioned_intent = zset.versioned().intent_key_bytes().to_vec();
        let delta_versioned_intent = zset.delta_versioned_intent_key();
        (
            stream_intent,
            delta_stream_intent,
            versioned_intent,
            delta_versioned_intent,
        )
    };

    let mut batch = WriteBatch::new();
    batch.put(stream_intent.clone(), vec![1]);
    batch.put(delta_stream_intent.clone(), vec![1]);
    batch.put(versioned_intent.clone(), vec![1]);
    batch.put(delta_versioned_intent.clone(), vec![1]);
    table
        .write_batch(batch)
        .await
        .expect("write lingering intents");

    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), namespace, None)
            .await
            .expect("dictionary"),
    );
    ZSetStream::new(
        dict,
        table.clone(),
        namespace.to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("reopen zset stream");

    assert!(
        table
            .get(&stream_intent)
            .await
            .expect("get stream intent")
            .is_none(),
        "stream intent should be cleared"
    );
    assert!(
        table
            .get(&delta_stream_intent)
            .await
            .expect("get delta stream intent")
            .is_none(),
        "delta stream intent should be cleared"
    );
    assert!(
        table
            .get(&versioned_intent)
            .await
            .expect("get versioned intent")
            .is_none(),
        "versioned intent should be cleared"
    );
    assert!(
        table
            .get(&delta_versioned_intent)
            .await
            .expect("get delta versioned intent")
            .is_none(),
        "delta versioned intent should be cleared"
    );
}
