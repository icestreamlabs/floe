use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::stream::core::stream::Stream;
use crate::stream::tests::common::{IntegerGroup, build_db};
use crate::stream::util::{set_default_at_in_place, set_default_in_place};
use slatedb::WriteBatch;

#[tokio::test]
async fn send_and_get_values() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "ints", group).await.unwrap();

    assert_eq!(stream.current_time(), 0);
    assert_eq!(stream.get(0).await.unwrap(), 0);

    stream.send(5).await.unwrap();
    stream.flush().await.unwrap();

    assert_eq!(stream.current_time(), 1);
    assert_eq!(stream.get(1).await.unwrap(), 5);
    assert_eq!(stream.latest().await.unwrap(), 5);

    let mut reload = Stream::new(db, "ints", Arc::new(IntegerGroup))
        .await
        .unwrap();
    assert_eq!(reload.current_time(), 1);
    assert_eq!(reload.get(1).await.unwrap(), 5);
}

#[tokio::test]
async fn committed_frontier_advances_after_flush() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db, "frontier_commit", group).await.unwrap();

    assert_eq!(stream.current_time(), 0);
    assert_eq!(stream.committed_frontier(), 0);

    stream.send(5).await.unwrap();
    assert_eq!(stream.current_time(), 1);
    assert_eq!(stream.committed_frontier(), 0);

    stream.flush().await.unwrap();
    assert_eq!(stream.committed_frontier(), 1);

    stream.send(7).await.unwrap();
    stream.send(9).await.unwrap();
    assert_eq!(stream.current_time(), 3);
    assert_eq!(stream.committed_frontier(), 1);

    stream.flush().await.unwrap();
    assert_eq!(stream.committed_frontier(), 3);
}

#[tokio::test]
async fn reading_ahead_is_non_mutating() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db, "ahead", group).await.unwrap();

    let value = stream.get(5).await.unwrap();
    assert_eq!(value, 0);
    assert_eq!(stream.current_time(), 0);
}

#[tokio::test]
async fn advance_to_explicitly_advances_with_defaults() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db, "advance_to", group).await.unwrap();

    stream.advance_to(5).await.unwrap();

    assert_eq!(stream.current_time(), 5);
    assert_eq!(stream.get(5).await.unwrap(), 0);
}

#[tokio::test]
async fn persists_default_changes() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "defaults", group).await.unwrap();

    stream.send(0).await.unwrap();
    stream.set_default(10).await.unwrap();
    stream.send(10).await.unwrap();
    stream.flush().await.unwrap();

    let mut reload = Stream::new(db, "defaults", Arc::new(IntegerGroup))
        .await
        .unwrap();
    assert_eq!(reload.get(2).await.unwrap(), 10);
}

#[tokio::test]
async fn remembers_last_default_ts() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "last_default", group.clone())
        .await
        .expect("build stream");

    stream.set_default(5).await.expect("set default");
    stream.flush().await.expect("flush default");
    stream.send(5).await.expect("send value");
    stream.set_default(7).await.expect("set second default");
    stream.flush().await.expect("flush stream");

    let mut reopened = Stream::new(db, "last_default", group)
        .await
        .expect("reopen stream");

    assert_eq!(reopened.last_default_ts(), 1);
    assert_eq!(reopened.get(1).await.expect("get value"), 7);
    assert_eq!(reopened.get(2).await.expect("get value"), 7);
}

#[tokio::test]
async fn set_default_preserves_later_scheduled_tail() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "scheduled_tail", group.clone())
        .await
        .expect("build stream");

    set_default_in_place(&mut stream, 5);
    set_default_at_in_place(&stream, 2, 0);
    stream.advance_to(1).await.expect("advance to t1");

    assert_eq!(stream.default_value(), 0);

    stream.set_default(7).await.expect("set default at t1");
    assert_eq!(stream.get(1).await.expect("get t1"), 7);
    assert_eq!(stream.get(2).await.expect("get t2"), 0);
    assert_eq!(stream.default_value(), 0);

    stream.flush().await.expect("flush stream");
    let mut reopened = Stream::new(db, "scheduled_tail", group)
        .await
        .expect("reopen stream");
    assert_eq!(reopened.default_value(), 0);
    assert_eq!(reopened.get(1).await.expect("reopened t1"), 7);
    assert_eq!(reopened.get(2).await.expect("reopened t2"), 0);
}

#[tokio::test]
async fn clears_intent_on_restart() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "intent", group.clone())
        .await
        .expect("create stream");

    stream.send(42).await.expect("send value");
    stream.flush().await.expect("flush stream");

    let intent_key = stream.encode_intent_key();

    let mut batch = WriteBatch::new();
    batch.put(intent_key.clone(), vec![1]);
    stream
        .table()
        .write_batch(batch)
        .await
        .expect("write leftover intent");

    let mut recovered = Stream::new(db, "intent", group)
        .await
        .expect("reopen stream");

    assert!(
        recovered
            .table()
            .get(&intent_key)
            .await
            .expect("get intent")
            .is_none(),
        "intent key should be cleared on reopen"
    );
    assert_eq!(recovered.get(1).await.expect("get value"), 42);
}
