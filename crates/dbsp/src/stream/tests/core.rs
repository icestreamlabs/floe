use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::stream::core::stream::Stream;
use crate::stream::operations::basic::{delay, differentiate, integrate, lift1, lift2};
use crate::stream::tests::common::{IntegerGroup, build_db};
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
async fn fills_with_default_when_reading_ahead() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db, "ahead", group).await.unwrap();

    let value = stream.get(5).await.unwrap();
    assert_eq!(value, 0);
    assert_eq!(stream.current_time(), 5);
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

    assert_eq!(reopened.last_default_ts, 1);
    assert_eq!(reopened.get(1).await.expect("get value"), 7);
    assert_eq!(reopened.get(2).await.expect("get value"), 7);
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
        .table
        .write_batch(batch)
        .await
        .expect("write leftover intent");

    let mut recovered = Stream::new(db, "intent", group)
        .await
        .expect("reopen stream");

    assert!(
        recovered
            .table
            .get(&intent_key)
            .await
            .expect("get intent")
            .is_none(),
        "intent key should be cleared on reopen"
    );
    assert_eq!(recovered.get(1).await.expect("get value"), 42);
}

#[tokio::test]
async fn stream_addition_and_negation() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut left = Stream::new(db.clone(), "left", group.clone())
        .await
        .expect("create left stream");
    let mut right = Stream::new(db.clone(), "right", group.clone())
        .await
        .expect("create right stream");

    left.send(1).await.expect("send left t1");
    left.send(4).await.expect("send left t2");

    right.set_default(2).await.expect("set right default");
    right.send(2).await.expect("send right t1");

    let addition = crate::stream::addition::StreamAddition::from_stream(&left);
    let mut sum = addition.add(&left, &right).await;
    assert_eq!(sum.get(1).await.expect("sum t1"), 3);
    assert_eq!(sum.get(2).await.expect("sum t2"), 6);

    let mut neg = addition.neg(&left).await;
    assert_eq!(neg.get(1).await.expect("neg t1"), -1);
    assert_eq!(neg.get(2).await.expect("neg t2"), -4);
}

#[tokio::test]
async fn delay_shifts_stream_values() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "delay_input", group.clone())
        .await
        .expect("create stream");
    source.send(5).await.expect("send t1");
    source.send(10).await.expect("send t2");
    source.send(15).await.expect("send t3");

    let mut delayed = delay(&source).await.expect("apply delay");
    assert_eq!(delayed.get(0).await.expect("t0"), 0);
    assert_eq!(delayed.get(1).await.expect("t1"), 0);
    assert_eq!(delayed.get(2).await.expect("t2"), 5);
    assert_eq!(delayed.get(3).await.expect("t3"), 10);
    assert_eq!(delayed.get(4).await.expect("t4"), 10);
}

#[tokio::test]
async fn differentiate_computes_deltas() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "differentiate_input", group.clone())
        .await
        .expect("create stream");
    source.send(2).await.expect("send t1");
    source.send(6).await.expect("send t2");
    source.send(9).await.expect("send t3");

    let mut diff = differentiate(&source).await.expect("apply diff");
    assert_eq!(diff.get(0).await.expect("t0"), 0);
    assert_eq!(diff.get(1).await.expect("t1"), 2);
    assert_eq!(diff.get(2).await.expect("t2"), 4);
    assert_eq!(diff.get(3).await.expect("t3"), 3);
    assert_eq!(diff.get(4).await.expect("t4"), 3);
}

#[tokio::test]
async fn integrate_accumulates_stream() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "integrate_input", group.clone())
        .await
        .expect("create stream");
    source.send(1).await.expect("send t1");
    source.send(2).await.expect("send t2");
    source.send(3).await.expect("send t3");

    let mut integrated = integrate(&source).await.expect("apply integrate");
    assert_eq!(integrated.get(0).await.expect("t0"), 0);
    assert_eq!(integrated.get(1).await.expect("t1"), 1);
    assert_eq!(integrated.get(2).await.expect("t2"), 3);
    assert_eq!(integrated.get(3).await.expect("t3"), 6);
    assert_eq!(integrated.get(4).await.expect("t4"), 6);
}

#[tokio::test]
async fn lift1_applies_function_to_stream() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "lift1_input", group.clone())
        .await
        .expect("create stream");
    source.send(3).await.expect("send t1");
    source.send(5).await.expect("send t2");

    let mut lifted = lift1(&source, group.clone(), |value: &i64| value * 2)
        .await
        .expect("apply lift1");
    assert_eq!(lifted.get(0).await.expect("t0"), 0);
    assert_eq!(lifted.get(1).await.expect("t1"), 6);
    assert_eq!(lifted.get(2).await.expect("t2"), 10);
    assert_eq!(lifted.get(3).await.expect("t3"), 10);
}

#[tokio::test]
async fn lift2_combines_two_streams() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut left = Stream::new(db.clone(), "lift2_left", group.clone())
        .await
        .expect("create left");
    left.send(1).await.expect("left t1");
    left.send(3).await.expect("left t2");

    let mut right = Stream::new(db.clone(), "lift2_right", group.clone())
        .await
        .expect("create right");
    right.set_default(5).await.expect("set right default");
    right.send(5).await.expect("right t1");
    right.send(7).await.expect("right t2");

    let mut combined = lift2(&left, &right, group.clone(), |l: &i64, r: &i64| l + r)
        .await
        .expect("apply lift2");
    assert_eq!(combined.get(0).await.expect("t0"), 5);
    assert_eq!(combined.get(1).await.expect("t1"), 6);
    assert_eq!(combined.get(2).await.expect("t2"), 10);
    assert_eq!(combined.get(3).await.expect("t3"), 10);
}
