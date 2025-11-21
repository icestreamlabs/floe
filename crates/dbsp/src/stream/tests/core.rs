use std::sync::Arc;

use async_trait::async_trait;
use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::operations::basic::{delay, differentiate, integrate, lift1, lift2};
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, PipelineBuilder, single_input_pipeline,
};
use crate::stream::tests::common::{IntegerGroup, build_db};
use crate::stream::{StreamCursor, StreamRetention, ZSetStream};
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

    assert_eq!(reopened.last_default_ts(), 1);
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
    let mut cursor = StreamCursor::new(stream);

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

#[tokio::test]
async fn handle_operator_runtime_waits_for_alignment() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict_left = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "runtime_left", None)
            .await
            .expect("left dict"),
    );
    let dict_right = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "runtime_right", None)
            .await
            .expect("right dict"),
    );

    let mut left = ZSetStream::new(
        dict_left,
        table.clone(),
        "runtime_left".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("left stream");
    let mut right = ZSetStream::new(
        dict_right,
        table.clone(),
        "runtime_right".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("right stream");

    let records: Arc<tokio::sync::Mutex<Vec<(i64, u64, u64)>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let records_clone = Arc::clone(&records);

    let mut runtime = HandleOperatorRuntime::new(
        vec![left.handle_stream(), right.handle_stream()],
        move |ts, handles| {
            let records = Arc::clone(&records_clone);
            let snapshot = handles.to_vec();
            async move {
                let mut guard = records.lock().await;
                guard.push((ts, snapshot[0].version, snapshot[1].version));
                Ok(())
            }
        },
    );

    left.add_delta(vec![1], 1);
    left.flush().await.expect("flush left t1");
    right.add_delta(vec![2], 1);
    right.flush().await.expect("flush right t1");
    runtime.step().await.expect("process t1");

    left.add_delta(vec![3], 1);
    left.flush().await.expect("flush left t2");
    right.add_delta(vec![4], 1);
    right.flush().await.expect("flush right t2");
    runtime.step().await.expect("process t2");

    let collected = records.lock().await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0], (1, 1, 1));
    assert_eq!(collected[1], (2, 2, 2));
}

struct RecordingOp {
    observed: Arc<tokio::sync::Mutex<Vec<(i64, Vec<ZSetHandle>)>>>,
}

#[async_trait]
impl DeltaOperator for RecordingOp {
    async fn on_step(
        &mut self,
        ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let snapshot = inputs.to_vec();
        let mut guard = self.observed.lock().await;
        guard.push((ts, snapshot));
        Ok(None)
    }
}

struct PassthroughOp;

#[async_trait]
impl DeltaOperator for PassthroughOp {
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        Ok(inputs.get(0).cloned())
    }
}

#[tokio::test]
async fn pipeline_invokes_operator_with_aligned_inputs() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict_left = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "pipe_left", None)
            .await
            .expect("left dict"),
    );
    let dict_right = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "pipe_right", None)
            .await
            .expect("right dict"),
    );

    let mut left = ZSetStream::new(
        dict_left,
        table.clone(),
        "pipe_left".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("left stream");
    let mut right = ZSetStream::new(
        dict_right,
        table.clone(),
        "pipe_right".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("right stream");

    let observed: Arc<tokio::sync::Mutex<Vec<(i64, Vec<ZSetHandle>)>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut pipeline = PipelineBuilder::new(vec![left.handle_stream(), right.handle_stream()])
        .push_op(RecordingOp {
            observed: Arc::clone(&observed),
        })
        .build();

    left.add_delta(vec![1], 1);
    left.flush().await.expect("flush left t1");
    right.add_delta(vec![2], 1);
    right.flush().await.expect("flush right t1");
    pipeline.step_once().await.expect("process t1");

    left.add_delta(vec![3], 1);
    left.flush().await.expect("flush left t2");
    right.add_delta(vec![4], 1);
    right.flush().await.expect("flush right t2");
    pipeline.step_once().await.expect("process t2");

    let collected = observed.lock().await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].0, 1);
    assert_eq!(collected[0].1[0].version, 1);
    assert_eq!(collected[0].1[1].version, 1);
    assert_eq!(collected[1].0, 2);
    assert_eq!(collected[1].1[0].version, 2);
    assert_eq!(collected[1].1[1].version, 2);
}

#[tokio::test]
async fn single_input_pipeline_passes_through_operator() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "single_pipe", None)
            .await
            .expect("dict"),
    );

    let mut stream = ZSetStream::new(
        dict,
        table.clone(),
        "single_pipe".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("stream");

    let observed: Arc<tokio::sync::Mutex<Vec<(i64, Vec<ZSetHandle>)>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut pipeline = single_input_pipeline(
        stream.handle_stream(),
        vec![
            Box::new(PassthroughOp),
            Box::new(RecordingOp {
                observed: Arc::clone(&observed),
            }),
        ],
    );

    stream.add_delta(vec![1], 1);
    stream.flush().await.expect("flush t1");
    pipeline.step_once().await.expect("process t1");

    stream.add_delta(vec![2], 1);
    stream.flush().await.expect("flush t2");
    pipeline.step_once().await.expect("process t2");

    let collected = observed.lock().await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].0, 1);
    assert_eq!(collected[0].1[0].version, 1);
    assert_eq!(collected[1].0, 2);
    assert_eq!(collected[1].1[0].version, 2);
}
