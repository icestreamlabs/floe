use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::stream::core::stream::Stream;
use crate::stream::tests::common::{IntegerGroup, build_db};
use crate::stream::util::collect_values;

async fn assert_stream_values(stream: &Stream<i64>, expected: &[i64]) {
    let max_ts = (expected.len() as i64) - 1;
    let values = collect_values(stream, max_ts)
        .await
        .expect("collect stream values");
    assert_eq!(values, expected);
}

#[tokio::test]
async fn sequence_updates_default_after_explicit_advance() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "pydbsp_seq_default", group.clone())
        .await
        .expect("create stream");

    stream.set_default(5).await.expect("set default");
    assert_eq!(stream.get(0).await.expect("get t0"), 5);

    stream.send(5).await.expect("send t1");
    stream.send(7).await.expect("send t2");
    stream.advance_to(4).await.expect("advance to t4");

    stream.set_default(2).await.expect("set default at t4");
    assert_eq!(stream.get(4).await.expect("get t4"), 2);
    stream.advance_to(5).await.expect("advance to t5");

    assert_stream_values(&stream, &[5, 5, 7, 5, 2, 2]).await;

    stream.flush().await.expect("flush stream");
    let reopened = Stream::new(db, "pydbsp_seq_default", group)
        .await
        .expect("reopen stream");
    assert_stream_values(&reopened, &[5, 5, 7, 5, 2, 2]).await;
}

#[tokio::test]
async fn sequence_advance_and_default_changes_match() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let mut stream = Stream::new(db.clone(), "pydbsp_seq_get", group.clone())
        .await
        .expect("create stream");

    assert_eq!(stream.get(2).await.expect("get t2"), 0);
    stream.advance_to(2).await.expect("advance to t2");
    stream.set_default(3).await.expect("set default at t2");
    assert_eq!(stream.get(2).await.expect("get t2"), 3);

    stream.send(4).await.expect("send t3");
    stream.send(3).await.expect("send t4 default");

    stream.set_default(1).await.expect("set default at t4");
    assert_eq!(stream.get(4).await.expect("get t4"), 1);
    assert_eq!(stream.get(1).await.expect("get t1"), 0);
    stream.advance_to(5).await.expect("advance to t5");

    assert_stream_values(&stream, &[0, 0, 3, 4, 1, 1]).await;

    stream.flush().await.expect("flush stream");
    let reopened = Stream::new(db, "pydbsp_seq_get", group)
        .await
        .expect("reopen stream");
    assert_stream_values(&reopened, &[0, 0, 3, 4, 1, 1]).await;
}
