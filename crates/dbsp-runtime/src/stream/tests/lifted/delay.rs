use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::lifted_delay;
use crate::stream::tests::common::{IntegerGroup, build_db};

#[tokio::test]
async fn lifted_delay_operates_on_stream_handles() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut inner_a = Stream::new(db.clone(), "lifted_delay_inner_a", group.clone())
        .await
        .expect("create inner stream a");
    inner_a.send(1).await.expect("inner a t1");
    inner_a.send(2).await.expect("inner a t2");
    inner_a.flush().await.expect("flush inner a");

    let mut inner_b = Stream::new(db.clone(), "lifted_delay_inner_b", group.clone())
        .await
        .expect("create inner stream b");
    inner_b.send(5).await.expect("inner b t1");
    inner_b.send(6).await.expect("inner b t2");
    inner_b.flush().await.expect("flush inner b");

    let handle_a = inner_a.handle();
    let handle_b = inner_b.handle();
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(handle_a.clone()));

    let mut outer = Stream::new(db.clone(), "lifted_delay_outer", handle_group)
        .await
        .expect("create outer stream");
    outer.send(handle_a.clone()).await.expect("outer t1");
    outer.send(handle_b.clone()).await.expect("outer t2");

    let mut delayed = lifted_delay(&outer, group.clone())
        .await
        .expect("apply lifted delay");

    let mut handles = Vec::new();
    for t in 0..=delayed.current_time() {
        handles.push(
            delayed
                .get(t)
                .await
                .expect("read delayed handle for timeline"),
        );
    }

    let mut resolved_first = delayed
        .resolve_handle(&handles[0], group.clone())
        .await
        .expect("resolve first delayed stream");
    assert_eq!(resolved_first.get(0).await.expect("first t0"), 0);
    assert_eq!(resolved_first.get(1).await.expect("first t1"), 0);
    assert_eq!(resolved_first.get(2).await.expect("first t2"), 1);

    let mut resolved_second = delayed
        .resolve_handle(handles.last().expect("last delayed handle"), group.clone())
        .await
        .expect("resolve second delayed stream");
    assert_eq!(resolved_second.get(1).await.expect("second t1"), 0);
    assert_eq!(resolved_second.get(2).await.expect("second t2"), 5);
}
