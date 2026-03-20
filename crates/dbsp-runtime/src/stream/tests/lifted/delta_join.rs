use std::collections::HashMap;
use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::delta_lifted_delta_lifted_join;
use crate::stream::tests::common::build_db;
use crate::stream::util::{
    collect_values, materialize_zset_handle, push_value_in_place, set_default_in_place,
};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};

#[tokio::test]
async fn delta_lifted_delta_lifted_join_produces_handles() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));

    let dict_a = Arc::new(
        Dictionary::with_table(table.clone(), "delta_join_a", None)
            .await
            .expect("dictionary a"),
    );
    let mut stream_a =
        ZSetStream::new(dict_a, table.clone(), "delta_join_a", StreamRetention::None)
            .await
            .expect("create zset stream a");

    stream_a.add_delta((0_i32, 1_i32), 1);
    stream_a.flush().await.expect("flush a t0");
    stream_a.add_delta((1, 2), 1);
    stream_a.flush().await.expect("flush a t1");

    let dict_b = Arc::new(
        Dictionary::with_table(table.clone(), "delta_join_b", None)
            .await
            .expect("dictionary b"),
    );
    let mut stream_b =
        ZSetStream::new(dict_b, table.clone(), "delta_join_b", StreamRetention::None)
            .await
            .expect("create zset stream b");
    stream_b.add_delta((1_i32, 3_i32), 1);
    stream_b.flush().await.expect("flush b t0");
    stream_b.add_delta((2, 4), 1);
    stream_b.flush().await.expect("flush b t1");

    let a_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(stream_a.stream.handle()));
    let b_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(stream_b.stream.handle()));

    let mut outer_a =
        Stream::with_table(table.clone(), "delta_join_outer_a", a_handle_group.clone())
            .await
            .expect("create outer stream a");
    let handle_a0 = stream_a.stream.handle();
    set_default_in_place(&mut outer_a, handle_a0.clone());
    let handle_a1 = stream_a.stream.handle();
    push_value_in_place(&mut outer_a, handle_a1.clone());
    outer_a.flush().await.expect("flush outer a");

    let mut outer_b =
        Stream::with_table(table.clone(), "delta_join_outer_b", b_handle_group.clone())
            .await
            .expect("create outer stream b");
    let handle_b0 = stream_b.stream.handle();
    set_default_in_place(&mut outer_b, handle_b0.clone());
    let handle_b1 = stream_b.stream.handle();
    push_value_in_place(&mut outer_b, handle_b1.clone());
    outer_b.flush().await.expect("flush outer b");

    let mut result = delta_lifted_delta_lifted_join(
        &outer_a,
        &outer_b,
        |left: &(i32, i32), right: &(i32, i32)| left.1 == right.0,
        |left: &(i32, i32), right: &(i32, i32)| (left.0, right.1),
    )
    .await
    .expect("compute delta lifted join");
    result
        .flush()
        .await
        .expect("flush delta lifted join output");

    let handles = collect_values(&result, result.current_time())
        .await
        .expect("collect delta lifted join handles");
    assert!(!handles.is_empty());

    let mut cache = HashMap::new();
    for handle in handles {
        let group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
            ns: handle.ns.clone(),
            version: 0,
        }));
        let mut resolved = result
            .resolve_handle(&handle, group.clone())
            .await
            .expect("resolve nested join stream");
        let zset_handle = resolved.latest().await.expect("load nested handle");
        let map = materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &zset_handle)
            .await
            .expect("materialize nested zset");
        if handle.frontier > 0 {
            assert!(
                !map.is_empty(),
                "expected non-empty map at frontier {}, map {:?}",
                handle.frontier,
                map
            );
        }
    }
}

#[tokio::test]
async fn delta_lifted_delta_lifted_join_preserves_ticks_when_one_side_stops() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));

    let dict_left = Arc::new(
        Dictionary::with_table(table.clone(), "delta_join_align_left", None)
            .await
            .expect("dictionary left"),
    );
    let mut stream_left = ZSetStream::new(
        dict_left,
        table.clone(),
        "delta_join_align_left",
        StreamRetention::None,
    )
    .await
    .expect("create left zset stream");

    stream_left.add_delta((0_i32, 1_i32), 1);
    stream_left.flush().await.expect("flush left t0");
    stream_left.add_delta((1_i32, 2_i32), 1);
    stream_left.flush().await.expect("flush left t1");

    let dict_right = Arc::new(
        Dictionary::with_table(table.clone(), "delta_join_align_right", None)
            .await
            .expect("dictionary right"),
    );
    let mut stream_right = ZSetStream::new(
        dict_right,
        table.clone(),
        "delta_join_align_right",
        StreamRetention::None,
    )
    .await
    .expect("create right zset stream");

    stream_right.add_delta((1_i32, 3_i32), 1);
    stream_right.flush().await.expect("flush right t0");

    let left_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(stream_left.stream.handle()));
    let mut outer_left = Stream::with_table(
        table.clone(),
        "delta_join_align_outer_left",
        left_handle_group,
    )
    .await
    .expect("create outer left stream");
    let left_default = stream_left.stream.handle();
    set_default_in_place(&mut outer_left, left_default.clone());
    let left_latest = stream_left.stream.handle();
    push_value_in_place(&mut outer_left, left_latest.clone());
    outer_left.flush().await.expect("flush outer left stream");

    let right_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(stream_right.stream.handle()));
    let mut outer_right = Stream::with_table(
        table.clone(),
        "delta_join_align_outer_right",
        right_handle_group,
    )
    .await
    .expect("create outer right stream");
    let right_default = stream_right.stream.handle();
    set_default_in_place(&mut outer_right, right_default.clone());
    outer_right.flush().await.expect("flush outer right stream");

    let mut result = delta_lifted_delta_lifted_join(
        &outer_left,
        &outer_right,
        |left: &(i32, i32), right: &(i32, i32)| left.1 == right.0,
        |left: &(i32, i32), right: &(i32, i32)| (left.0, right.1),
    )
    .await
    .expect("compute aligned delta lifted join");
    result
        .flush()
        .await
        .expect("flush aligned delta lifted join output");

    assert_eq!(result.current_time(), outer_left.current_time());

    let handles = collect_values(&result, result.current_time())
        .await
        .expect("collect aligned result handles");
    assert_eq!(
        handles.len(),
        usize::try_from(outer_left.current_time().saturating_add(1))
            .expect("convert timestamp to length"),
        "expected handles for each logical tick through the longer stream frontier"
    );

    let nested_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: handles[0].ns.clone(),
        version: 0,
    }));
    let mut first_stream = result
        .resolve_handle(&handles[0], nested_group.clone())
        .await
        .expect("resolve first aligned stream");
    let first_handle = first_stream
        .latest()
        .await
        .expect("load first aligned handle");
    let mut second_stream = result
        .resolve_handle(&handles[1], nested_group)
        .await
        .expect("resolve second aligned stream");
    let second_handle = second_stream
        .latest()
        .await
        .expect("load second aligned handle");

    let mut cache = HashMap::new();
    let first = materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &first_handle)
        .await
        .expect("materialize first aligned handle");
    let second = materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &second_handle)
        .await
        .expect("materialize second aligned handle");
    assert_eq!(
        first, second,
        "extra tick should preserve the previous state"
    );
}
