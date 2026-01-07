use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::cursor::StreamCursor;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::{
    delta_lifted_delta_lifted_join, lifted_delay, lifted_h_zset_stream, lifted_join_zset_stream,
    lifted_lifted_select_zset_stream, lifted_select_zset_stream, lifted_stream_introduction,
};
use crate::stream::tests::common::{IntegerGroup, build_db};
use crate::stream::util::{
    collect_values, materialize_zset_handle, push_value_in_place, set_default_in_place,
};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};
use tokio::time::timeout;

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

#[tokio::test]
async fn lifted_select_zset_stream_filters_elements() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_select_input", None)
            .await
            .expect("build dictionary"),
    );

    let mut zset_stream = ZSetStream::new(
        dict,
        table.clone(),
        "lifted_select_input",
        StreamRetention::None,
    )
    .await
    .expect("create zset stream");

    zset_stream.add_delta("keep".to_string(), 1);
    let handle0 = zset_stream.flush().await.expect("flush first");

    zset_stream.add_delta("keep".to_string(), -1);
    zset_stream.add_delta("drop".to_string(), 1);
    let handle1 = zset_stream.flush().await.expect("flush second");

    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(handle0.clone()));
    let mut input_stream = Stream::with_table(table.clone(), "lifted_select_stream", handle_group)
        .await
        .expect("create stream of handles");
    set_default_in_place(&mut input_stream, handle0.clone());
    push_value_in_place(&mut input_stream, handle1.clone());
    input_stream.flush().await.expect("flush input stream");

    let mut result = lifted_select_zset_stream::<String, _>(&input_stream, |value: &String| {
        value.starts_with('k')
    })
    .await
    .expect("apply lifted select stream");
    result.flush().await.expect("flush result stream");

    let handles = collect_values(&result, result.current_time())
        .await
        .expect("collect handles");
    let mut cache = HashMap::new();

    let first = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[0])
        .await
        .expect("materialize first handle");
    assert_eq!(first.get("keep"), Some(&1));
    assert!(first.get("drop").is_none());

    let second = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[1])
        .await
        .expect("materialize second handle");
    assert!(second.get("keep").is_none());
    assert!(second.get("drop").is_none());
}

#[tokio::test]
async fn lifted_select_zset_stream_emits_updates_after_build() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_select_live", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "lifted_select_live",
        StreamRetention::KeepLast { keep_last: 2 },
    )
    .await
    .expect("create zset stream");

    let derived =
        lifted_select_zset_stream::<String, _>(&source.handle_stream(), |value: &String| {
            value.starts_with('k')
        })
        .await
        .expect("build lifted select");

    let mut cursor = StreamCursor::new(derived);
    // Consume the initial snapshot to align the cursor with future handles.
    let _ = cursor.snapshot().await.expect("initial snapshot");

    source.add_delta("keep-me".to_string(), 1);
    source.flush().await.expect("flush new delta");

    let (_ts, handle) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("select update wait")
        .expect("select update");

    let mut cache = HashMap::new();
    let materialized = materialize_zset_handle::<String>(table.clone(), &mut cache, &handle)
        .await
        .unwrap();
    assert_eq!(materialized.get("keep-me"), Some(&1));
}

#[tokio::test]
async fn lifted_join_zset_stream_emits_updates_after_build() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_join_left", None)
            .await
            .expect("build left dictionary"),
    );
    let right_dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_join_right", None)
            .await
            .expect("build right dictionary"),
    );

    let mut left = ZSetStream::new(
        left_dict,
        table.clone(),
        "lifted_join_left",
        StreamRetention::KeepLast { keep_last: 2 },
    )
    .await
    .expect("create left stream");
    let mut right = ZSetStream::new(
        right_dict,
        table.clone(),
        "lifted_join_right",
        StreamRetention::KeepLast { keep_last: 2 },
    )
    .await
    .expect("create right stream");

    let derived = lifted_join_zset_stream::<i64, i64, (i64, i64), _, _>(
        &left.handle_stream(),
        &right.handle_stream(),
        |l, r| l == r,
        |l, r| (*l, *r),
    )
    .await
    .expect("build lifted join");

    let mut cursor = StreamCursor::new(derived);
    let _ = cursor.snapshot().await.expect("initial join snapshot");

    left.add_delta(7_i64, 1);
    left.flush().await.expect("flush left");
    right.add_delta(7_i64, 1);
    right.flush().await.expect("flush right");

    // Ensure source handles materialize to catch upstream issues early.
    let mut cache = HashMap::new();
    let left_handle = left.current_handle().clone();
    materialize_zset_handle::<i64>(table.clone(), &mut cache, &left_handle)
        .await
        .expect("materialize left handle");
    let right_handle = right.current_handle().clone();
    materialize_zset_handle::<i64>(table.clone(), &mut cache, &right_handle)
        .await
        .expect("materialize right handle");

    let (_ts, handle) = timeout(Duration::from_secs(2), cursor.next())
        .await
        .expect("join update wait")
        .expect("join update");
    let mut cache = HashMap::new();
    let materialized = materialize_zset_handle::<(i64, i64)>(table.clone(), &mut cache, &handle)
        .await
        .unwrap();
    assert_eq!(materialized.get(&(7, 7)), Some(&1));
}

#[tokio::test]
async fn lifted_h_zset_stream_detects_transitions() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let diff_dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_h_diff", None)
            .await
            .expect("diff dictionary"),
    );
    let state_dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_h_state", None)
            .await
            .expect("state dictionary"),
    );

    let mut diff_stream = ZSetStream::new(
        diff_dict,
        table.clone(),
        "lifted_h_diff",
        StreamRetention::None,
    )
    .await
    .expect("create diff stream");
    let mut state_stream = ZSetStream::new(
        state_dict,
        table.clone(),
        "lifted_h_state",
        StreamRetention::None,
    )
    .await
    .expect("create state stream");

    diff_stream.add_delta("a".to_string(), 1);
    let diff_handle0 = diff_stream.flush().await.expect("flush diff0");

    let state_handle0 = state_stream.flush().await.expect("flush state0");
    state_stream.add_delta("a".to_string(), 1);
    let state_handle1 = state_stream.flush().await.expect("flush state1");

    diff_stream.add_delta("a".to_string(), -2);
    let diff_handle1 = diff_stream.flush().await.expect("flush diff1");

    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(diff_handle0.clone()));
    let mut diff_handle_stream =
        Stream::with_table(table.clone(), "lifted_h_diff_handles", handle_group.clone())
            .await
            .expect("create diff handle stream");
    set_default_in_place(&mut diff_handle_stream, diff_handle0.clone());
    push_value_in_place(&mut diff_handle_stream, diff_handle1.clone());
    diff_handle_stream
        .flush()
        .await
        .expect("flush diff handles");

    let mut state_handle_stream =
        Stream::with_table(table.clone(), "lifted_h_state_handles", handle_group)
            .await
            .expect("create state handle stream");
    set_default_in_place(&mut state_handle_stream, state_handle0.clone());
    push_value_in_place(&mut state_handle_stream, state_handle1.clone());
    state_handle_stream
        .flush()
        .await
        .expect("flush state handles");

    let mut debug_cache = HashMap::new();
    let diff_second =
        materialize_zset_handle::<String>(table.clone(), &mut debug_cache, &diff_handle1)
            .await
            .expect("materialize diff second");
    let state_second =
        materialize_zset_handle::<String>(table.clone(), &mut debug_cache, &state_handle1)
            .await
            .expect("materialize state second");
    assert_eq!(diff_second.get("a"), Some(&-1));
    assert_eq!(state_second.get("a"), Some(&1));

    let mut result = lifted_h_zset_stream::<String>(&diff_handle_stream, &state_handle_stream)
        .await
        .expect("apply lifted H stream");
    result.flush().await.expect("flush lifted H result");

    let handles = collect_values(&result, result.current_time())
        .await
        .expect("collect H handles");
    let mut cache = HashMap::new();

    let first = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[0])
        .await
        .expect("materialize first H result");
    assert_eq!(first.get("a"), Some(&1));

    let second = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[1])
        .await
        .expect("materialize second H result");
    assert_eq!(second.get("a"), Some(&-1), "second H map: {:?}", second);
}

#[tokio::test]
async fn lifted_lifted_select_zset_stream_operates_on_nested_streams() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_lifted_select", None)
            .await
            .expect("dictionary"),
    );

    let mut zset_stream = ZSetStream::new(
        dict,
        table.clone(),
        "lifted_lifted_select",
        StreamRetention::None,
    )
    .await
    .expect("create zset stream");

    zset_stream.add_delta("keep".to_string(), 2);
    let handle0 = zset_stream.flush().await.expect("flush handle0");

    zset_stream.add_delta("drop".to_string(), 3);
    let handle1 = zset_stream.flush().await.expect("flush handle1");

    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(handle0.clone()));
    let mut inner_stream =
        Stream::with_table(table.clone(), "lifted_lifted_select_inner", handle_group)
            .await
            .expect("create inner stream");
    set_default_in_place(&mut inner_stream, handle0.clone());
    push_value_in_place(&mut inner_stream, handle1.clone());
    inner_stream.flush().await.expect("flush inner stream");

    let mut selected =
        lifted_select_zset_stream::<String, _>(&inner_stream, |value: &String| value == "keep")
            .await
            .expect("apply inner lifted select");
    selected.flush().await.expect("flush selected");
    let selected_handle = selected.handle();

    let stream_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(selected_handle.clone()));
    let mut outer_stream =
        Stream::with_table(table.clone(), "lifted_lifted_select_outer", stream_group)
            .await
            .expect("create outer stream");
    set_default_in_place(&mut outer_stream, selected_handle.clone());
    outer_stream.flush().await.expect("flush outer stream");

    let mut result =
        lifted_lifted_select_zset_stream::<String, _>(&outer_stream, |value: &String| {
            value == "keep"
        })
        .await
        .expect("apply lifted-lifted select");
    result.flush().await.expect("flush lifted-lifted result");

    let handles = collect_values(&result, result.current_time())
        .await
        .expect("collect outer handles");
    let resolved_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(handle0.clone()));
    let mut resolved = result
        .resolve_handle(&handles[0], resolved_group)
        .await
        .expect("resolve nested stream");
    resolved.flush().await.expect("flush resolved stream");

    let resolved_handles = collect_values(&resolved, resolved.current_time())
        .await
        .expect("collect resolved handles");
    let mut cache = HashMap::new();
    let first = materialize_zset_handle::<String>(table.clone(), &mut cache, &resolved_handles[0])
        .await
        .expect("materialize resolved first");
    assert_eq!(first.get("keep"), Some(&2));
    assert!(first.get("drop").is_none());
}

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
async fn delta_lifted_delta_lifted_join_aligns_to_shortest_stream() {
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

    assert_eq!(
        result.current_time(),
        outer_right.current_time(),
        "aggregator should stop at shortest timeline"
    );

    let handles = collect_values(&result, result.current_time())
        .await
        .expect("collect aligned result handles");
    assert_eq!(
        handles.len(),
        usize::try_from(outer_right.current_time().saturating_add(1))
            .expect("convert timestamp to length"),
        "expected handles only up to shortest stream frontier"
    );
}

#[tokio::test]
async fn lifted_join_covers_each_delta_term() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));

    let predicate = |left: &(i32, i32), right: &(i32, i32)| left.0 == right.0;
    let projector = |left: &(i32, i32), right: &(i32, i32)| (left.1, right.1);
    let mut cache = HashMap::new();

    // ΔR ⋈ S
    let mut left_delta = ZSetStream::new(
        Arc::new(
            Dictionary::with_table(table.clone(), "delta_r", None)
                .await
                .unwrap(),
        ),
        table.clone(),
        "delta_r",
        StreamRetention::None,
    )
    .await
    .unwrap();
    left_delta.add_delta((1, 10), 1);
    left_delta.flush().await.unwrap();

    let mut right_state = ZSetStream::new(
        Arc::new(
            Dictionary::with_table(table.clone(), "state_s", None)
                .await
                .unwrap(),
        ),
        table.clone(),
        "state_s",
        StreamRetention::None,
    )
    .await
    .unwrap();
    right_state.add_delta((1, 100), 1);
    right_state.flush().await.unwrap();

    let left_handles = left_delta.handle_stream();
    let right_handles = right_state.handle_stream();
    let left_outer = lifted_stream_introduction(&left_handles).await.unwrap();
    let right_outer = lifted_stream_introduction(&right_handles).await.unwrap();
    let mut join_state =
        delta_lifted_delta_lifted_join(&left_outer, &right_outer, predicate, projector)
            .await
            .unwrap();
    join_state.flush().await.unwrap();
    let handles = collect_values(&join_state, join_state.current_time())
        .await
        .unwrap();
    assert!(
        !handles.is_empty(),
        "ΔR⋈S should produce at least one handle"
    );
    let nested_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: handles[0].ns.clone(),
        version: 0,
    }));
    let mut resolved = join_state
        .resolve_handle(handles.last().unwrap(), nested_group)
        .await
        .unwrap();
    let latest = resolved.latest().await.unwrap();
    let map = materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &latest)
        .await
        .unwrap();
    assert!(map.contains_key(&(10, 100)), "ΔR⋈S term missing");

    // R ⋈ ΔS
    let mut left_state = ZSetStream::new(
        Arc::new(
            Dictionary::with_table(table.clone(), "state_r", None)
                .await
                .unwrap(),
        ),
        table.clone(),
        "state_r",
        StreamRetention::None,
    )
    .await
    .unwrap();
    left_state.add_delta((2, 20), 1);
    left_state.flush().await.unwrap();

    let mut right_delta = ZSetStream::new(
        Arc::new(
            Dictionary::with_table(table.clone(), "delta_s", None)
                .await
                .unwrap(),
        ),
        table.clone(),
        "delta_s",
        StreamRetention::None,
    )
    .await
    .unwrap();
    right_delta.add_delta((2, 200), 1);
    right_delta.flush().await.unwrap();

    let left_handles = left_state.handle_stream();
    let right_handles = right_delta.handle_stream();
    let left_outer = lifted_stream_introduction(&left_handles).await.unwrap();
    let right_outer = lifted_stream_introduction(&right_handles).await.unwrap();
    let mut join_delta_s =
        delta_lifted_delta_lifted_join(&left_outer, &right_outer, predicate, projector)
            .await
            .unwrap();
    join_delta_s.flush().await.unwrap();
    let handles = collect_values(&join_delta_s, join_delta_s.current_time())
        .await
        .unwrap();
    assert!(
        !handles.is_empty(),
        "R⋈ΔS should produce at least one handle"
    );
    let nested_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: handles[0].ns.clone(),
        version: 0,
    }));
    let mut resolved = join_delta_s
        .resolve_handle(handles.last().unwrap(), nested_group)
        .await
        .unwrap();
    let latest = resolved.latest().await.unwrap();
    let map = materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &latest)
        .await
        .unwrap();
    assert!(map.contains_key(&(20, 200)), "R⋈ΔS term missing");

    // ΔR ⋈ ΔS
    let mut left_both = ZSetStream::new(
        Arc::new(
            Dictionary::with_table(table.clone(), "delta_rr", None)
                .await
                .unwrap(),
        ),
        table.clone(),
        "delta_rr",
        StreamRetention::None,
    )
    .await
    .unwrap();
    left_both.add_delta((3, 30), 1);
    left_both.flush().await.unwrap();

    let mut right_both = ZSetStream::new(
        Arc::new(
            Dictionary::with_table(table.clone(), "delta_ss", None)
                .await
                .unwrap(),
        ),
        table.clone(),
        "delta_ss",
        StreamRetention::None,
    )
    .await
    .unwrap();
    right_both.add_delta((3, 300), 1);
    right_both.flush().await.unwrap();

    let left_handles = left_both.handle_stream();
    let right_handles = right_both.handle_stream();
    let left_outer = lifted_stream_introduction(&left_handles).await.unwrap();
    let right_outer = lifted_stream_introduction(&right_handles).await.unwrap();
    let mut join_both =
        delta_lifted_delta_lifted_join(&left_outer, &right_outer, predicate, projector)
            .await
            .unwrap();
    join_both.flush().await.unwrap();
    let handles = collect_values(&join_both, join_both.current_time())
        .await
        .unwrap();
    assert!(
        !handles.is_empty(),
        "ΔR⋈ΔS should produce at least one handle"
    );
    let nested_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: handles[0].ns.clone(),
        version: 0,
    }));
    let mut resolved = join_both
        .resolve_handle(handles.last().unwrap(), nested_group)
        .await
        .unwrap();
    let latest = resolved.latest().await.unwrap();
    let map = materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &latest)
        .await
        .unwrap();
    assert!(map.contains_key(&(30, 300)), "ΔR⋈ΔS term missing");
}
