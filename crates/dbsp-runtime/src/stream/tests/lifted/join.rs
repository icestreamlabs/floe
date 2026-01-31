use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::cursor::StreamCursor;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::{
    delta_lifted_delta_lifted_join, lifted_join_zset_stream, lifted_stream_introduction,
};
use crate::stream::tests::common::build_db;
use crate::stream::util::{collect_values, materialize_zset_handle};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};
use tokio::time::timeout;

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
