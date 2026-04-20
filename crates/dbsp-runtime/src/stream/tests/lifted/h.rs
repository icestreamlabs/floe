use std::collections::HashMap;
use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::{lifted_h_zset_stream, lifted_lifted_h_zset_stream};
use crate::stream::tests::common::build_db;
use crate::stream::util::{
    collect_values, materialize_zset_handle, push_value_in_place, set_default_in_place,
};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};

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
async fn lifted_lifted_h_zset_stream_detects_nested_transitions() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let diff_dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_lifted_h_diff", None)
            .await
            .expect("diff dictionary"),
    );
    let state_dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_lifted_h_state", None)
            .await
            .expect("state dictionary"),
    );

    let mut diff_stream = ZSetStream::new(
        diff_dict,
        table.clone(),
        "lifted_lifted_h_diff",
        StreamRetention::None,
    )
    .await
    .expect("create diff stream");
    let mut state_stream = ZSetStream::new(
        state_dict,
        table.clone(),
        "lifted_lifted_h_state",
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

    let inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(diff_handle0.clone()));
    let mut diff_handle_stream = Stream::with_table(
        table.clone(),
        "lifted_lifted_h_diff_handles",
        inner_group.clone(),
    )
    .await
    .expect("create diff handle stream");
    set_default_in_place(&mut diff_handle_stream, diff_handle0.clone());
    push_value_in_place(&mut diff_handle_stream, diff_handle1.clone());
    diff_handle_stream
        .flush()
        .await
        .expect("flush diff handles");

    let mut state_handle_stream = Stream::with_table(
        table.clone(),
        "lifted_lifted_h_state_handles",
        inner_group.clone(),
    )
    .await
    .expect("create state handle stream");
    set_default_in_place(&mut state_handle_stream, state_handle0.clone());
    push_value_in_place(&mut state_handle_stream, state_handle1.clone());
    state_handle_stream
        .flush()
        .await
        .expect("flush state handles");

    let diff_outer_handle = diff_handle_stream.handle();
    let state_outer_handle = state_handle_stream.handle();

    let outer_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(diff_outer_handle.clone()));
    let mut diff_outer = Stream::with_table(
        table.clone(),
        "lifted_lifted_h_diff_outer",
        outer_group.clone(),
    )
    .await
    .expect("create diff outer stream");
    set_default_in_place(&mut diff_outer, diff_outer_handle.clone());
    push_value_in_place(&mut diff_outer, diff_outer_handle.clone());
    diff_outer.flush().await.expect("flush diff outer stream");

    let mut state_outer =
        Stream::with_table(table.clone(), "lifted_lifted_h_state_outer", outer_group)
            .await
            .expect("create state outer stream");
    set_default_in_place(&mut state_outer, state_outer_handle.clone());
    push_value_in_place(&mut state_outer, state_outer_handle.clone());
    state_outer.flush().await.expect("flush state outer stream");

    let mut nested_h = lifted_lifted_h_zset_stream::<String>(&diff_outer, &state_outer)
        .await
        .expect("apply lifted-lifted H");
    nested_h.flush().await.expect("flush lifted-lifted H");

    let outer_handles = collect_values(&nested_h, nested_h.current_time())
        .await
        .expect("collect nested outer handles");
    let input_diff_handles = collect_values(&diff_outer, diff_outer.current_time())
        .await
        .expect("collect diff outer input handles");
    let input_state_handles = collect_values(&state_outer, state_outer.current_time())
        .await
        .expect("collect state outer input handles");
    let resolve_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(diff_handle0));

    for (idx, out_handle) in outer_handles.iter().enumerate() {
        let mut observed_inner = nested_h
            .resolve_handle(out_handle, resolve_group.clone())
            .await
            .expect("resolve observed nested H stream");
        observed_inner
            .flush()
            .await
            .expect("flush observed nested H stream");

        let expected_diff_inner = diff_outer
            .resolve_handle(&input_diff_handles[idx], resolve_group.clone())
            .await
            .expect("resolve expected diff inner stream");
        let expected_state_inner = state_outer
            .resolve_handle(&input_state_handles[idx], resolve_group.clone())
            .await
            .expect("resolve expected state inner stream");
        let mut expected =
            lifted_h_zset_stream::<String>(&expected_diff_inner, &expected_state_inner)
                .await
                .expect("build expected nested H stream");
        expected
            .flush()
            .await
            .expect("flush expected nested H stream");

        let observed_handles = collect_values(&observed_inner, observed_inner.current_time())
            .await
            .expect("collect observed nested H handles");
        let expected_handles = collect_values(&expected, expected.current_time())
            .await
            .expect("collect expected nested H handles");

        assert_eq!(observed_handles.len(), expected_handles.len());
        let mut observed_cache = HashMap::new();
        let mut expected_cache = HashMap::new();
        for (observed_handle, expected_handle) in
            observed_handles.iter().zip(expected_handles.iter())
        {
            let observed_map = materialize_zset_handle::<String>(
                table.clone(),
                &mut observed_cache,
                observed_handle,
            )
            .await
            .expect("materialize observed nested H handle");
            let expected_map = materialize_zset_handle::<String>(
                table.clone(),
                &mut expected_cache,
                expected_handle,
            )
            .await
            .expect("materialize expected nested H handle");
            assert_eq!(observed_map, expected_map);
        }
    }
}
