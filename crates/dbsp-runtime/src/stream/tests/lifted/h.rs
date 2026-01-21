use std::collections::HashMap;
use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::lifted_h_zset_stream;
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
