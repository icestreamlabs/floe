use std::collections::HashMap;
use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::{lifted_lifted_select_zset_stream, lifted_select_zset_stream};
use crate::stream::tests::common::build_db;
use crate::stream::util::{
    collect_values, materialize_zset_handle, push_value_in_place, set_default_in_place,
};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};

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
    assert!(!first.contains_key("drop"));
}
