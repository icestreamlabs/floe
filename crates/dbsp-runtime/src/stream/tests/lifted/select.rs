use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::cursor::StreamCursor;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::lifted_select_zset_stream;
use crate::stream::tests::common::build_db;
use crate::stream::util::{
    collect_values, materialize_zset_handle, push_value_in_place, set_default_in_place,
};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};
use tokio::time::timeout;

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
    assert!(!first.contains_key("drop"));

    let second = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[1])
        .await
        .expect("materialize second handle");
    assert!(!second.contains_key("keep"));
    assert!(!second.contains_key("drop"));
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
