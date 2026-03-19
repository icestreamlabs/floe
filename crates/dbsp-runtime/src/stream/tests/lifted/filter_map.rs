use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::filter_map::DbspFilterMap;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::cursor::StreamCursor;
use crate::stream::tests::common::build_db;
use crate::stream::util::materialize_zset_handle;
use crate::stream::zset_stream::{StreamRetention, ZSetStream};
use tokio::time::timeout;

#[tokio::test]
async fn filter_map_emits_handle_for_empty_output_ticks() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "filter_map_empty_ticks_input", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "filter_map_empty_ticks_input",
        StreamRetention::KeepLast { keep_last: 3 },
    )
    .await
    .expect("create source stream");

    let derived = DbspFilterMap::new_batch::<String, String, _>(
        &source.delta_handle_stream(),
        |delta_values| {
            Ok(delta_values
                .into_iter()
                .filter(|(value, _)| value.starts_with('k'))
                .collect())
        },
        None,
    )
    .await
    .expect("build filter_map");

    let mut cursor = StreamCursor::new(derived.stream().stream());
    let _ = cursor.snapshot().await.expect("initial snapshot");

    source.add_delta("keep".to_string(), 1);
    source.flush().await.expect("flush keep");
    let (_ts1, handle1) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("wait for keep tick")
        .expect("keep tick");

    source.add_delta("drop".to_string(), 1);
    source.flush().await.expect("flush drop");
    let (_ts2, handle2) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("wait for empty-output tick")
        .expect("empty-output tick");

    let mut cache = HashMap::new();
    let first = materialize_zset_handle::<String>(table.clone(), &mut cache, &handle1)
        .await
        .expect("materialize first handle");
    let second = materialize_zset_handle::<String>(table.clone(), &mut cache, &handle2)
        .await
        .expect("materialize second handle");

    assert_eq!(first.get("keep"), Some(&1));
    assert_eq!(
        second,
        HashMap::new(),
        "empty-output tick should advance with an explicit empty delta handle",
    );
    assert_eq!(
        handle2.version, 0,
        "no-output tick should use the empty delta handle version",
    );
}
