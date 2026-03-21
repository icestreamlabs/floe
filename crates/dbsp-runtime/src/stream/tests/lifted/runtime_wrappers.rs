use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use crate::filter::DbspFilter;
use crate::join::DbspJoin;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::cursor::StreamCursor;
use crate::stream::tests::common::build_db;
use crate::stream::util::{materialize_zset_handle, publish_scheduled_value};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};
use crate::stream::{DeltaHandleStream, delay};

#[tokio::test]
async fn filter_runtime_publishes_future_scheduled_tick() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "dbsp_filter_scheduled", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "dbsp_filter_scheduled",
        StreamRetention::KeepLast { keep_last: 3 },
    )
    .await
    .expect("create source stream");
    source.add_delta("drop".to_string(), 1);
    source.flush().await.expect("flush t1");
    source.add_delta("keep".to_string(), 1);
    source.flush().await.expect("flush t2");

    let mut delayed_stream = delay(&source.delta_handle_stream().stream())
        .await
        .expect("delay delta stream");
    let delayed = DeltaHandleStream::new(delayed_stream.clone());
    let derived =
        DbspFilter::new::<String, _>(&delayed, |value: &String| value.starts_with('k'), None)
            .await
            .expect("build filter");

    let mut cursor = StreamCursor::new(derived.stream().stream());
    let (frontier, _handle) = cursor.snapshot().await.expect("snapshot derived");
    assert_eq!(
        frontier, 2,
        "derived frontier should start before scheduled t3"
    );

    publish_scheduled_value(&mut delayed_stream, 3)
        .await
        .expect("commit delayed t3");

    let (ts, handle) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("wait for derived t3")
        .expect("derived t3");
    assert_eq!(ts, 3);

    let mut cache = HashMap::new();
    let materialized = materialize_zset_handle::<String>(table.clone(), &mut cache, &handle)
        .await
        .expect("materialize derived handle");
    assert_eq!(materialized.get("keep"), Some(&1));
}

#[tokio::test]
async fn join_preserves_future_scheduled_handle() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::with_table(table.clone(), "dbsp_join_scheduled_left", None)
            .await
            .expect("build left dictionary"),
    );
    let right_dict = Arc::new(
        Dictionary::with_table(table.clone(), "dbsp_join_scheduled_right", None)
            .await
            .expect("build right dictionary"),
    );

    let mut left = ZSetStream::new(
        left_dict,
        table.clone(),
        "dbsp_join_scheduled_left",
        StreamRetention::KeepLast { keep_last: 3 },
    )
    .await
    .expect("create left stream");
    let mut right = ZSetStream::new(
        right_dict,
        table.clone(),
        "dbsp_join_scheduled_right",
        StreamRetention::KeepLast { keep_last: 3 },
    )
    .await
    .expect("create right stream");

    left.add_delta("miss".to_string(), 1);
    left.flush().await.expect("flush left t1");
    left.add_delta("match".to_string(), 1);
    left.flush().await.expect("flush left t2");

    right.add_delta("match".to_string(), 1);
    right.flush().await.expect("flush right t1");

    let delayed_left = DeltaHandleStream::new(
        delay(&left.delta_handle_stream().stream())
            .await
            .expect("delay left delta stream"),
    );
    let derived = DbspJoin::new::<String, String, String, String, _, _, _, _>(
        &delayed_left,
        &right.delta_handle_stream(),
        |value: &String| Some(value.clone()),
        |value: &String| Some(value.clone()),
        |_left, _right| true,
        |left, _right| left.clone(),
        None,
    )
    .await
    .expect("build join");

    let mut cache = HashMap::new();
    let materialized = materialize_zset_handle::<String>(
        table.clone(),
        &mut cache,
        &derived
            .stream()
            .get(3)
            .await
            .expect("load scheduled join handle"),
    )
    .await
    .expect("materialize join handle");

    assert_eq!(materialized.get("match"), Some(&1));
}
