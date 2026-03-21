use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::collections::CompactionPolicy;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::cursor::StreamCursor;
use crate::stream::operations::basic::delay;
use crate::stream::operations::lifted_project_zset_stream;
use crate::stream::tests::common::build_db;
use crate::stream::util::materialize_zset_handle;
use crate::stream::zset_stream::{StreamRetention, ZSetStream};
use tokio::time::timeout;

#[tokio::test]
async fn lifted_project_stream_handles_compaction_noop_without_double_counting() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_project_compaction", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "lifted_project_compaction",
        StreamRetention::KeepLast { keep_last: 3 },
    )
    .await
    .expect("create source stream");
    source.set_compaction_policy(CompactionPolicy {
        max_chain_len: 1,
        max_segments: 1,
        max_bucket_segments: 1,
    });

    let derived = lifted_project_zset_stream::<String, usize, _>(
        &source.handle_stream(),
        |value: &String| value.len(),
    )
    .await
    .expect("build lifted project stream");

    let mut cursor = StreamCursor::new(derived);
    let _ = cursor.snapshot().await.expect("initial snapshot");

    source.add_delta("cat".to_string(), 1);
    source.flush().await.expect("flush t1");
    let (_ts1, h1) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("wait for t1")
        .expect("t1 handle");

    let _ = source
        .wait_for_background_compaction()
        .await
        .expect("wait for compaction");
    source.flush().await.expect("flush t2 noop");
    let (_ts2, h2) = timeout(Duration::from_secs(1), cursor.next())
        .await
        .expect("wait for t2")
        .expect("t2 handle");

    let mut cache = HashMap::new();
    let t1 = materialize_zset_handle::<usize>(table.clone(), &mut cache, &h1)
        .await
        .expect("materialize t1");
    let t2 = materialize_zset_handle::<usize>(table.clone(), &mut cache, &h2)
        .await
        .expect("materialize t2");

    assert_eq!(t1, HashMap::from([(3_usize, 1_i64)]));
    assert_eq!(
        t2,
        HashMap::from([(3_usize, 1_i64)]),
        "noop tick should not replay full snapshot deltas"
    );
}

#[tokio::test]
async fn lifted_project_preserves_future_scheduled_handle() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_project_scheduled", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "lifted_project_scheduled",
        StreamRetention::KeepLast { keep_last: 3 },
    )
    .await
    .expect("create source stream");
    source.add_delta("cat".to_string(), 1);
    source.flush().await.expect("flush t1");
    source.add_delta("cat".to_string(), -1);
    source.add_delta("dogs".to_string(), 1);
    source.flush().await.expect("flush t2");

    let delayed = delay(&source.handle_stream())
        .await
        .expect("delay handle stream");
    let mut derived = lifted_project_zset_stream::<String, usize, _>(&delayed, |value| value.len())
        .await
        .expect("build lifted project");
    derived.flush().await.expect("flush derived stream");

    let mut cache = HashMap::new();
    let t2 = materialize_zset_handle::<usize>(
        table.clone(),
        &mut cache,
        &derived.get(2).await.expect("derived t2"),
    )
    .await
    .expect("materialize t2");
    let t3 = materialize_zset_handle::<usize>(
        table.clone(),
        &mut cache,
        &derived.get(3).await.expect("derived t3"),
    )
    .await
    .expect("materialize t3");

    assert_eq!(t2, HashMap::from([(3_usize, 1_i64)]));
    assert_eq!(t3, HashMap::from([(4_usize, 1_i64)]));
}
