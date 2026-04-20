use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::algebra::AbelianGroup;
use crate::collections::CompactionPolicy;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::cursor::StreamCursor;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::basic::delay;
use crate::stream::operations::{lifted_lifted_project_zset_stream, lifted_project_zset_stream};
use crate::stream::tests::common::build_db;
use crate::stream::util::{
    collect_values, materialize_zset_handle, push_value_in_place, set_default_in_place,
};
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

#[tokio::test]
async fn lifted_lifted_project_zset_stream_projects_nested_stream_handles() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "lifted_lifted_project_nested", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "lifted_lifted_project_nested",
        StreamRetention::None,
    )
    .await
    .expect("create source zset stream");
    source.add_delta("cat".to_string(), 1);
    source.flush().await.expect("flush t1");
    source.add_delta("dogs".to_string(), 1);
    source.flush().await.expect("flush t2");

    let mut projected =
        lifted_project_zset_stream::<String, usize, _>(&source.handle_stream(), |value| {
            value.len()
        })
        .await
        .expect("build projected stream");
    projected.flush().await.expect("flush projected stream");
    let projected_handle = projected.handle();

    let stream_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(projected_handle.clone()));
    let mut outer = Stream::with_table(
        table.clone(),
        "lifted_lifted_project_nested_outer",
        stream_group,
    )
    .await
    .expect("create outer stream");
    set_default_in_place(&mut outer, projected_handle.clone());
    push_value_in_place(&mut outer, projected_handle.clone());
    outer.flush().await.expect("flush outer stream");

    let mut nested_projected =
        lifted_lifted_project_zset_stream::<usize, usize, _>(&outer, |value: &usize| value + 10)
            .await
            .expect("apply lifted-lifted project");
    nested_projected
        .flush()
        .await
        .expect("flush lifted-lifted project");

    let outer_handles = collect_values(&nested_projected, nested_projected.current_time())
        .await
        .expect("collect outer handles");
    let input_handles = collect_values(&outer, outer.current_time())
        .await
        .expect("collect outer input handles");
    let resolve_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: projected_handle.ns.clone(),
        version: 0,
    }));

    for (idx, out_handle) in outer_handles.iter().enumerate() {
        let mut observed_inner = nested_projected
            .resolve_handle(out_handle, resolve_group.clone())
            .await
            .expect("resolve observed nested stream");
        observed_inner
            .flush()
            .await
            .expect("flush observed nested stream");

        let expected_input = outer
            .resolve_handle(&input_handles[idx], resolve_group.clone())
            .await
            .expect("resolve expected nested input stream");
        let mut expected =
            lifted_project_zset_stream::<usize, usize, _>(&expected_input, |value: &usize| {
                value + 10
            })
            .await
            .expect("build expected nested project stream");
        expected
            .flush()
            .await
            .expect("flush expected nested project");

        let observed_handles = collect_values(&observed_inner, observed_inner.current_time())
            .await
            .expect("collect observed nested handles");
        let expected_handles = collect_values(&expected, expected.current_time())
            .await
            .expect("collect expected nested handles");

        assert_eq!(observed_handles.len(), expected_handles.len());
        let mut observed_cache = HashMap::new();
        let mut expected_cache = HashMap::new();
        for (observed_handle, expected_handle) in
            observed_handles.iter().zip(expected_handles.iter())
        {
            let observed_map = materialize_zset_handle::<usize>(
                table.clone(),
                &mut observed_cache,
                observed_handle,
            )
            .await
            .expect("materialize observed nested project handle");
            let expected_map = materialize_zset_handle::<usize>(
                table.clone(),
                &mut expected_cache,
                expected_handle,
            )
            .await
            .expect("materialize expected nested project handle");
            assert_eq!(observed_map, expected_map);
        }
    }
}
