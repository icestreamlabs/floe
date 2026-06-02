use super::*;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::storage::KeyValueTable;
use crate::storage::SlateTable;
use crate::storage::dictionary::Dictionary;
use crate::stream::util::materialize_zset_handle;
use object_store::memory::InMemory;
use slatedb::Db;
use std::collections::BTreeMap;

async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(
        Db::open("session_window_aggregate", store)
            .await
            .expect("open SlateDB"),
    )
}

async fn stage_version<K>(
    dict: Arc<Dictionary<K>>,
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
    deltas: &[(K, i64)],
) -> ZSetHandle
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        let id = dict_batch
            .intern(key)
            .await
            .expect("intern key for session window test");
        buckets
            .entry((id >> 48) as u16)
            .or_default()
            .push((id, *delta));
    }
    drop(dict_batch);

    let mut segments = Vec::new();
    for (bucket, mut bucket_deltas) in buckets {
        bucket_deltas.retain(|(_, delta)| *delta != 0);
        if bucket_deltas.is_empty() {
            continue;
        }
        bucket_deltas.sort_by_key(|(id, _)| *id);
        segments.push(SegmentRecord {
            id: 0,
            bucket,
            deltas: bucket_deltas,
        });
    }

    let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
        .await
        .expect("build versioned input");
    let version = versioned
        .create_version_with_base(segments, None)
        .await
        .expect("create input version");
    versioned.handle_for_version(version)
}

#[tokio::test]
async fn session_window_aggregate_merges_splits_and_evicts() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    let input_dict = Arc::new(
        Dictionary::<(i64, i64, i64)>::with_table(table.clone(), "session_window_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<(WindowKey<i64>, i64)>::with_table(
            table.clone(),
            "session_window_output",
            None,
        )
        .await
        .expect("output dict"),
    );
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "session_window_output".to_string(),
    )
    .await
    .expect("output zset");
    let mut op = SessionWindowAggregateOp {
        table: table.clone(),
        dict_cache: HashMap::new(),
        row_extractor: Arc::new(|rows: &[((i64, i64, i64), i64)]| {
            rows.iter()
                .map(|((group, event_ts, value), weight)| {
                    ((*group, *event_ts, *value), *weight, *group, *event_ts)
                })
                .collect()
        }),
        aggregator: Arc::new(|_key: &i64, rows: &[((i64, i64, i64), i64)]| {
            let sum = rows
                .iter()
                .map(|((_, _, value), weight)| value * weight)
                .sum::<i64>();
            Some(sum)
        }),
        input_index: IndexedBatchZSet::with_hot_key_compaction_threshold(
            table.clone(),
            "session_window_index",
            2,
        ),
        state: RelationState::<(WindowKey<i64>, i64)>::empty(
            table.clone(),
            "session_window_state".to_string(),
        )
        .await
        .expect("state"),
        output,
        session_cache: None,
        watermark: Arc::new(AtomicI64::new(-1)),
        gap_ms: 15,
        allowed_lateness_ms: 0,
        logical_work: metrics::LogicalWorkCollector::default(),
    };

    let first = stage_version(
        input_dict.clone(),
        table.clone(),
        "session_window_input",
        &[((1, 0, 10), 1), ((1, 25, 5), 1)],
    )
    .await;
    let mut cache = HashMap::new();
    let step_one = materialize_zset_handle::<(WindowKey<i64>, i64)>(
        table.clone(),
        &mut cache,
        &op.on_step(0, &[first])
            .await
            .expect("run t1")
            .expect("t1 output"),
    )
    .await
    .expect("materialize t1");
    assert_eq!(
        step_one,
        HashMap::from([
            (
                (
                    WindowKey {
                        start: 0,
                        end: 15,
                        key: 1,
                    },
                    10,
                ),
                1,
            ),
            (
                (
                    WindowKey {
                        start: 25,
                        end: 40,
                        key: 1,
                    },
                    5,
                ),
                1,
            ),
        ])
    );

    let bridge = stage_version(
        input_dict.clone(),
        table.clone(),
        "session_window_input",
        &[((1, 12, 7), 1)],
    )
    .await;
    let step_two = materialize_zset_handle::<(WindowKey<i64>, i64)>(
        table.clone(),
        &mut cache,
        &op.on_step(1, &[bridge])
            .await
            .expect("run t2")
            .expect("t2 output"),
    )
    .await
    .expect("materialize t2");
    assert_eq!(
        step_two,
        HashMap::from([
            (
                (
                    WindowKey {
                        start: 0,
                        end: 15,
                        key: 1,
                    },
                    10,
                ),
                -1,
            ),
            (
                (
                    WindowKey {
                        start: 25,
                        end: 40,
                        key: 1,
                    },
                    5,
                ),
                -1,
            ),
            (
                (
                    WindowKey {
                        start: 0,
                        end: 40,
                        key: 1,
                    },
                    22,
                ),
                1,
            ),
        ])
    );

    let retract_bridge = stage_version(
        input_dict,
        table.clone(),
        "session_window_input",
        &[((1, 12, 7), -1)],
    )
    .await;
    let step_three = materialize_zset_handle::<(WindowKey<i64>, i64)>(
        table.clone(),
        &mut cache,
        &op.on_step(2, &[retract_bridge])
            .await
            .expect("run t3")
            .expect("t3 output"),
    )
    .await
    .expect("materialize t3");
    assert_eq!(
        step_three,
        HashMap::from([
            (
                (
                    WindowKey {
                        start: 0,
                        end: 40,
                        key: 1,
                    },
                    22,
                ),
                -1,
            ),
            (
                (
                    WindowKey {
                        start: 0,
                        end: 15,
                        key: 1,
                    },
                    10,
                ),
                1,
            ),
            (
                (
                    WindowKey {
                        start: 25,
                        end: 40,
                        key: 1,
                    },
                    5,
                ),
                1,
            ),
        ])
    );

    op.watermark.store(45, Ordering::Relaxed);
    let step_four = materialize_zset_handle::<(WindowKey<i64>, i64)>(
        table.clone(),
        &mut cache,
        &op.on_step(
            3,
            &[ZSetHandle {
                ns: "session_window_input".to_string(),
                version: 0,
            }],
        )
        .await
        .expect("run t4")
        .expect("t4 output"),
    )
    .await
    .expect("materialize t4");
    assert_eq!(
        step_four,
        HashMap::from([
            (
                (
                    WindowKey {
                        start: 0,
                        end: 15,
                        key: 1,
                    },
                    10,
                ),
                -1,
            ),
            (
                (
                    WindowKey {
                        start: 25,
                        end: 40,
                        key: 1,
                    },
                    5,
                ),
                -1,
            ),
        ])
    );
}
