use super::*;
use crate::stream::util::{
    delta_zset_handle_batch, materialize_zset_handle, publish_transient_zset_batch,
};
use object_store::memory::InMemory;
use slatedb::Db;
use std::sync::atomic::{AtomicU64, Ordering};

static FILTER_MAP_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_suffix() -> u64 {
    FILTER_MAP_TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
}

async fn build_db(suffix: u64) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(
        Db::open(format!("filter_map_test_{suffix}"), store)
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
        + std::hash::Hash
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
            .expect("intern key for filter_map test");
        buckets
            .entry(bucket_for(id))
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
        .expect("build versioned zset");
    let version = versioned
        .create_version_with_base(segments, None)
        .await
        .expect("create version");
    versioned.handle_for_version(version)
}

fn coalesce_rows(rows: &[(i64, i64)]) -> HashMap<i64, i64> {
    let mut out = HashMap::new();
    for (key, weight) in rows {
        let entry = out.entry(*key).or_insert(0);
        *entry += *weight;
        if *entry == 0 {
            out.remove(key);
        }
    }
    out
}

#[tokio::test]
async fn zset_handle_group_and_bucket_helpers_work() {
    let default = ZSetHandle {
        ns: "group-default".to_string(),
        version: 7,
    };
    let group = ZSetHandleGroup {
        default: default.clone(),
    };

    let a = ZSetHandle {
        ns: "a".to_string(),
        version: 1,
    };
    let b = ZSetHandle {
        ns: "b".to_string(),
        version: 2,
    };

    assert_eq!(group.add(&a, &b).await, a);
    assert_eq!(group.neg(&b).await, b);
    assert_eq!(group.identity().await, default);

    assert_eq!(bucket_for(0), 0);
    assert_eq!(bucket_for(1 << 48), 1);
    assert_eq!(bucket_for(u64::MAX), u16::MAX);
}

#[tokio::test]
async fn apply_deltas_to_versioned_covers_persistent_replayable_and_empty_paths() {
    let suffix = next_suffix();
    let db = build_db(suffix).await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));

    let ns = format!("apply_versioned_{suffix}");
    let dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), ns.clone(), None)
            .await
            .expect("dict"),
    );
    let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), ns.clone())
        .await
        .expect("zset");

    let handle1 = apply_deltas_to_versioned(&mut versioned, &[(1, 1), (2, 2), (1, -1), (3, 0)])
        .await
        .expect("apply persistent deltas");
    let mut cache = HashMap::new();
    let materialized1 = materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle1)
        .await
        .expect("materialize handle1");
    assert_eq!(materialized1, HashMap::from([(2_i64, 2_i64)]));

    let handle2 = apply_deltas_to_versioned(&mut versioned, &[(2, -2), (5, 0)])
        .await
        .expect("apply second persistent deltas");
    let materialized2 = materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle2)
        .await
        .expect("materialize handle2");
    assert_eq!(materialized2, HashMap::from([(2_i64, -2_i64)]));

    let handle3 = apply_deltas_to_versioned(&mut versioned, &[(9, 0)])
        .await
        .expect("apply empty staged deltas");
    assert_eq!(handle3.version, handle2.version);

    let replay_ns = format!("apply_versioned_replay_{suffix}");
    let replay_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), replay_ns.clone(), None)
            .await
            .expect("replay dict"),
    );
    let mut replay = VersionedZSet::new(replay_dict, table.clone(), replay_ns)
        .await
        .expect("replay zset");
    replay.enable_replayable_persistence();
    let replay_handle = apply_deltas_to_versioned(&mut replay, &[(11, 3), (12, -1)])
        .await
        .expect("apply replayable deltas");
    let replay_rows = delta_zset_handle_batch::<i64>(table, &mut HashMap::new(), &replay_handle)
        .await
        .expect("delta replay rows");
    assert_eq!(
        coalesce_rows(replay_rows.as_ref()),
        HashMap::from([(11_i64, 3_i64), (12_i64, -1_i64)])
    );
}

#[tokio::test]
async fn filter_map_batch_state_on_step_handles_success_empty_and_error_paths() {
    let suffix = next_suffix();
    let db = build_db(suffix).await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));

    let input_ns = format!("filter_map_batch_input_{suffix}");
    let output_ns = format!("filter_map_batch_output_{suffix}");
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), output_ns.clone(), None)
            .await
            .expect("output dict"),
    );
    let output = VersionedZSet::new(output_dict.clone(), table.clone(), output_ns.clone())
        .await
        .expect("output zset");

    let mut state = FilterMapBatchState {
        transform: Arc::new(|rows: &[(i64, i64)]| {
            Ok(rows
                .iter()
                .filter_map(|(value, weight)| (value % 2 != 0).then_some((value * 100, *weight)))
                .collect::<Vec<_>>())
        }),
        table: table.clone(),
        output,
        dict_cache: HashMap::new(),
        logical_work: metrics::LogicalWorkCollector::default(),
    };

    let input_h1 = stage_version(
        input_dict.clone(),
        table.clone(),
        input_ns.as_str(),
        &[(1, 2), (2, 1), (3, -1)],
    )
    .await;
    let out_h1 = state
        .on_step(1, &input_h1)
        .await
        .expect("batch state step 1");
    let out_rows_1 = delta_zset_handle_batch::<i64>(table.clone(), &mut HashMap::new(), &out_h1)
        .await
        .expect("batch output rows 1");
    assert_eq!(
        coalesce_rows(out_rows_1.as_ref()),
        HashMap::from([(100_i64, 2_i64), (300_i64, -1_i64)])
    );
    let work1 = state.last_logical_work();
    assert_eq!(work1.input_delta_rows, 3);
    assert_eq!(work1.output_delta_rows, 2);
    assert_eq!(work1.persisted_rows, 2);

    let input_h2 = stage_version(
        input_dict.clone(),
        table.clone(),
        input_ns.as_str(),
        &[(2, 5), (4, 1)],
    )
    .await;
    let out_h2 = state
        .on_step(2, &input_h2)
        .await
        .expect("batch state step 2");
    assert_eq!(out_h2.version, 0);
    let work2 = state.last_logical_work();
    assert_eq!(work2.input_delta_rows, 2);
    assert_eq!(work2.output_delta_rows, 0);
    assert_eq!(work2.state_full_scan_count, 0);

    let input_h3 = stage_version(
        input_dict.clone(),
        table.clone(),
        input_ns.as_str(),
        &[(5, 3)],
    )
    .await;
    let out_h3 = state
        .on_step(3, &input_h3)
        .await
        .expect("batch state step 3");
    assert!(out_h3.version > 0);
    for version in 1..=600 {
        publish_transient_zset_batch(
            &ZSetHandle {
                ns: format!("evict_filter_map_batch_{suffix}_{version}"),
                version,
            },
            Arc::new(vec![(version as i64, 1)]),
        );
    }
    let out_rows_3 = delta_zset_handle_batch::<i64>(table.clone(), &mut HashMap::new(), &out_h3)
        .await
        .expect("batch output rows 3 after transient registry churn");
    assert_eq!(
        coalesce_rows(out_rows_3.as_ref()),
        HashMap::from([(500, 3)])
    );

    let output_err_ns = format!("filter_map_batch_output_err_{suffix}");
    let output_err_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), output_err_ns.clone(), None)
            .await
            .expect("output err dict"),
    );
    let output_err = VersionedZSet::new(output_err_dict, table.clone(), output_err_ns)
        .await
        .expect("output err zset");
    let mut err_state = FilterMapBatchState {
        transform: Arc::new(|rows: &[(i64, i64)]| {
            if rows.iter().any(|(value, _)| *value < 0) {
                anyhow::bail!("negative keys not allowed")
            }
            Ok(Vec::new())
        }),
        table: table.clone(),
        output: output_err,
        dict_cache: HashMap::new(),
        logical_work: metrics::LogicalWorkCollector::default(),
    };

    let input_h_err = stage_version(input_dict, table, input_ns.as_str(), &[(-1, 1)]).await;
    let err = err_state
        .on_step(4, &input_h_err)
        .await
        .expect_err("batch transform should fail");
    assert!(err.to_string().contains("run filter_map batch transform"));
}

async fn run_filter_map_batch_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
    let suffix = next_suffix();
    let db = build_db(suffix).await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));

    let input_ns = format!("filter_map_batch_history_input_{history_rows}_{suffix}");
    let output_ns = format!("filter_map_batch_history_output_{history_rows}_{suffix}");
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
            .await
            .expect("input dict"),
    );
    let output = VersionedZSet::new(
        Arc::new(
            Dictionary::<i64>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .expect("output dict"),
        ),
        table.clone(),
        output_ns,
    )
    .await
    .expect("output zset");
    let mut state = FilterMapBatchState {
        transform: Arc::new(|rows: &[(i64, i64)]| {
            Ok(rows
                .iter()
                .filter_map(|(value, weight)| (value % 2 == 0).then_some((value * 10, *weight)))
                .collect::<Vec<_>>())
        }),
        table: table.clone(),
        output,
        dict_cache: HashMap::new(),
        logical_work: metrics::LogicalWorkCollector::default(),
    };

    let history = (0..history_rows)
        .map(|idx| (1_000_000 + idx * 2, 1))
        .collect::<Vec<_>>();
    let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
    state.on_step(1, &seed).await.expect("seed filter_map");

    let fixed = stage_version(input_dict, table.clone(), &input_ns, &[(8, 1)]).await;
    let output = state.on_step(2, &fixed).await.expect("fixed filter_map");
    let materialized = delta_zset_handle_batch::<i64>(table, &mut HashMap::new(), &output)
        .await
        .expect("materialize fixed filter_map");
    assert_eq!(
        coalesce_rows(materialized.as_ref()),
        HashMap::from([(80, 1)])
    );

    state.last_logical_work()
}

#[tokio::test]
async fn filter_map_batch_logical_work_is_delta_local() {
    let baseline = run_filter_map_batch_history_probe(8).await;
    for history_rows in [128, 1024] {
        let actual = run_filter_map_batch_history_probe(history_rows).await;
        assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
        assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
        assert_eq!(actual.persisted_rows, baseline.persisted_rows);
        assert_eq!(actual.state_full_scan_count, 0);
        assert_eq!(actual.cache_rebuild_rows, 0);
    }

    assert_eq!(baseline.input_delta_rows, 1);
    assert_eq!(baseline.output_delta_rows, 1);
    assert_eq!(baseline.persisted_rows, 1);
}
