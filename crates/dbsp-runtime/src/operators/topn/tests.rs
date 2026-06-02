use super::*;
use crate::collections::zset::SegmentRecord;
use crate::stream::util::materialize_zset_handle;
use object_store::memory::InMemory;
use slatedb::Db;
use std::collections::BTreeMap;

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

type ScalarTopNKeyFn<T> = Arc<dyn Fn(&i64) -> Option<T> + Send + Sync>;

fn scalar_topn_key_parts<P, O>(
    partition_key: ScalarTopNKeyFn<P>,
    order_key: ScalarTopNKeyFn<O>,
) -> BatchKeyPartsFn<i64, P, O>
where
    P: Ord + Clone + Send + Sync + 'static,
    O: Ord + Clone + Send + Sync + 'static,
{
    Arc::new(move |deltas: &[(i64, i64)]| {
        deltas
            .iter()
            .map(|(key, weight)| (*key, *weight, partition_key(key), order_key(key)))
            .collect()
    })
}

async fn stage_version(
    dict: Arc<Dictionary<i64>>,
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
    deltas: &[(i64, i64)],
) -> ZSetHandle {
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        let id = dict_batch
            .intern(key)
            .await
            .expect("intern test key for topn");
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
        .expect("build versioned");
    let version = versioned
        .create_version_with_base(segments, None)
        .await
        .expect("create version");
    versioned.handle_for_version(version)
}

async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("topn", store).await.expect("open SlateDB"))
}

fn apply_deltas(state: &mut HashMap<i64, i64>, deltas: &[(i64, i64)]) {
    for (key, delta) in deltas {
        let entry = state.entry(*key).or_insert(0);
        *entry += *delta;
        if *entry == 0 {
            state.remove(key);
        }
    }
}

fn recompute_topn(state: &HashMap<i64, i64>, limit: usize, offset: usize) -> HashMap<i64, i64> {
    let mut entries: Vec<(i64, i64)> = state
        .iter()
        .filter_map(|(key, weight)| (*weight > 0).then_some((*key, *weight)))
        .collect();
    entries.sort_by_key(|(key, _)| *key);

    let mut remaining_skip = offset;
    let mut remaining_take = limit;
    let mut output = HashMap::new();
    for (key, weight) in entries {
        if remaining_take == 0 {
            break;
        }
        let mut remaining_weight = weight;
        if remaining_skip > 0 {
            let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
            let skip = remaining_skip.min(available);
            remaining_skip -= skip;
            remaining_weight -= skip as i64;
        }
        if remaining_weight <= 0 {
            continue;
        }
        let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
        let take = remaining_take.min(available);
        output.insert(key, take as i64);
        remaining_take -= take;
    }
    output
}

#[tokio::test]
async fn topn_operator_emits_ordered_limit_deltas() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_output", None)
            .await
            .expect("output dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_state", None)
            .await
            .expect("state dict"),
    );

    let state = RelationState {
        integrated: VersionedZSet::new(integrated_dict, table.clone(), "topn_state".to_string())
            .await
            .expect("integrated state"),
        latest_handle: ZSetHandle {
            ns: "topn_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "topn_output".to_string(),
    )
    .await
    .expect("output");

    let partition_key: ScalarTopNKeyFn<()> = Arc::new(|_| Some(()));
    let order_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value));
    let mut op = TopNOp::new_with_batch_key_extractor(
        state,
        table.clone(),
        output,
        scalar_topn_key_parts(partition_key, order_key),
        2,
        0,
    );

    let first_delta = stage_version(
        input_dict.clone(),
        table.clone(),
        "topn_input",
        &[(3, 1), (1, 1), (2, 1)],
    )
    .await;
    let out1 = op
        .on_step(1, &[first_delta])
        .await
        .expect("topn t1")
        .expect("non-empty t1");

    let mut cache = HashMap::new();
    cache.insert("topn_output".to_string(), output_dict.clone());
    let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
        .await
        .expect("materialize output t1");
    assert_eq!(out1_materialized, HashMap::from([(1, 1), (2, 1)]));

    let second_delta = stage_version(
        input_dict.clone(),
        table.clone(),
        "topn_input",
        &[(2, -1), (4, 1)],
    )
    .await;
    let out2 = op
        .on_step(2, &[second_delta])
        .await
        .expect("topn t2")
        .expect("non-empty t2");
    let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
        .await
        .expect("materialize output t2");
    assert_eq!(out2_materialized, HashMap::from([(2, -1), (3, 1)]));
}

#[tokio::test]
async fn topn_operator_matches_full_recompute() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_recompute_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_recompute_output", None)
            .await
            .expect("output dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_recompute_state", None)
            .await
            .expect("state dict"),
    );

    let state = RelationState {
        integrated: VersionedZSet::new(
            integrated_dict,
            table.clone(),
            "topn_recompute_state".to_string(),
        )
        .await
        .expect("integrated state"),
        latest_handle: ZSetHandle {
            ns: "topn_recompute_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "topn_recompute_output".to_string(),
    )
    .await
    .expect("output");

    let partition_key: ScalarTopNKeyFn<()> = Arc::new(|_| Some(()));
    let order_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value));
    let mut op = TopNOp::new_with_batch_key_extractor(
        state,
        table.clone(),
        output,
        scalar_topn_key_parts(partition_key, order_key),
        2,
        1,
    );

    let steps = vec![vec![(5, 1), (2, 1), (1, 1)], vec![(1, -1), (3, 2)]];

    let mut full_input: HashMap<i64, i64> = HashMap::new();
    let mut full_output: HashMap<i64, i64> = HashMap::new();

    for (idx, deltas) in steps.into_iter().enumerate() {
        let delta_handle = stage_version(
            input_dict.clone(),
            table.clone(),
            "topn_recompute_input",
            &deltas,
        )
        .await;
        let output_handle = op
            .on_step(idx as i64 + 1, &[delta_handle])
            .await
            .expect("run topn step");

        apply_deltas(&mut full_input, &deltas);
        let recompute = recompute_topn(&full_input, 2, 1);
        let expected_delta_vec = compute_delta(&full_output, &recompute);
        let expected_delta: HashMap<i64, i64> = expected_delta_vec.into_iter().collect();

        if let Some(handle) = output_handle {
            let mut cache = HashMap::new();
            cache.insert("topn_recompute_output".to_string(), output_dict.clone());
            let actual_delta = materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle)
                .await
                .expect("materialize topn output");
            assert_eq!(actual_delta, expected_delta);
        } else {
            assert!(expected_delta.is_empty());
        }

        let integrated_after = op
            .state
            .integrated
            .materialize()
            .await
            .expect("materialize integrated");
        assert_eq!(integrated_after, full_input);

        full_output = recompute;
    }
}

#[tokio::test]
async fn topn_operator_applies_limit_per_partition() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_partition_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_partition_output", None)
            .await
            .expect("output dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_partition_state", None)
            .await
            .expect("state dict"),
    );

    let state = RelationState {
        integrated: VersionedZSet::new(
            integrated_dict,
            table.clone(),
            "topn_partition_state".to_string(),
        )
        .await
        .expect("integrated state"),
        latest_handle: ZSetHandle {
            ns: "topn_partition_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "topn_partition_output".to_string(),
    )
    .await
    .expect("output");

    // Key encoding: partition = key / 100, order = key % 100.
    let partition_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value / 100));
    let order_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value % 100));
    let mut op = TopNOp::new_with_batch_key_extractor(
        state,
        table.clone(),
        output,
        scalar_topn_key_parts(partition_key, order_key),
        1,
        0,
    );

    let delta = stage_version(
        input_dict.clone(),
        table.clone(),
        "topn_partition_input",
        &[(101, 1), (102, 1), (201, 1), (203, 1)],
    )
    .await;
    let out = op
        .on_step(1, &[delta])
        .await
        .expect("topn partition step")
        .expect("non-empty delta");

    let mut cache = HashMap::new();
    cache.insert("topn_partition_output".to_string(), output_dict.clone());
    let materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
        .await
        .expect("materialize output");
    assert_eq!(materialized, HashMap::from([(101, 1), (201, 1)]));
}

#[tokio::test]
async fn topn_operator_updates_only_affected_partition_output() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_partition_local_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_partition_local_output", None)
            .await
            .expect("output dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_partition_local_state", None)
            .await
            .expect("state dict"),
    );

    let state = RelationState {
        integrated: VersionedZSet::new(
            integrated_dict,
            table.clone(),
            "topn_partition_local_state".to_string(),
        )
        .await
        .expect("integrated state"),
        latest_handle: ZSetHandle {
            ns: "topn_partition_local_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "topn_partition_local_output".to_string(),
    )
    .await
    .expect("output");

    let partition_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value / 100));
    let order_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value % 100));
    let mut op = TopNOp::new_with_batch_key_extractor(
        state,
        table.clone(),
        output,
        scalar_topn_key_parts(partition_key, order_key),
        1,
        0,
    );

    let initial = stage_version(
        input_dict.clone(),
        table.clone(),
        "topn_partition_local_input",
        &[(101, 1), (102, 1), (201, 1), (202, 1)],
    )
    .await;
    op.on_step(1, &[initial])
        .await
        .expect("initial step")
        .expect("initial output");

    let update = stage_version(
        input_dict,
        table.clone(),
        "topn_partition_local_input",
        &[(100, 1)],
    )
    .await;
    let out = op
        .on_step(2, &[update])
        .await
        .expect("partition-local update")
        .expect("non-empty delta");

    let mut cache = HashMap::new();
    cache.insert(
        "topn_partition_local_output".to_string(),
        output_dict.clone(),
    );
    let materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
        .await
        .expect("materialize output");
    assert_eq!(materialized, HashMap::from([(101, -1), (100, 1)]));
}

#[tokio::test]
async fn topn_operator_uses_stable_tie_breaking_and_retractions() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_tie_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_tie_output", None)
            .await
            .expect("output dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "topn_tie_state", None)
            .await
            .expect("state dict"),
    );

    let state = RelationState {
        integrated: VersionedZSet::new(
            integrated_dict,
            table.clone(),
            "topn_tie_state".to_string(),
        )
        .await
        .expect("integrated state"),
        latest_handle: ZSetHandle {
            ns: "topn_tie_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "topn_tie_output".to_string(),
    )
    .await
    .expect("output");

    let partition_key: ScalarTopNKeyFn<()> = Arc::new(|_| Some(()));
    // All inserted rows tie on this key (value % 10 == 1).
    let order_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value % 10));
    let mut op = TopNOp::new_with_batch_key_extractor(
        state,
        table.clone(),
        output,
        scalar_topn_key_parts(partition_key, order_key),
        2,
        0,
    );

    let first_delta = stage_version(
        input_dict.clone(),
        table.clone(),
        "topn_tie_input",
        &[(11, 1), (21, 1), (31, 1)],
    )
    .await;
    let out1 = op
        .on_step(1, &[first_delta])
        .await
        .expect("topn tie t1")
        .expect("non-empty t1");

    let mut cache = HashMap::new();
    cache.insert("topn_tie_output".to_string(), output_dict.clone());
    let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
        .await
        .expect("materialize output t1");
    assert_eq!(out1_materialized, HashMap::from([(11, 1), (21, 1)]));

    let second_delta = stage_version(
        input_dict.clone(),
        table.clone(),
        "topn_tie_input",
        &[(11, -1), (41, 1)],
    )
    .await;
    let out2 = op
        .on_step(2, &[second_delta])
        .await
        .expect("topn tie t2")
        .expect("non-empty t2");
    let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
        .await
        .expect("materialize output t2");
    assert_eq!(out2_materialized, HashMap::from([(11, -1), (31, 1)]));
}

async fn run_topn_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let input_ns = format!("topn_history_{history_rows}_input");
    let output_ns = format!("topn_history_{history_rows}_output");
    let state_ns = format!("topn_history_{history_rows}_state");
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
    let state_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), state_ns.clone(), None)
            .await
            .expect("state dict"),
    );

    let state = RelationState {
        integrated: VersionedZSet::new(state_dict, table.clone(), state_ns.clone())
            .await
            .expect("integrated state"),
        latest_handle: ZSetHandle {
            ns: state_ns,
            version: 0,
        },
    };
    let output = VersionedZSet::new(output_dict.clone(), table.clone(), output_ns.clone())
        .await
        .expect("output");
    let partition_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value / 100));
    let order_key: ScalarTopNKeyFn<i64> = Arc::new(|value| Some(*value % 100));
    let mut op = TopNOp::new_with_batch_key_extractor(
        state,
        table.clone(),
        output,
        scalar_topn_key_parts(partition_key, order_key),
        1,
        0,
    );

    let mut history = (0..history_rows)
        .map(|idx| ((10_000 + idx) * 100, 1))
        .collect::<Vec<_>>();
    history.push((102, 1));
    let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
    op.on_step(1, &[seed]).await.expect("seed topn history");

    let fixed = stage_version(input_dict, table.clone(), &input_ns, &[(101, 1)]).await;
    let output = op
        .on_step(2, &[fixed])
        .await
        .expect("fixed topn history")
        .expect("topn output");
    let mut cache = HashMap::new();
    cache.insert(output_ns, output_dict);
    let materialized = materialize_zset_handle::<i64>(table, &mut cache, &output)
        .await
        .expect("materialize fixed topn");
    assert_eq!(materialized, HashMap::from([(102, -1), (101, 1)]));

    op.last_logical_work()
}

#[tokio::test]
async fn topn_logical_work_uses_changed_partitions() {
    let baseline = run_topn_history_probe(8).await;
    for history_rows in [128, 1024] {
        let actual = run_topn_history_probe(history_rows).await;
        assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
        assert_eq!(actual.changed_partitions, baseline.changed_partitions);
        assert_eq!(
            actual.partition_rows_examined,
            baseline.partition_rows_examined
        );
        assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
        assert_eq!(actual.state_full_scan_count, 0);
        assert_eq!(actual.cache_rebuild_rows, 0);
    }

    assert_eq!(baseline.input_delta_rows, 1);
    assert_eq!(baseline.changed_partitions, 1);
    assert_eq!(baseline.partition_rows_examined, 2);
    assert_eq!(baseline.output_delta_rows, 2);
    assert_eq!(baseline.replacement_rows, 2);
}
