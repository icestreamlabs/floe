use super::*;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::storage::SlateTable;
use crate::storage::dictionary::Dictionary;
use crate::stream::util::materialize_zset_handle;
use object_store::memory::InMemory;
use slatedb::Db;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
struct CountRow {
    group_key: i64,
    value: Option<i64>,
    flag: bool,
}

fn count_batch_rows<K, D, F>(row_evaluator: F) -> BatchRowEvaluator<CountRow, K, D>
where
    K: Send + Sync + 'static,
    D: Send + Sync + 'static,
    F: Fn(&CountRow) -> Option<CountAggregateRow<K, D>> + Send + Sync + 'static,
{
    Arc::new(move |deltas: &[(CountRow, i64)]| {
        deltas
            .iter()
            .filter_map(|(row, weight)| row_evaluator(row).map(|update| (update, *weight)))
            .collect()
    })
}

async fn stage_version<T>(
    dict: Arc<Dictionary<T>>,
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
    deltas: &[(T, i64)],
) -> ZSetHandle
where
    T: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        let id = dict_batch
            .intern(key)
            .await
            .expect("intern test key for grouped count");
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

async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
    Arc::new(SlateTable::new(db))
}

#[tokio::test]
async fn grouped_count_tracks_filtered_and_nullable_counts() {
    let table = build_table("grouped-count").await;
    let input_dict = Arc::new(
        Dictionary::<CountRow>::with_table(table.clone(), "grouped_count_input".to_string(), None)
            .await
            .expect("create input dictionary"),
    );
    let state = RelationState::<(i64, GroupedCountState)>::empty(
        table.clone(),
        "grouped_count_state".to_string(),
    )
    .await
    .expect("create grouped-count state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<i64>)>::with_table(
            table.clone(),
            "grouped_count_output".to_string(),
            None,
        )
        .await
        .expect("create grouped-count output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "grouped_count_output".to_string(),
    )
    .await
    .expect("create grouped-count output");

    let mut op = CountAggregateOp::new_batch(
        state,
        table.clone(),
        count_batch_rows(|row: &CountRow| {
            Some(CountAggregateRow {
                key: row.group_key,
                slots: vec![
                    CountAggregateSlotUpdate::Linear(1),
                    CountAggregateSlotUpdate::Linear(i64::from(row.flag)),
                    CountAggregateSlotUpdate::Linear(i64::from(row.value.is_some())),
                ],
            })
        }),
        output,
        vec![
            CountAggregateSlotKind::Linear,
            CountAggregateSlotKind::Linear,
            CountAggregateSlotKind::Linear,
        ],
        None::<IndexedBatchZSet<DistinctGroupKey<i64>, i64>>,
    );

    let batch_one = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_input",
        &[
            (
                CountRow {
                    group_key: 1,
                    value: Some(10),
                    flag: true,
                },
                1,
            ),
            (
                CountRow {
                    group_key: 1,
                    value: None,
                    flag: false,
                },
                1,
            ),
            (
                CountRow {
                    group_key: 2,
                    value: Some(7),
                    flag: false,
                },
                1,
            ),
        ],
    )
    .await;
    let out_one = op
        .on_step(0, std::slice::from_ref(&batch_one))
        .await
        .expect("run grouped-count t1")
        .expect("grouped-count t1 output");
    let mut cache = HashMap::new();
    let delta_one = materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_one)
        .await
        .expect("materialize grouped-count t1");
    assert_eq!(
        delta_one,
        HashMap::from([((1, vec![2, 1, 1]), 1), ((2, vec![1, 0, 1]), 1),])
    );

    let batch_two = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_input",
        &[(
            CountRow {
                group_key: 1,
                value: Some(10),
                flag: true,
            },
            -1,
        )],
    )
    .await;
    let out_two = op
        .on_step(1, std::slice::from_ref(&batch_two))
        .await
        .expect("run grouped-count t2")
        .expect("grouped-count t2 output");
    let delta_two = materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_two)
        .await
        .expect("materialize grouped-count t2");
    assert_eq!(
        delta_two,
        HashMap::from([((1, vec![2, 1, 1]), -1), ((1, vec![1, 0, 0]), 1)])
    );
}

#[tokio::test]
async fn grouped_count_preserves_zero_outputs_while_rows_remain() {
    let table = build_table("grouped-count-zero").await;
    let input_dict = Arc::new(
        Dictionary::<CountRow>::with_table(
            table.clone(),
            "grouped_count_zero_input".to_string(),
            None,
        )
        .await
        .expect("create zero-output input dictionary"),
    );
    let state = RelationState::<(i64, GroupedCountState)>::empty(
        table.clone(),
        "grouped_count_zero_state".to_string(),
    )
    .await
    .expect("create zero-output state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<i64>)>::with_table(
            table.clone(),
            "grouped_count_zero_output".to_string(),
            None,
        )
        .await
        .expect("create zero-output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "grouped_count_zero_output".to_string(),
    )
    .await
    .expect("create zero-output zset");

    let mut op = CountAggregateOp::new_batch(
        state,
        table.clone(),
        count_batch_rows(|row: &CountRow| {
            Some(CountAggregateRow {
                key: row.group_key,
                slots: vec![CountAggregateSlotUpdate::Linear(i64::from(
                    row.value.is_some(),
                ))],
            })
        }),
        output,
        vec![CountAggregateSlotKind::Linear],
        None::<IndexedBatchZSet<DistinctGroupKey<i64>, i64>>,
    );

    let first = CountRow {
        group_key: 1,
        value: None,
        flag: false,
    };
    let second = CountRow {
        group_key: 1,
        value: None,
        flag: true,
    };

    let batch_one = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_zero_input",
        &[(first.clone(), 1)],
    )
    .await;
    let out_one = op
        .on_step(0, std::slice::from_ref(&batch_one))
        .await
        .expect("run zero-output t1")
        .expect("zero-output t1 handle");
    let mut cache = HashMap::new();
    let delta_one = materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_one)
        .await
        .expect("materialize zero-output t1");
    assert_eq!(delta_one, HashMap::from([((1, vec![0]), 1)]));

    let batch_two = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_zero_input",
        &[(second.clone(), 1)],
    )
    .await;
    let out_two = op
        .on_step(1, std::slice::from_ref(&batch_two))
        .await
        .expect("run zero-output t2")
        .expect("zero-output t2 handle");
    let delta_two = materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_two)
        .await
        .expect("materialize zero-output t2");
    assert!(delta_two.is_empty());

    let batch_three = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_zero_input",
        &[(first, -1)],
    )
    .await;
    let out_three = op
        .on_step(2, std::slice::from_ref(&batch_three))
        .await
        .expect("run zero-output t3")
        .expect("zero-output t3 handle");
    let delta_three =
        materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_three)
            .await
            .expect("materialize zero-output t3");
    assert!(delta_three.is_empty());

    let batch_four = stage_version(
        input_dict,
        table.clone(),
        "grouped_count_zero_input",
        &[(second, -1)],
    )
    .await;
    let out_four = op
        .on_step(3, std::slice::from_ref(&batch_four))
        .await
        .expect("run zero-output t4")
        .expect("zero-output t4 handle");
    let delta_four =
        materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_four)
            .await
            .expect("materialize zero-output t4");
    assert_eq!(delta_four, HashMap::from([((1, vec![0]), -1)]));
}

#[tokio::test]
async fn grouped_count_tracks_distinct_membership_by_group_and_value() {
    let table = build_table("grouped-count-distinct").await;
    let input_dict = Arc::new(
        Dictionary::<CountRow>::with_table(
            table.clone(),
            "grouped_count_distinct_input".to_string(),
            None,
        )
        .await
        .expect("create distinct input dictionary"),
    );
    let state = RelationState::<(i64, GroupedCountState)>::empty(
        table.clone(),
        "grouped_count_distinct_state".to_string(),
    )
    .await
    .expect("create distinct state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<i64>)>::with_table(
            table.clone(),
            "grouped_count_distinct_output".to_string(),
            None,
        )
        .await
        .expect("create distinct output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "grouped_count_distinct_output".to_string(),
    )
    .await
    .expect("create distinct output zset");
    let distinct_index = IndexedBatchZSet::new(table.clone(), "grouped_count_distinct_index");

    let mut op = CountAggregateOp::new_batch(
        state,
        table.clone(),
        count_batch_rows(|row: &CountRow| {
            Some(CountAggregateRow {
                key: row.group_key,
                slots: vec![CountAggregateSlotUpdate::Distinct(row.value)],
            })
        }),
        output,
        vec![CountAggregateSlotKind::Distinct],
        Some(distinct_index),
    );

    let first = CountRow {
        group_key: 1,
        value: Some(10),
        flag: false,
    };

    let batch_one = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_distinct_input",
        &[(first.clone(), 1)],
    )
    .await;
    let out_one = op
        .on_step(0, std::slice::from_ref(&batch_one))
        .await
        .expect("run distinct t1")
        .expect("distinct t1 handle");
    let mut cache = HashMap::new();
    let delta_one = materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_one)
        .await
        .expect("materialize distinct t1");
    assert_eq!(delta_one, HashMap::from([((1, vec![1]), 1)]));

    let batch_two = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_distinct_input",
        &[(first.clone(), 1)],
    )
    .await;
    let out_two = op
        .on_step(1, std::slice::from_ref(&batch_two))
        .await
        .expect("run distinct t2")
        .expect("distinct t2 handle");
    let delta_two = materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_two)
        .await
        .expect("materialize distinct t2");
    assert!(delta_two.is_empty());

    let batch_three = stage_version(
        input_dict.clone(),
        table.clone(),
        "grouped_count_distinct_input",
        &[(first.clone(), -1)],
    )
    .await;
    let out_three = op
        .on_step(2, std::slice::from_ref(&batch_three))
        .await
        .expect("run distinct t3")
        .expect("distinct t3 handle");
    let delta_three =
        materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_three)
            .await
            .expect("materialize distinct t3");
    assert!(delta_three.is_empty());

    let batch_four = stage_version(
        input_dict,
        table.clone(),
        "grouped_count_distinct_input",
        &[(first, -1)],
    )
    .await;
    let out_four = op
        .on_step(3, std::slice::from_ref(&batch_four))
        .await
        .expect("run distinct t4")
        .expect("distinct t4 handle");
    let delta_four =
        materialize_zset_handle::<(i64, Vec<i64>)>(table.clone(), &mut cache, &out_four)
            .await
            .expect("materialize distinct t4");
    assert_eq!(delta_four, HashMap::from([((1, vec![1]), -1)]));
}

#[tokio::test]
async fn append_only_grouped_count_distinct_persists_membership_once() {
    let table = build_table("append-only-grouped-count-distinct").await;
    let input_dict = Arc::new(
        Dictionary::<CountRow>::with_table(
            table.clone(),
            "append_grouped_count_distinct_input".to_string(),
            None,
        )
        .await
        .expect("create distinct input dictionary"),
    );
    let state = RelationState::<(i64, GroupedCountState)>::empty(
        table.clone(),
        "append_grouped_count_distinct_state".to_string(),
    )
    .await
    .expect("create distinct state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<i64>)>::with_table(
            table.clone(),
            "append_grouped_count_distinct_output".to_string(),
            None,
        )
        .await
        .expect("create distinct output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "append_grouped_count_distinct_output".to_string(),
    )
    .await
    .expect("create distinct output zset");
    let distinct_index =
        IndexedBatchZSet::new(table.clone(), "append_grouped_count_distinct_index");

    let mut op = CountAggregateOp::new_batch(
        state,
        table.clone(),
        count_batch_rows(|row: &CountRow| {
            Some(CountAggregateRow {
                key: row.group_key,
                slots: vec![CountAggregateSlotUpdate::Distinct(row.value)],
            })
        }),
        output,
        vec![CountAggregateSlotKind::Distinct],
        Some(distinct_index),
    );
    op.enable_append_only_input();

    let row = CountRow {
        group_key: 1,
        value: Some(10),
        flag: false,
    };
    let batch_one = stage_version(
        input_dict.clone(),
        table.clone(),
        "append_grouped_count_distinct_input",
        &[(row.clone(), 3)],
    )
    .await;
    op.on_step(0, std::slice::from_ref(&batch_one))
        .await
        .expect("run append-only distinct t1")
        .expect("append-only distinct t1 handle");

    let distinct_key = DistinctGroupKey {
        group_key: 1,
        slot: 0,
    };
    assert_eq!(
        op.distinct_index
            .as_ref()
            .expect("distinct index")
            .values_for_key(&distinct_key)
            .await
            .expect("distinct index after t1"),
        vec![(10, 1)]
    );

    let batch_two = stage_version(
        input_dict,
        table,
        "append_grouped_count_distinct_input",
        &[(row, 2)],
    )
    .await;
    op.on_step(1, std::slice::from_ref(&batch_two))
        .await
        .expect("run append-only distinct duplicate");
    assert_eq!(
        op.distinct_index
            .as_ref()
            .expect("distinct index")
            .values_for_key(&distinct_key)
            .await
            .expect("distinct index after duplicate"),
        vec![(10, 1)]
    );
}

async fn run_grouped_count_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
    let table = build_table(&format!("grouped-count-history-{history_rows}")).await;
    let input_ns = format!("grouped_count_history_{history_rows}_input");
    let state_ns = format!("grouped_count_history_{history_rows}_state");
    let output_ns = format!("grouped_count_history_{history_rows}_output");
    let input_dict = Arc::new(
        Dictionary::<CountRow>::with_table(table.clone(), input_ns.clone(), None)
            .await
            .expect("create count history input dictionary"),
    );
    let state = RelationState::<(i64, GroupedCountState)>::empty(table.clone(), state_ns)
        .await
        .expect("create count history state");
    let output = VersionedZSet::new(
        Arc::new(
            Dictionary::<(i64, Vec<i64>)>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .expect("create count history output dictionary"),
        ),
        table.clone(),
        output_ns,
    )
    .await
    .expect("create count history output");

    let mut op = CountAggregateOp::new_batch(
        state,
        table.clone(),
        count_batch_rows(|row: &CountRow| {
            Some(CountAggregateRow {
                key: row.group_key,
                slots: vec![CountAggregateSlotUpdate::Linear(1)],
            })
        }),
        output,
        vec![CountAggregateSlotKind::Linear],
        None::<IndexedBatchZSet<DistinctGroupKey<i64>, i64>>,
    );

    let history = (0..history_rows)
        .map(|idx| {
            (
                CountRow {
                    group_key: 1_000_000 + idx,
                    value: Some(idx),
                    flag: false,
                },
                1,
            )
        })
        .collect::<Vec<_>>();
    let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
    op.on_step(1, std::slice::from_ref(&seed))
        .await
        .expect("seed grouped-count history");

    let fixed = CountRow {
        group_key: 7,
        value: Some(70),
        flag: true,
    };
    let fixed_delta = stage_version(input_dict, table.clone(), &input_ns, &[(fixed, 1)]).await;
    let output = op
        .on_step(2, std::slice::from_ref(&fixed_delta))
        .await
        .expect("fixed grouped-count history")
        .expect("grouped-count output");
    let mut cache = HashMap::new();
    let materialized = materialize_zset_handle::<(i64, Vec<i64>)>(table, &mut cache, &output)
        .await
        .expect("materialize grouped-count history output");
    assert_eq!(materialized, HashMap::from([((7, vec![1]), 1)]));

    op.last_logical_work()
}

#[tokio::test]
async fn grouped_count_logical_work_uses_changed_groups_not_unrelated_history() {
    let baseline = run_grouped_count_history_probe(8).await;
    for history_rows in [128, 1024] {
        let actual = run_grouped_count_history_probe(history_rows).await;
        assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
        assert_eq!(actual.changed_groups, baseline.changed_groups);
        assert_eq!(
            actual.group_state_rows_examined,
            baseline.group_state_rows_examined
        );
        assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
        assert_eq!(actual.state_full_scan_count, 0);
        assert_eq!(actual.cache_rebuild_rows, 0);
    }

    assert_eq!(baseline.input_delta_rows, 1);
    assert_eq!(baseline.changed_groups, 1);
    assert_eq!(baseline.group_state_rows_examined, 1);
    assert_eq!(baseline.output_delta_rows, 1);
}

async fn run_grouped_count_distinct_history_probe(
    history_rows: i64,
) -> metrics::LogicalWorkSnapshot {
    let table = build_table(&format!("grouped-count-distinct-history-{history_rows}")).await;
    let input_ns = format!("grouped_count_distinct_history_{history_rows}_input");
    let state_ns = format!("grouped_count_distinct_history_{history_rows}_state");
    let output_ns = format!("grouped_count_distinct_history_{history_rows}_output");
    let input_dict = Arc::new(
        Dictionary::<CountRow>::with_table(table.clone(), input_ns.clone(), None)
            .await
            .expect("create distinct history input dictionary"),
    );
    let state = RelationState::<(i64, GroupedCountState)>::empty(table.clone(), state_ns)
        .await
        .expect("create distinct history state");
    let output = VersionedZSet::new(
        Arc::new(
            Dictionary::<(i64, Vec<i64>)>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .expect("create distinct history output dictionary"),
        ),
        table.clone(),
        output_ns,
    )
    .await
    .expect("create distinct history output");

    let mut op = CountAggregateOp::new_batch(
        state,
        table.clone(),
        count_batch_rows(|row: &CountRow| {
            Some(CountAggregateRow {
                key: row.group_key,
                slots: vec![CountAggregateSlotUpdate::Distinct(row.value)],
            })
        }),
        output,
        vec![CountAggregateSlotKind::Distinct],
        Some(IndexedBatchZSet::new(
            table.clone(),
            format!("grouped_count_distinct_history_{history_rows}_index"),
        )),
    );

    let history = (0..history_rows)
        .map(|idx| {
            (
                CountRow {
                    group_key: 1_000_000 + idx,
                    value: Some(idx),
                    flag: false,
                },
                1,
            )
        })
        .collect::<Vec<_>>();
    let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
    op.on_step(1, std::slice::from_ref(&seed))
        .await
        .expect("seed grouped-count distinct history");

    let fixed = CountRow {
        group_key: 7,
        value: Some(70),
        flag: true,
    };
    let fixed_delta = stage_version(input_dict, table.clone(), &input_ns, &[(fixed, 1)]).await;
    let output = op
        .on_step(2, std::slice::from_ref(&fixed_delta))
        .await
        .expect("fixed grouped-count distinct history")
        .expect("grouped-count distinct output");
    let mut cache = HashMap::new();
    let materialized = materialize_zset_handle::<(i64, Vec<i64>)>(table, &mut cache, &output)
        .await
        .expect("materialize grouped-count distinct history output");
    assert_eq!(materialized, HashMap::from([((7, vec![1]), 1)]));

    op.last_logical_work()
}

#[tokio::test]
async fn grouped_count_distinct_logical_work_uses_changed_groups() {
    let baseline = run_grouped_count_distinct_history_probe(8).await;
    for history_rows in [128, 1024] {
        let actual = run_grouped_count_distinct_history_probe(history_rows).await;
        assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
        assert_eq!(actual.changed_groups, baseline.changed_groups);
        assert_eq!(
            actual.distinct_aux_rows_examined,
            baseline.distinct_aux_rows_examined
        );
        assert_eq!(
            actual.group_state_rows_examined,
            baseline.group_state_rows_examined
        );
        assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
        assert_eq!(actual.state_full_scan_count, 0);
        assert_eq!(actual.cache_rebuild_rows, 0);
    }

    assert_eq!(baseline.input_delta_rows, 1);
    assert_eq!(baseline.changed_groups, 1);
    assert_eq!(baseline.distinct_aux_rows_examined, 1);
    assert_eq!(baseline.group_state_rows_examined, 1);
    assert_eq!(baseline.output_delta_rows, 1);
}
