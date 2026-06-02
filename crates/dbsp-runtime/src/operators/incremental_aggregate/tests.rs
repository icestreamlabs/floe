use super::*;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::operators::incremental_aggregate::persistence::bucket_for;
use crate::storage::SlateTable;
use crate::storage::dictionary::Dictionary;
use crate::stream::util::materialize_zset_handle;
use object_store::memory::InMemory;
use slatedb::Db;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
struct AggregateRow {
    group_key: i64,
    price: Option<i64>,
    category: String,
}

fn incremental_batch_rows<K, F>(row_evaluator: F) -> BatchRowEvaluator<AggregateRow, K>
where
    K: Send + Sync + 'static,
    F: Fn(&AggregateRow) -> Option<IncrementalAggregateRow<K>> + Send + Sync + 'static,
{
    Arc::new(move |deltas: &[(AggregateRow, i64)]| {
        deltas
            .iter()
            .filter_map(|(row, weight)| {
                row_evaluator(row).map(|update| (row.clone(), update, *weight))
            })
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
            .expect("intern test key for incremental aggregate");
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
async fn incremental_aggregate_tracks_mixed_slots_and_delete_recompute() {
    let table = build_table("incremental-aggregate").await;
    let input_dict = Arc::new(
        Dictionary::<AggregateRow>::with_table(
            table.clone(),
            "incremental_aggregate_input".to_string(),
            None,
        )
        .await
        .expect("create input dictionary"),
    );
    let state = RelationState::<(i64, GroupedIncrementalAggregateState)>::empty(
        table.clone(),
        "incremental_aggregate_state".to_string(),
    )
    .await
    .expect("create incremental aggregate state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<AggregateValue>)>::with_table(
            table.clone(),
            "incremental_aggregate_output".to_string(),
            None,
        )
        .await
        .expect("create incremental aggregate output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "incremental_aggregate_output".to_string(),
    )
    .await
    .expect("create incremental aggregate output");

    let mut op = IncrementalAggregateOp::new_batch(
        state,
        table.clone(),
        incremental_batch_rows(|row: &AggregateRow| {
            Some(IncrementalAggregateRow {
                key: row.group_key,
                slots: vec![
                    IncrementalAggregateSlotUpdate::Count(1),
                    IncrementalAggregateSlotUpdate::Value(row.price.map(AggregateValue::Int64)),
                    IncrementalAggregateSlotUpdate::Value(row.price.map(AggregateValue::Int64)),
                    IncrementalAggregateSlotUpdate::Value(row.price.map(AggregateValue::Int64)),
                    IncrementalAggregateSlotUpdate::Value(Some(AggregateValue::Utf8(
                        row.category.clone(),
                    ))),
                ],
            })
        }),
        output,
        vec![
            IncrementalAggregateSlotKind::Count,
            IncrementalAggregateSlotKind::Sum(AggregateValueType::Int64),
            IncrementalAggregateSlotKind::Avg,
            IncrementalAggregateSlotKind::Min(AggregateValueType::Int64),
            IncrementalAggregateSlotKind::Max(AggregateValueType::Utf8),
        ],
        IncrementalAggregateIndexes::new(
            None,
            Some(IndexedBatchZSet::new(
                table.clone(),
                "incremental_aggregate_input_index".to_string(),
            )),
            Some(IndexedBatchZSet::with_range_index(
                table.clone(),
                "incremental_aggregate_extrema_index".to_string(),
            )),
        ),
    );

    let batch_one = stage_version(
        input_dict.clone(),
        table.clone(),
        "incremental_aggregate_input",
        &[
            (
                AggregateRow {
                    group_key: 1,
                    price: Some(10),
                    category: "b".to_string(),
                },
                1,
            ),
            (
                AggregateRow {
                    group_key: 1,
                    price: Some(30),
                    category: "c".to_string(),
                },
                1,
            ),
        ],
    )
    .await;
    let out_one = op
        .on_step(0, std::slice::from_ref(&batch_one))
        .await
        .expect("run incremental aggregate t1")
        .expect("incremental aggregate t1 output");
    let mut cache = HashMap::new();
    let delta_one =
        materialize_zset_handle::<(i64, Vec<AggregateValue>)>(table.clone(), &mut cache, &out_one)
            .await
            .expect("materialize incremental aggregate t1");
    assert_eq!(
        delta_one,
        HashMap::from([(
            (
                1,
                vec![
                    AggregateValue::Int64(2),
                    AggregateValue::Int64(40),
                    AggregateValue::Int64(20),
                    AggregateValue::Int64(10),
                    AggregateValue::Utf8("c".to_string()),
                ],
            ),
            1,
        )])
    );

    let batch_two = stage_version(
        input_dict.clone(),
        table.clone(),
        "incremental_aggregate_input",
        &[(
            AggregateRow {
                group_key: 1,
                price: Some(30),
                category: "c".to_string(),
            },
            -1,
        )],
    )
    .await;
    let out_two = op
        .on_step(1, std::slice::from_ref(&batch_two))
        .await
        .expect("run incremental aggregate t2")
        .expect("incremental aggregate t2 output");
    let delta_two =
        materialize_zset_handle::<(i64, Vec<AggregateValue>)>(table.clone(), &mut cache, &out_two)
            .await
            .expect("materialize incremental aggregate t2");
    assert_eq!(
        delta_two,
        HashMap::from([
            (
                (
                    1,
                    vec![
                        AggregateValue::Int64(2),
                        AggregateValue::Int64(40),
                        AggregateValue::Int64(20),
                        AggregateValue::Int64(10),
                        AggregateValue::Utf8("c".to_string()),
                    ],
                ),
                -1
            ),
            (
                (
                    1,
                    vec![
                        AggregateValue::Int64(1),
                        AggregateValue::Int64(10),
                        AggregateValue::Int64(10),
                        AggregateValue::Int64(10),
                        AggregateValue::Utf8("b".to_string()),
                    ],
                ),
                1
            ),
        ])
    );
}

#[tokio::test]
async fn incremental_aggregate_extrema_delete_uses_ordered_index() {
    let table = build_table("incremental-extrema-ordered-index").await;
    let input_dict = Arc::new(
        Dictionary::<AggregateRow>::with_table(
            table.clone(),
            "incremental_extrema_ordered_input".to_string(),
            None,
        )
        .await
        .expect("create input dictionary"),
    );
    let state = RelationState::<(i64, GroupedIncrementalAggregateState)>::empty(
        table.clone(),
        "incremental_extrema_ordered_state".to_string(),
    )
    .await
    .expect("create incremental aggregate state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<AggregateValue>)>::with_table(
            table.clone(),
            "incremental_extrema_ordered_output".to_string(),
            None,
        )
        .await
        .expect("create output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "incremental_extrema_ordered_output".to_string(),
    )
    .await
    .expect("create output zset");

    let mut op = IncrementalAggregateOp::new_batch(
        state,
        table.clone(),
        incremental_batch_rows(|row: &AggregateRow| {
            Some(IncrementalAggregateRow {
                key: row.group_key,
                slots: vec![
                    IncrementalAggregateSlotUpdate::Count(1),
                    IncrementalAggregateSlotUpdate::Value(row.price.map(AggregateValue::Int64)),
                ],
            })
        }),
        output,
        vec![
            IncrementalAggregateSlotKind::Count,
            IncrementalAggregateSlotKind::Min(AggregateValueType::Int64),
        ],
        IncrementalAggregateIndexes::new(
            None,
            Some(IndexedBatchZSet::new(
                table.clone(),
                "incremental_extrema_ordered_input_index".to_string(),
            )),
            Some(IndexedBatchZSet::with_range_index(
                table.clone(),
                "incremental_extrema_ordered_extrema_index".to_string(),
            )),
        ),
    );

    let seed = (0..100)
        .map(|price| {
            (
                AggregateRow {
                    group_key: 1,
                    price: Some(price),
                    category: format!("c{price}"),
                },
                1,
            )
        })
        .collect::<Vec<_>>();
    let batch_one = stage_version(
        input_dict.clone(),
        table.clone(),
        "incremental_extrema_ordered_input",
        &seed,
    )
    .await;
    op.on_step(0, std::slice::from_ref(&batch_one))
        .await
        .expect("seed extrema aggregate");

    let batch_two = stage_version(
        input_dict,
        table.clone(),
        "incremental_extrema_ordered_input",
        &[(
            AggregateRow {
                group_key: 1,
                price: Some(0),
                category: "c0".to_string(),
            },
            -1,
        )],
    )
    .await;
    let out_two = op
        .on_step(1, std::slice::from_ref(&batch_two))
        .await
        .expect("delete extrema row")
        .expect("output handle");
    let delta_two = materialize_zset_handle::<(i64, Vec<AggregateValue>)>(
        table.clone(),
        &mut HashMap::new(),
        &out_two,
    )
    .await
    .expect("materialize output");

    assert_eq!(
        delta_two.get(&(1, vec![AggregateValue::Int64(99), AggregateValue::Int64(1)])),
        Some(&1)
    );
    assert_eq!(
        op.last_logical_work().extrema_rebuild_rows,
        1,
        "ordered extrema refresh should examine only the next extrema row, not the whole group",
    );
}

#[tokio::test]
async fn incremental_aggregate_tracks_decimal_sum_natively() {
    let table = build_table("incremental-decimal-sum").await;
    let input_dict = Arc::new(
        Dictionary::<AggregateRow>::with_table(
            table.clone(),
            "incremental_decimal_input".to_string(),
            None,
        )
        .await
        .expect("create input dictionary"),
    );
    let state = RelationState::<(i64, GroupedIncrementalAggregateState)>::empty(
        table.clone(),
        "incremental_decimal_state".to_string(),
    )
    .await
    .expect("create incremental aggregate state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<AggregateValue>)>::with_table(
            table.clone(),
            "incremental_decimal_output".to_string(),
            None,
        )
        .await
        .expect("create incremental aggregate output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "incremental_decimal_output".to_string(),
    )
    .await
    .expect("create incremental aggregate output");

    let mut op = IncrementalAggregateOp::new_batch(
        state,
        table.clone(),
        incremental_batch_rows(|row: &AggregateRow| {
            Some(IncrementalAggregateRow {
                key: row.group_key,
                slots: vec![IncrementalAggregateSlotUpdate::Value(
                    row.price
                        .map(|value| AggregateValue::Decimal128(i128::from(value))),
                )],
            })
        }),
        output,
        vec![IncrementalAggregateSlotKind::Sum(
            AggregateValueType::Decimal128 {
                precision: 18,
                scale: 2,
            },
        )],
        IncrementalAggregateIndexes::new(None, None, None),
    );

    let batch_one = stage_version(
        input_dict.clone(),
        table.clone(),
        "incremental_decimal_input",
        &[
            (
                AggregateRow {
                    group_key: 1,
                    price: Some(1234),
                    category: "a".to_string(),
                },
                1,
            ),
            (
                AggregateRow {
                    group_key: 1,
                    price: Some(566),
                    category: "b".to_string(),
                },
                1,
            ),
        ],
    )
    .await;
    let out_one = op
        .on_step(0, std::slice::from_ref(&batch_one))
        .await
        .expect("run decimal aggregate t1")
        .expect("decimal aggregate t1 output");
    let mut cache = HashMap::new();
    let delta_one =
        materialize_zset_handle::<(i64, Vec<AggregateValue>)>(table.clone(), &mut cache, &out_one)
            .await
            .expect("materialize decimal aggregate t1");
    assert_eq!(
        delta_one,
        HashMap::from([((1, vec![AggregateValue::Decimal128(1800)]), 1)])
    );

    let batch_two = stage_version(
        input_dict,
        table.clone(),
        "incremental_decimal_input",
        &[(
            AggregateRow {
                group_key: 1,
                price: Some(566),
                category: "b".to_string(),
            },
            -1,
        )],
    )
    .await;
    let out_two = op
        .on_step(1, std::slice::from_ref(&batch_two))
        .await
        .expect("run decimal aggregate t2")
        .expect("decimal aggregate t2 output");
    let delta_two =
        materialize_zset_handle::<(i64, Vec<AggregateValue>)>(table, &mut cache, &out_two)
            .await
            .expect("materialize decimal aggregate t2");
    assert_eq!(
        delta_two,
        HashMap::from([
            ((1, vec![AggregateValue::Decimal128(1800)]), -1),
            ((1, vec![AggregateValue::Decimal128(1234)]), 1),
        ])
    );
}

#[tokio::test]
async fn append_only_incremental_count_distinct_persists_membership_once() {
    let table = build_table("append-only-incremental-count-distinct").await;
    let input_dict = Arc::new(
        Dictionary::<AggregateRow>::with_table(
            table.clone(),
            "append_incremental_input".to_string(),
            None,
        )
        .await
        .expect("create input dictionary"),
    );
    let state = RelationState::<(i64, GroupedIncrementalAggregateState)>::empty(
        table.clone(),
        "append_incremental_state".to_string(),
    )
    .await
    .expect("create incremental aggregate state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<AggregateValue>)>::with_table(
            table.clone(),
            "append_incremental_output".to_string(),
            None,
        )
        .await
        .expect("create incremental aggregate output dictionary"),
    );
    let output = VersionedZSet::new(
        output_dict,
        table.clone(),
        "append_incremental_output".to_string(),
    )
    .await
    .expect("create incremental aggregate output");
    let distinct_index = IndexedBatchZSet::new(
        table.clone(),
        "append_incremental_distinct_index".to_string(),
    );

    let mut op = IncrementalAggregateOp::new_batch(
        state,
        table.clone(),
        incremental_batch_rows(|row: &AggregateRow| {
            Some(IncrementalAggregateRow {
                key: row.group_key,
                slots: vec![IncrementalAggregateSlotUpdate::Value(Some(
                    AggregateValue::Utf8(row.category.clone()),
                ))],
            })
        }),
        output,
        vec![IncrementalAggregateSlotKind::CountDistinct],
        IncrementalAggregateIndexes::new(Some(distinct_index), None, None),
    );
    op.enable_append_only_input();

    let first = stage_version(
        input_dict.clone(),
        table.clone(),
        "append_incremental_input",
        &[
            (
                AggregateRow {
                    group_key: 1,
                    price: None,
                    category: "a".to_string(),
                },
                2,
            ),
            (
                AggregateRow {
                    group_key: 1,
                    price: None,
                    category: "b".to_string(),
                },
                1,
            ),
        ],
    )
    .await;
    op.on_step(0, std::slice::from_ref(&first))
        .await
        .expect("run append-only incremental aggregate t1")
        .expect("output t1");

    let distinct_key = DistinctGroupKey {
        group_key: 1,
        slot: 0,
    };
    {
        let distinct_index = op.distinct_index.as_ref().expect("distinct index");
        let mut values = distinct_index
            .values_for_key(&distinct_key)
            .await
            .expect("distinct values after t1");
        values.sort_by(|left, right| format!("{:?}", left.0).cmp(&format!("{:?}", right.0)));
        assert_eq!(
            values,
            vec![
                (AggregateValue::Utf8("a".to_string()), 1),
                (AggregateValue::Utf8("b".to_string()), 1),
            ]
        );
    }

    let duplicate = stage_version(
        input_dict,
        table,
        "append_incremental_input",
        &[(
            AggregateRow {
                group_key: 1,
                price: None,
                category: "a".to_string(),
            },
            3,
        )],
    )
    .await;
    op.on_step(1, std::slice::from_ref(&duplicate))
        .await
        .expect("run append-only incremental aggregate t2");
    let distinct_index = op.distinct_index.as_ref().expect("distinct index");
    let mut values = distinct_index
        .values_for_key(&distinct_key)
        .await
        .expect("distinct values after duplicate");
    values.sort_by(|left, right| format!("{:?}", left.0).cmp(&format!("{:?}", right.0)));
    assert_eq!(
        values,
        vec![
            (AggregateValue::Utf8("a".to_string()), 1),
            (AggregateValue::Utf8("b".to_string()), 1),
        ]
    );
}

async fn run_incremental_count_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
    let table = build_table(&format!("incremental-count-history-{history_rows}")).await;
    let input_ns = format!("incremental_count_history_{history_rows}_input");
    let state_ns = format!("incremental_count_history_{history_rows}_state");
    let output_ns = format!("incremental_count_history_{history_rows}_output");
    let input_dict = Arc::new(
        Dictionary::<AggregateRow>::with_table(table.clone(), input_ns.clone(), None)
            .await
            .expect("create incremental count history input dictionary"),
    );
    let state =
        RelationState::<(i64, GroupedIncrementalAggregateState)>::empty(table.clone(), state_ns)
            .await
            .expect("create incremental count history state");
    let output = VersionedZSet::new(
        Arc::new(
            Dictionary::<(i64, Vec<AggregateValue>)>::with_table(
                table.clone(),
                output_ns.clone(),
                None,
            )
            .await
            .expect("create incremental count history output dictionary"),
        ),
        table.clone(),
        output_ns,
    )
    .await
    .expect("create incremental count history output");

    let mut op = IncrementalAggregateOp::new_batch(
        state,
        table.clone(),
        incremental_batch_rows(|row: &AggregateRow| {
            Some(IncrementalAggregateRow {
                key: row.group_key,
                slots: vec![IncrementalAggregateSlotUpdate::Count(1)],
            })
        }),
        output,
        vec![IncrementalAggregateSlotKind::Count],
        IncrementalAggregateIndexes::new(None, None, None),
    );

    let history = (0..history_rows)
        .map(|idx| {
            (
                AggregateRow {
                    group_key: 1_000_000 + idx,
                    price: Some(idx),
                    category: format!("h{idx}"),
                },
                1,
            )
        })
        .collect::<Vec<_>>();
    let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
    op.on_step(1, std::slice::from_ref(&seed))
        .await
        .expect("seed incremental count history");

    let fixed = AggregateRow {
        group_key: 7,
        price: Some(70),
        category: "fixed".to_string(),
    };
    let fixed_delta = stage_version(input_dict, table.clone(), &input_ns, &[(fixed, 1)]).await;
    let output = op
        .on_step(2, std::slice::from_ref(&fixed_delta))
        .await
        .expect("fixed incremental count history")
        .expect("incremental count output");
    let mut cache = HashMap::new();
    let materialized =
        materialize_zset_handle::<(i64, Vec<AggregateValue>)>(table, &mut cache, &output)
            .await
            .expect("materialize incremental count history output");
    assert_eq!(
        materialized,
        HashMap::from([((7, vec![AggregateValue::Int64(1)]), 1)])
    );

    op.last_logical_work()
}

#[tokio::test]
async fn incremental_count_logical_work_uses_changed_groups() {
    let baseline = run_incremental_count_history_probe(8).await;
    for history_rows in [128, 1024] {
        let actual = run_incremental_count_history_probe(history_rows).await;
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

async fn run_incremental_count_distinct_history_probe(
    history_rows: i64,
) -> metrics::LogicalWorkSnapshot {
    let table = build_table(&format!(
        "incremental-count-distinct-history-{history_rows}"
    ))
    .await;
    let input_ns = format!("incremental_count_distinct_history_{history_rows}_input");
    let state_ns = format!("incremental_count_distinct_history_{history_rows}_state");
    let output_ns = format!("incremental_count_distinct_history_{history_rows}_output");
    let input_dict = Arc::new(
        Dictionary::<AggregateRow>::with_table(table.clone(), input_ns.clone(), None)
            .await
            .expect("create incremental distinct history input dictionary"),
    );
    let state =
        RelationState::<(i64, GroupedIncrementalAggregateState)>::empty(table.clone(), state_ns)
            .await
            .expect("create incremental distinct history state");
    let output = VersionedZSet::new(
        Arc::new(
            Dictionary::<(i64, Vec<AggregateValue>)>::with_table(
                table.clone(),
                output_ns.clone(),
                None,
            )
            .await
            .expect("create incremental distinct history output dictionary"),
        ),
        table.clone(),
        output_ns,
    )
    .await
    .expect("create incremental distinct history output");

    let mut op = IncrementalAggregateOp::new_batch(
        state,
        table.clone(),
        incremental_batch_rows(|row: &AggregateRow| {
            Some(IncrementalAggregateRow {
                key: row.group_key,
                slots: vec![IncrementalAggregateSlotUpdate::Value(Some(
                    AggregateValue::Utf8(row.category.clone()),
                ))],
            })
        }),
        output,
        vec![IncrementalAggregateSlotKind::CountDistinct],
        IncrementalAggregateIndexes::new(
            Some(IndexedBatchZSet::new(
                table.clone(),
                format!("incremental_count_distinct_history_{history_rows}_index"),
            )),
            None,
            None,
        ),
    );

    let history = (0..history_rows)
        .map(|idx| {
            (
                AggregateRow {
                    group_key: 1_000_000 + idx,
                    price: Some(idx),
                    category: format!("h{idx}"),
                },
                1,
            )
        })
        .collect::<Vec<_>>();
    let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
    op.on_step(1, std::slice::from_ref(&seed))
        .await
        .expect("seed incremental distinct history");

    let fixed = AggregateRow {
        group_key: 7,
        price: Some(70),
        category: "fixed".to_string(),
    };
    let fixed_delta = stage_version(input_dict, table.clone(), &input_ns, &[(fixed, 1)]).await;
    let output = op
        .on_step(2, std::slice::from_ref(&fixed_delta))
        .await
        .expect("fixed incremental distinct history")
        .expect("incremental distinct output");
    let mut cache = HashMap::new();
    let materialized =
        materialize_zset_handle::<(i64, Vec<AggregateValue>)>(table, &mut cache, &output)
            .await
            .expect("materialize incremental distinct history output");
    assert_eq!(
        materialized,
        HashMap::from([((7, vec![AggregateValue::Int64(1)]), 1)])
    );

    op.last_logical_work()
}

#[tokio::test]
async fn incremental_count_distinct_logical_work_uses_changed_groups() {
    let baseline = run_incremental_count_distinct_history_probe(8).await;
    for history_rows in [128, 1024] {
        let actual = run_incremental_count_distinct_history_probe(history_rows).await;
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
