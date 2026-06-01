use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use object_store::memory::InMemory;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::Db;

use super::{JoinInputRetention, JoinOp, JoinTransientInputs};
use crate::collections::IndexedBatchZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{compute_delta, materialize_zset_handle};

async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("joinop", store).await.expect("open SlateDB"))
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
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
        let id = dict_batch.intern(key).await.expect("intern key for join");
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

fn project_sum(l: &i64, r: &i64) -> i64 {
    l + r
}

fn empty_handle(namespace: &str) -> ZSetHandle {
    ZSetHandle {
        ns: namespace.to_string(),
        version: 0,
    }
}

type RowKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;
type BatchJoinKeyExtractor<T, K> = Arc<dyn Fn(&[(T, i64)]) -> Vec<(K, T, i64)> + Send + Sync>;

fn batch_join_key<T, K>(key_extractor: RowKeyExtractor<T, K>) -> BatchJoinKeyExtractor<T, K>
where
    T: Clone + 'static,
    K: 'static,
{
    Arc::new(move |deltas: &[(T, i64)]| {
        deltas
            .iter()
            .filter_map(|(row, weight)| key_extractor(row).map(|key| (key, row.clone(), *weight)))
            .collect()
    })
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

fn recompute_join(left: &HashMap<i64, i64>, right: &HashMap<i64, i64>) -> HashMap<i64, i64> {
    let mut out = HashMap::new();
    for (lk, lw) in left {
        for (rk, rw) in right {
            if lk == rk {
                *out.entry(lk + rk).or_insert(0) += lw * rw;
            }
        }
    }
    out.retain(|_, weight| *weight != 0);
    out
}

fn batch_to_map(batch: &Arc<Vec<(i64, i64)>>) -> HashMap<i64, i64> {
    let mut out = HashMap::new();
    for (key, weight) in batch.iter() {
        let next = out.get(key).copied().unwrap_or(0) + *weight;
        if next == 0 {
            out.remove(key);
        } else {
            out.insert(*key, next);
        }
    }
    out
}

#[tokio::test]
async fn join_operator_matches_batch_join_over_time() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_right_stream", None)
            .await
            .expect("right dict"),
    );
    let left_state_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_left_state", None)
            .await
            .expect("left state dict"),
    );
    let right_state_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_right_state", None)
            .await
            .expect("right state dict"),
    );
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_output", None)
            .await
            .expect("out dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_integrated", None)
            .await
            .expect("join integrated dict"),
    );

    let left_state = RelationState {
        integrated: VersionedZSet::new(
            left_state_dict.clone(),
            table.clone(),
            "join_left_state".to_string(),
        )
        .await
        .expect("left integrated"),
        latest_handle: ZSetHandle {
            ns: "join_left_state".to_string(),
            version: 0,
        },
    };
    let right_state = RelationState {
        integrated: VersionedZSet::new(
            right_state_dict.clone(),
            table.clone(),
            "join_right_state".to_string(),
        )
        .await
        .expect("right integrated"),
        latest_handle: ZSetHandle {
            ns: "join_right_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(out_dict.clone(), table.clone(), "join_output".to_string())
        .await
        .expect("output");
    let match_sum = Arc::new(|l: &i64, r: &i64| *l == *r);
    let projector = Arc::new(project_sum);
    let left_index = IndexedBatchZSet::new(table.clone(), "join_left_index");
    let right_index = IndexedBatchZSet::new(table.clone(), "join_right_index");
    let left_key = Arc::new(|value: &i64| Some(*value));
    let right_key = Arc::new(|value: &i64| Some(*value));
    let integrated_join = RelationState {
        integrated: VersionedZSet::new(
            integrated_dict.clone(),
            table.clone(),
            "join_integrated".to_string(),
        )
        .await
        .expect("join integrated"),
        latest_handle: ZSetHandle {
            ns: "join_integrated".to_string(),
            version: 0,
        },
    };

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        left_index,
        right_index,
        batch_join_key(left_key),
        batch_join_key(right_key),
        match_sum,
        projector,
        table.clone(),
        output,
        Some(integrated_join),
    );

    let mut full_left: HashMap<i64, i64> = HashMap::new();
    let mut full_right: HashMap<i64, i64> = HashMap::new();

    // t1
    let left_delta1 = stage_version(
        left_dict.clone(),
        table.clone(),
        "join_left_stream",
        &[(1, 1)],
    )
    .await;
    let right_delta1 = stage_version(
        right_dict.clone(),
        table.clone(),
        "join_right_stream",
        &[(1, 2)],
    )
    .await;
    full_left.insert(1, 1);
    full_right.insert(1, 2);
    let out1 = op
        .on_step(1, &[left_delta1, right_delta1])
        .await
        .expect("run join t1")
        .expect("non-empty t1");

    let mut cache = HashMap::new();
    cache.insert("join_output".to_string(), out_dict.clone());
    let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
        .await
        .expect("materialize t1 output");
    assert_eq!(out1_materialized, HashMap::from([(2, 2)]));
    let integrated_t1 = op
        .integrated
        .as_ref()
        .unwrap()
        .integrated
        .materialize()
        .await
        .expect("integrated t1");
    assert_eq!(integrated_t1.get(&2), Some(&2));

    // t2: add additional matches/mismatches
    let left_delta2 = stage_version(
        left_dict.clone(),
        table.clone(),
        "join_left_stream",
        &[(2, 1)],
    )
    .await;
    let right_delta2 = stage_version(
        right_dict.clone(),
        table.clone(),
        "join_right_stream",
        &[(2, 3)],
    )
    .await;
    full_left.insert(2, 1);
    full_right.insert(2, 3);
    let out2 = op
        .on_step(2, &[left_delta2, right_delta2])
        .await
        .expect("run join t2")
        .expect("non-empty t2");
    let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
        .await
        .expect("materialize t2 output");

    // Expected joins: (1,1) persists, (2,2) => 4, (1,2) none
    assert_eq!(out2_materialized, HashMap::from([(4, 3)]));

    let mut expected_full_join: HashMap<i64, i64> = HashMap::new();
    for (lk, lw) in &full_left {
        for (rk, rw) in &full_right {
            if lk == rk {
                *expected_full_join.entry(lk + rk).or_insert(0) += lw * rw;
            }
        }
    }
    expected_full_join.retain(|_, w| *w != 0);
    let integrated_t2 = op
        .integrated
        .as_ref()
        .unwrap()
        .integrated
        .materialize()
        .await
        .expect("integrated t2");
    assert_eq!(integrated_t2, expected_full_join);
}

#[tokio::test]
async fn join_operator_handles_negative_deltas() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "neg_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "neg_right_stream", None)
            .await
            .expect("right dict"),
    );
    let left_state_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "neg_left_state", None)
            .await
            .expect("left state dict"),
    );
    let right_state_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "neg_right_state", None)
            .await
            .expect("right state dict"),
    );
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "neg_output", None)
            .await
            .expect("out dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "neg_integrated", None)
            .await
            .expect("integrated dict"),
    );

    let left_state = RelationState {
        integrated: VersionedZSet::new(
            left_state_dict.clone(),
            table.clone(),
            "neg_left_state".to_string(),
        )
        .await
        .expect("left integrated"),
        latest_handle: ZSetHandle {
            ns: "neg_left_state".to_string(),
            version: 0,
        },
    };
    let right_state = RelationState {
        integrated: VersionedZSet::new(
            right_state_dict.clone(),
            table.clone(),
            "neg_right_state".to_string(),
        )
        .await
        .expect("right integrated"),
        latest_handle: ZSetHandle {
            ns: "neg_right_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(out_dict.clone(), table.clone(), "neg_output".to_string())
        .await
        .expect("output");
    let integrated_join = RelationState {
        integrated: VersionedZSet::new(
            integrated_dict.clone(),
            table.clone(),
            "neg_integrated".to_string(),
        )
        .await
        .expect("integrated join"),
        latest_handle: ZSetHandle {
            ns: "neg_integrated".to_string(),
            version: 0,
        },
    };
    let left_index = IndexedBatchZSet::new(table.clone(), "neg_left_index");
    let right_index = IndexedBatchZSet::new(table.clone(), "neg_right_index");
    let left_key = Arc::new(|value: &i64| Some(*value));
    let right_key = Arc::new(|value: &i64| Some(*value));

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        left_index,
        right_index,
        batch_join_key(left_key),
        batch_join_key(right_key),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        output,
        Some(integrated_join),
    );

    let left_delta1 = stage_version(
        left_dict.clone(),
        table.clone(),
        "neg_left_stream",
        &[(1, 2)],
    )
    .await;
    let right_delta1 = stage_version(
        right_dict.clone(),
        table.clone(),
        "neg_right_stream",
        &[(1, 3)],
    )
    .await;
    let out1 = op
        .on_step(1, &[left_delta1, right_delta1])
        .await
        .expect("run join t1")
        .expect("non-empty t1");

    let mut cache = HashMap::new();
    cache.insert("neg_output".to_string(), out_dict.clone());
    let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
        .await
        .expect("materialize t1 output");
    assert_eq!(out1_materialized, HashMap::from([(2, 6)]));

    let left_delta2 = stage_version(
        left_dict.clone(),
        table.clone(),
        "neg_left_stream",
        &[(1, -1)],
    )
    .await;
    let right_empty = ZSetHandle {
        ns: "neg_right_stream".to_string(),
        version: 0,
    };
    let out2 = op
        .on_step(2, &[left_delta2, right_empty])
        .await
        .expect("run join t2")
        .expect("non-empty t2");
    let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
        .await
        .expect("materialize t2 output");
    assert_eq!(out2_materialized, HashMap::from([(2, -3)]));

    let integrated_t2 = op
        .integrated
        .as_ref()
        .unwrap()
        .integrated
        .materialize()
        .await
        .expect("integrated t2");
    assert_eq!(integrated_t2, HashMap::from([(2, 3)]));
}

#[tokio::test]
async fn join_operator_skips_null_keys() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<Option<i64>>::with_table(table.clone(), "null_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<Option<i64>>::with_table(table.clone(), "null_right_stream", None)
            .await
            .expect("right dict"),
    );
    let left_state_dict = Arc::new(
        Dictionary::<Option<i64>>::with_table(table.clone(), "null_left_state", None)
            .await
            .expect("left state dict"),
    );
    let right_state_dict = Arc::new(
        Dictionary::<Option<i64>>::with_table(table.clone(), "null_right_state", None)
            .await
            .expect("right state dict"),
    );
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "null_output", None)
            .await
            .expect("out dict"),
    );

    let left_state = RelationState {
        integrated: VersionedZSet::new(
            left_state_dict.clone(),
            table.clone(),
            "null_left_state".to_string(),
        )
        .await
        .expect("left integrated"),
        latest_handle: ZSetHandle {
            ns: "null_left_state".to_string(),
            version: 0,
        },
    };
    let right_state = RelationState {
        integrated: VersionedZSet::new(
            right_state_dict.clone(),
            table.clone(),
            "null_right_state".to_string(),
        )
        .await
        .expect("right integrated"),
        latest_handle: ZSetHandle {
            ns: "null_right_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(out_dict.clone(), table.clone(), "null_output".to_string())
        .await
        .expect("output");
    let left_index = IndexedBatchZSet::new(table.clone(), "null_left_index");
    let right_index = IndexedBatchZSet::new(table.clone(), "null_right_index");
    let left_key = Arc::new(|value: &Option<i64>| *value);
    let right_key = Arc::new(|value: &Option<i64>| *value);

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        left_index,
        right_index,
        batch_join_key(left_key),
        batch_join_key(right_key),
        Arc::new(|l: &Option<i64>, r: &Option<i64>| matches!((l, r), (Some(a), Some(b)) if a == b)),
        Arc::new(|l: &Option<i64>, r: &Option<i64>| l.unwrap_or(0) + r.unwrap_or(0)),
        table.clone(),
        output,
        None,
    );

    let left_delta = stage_version(
        left_dict.clone(),
        table.clone(),
        "null_left_stream",
        &[(Some(1), 1), (None, 1)],
    )
    .await;
    let right_delta = stage_version(
        right_dict.clone(),
        table.clone(),
        "null_right_stream",
        &[(Some(1), 1), (None, 1)],
    )
    .await;
    let out = op
        .on_step(1, &[left_delta, right_delta])
        .await
        .expect("run join")
        .expect("non-empty join");

    let mut cache = HashMap::new();
    cache.insert("null_output".to_string(), out_dict.clone());
    let out_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
        .await
        .expect("materialize join output");
    assert_eq!(out_materialized, HashMap::from([(2, 1)]));
}

#[tokio::test]
async fn join_operator_matches_full_recompute() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "recompute_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "recompute_right_stream", None)
            .await
            .expect("right dict"),
    );
    let left_state_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "recompute_left_state", None)
            .await
            .expect("left state dict"),
    );
    let right_state_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "recompute_right_state", None)
            .await
            .expect("right state dict"),
    );
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "recompute_output", None)
            .await
            .expect("out dict"),
    );
    let integrated_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "recompute_integrated", None)
            .await
            .expect("integrated dict"),
    );

    let left_state = RelationState {
        integrated: VersionedZSet::new(
            left_state_dict.clone(),
            table.clone(),
            "recompute_left_state".to_string(),
        )
        .await
        .expect("left integrated"),
        latest_handle: ZSetHandle {
            ns: "recompute_left_state".to_string(),
            version: 0,
        },
    };
    let right_state = RelationState {
        integrated: VersionedZSet::new(
            right_state_dict.clone(),
            table.clone(),
            "recompute_right_state".to_string(),
        )
        .await
        .expect("right integrated"),
        latest_handle: ZSetHandle {
            ns: "recompute_right_state".to_string(),
            version: 0,
        },
    };
    let output = VersionedZSet::new(
        out_dict.clone(),
        table.clone(),
        "recompute_output".to_string(),
    )
    .await
    .expect("output");
    let integrated_join = RelationState {
        integrated: VersionedZSet::new(
            integrated_dict.clone(),
            table.clone(),
            "recompute_integrated".to_string(),
        )
        .await
        .expect("integrated join"),
        latest_handle: ZSetHandle {
            ns: "recompute_integrated".to_string(),
            version: 0,
        },
    };
    let left_index = IndexedBatchZSet::new(table.clone(), "recompute_left_index");
    let right_index = IndexedBatchZSet::new(table.clone(), "recompute_right_index");
    let left_key = Arc::new(|value: &i64| Some(*value));
    let right_key = Arc::new(|value: &i64| Some(*value));

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        left_index,
        right_index,
        batch_join_key(left_key),
        batch_join_key(right_key),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        output,
        Some(integrated_join),
    );

    let steps = vec![
        (vec![(1, 1), (2, 1)], vec![(1, 2)]),
        (vec![(1, -1), (3, 1)], vec![(2, 3), (3, 1)]),
    ];

    let mut full_left: HashMap<i64, i64> = HashMap::new();
    let mut full_right: HashMap<i64, i64> = HashMap::new();
    let mut full_join: HashMap<i64, i64> = HashMap::new();

    for (idx, (left_deltas, right_deltas)) in steps.into_iter().enumerate() {
        let left_delta_handle = stage_version(
            left_dict.clone(),
            table.clone(),
            "recompute_left_stream",
            &left_deltas,
        )
        .await;
        let right_delta_handle = stage_version(
            right_dict.clone(),
            table.clone(),
            "recompute_right_stream",
            &right_deltas,
        )
        .await;

        let output_handle = op
            .on_step(idx as i64 + 1, &[left_delta_handle, right_delta_handle])
            .await
            .expect("run join step");

        apply_deltas(&mut full_left, &left_deltas);
        apply_deltas(&mut full_right, &right_deltas);

        let recompute = recompute_join(&full_left, &full_right);
        let expected_delta_vec = compute_delta(&full_join, &recompute);
        let expected_delta: HashMap<i64, i64> = expected_delta_vec.into_iter().collect();

        if let Some(handle) = output_handle {
            let mut cache = HashMap::new();
            cache.insert("recompute_output".to_string(), out_dict.clone());
            let actual_delta = materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle)
                .await
                .expect("materialize join output");
            assert_eq!(actual_delta, expected_delta);
        } else {
            assert!(expected_delta.is_empty());
        }

        let integrated_after = op
            .integrated
            .as_ref()
            .unwrap()
            .integrated
            .materialize()
            .await
            .expect("materialize join integrated");
        assert_eq!(integrated_after, recompute);

        full_join = recompute;
    }
}

async fn run_join_history_invariance_probe(
    unrelated_history_rows: i64,
) -> crate::metrics::LogicalWorkSnapshot {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_state = RelationState::empty(
        table.clone(),
        format!("history_probe_left_state_{unrelated_history_rows}"),
    )
    .await
    .expect("left state");
    let right_state = RelationState::empty(
        table.clone(),
        format!("history_probe_right_state_{unrelated_history_rows}"),
    )
    .await
    .expect("right state");

    let mut op = JoinOp::new_without_output_batch(
        left_state,
        right_state,
        IndexedBatchZSet::new(
            table.clone(),
            format!("history_probe_left_index_{unrelated_history_rows}"),
        ),
        IndexedBatchZSet::new(
            table.clone(),
            format!("history_probe_right_index_{unrelated_history_rows}"),
        ),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table,
        None,
    );

    let left_history = (0..unrelated_history_rows)
        .map(|idx| (1_000_000 + idx, 1))
        .collect::<Vec<_>>();
    let mut right_history = (0..unrelated_history_rows)
        .map(|idx| (2_000_000 + idx, 1))
        .collect::<Vec<_>>();
    right_history.push((7, 1));

    op.on_step_transient_with_inputs(
        1,
        &[
            empty_handle("history_probe_left_stream"),
            empty_handle("history_probe_right_stream"),
        ],
        Some(JoinTransientInputs {
            left: Some(Arc::new(left_history)),
            right: Some(Arc::new(right_history)),
            left_closed_keys: None,
            right_closed_keys: None,
        }),
    )
    .await
    .expect("seed history");

    let output = op
        .on_step_transient_with_inputs(
            2,
            &[
                empty_handle("history_probe_left_stream"),
                empty_handle("history_probe_right_stream"),
            ],
            Some(JoinTransientInputs {
                left: Some(Arc::new(vec![(7, 1)])),
                right: Some(Arc::new(Vec::new())),
                left_closed_keys: None,
                right_closed_keys: None,
            }),
        )
        .await
        .expect("steady-state join")
        .expect("steady-state output");
    assert_eq!(batch_to_map(&output), HashMap::from([(14, 1)]));

    op.last_logical_work()
}

#[tokio::test]
async fn join_logical_work_is_independent_of_unrelated_history() {
    let baseline = run_join_history_invariance_probe(8).await;

    for unrelated_history_rows in [128, 1024] {
        let actual = run_join_history_invariance_probe(unrelated_history_rows).await;
        assert_eq!(actual.left_delta_rows, baseline.left_delta_rows);
        assert_eq!(actual.right_delta_rows, baseline.right_delta_rows);
        assert_eq!(actual.left_changed_keys, baseline.left_changed_keys);
        assert_eq!(actual.right_changed_keys, baseline.right_changed_keys);
        assert_eq!(
            actual.left_state_rows_examined,
            baseline.left_state_rows_examined
        );
        assert_eq!(
            actual.right_state_rows_examined,
            baseline.right_state_rows_examined
        );
        assert_eq!(
            actual.delta_delta_rows_examined,
            baseline.delta_delta_rows_examined
        );
        assert_eq!(actual.state_scan_rows, baseline.state_scan_rows);
        assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
        assert_eq!(actual.join_output_rows, baseline.join_output_rows);
        assert_eq!(
            actual.index_postings_examined,
            baseline.index_postings_examined
        );
        assert_eq!(actual.state_full_scan_count, 0);
    }

    assert_eq!(baseline.left_delta_rows, 1);
    assert_eq!(baseline.right_delta_rows, 0);
    assert_eq!(baseline.left_changed_keys, 1);
    assert_eq!(baseline.right_changed_keys, 0);
    assert_eq!(baseline.left_state_rows_examined, 0);
    assert_eq!(baseline.right_state_rows_examined, 1);
    assert_eq!(baseline.delta_delta_rows_examined, 0);
    assert_eq!(baseline.state_scan_rows, 1);
    assert_eq!(baseline.output_delta_rows, 1);
    assert_eq!(baseline.join_output_rows, 1);
    assert_eq!(baseline.index_postings_examined, 0);
    assert_eq!(baseline.state_full_scan_count, 0);
}

#[tokio::test]
async fn join_operator_uses_arranged_state_as_canonical_persisted_input() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_canonical_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_canonical_right_stream", None)
            .await
            .expect("right dict"),
    );

    let left_state = RelationState::empty(table.clone(), "join_canonical_left_state".to_string())
        .await
        .expect("left state");
    let right_state = RelationState::empty(table.clone(), "join_canonical_right_state".to_string())
        .await
        .expect("right state");
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_canonical_output", None)
            .await
            .expect("output dict"),
    );
    let output = VersionedZSet::new(
        out_dict.clone(),
        table.clone(),
        "join_canonical_output".to_string(),
    )
    .await
    .expect("output zset");

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        IndexedBatchZSet::new(table.clone(), "join_canonical_left_index"),
        IndexedBatchZSet::new(table.clone(), "join_canonical_right_index"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        output,
        None,
    );

    let left_delta = stage_version(
        left_dict.clone(),
        table.clone(),
        "join_canonical_left_stream",
        &[(7, 1)],
    )
    .await;
    let right_delta = stage_version(
        right_dict.clone(),
        table.clone(),
        "join_canonical_right_stream",
        &[(7, 1)],
    )
    .await;
    let out = op
        .on_step(1, &[left_delta, right_delta])
        .await
        .expect("join step")
        .expect("join output");

    assert!(
        op.left_state.integrated.current_handle().is_none(),
        "left relation snapshots should not be persisted on the join critical path"
    );
    assert!(
        op.right_state.integrated.current_handle().is_none(),
        "right relation snapshots should not be persisted on the join critical path"
    );

    let mut left_entries = op
        .left_index
        .values_for_key(&7)
        .await
        .expect("left index lookup");
    left_entries.sort_unstable();
    assert_eq!(left_entries, vec![(7, 1)]);

    let mut right_entries = op
        .right_index
        .values_for_key(&7)
        .await
        .expect("right index lookup");
    right_entries.sort_unstable();
    assert_eq!(right_entries, vec![(7, 1)]);

    let mut cache = HashMap::new();
    cache.insert("join_canonical_output".to_string(), out_dict);
    let materialized = materialize_zset_handle::<i64>(table, &mut cache, &out)
        .await
        .expect("materialize join delta");
    assert_eq!(materialized.get(&14), Some(&1));
}

#[tokio::test]
async fn join_operator_can_drop_matched_append_only_left_rows() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_drop_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_drop_right_stream", None)
            .await
            .expect("right dict"),
    );

    let left_state = RelationState::empty(table.clone(), "join_drop_left_state".to_string())
        .await
        .expect("left state");
    let right_state = RelationState::empty(table.clone(), "join_drop_right_state".to_string())
        .await
        .expect("right state");
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_drop_output", None)
            .await
            .expect("output dict"),
    );
    let output = VersionedZSet::new(out_dict, table.clone(), "join_drop_output".to_string())
        .await
        .expect("output zset");

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        IndexedBatchZSet::new(table.clone(), "join_drop_left_index"),
        IndexedBatchZSet::new(table.clone(), "join_drop_right_index"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        output,
        None,
    )
    .with_input_retention(
        JoinInputRetention::DropMatchedAppendOnly,
        JoinInputRetention::RetainAll,
    );

    let left_first = stage_version(
        left_dict.clone(),
        table.clone(),
        "join_drop_left_stream",
        &[(7, 1)],
    )
    .await;
    op.on_step(1, &[left_first, empty_handle("join_drop_right_stream")])
        .await
        .expect("left-only join step");
    assert_eq!(
        op.left_index
            .values_for_key(&7)
            .await
            .expect("left index after unmatched left"),
        vec![(7, 1)]
    );

    let right_match = stage_version(
        right_dict.clone(),
        table.clone(),
        "join_drop_right_stream",
        &[(7, 1)],
    )
    .await;
    let out = op
        .on_step(2, &[empty_handle("join_drop_left_stream"), right_match])
        .await
        .expect("right match join step")
        .expect("right match output");
    let mut cache = HashMap::new();
    let materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
        .await
        .expect("materialize right match output");
    assert_eq!(materialized, HashMap::from([(14, 1)]));
    assert!(
        op.left_index
            .values_for_key(&7)
            .await
            .expect("left index after matched eviction")
            .is_empty()
    );
    assert_eq!(
        op.right_index
            .values_for_key(&7)
            .await
            .expect("right index retained"),
        vec![(7, 1)]
    );

    let left_after_right =
        stage_version(left_dict, table.clone(), "join_drop_left_stream", &[(7, 1)]).await;
    op.on_step(
        3,
        &[left_after_right, empty_handle("join_drop_right_stream")],
    )
    .await
    .expect("matched left join step");
    assert!(
        op.left_index
            .values_for_key(&7)
            .await
            .expect("left index after immediate match")
            .is_empty()
    );
}

#[tokio::test]
async fn join_operator_can_drop_closed_append_only_left_keys() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_closed_left_stream", None)
            .await
            .expect("left dict"),
    );
    let left_state = RelationState::empty(table.clone(), "join_closed_left_state".to_string())
        .await
        .expect("left state");
    let right_state = RelationState::empty(table.clone(), "join_closed_right_state".to_string())
        .await
        .expect("right state");
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_closed_output", None)
            .await
            .expect("output dict"),
    );
    let output = VersionedZSet::new(out_dict, table.clone(), "join_closed_output".to_string())
        .await
        .expect("output zset");

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        IndexedBatchZSet::new(table.clone(), "join_closed_left_index"),
        IndexedBatchZSet::new(table.clone(), "join_closed_right_index"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        output,
        None,
    )
    .with_input_retention(
        JoinInputRetention::DropMatchedAppendOnly,
        JoinInputRetention::RetainAll,
    );

    let left_first = stage_version(
        left_dict.clone(),
        table.clone(),
        "join_closed_left_stream",
        &[(7, 1)],
    )
    .await;
    op.on_step(1, &[left_first, empty_handle("join_closed_right_stream")])
        .await
        .expect("left-only join step");
    assert_eq!(
        op.left_index
            .values_for_key(&7)
            .await
            .expect("left index before closed key"),
        vec![(7, 1)]
    );

    op.on_step_transient_with_inputs(
        2,
        &[
            empty_handle("join_closed_left_stream"),
            empty_handle("join_closed_right_stream"),
        ],
        Some(JoinTransientInputs {
            left: None,
            right: Some(Arc::new(Vec::new())),
            left_closed_keys: None,
            right_closed_keys: Some(Arc::new(vec![(7, 1)])),
        }),
    )
    .await
    .expect("closed-key join step");
    assert!(
        op.left_index
            .values_for_key(&7)
            .await
            .expect("left index after closed key")
            .is_empty()
    );
    assert_eq!(
        op.right_closed_index
            .values_for_key(&7)
            .await
            .expect("right closed index"),
        vec![((), 1)]
    );

    let left_after_close = stage_version(
        left_dict,
        table.clone(),
        "join_closed_left_stream",
        &[(7, 1)],
    )
    .await;
    op.on_step(
        3,
        &[left_after_close, empty_handle("join_closed_right_stream")],
    )
    .await
    .expect("left-after-close join step");
    assert!(
        op.left_index
            .values_for_key(&7)
            .await
            .expect("left index after immediate closed key")
            .is_empty()
    );
}

#[tokio::test]
async fn join_operator_inmemory_indexes_preserve_cross_tick_matches() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_inmemory_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_inmemory_right_stream", None)
            .await
            .expect("right dict"),
    );
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_inmemory_output", None)
            .await
            .expect("out dict"),
    );
    let left_state = RelationState::empty(table.clone(), "join_inmemory_left_state".to_string())
        .await
        .expect("left state");
    let right_state = RelationState::empty(table.clone(), "join_inmemory_right_state".to_string())
        .await
        .expect("right state");
    let output = VersionedZSet::new(
        out_dict.clone(),
        table.clone(),
        "join_inmemory_output".to_string(),
    )
    .await
    .expect("output zset");

    let mut op = JoinOp::new_batch(
        left_state,
        right_state,
        IndexedBatchZSet::new(table.clone(), "join_inmemory_left_index"),
        IndexedBatchZSet::new(table.clone(), "join_inmemory_right_index"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        output,
        None,
    )
    .with_persist_indexes(false);

    let empty_left = empty_handle("join_inmemory_left_stream");
    let right_delta = stage_version(
        right_dict.clone(),
        table.clone(),
        "join_inmemory_right_stream",
        &[(7, 1)],
    )
    .await;
    let out = op
        .on_step(1, &[empty_left, right_delta])
        .await
        .expect("seed right inmemory index")
        .expect("empty handle");
    assert_eq!(out.version, 0);

    let left_delta = stage_version(
        left_dict,
        table.clone(),
        "join_inmemory_left_stream",
        &[(7, 1)],
    )
    .await;
    let empty_right = empty_handle("join_inmemory_right_stream");
    let out = op
        .on_step(2, &[left_delta, empty_right])
        .await
        .expect("join step")
        .expect("join output");

    let mut cache = HashMap::new();
    cache.insert("join_inmemory_output".to_string(), out_dict);
    let materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
        .await
        .expect("materialize inmemory join delta");
    assert_eq!(materialized.get(&14), Some(&1));

    assert!(
        op.right_index
            .values_for_key(&7)
            .await
            .expect("lookup persisted right index")
            .is_empty(),
        "in-memory join indexes should not persist arranged state on the hot path"
    );
}

#[tokio::test]
async fn join_operator_transient_batches_match_persisted_output() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_transient_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_transient_right_stream", None)
            .await
            .expect("right dict"),
    );
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_transient_output", None)
            .await
            .expect("out dict"),
    );

    let mut persisted = JoinOp::new_batch(
        RelationState::empty(
            table.clone(),
            "join_transient_left_state_persisted".to_string(),
        )
        .await
        .expect("persisted left state"),
        RelationState::empty(
            table.clone(),
            "join_transient_right_state_persisted".to_string(),
        )
        .await
        .expect("persisted right state"),
        IndexedBatchZSet::new(table.clone(), "join_transient_left_index_persisted"),
        IndexedBatchZSet::new(table.clone(), "join_transient_right_index_persisted"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        VersionedZSet::new(
            out_dict.clone(),
            table.clone(),
            "join_transient_output".to_string(),
        )
        .await
        .expect("persisted output"),
        None,
    )
    .with_persist_indexes(false);

    let mut transient = JoinOp::new_without_output_batch(
        RelationState::empty(
            table.clone(),
            "join_transient_left_state_transient".to_string(),
        )
        .await
        .expect("transient left state"),
        RelationState::empty(
            table.clone(),
            "join_transient_right_state_transient".to_string(),
        )
        .await
        .expect("transient right state"),
        IndexedBatchZSet::new(table.clone(), "join_transient_left_index_transient"),
        IndexedBatchZSet::new(table.clone(), "join_transient_right_index_transient"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        None,
    )
    .with_persist_indexes(false);

    let right_seed = stage_version(
        right_dict.clone(),
        table.clone(),
        "join_transient_right_stream",
        &[(7, 1)],
    )
    .await;
    let empty_left = empty_handle("join_transient_left_stream");
    let persisted_seed = persisted
        .on_step(1, &[empty_left.clone(), right_seed.clone()])
        .await
        .expect("seed persisted join")
        .expect("persisted empty handle");
    assert_eq!(persisted_seed.version, 0);
    assert!(
        transient
            .on_step_transient_with_inputs(1, &[empty_left, right_seed], None)
            .await
            .expect("seed transient join")
            .is_none()
    );

    let left_match = stage_version(
        left_dict.clone(),
        table.clone(),
        "join_transient_left_stream",
        &[(7, 2)],
    )
    .await;
    let empty_right = empty_handle("join_transient_right_stream");
    let persisted_t2 = persisted
        .on_step(2, &[left_match.clone(), empty_right.clone()])
        .await
        .expect("persisted t2")
        .expect("persisted t2 output");
    let transient_t2 = transient
        .on_step_transient_with_inputs(2, &[left_match, empty_right], None)
        .await
        .expect("transient t2")
        .expect("transient t2 output");

    let mut cache = HashMap::new();
    cache.insert("join_transient_output".to_string(), out_dict.clone());
    let persisted_t2_rows =
        materialize_zset_handle::<i64>(table.clone(), &mut cache, &persisted_t2)
            .await
            .expect("materialize persisted t2");
    assert_eq!(persisted_t2_rows, batch_to_map(&transient_t2));

    let right_retract = stage_version(
        right_dict,
        table.clone(),
        "join_transient_right_stream",
        &[(7, -1)],
    )
    .await;
    let empty_left = empty_handle("join_transient_left_stream");
    let persisted_t3 = persisted
        .on_step(3, &[empty_left.clone(), right_retract.clone()])
        .await
        .expect("persisted t3")
        .expect("persisted t3 output");
    let transient_t3 = transient
        .on_step_transient_with_inputs(3, &[empty_left, right_retract], None)
        .await
        .expect("transient t3")
        .expect("transient t3 output");

    let persisted_t3_rows = materialize_zset_handle::<i64>(table, &mut cache, &persisted_t3)
        .await
        .expect("materialize persisted t3");
    assert_eq!(persisted_t3_rows, batch_to_map(&transient_t3));
}

#[tokio::test]
async fn join_operator_preloaded_transient_inputs_match_handle_path() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_preloaded_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "join_preloaded_right_stream", None)
            .await
            .expect("right dict"),
    );

    let mut handle_path = JoinOp::new_without_output_batch(
        RelationState::empty(
            table.clone(),
            "join_preloaded_left_state_handle".to_string(),
        )
        .await
        .expect("handle left state"),
        RelationState::empty(
            table.clone(),
            "join_preloaded_right_state_handle".to_string(),
        )
        .await
        .expect("handle right state"),
        IndexedBatchZSet::new(table.clone(), "join_preloaded_left_index_handle"),
        IndexedBatchZSet::new(table.clone(), "join_preloaded_right_index_handle"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        None,
    )
    .with_persist_indexes(false);

    let mut preloaded_path = JoinOp::new_without_output_batch(
        RelationState::empty(
            table.clone(),
            "join_preloaded_left_state_transient".to_string(),
        )
        .await
        .expect("transient left state"),
        RelationState::empty(
            table.clone(),
            "join_preloaded_right_state_transient".to_string(),
        )
        .await
        .expect("transient right state"),
        IndexedBatchZSet::new(table.clone(), "join_preloaded_left_index_transient"),
        IndexedBatchZSet::new(table.clone(), "join_preloaded_right_index_transient"),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        batch_join_key(Arc::new(|value: &i64| Some(*value))),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(project_sum),
        table.clone(),
        None,
    )
    .with_persist_indexes(false);

    let empty_left = empty_handle("join_preloaded_left_stream");
    let empty_right = empty_handle("join_preloaded_right_stream");

    let right_seed = stage_version(
        right_dict.clone(),
        table.clone(),
        "join_preloaded_right_stream",
        &[(7, 1)],
    )
    .await;
    assert!(
        handle_path
            .on_step_transient_with_inputs(1, &[empty_left.clone(), right_seed], None)
            .await
            .expect("seed handle path")
            .is_none()
    );
    assert!(
        preloaded_path
            .on_step_transient_with_inputs(
                1,
                &[empty_left.clone(), empty_right.clone()],
                Some(JoinTransientInputs {
                    left: None,
                    right: Some(Arc::new(vec![(7, 1)])),
                    left_closed_keys: None,
                    right_closed_keys: None,
                }),
            )
            .await
            .expect("seed preloaded path")
            .is_none()
    );

    let left_match = stage_version(
        left_dict.clone(),
        table.clone(),
        "join_preloaded_left_stream",
        &[(7, 2)],
    )
    .await;
    let handle_t2 = handle_path
        .on_step_transient_with_inputs(2, &[left_match, empty_right.clone()], None)
        .await
        .expect("handle t2")
        .expect("handle t2 output");
    let preloaded_t2 = preloaded_path
        .on_step_transient_with_inputs(
            2,
            &[empty_left.clone(), empty_right.clone()],
            Some(JoinTransientInputs {
                left: Some(Arc::new(vec![(7, 2)])),
                right: None,
                left_closed_keys: None,
                right_closed_keys: None,
            }),
        )
        .await
        .expect("preloaded t2")
        .expect("preloaded t2 output");
    assert_eq!(batch_to_map(&handle_t2), batch_to_map(&preloaded_t2));

    let right_retract =
        stage_version(right_dict, table, "join_preloaded_right_stream", &[(7, -1)]).await;
    let handle_t3 = handle_path
        .on_step_transient_with_inputs(3, &[empty_left.clone(), right_retract], None)
        .await
        .expect("handle t3")
        .expect("handle t3 output");
    let preloaded_t3 = preloaded_path
        .on_step_transient_with_inputs(
            3,
            &[empty_left, empty_right],
            Some(JoinTransientInputs {
                left: None,
                right: Some(Arc::new(vec![(7, -1)])),
                left_closed_keys: None,
                right_closed_keys: None,
            }),
        )
        .await
        .expect("preloaded t3")
        .expect("preloaded t3 output");
    assert_eq!(batch_to_map(&handle_t3), batch_to_map(&preloaded_t3));
}
