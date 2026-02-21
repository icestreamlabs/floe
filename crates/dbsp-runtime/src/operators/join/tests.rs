use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use object_store::memory::InMemory;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::Db;

use super::JoinOp;
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

    let mut op = JoinOp::new(
        left_state,
        right_state,
        left_index,
        right_index,
        left_key,
        right_key,
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

    let mut op = JoinOp::new(
        left_state,
        right_state,
        left_index,
        right_index,
        left_key,
        right_key,
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
    let left_key = Arc::new(|value: &Option<i64>| value.clone());
    let right_key = Arc::new(|value: &Option<i64>| value.clone());

    let mut op = JoinOp::new(
        left_state,
        right_state,
        left_index,
        right_index,
        left_key,
        right_key,
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

    let mut op = JoinOp::new(
        left_state,
        right_state,
        left_index,
        right_index,
        left_key,
        right_key,
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
