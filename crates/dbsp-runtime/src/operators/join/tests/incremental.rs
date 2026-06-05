use super::*;

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
    let out_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "null_output", None)
            .await
            .expect("out dict"),
    );

    let output = VersionedZSet::new(out_dict.clone(), table.clone(), "null_output".to_string())
        .await
        .expect("output");
    let left_index = IndexedBatchZSet::new(table.clone(), "null_left_index");
    let right_index = IndexedBatchZSet::new(table.clone(), "null_right_index");
    let left_key = Arc::new(|value: &Option<i64>| *value);
    let right_key = Arc::new(|value: &Option<i64>| *value);

    let mut op = JoinOp::new_batch(
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
