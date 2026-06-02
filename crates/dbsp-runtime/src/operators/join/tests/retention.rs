use super::*;

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
