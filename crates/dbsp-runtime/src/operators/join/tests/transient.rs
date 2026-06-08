use super::*;

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

    let mut persisted = JoinOp::new_batch(JoinBatchConfig {
        left_index: IndexedBatchZSet::new(table.clone(), "join_transient_left_index_persisted"),
        right_index: IndexedBatchZSet::new(table.clone(), "join_transient_right_index_persisted"),
        left_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        right_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        predicate: Arc::new(|l: &i64, r: &i64| l == r),
        projector: Arc::new(project_sum),
        table: table.clone(),
        output: Some(
            VersionedZSet::new(
                out_dict.clone(),
                table.clone(),
                "join_transient_output".to_string(),
            )
            .await
            .expect("persisted output"),
        ),
        integrated: None,
    })
    .with_persist_indexes(false);

    let mut transient = JoinOp::new_batch(JoinBatchConfig {
        left_index: IndexedBatchZSet::new(table.clone(), "join_transient_left_index_transient"),
        right_index: IndexedBatchZSet::new(table.clone(), "join_transient_right_index_transient"),
        left_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        right_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        predicate: Arc::new(|l: &i64, r: &i64| l == r),
        projector: Arc::new(project_sum),
        table: table.clone(),
        output: None,
        integrated: None,
    })
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

    let mut handle_path = JoinOp::new_batch(JoinBatchConfig {
        left_index: IndexedBatchZSet::new(table.clone(), "join_preloaded_left_index_handle"),
        right_index: IndexedBatchZSet::new(table.clone(), "join_preloaded_right_index_handle"),
        left_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        right_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        predicate: Arc::new(|l: &i64, r: &i64| l == r),
        projector: Arc::new(project_sum),
        table: table.clone(),
        output: None,
        integrated: None,
    })
    .with_persist_indexes(false);

    let mut preloaded_path = JoinOp::new_batch(JoinBatchConfig {
        left_index: IndexedBatchZSet::new(table.clone(), "join_preloaded_left_index_transient"),
        right_index: IndexedBatchZSet::new(table.clone(), "join_preloaded_right_index_transient"),
        left_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        right_key: batch_join_key(Arc::new(|value: &i64| Some(*value))),
        predicate: Arc::new(|l: &i64, r: &i64| l == r),
        projector: Arc::new(project_sum),
        table: table.clone(),
        output: None,
        integrated: None,
    })
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
