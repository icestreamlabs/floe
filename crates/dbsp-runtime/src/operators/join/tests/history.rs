use super::*;

async fn run_join_history_invariance_probe(
    unrelated_history_rows: i64,
) -> crate::metrics::LogicalWorkSnapshot {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

    let mut op = JoinOp::new_without_output_batch(
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
