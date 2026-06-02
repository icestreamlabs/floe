use super::*;

async fn handle_versions(stream: &mut RuntimeStream<ZSetHandle>, len: usize) -> Vec<u64> {
    let mut versions = Vec::with_capacity(len);
    for t in 0..len {
        versions.push(
            stream
                .get(t as i64)
                .await
                .expect("read handle version")
                .version,
        );
    }
    versions
}

#[tokio::test]
async fn lowered_scalar_and_circuit_execution_match_reference_and_survive_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let input = Stream::from_prefix(vec![0_i64, 1, 3, 3, 6, 6, 6, 7], 7);
    let query = pointwise("double", |value: &i64| value * 2)
        .fanout(strict_delay::<i64>())
        .compose(add_circuit());
    let output = query.apply(input.clone());
    let delta_output = circuit_d(query.clone()).apply(input.clone());
    let reintegrated_output = circuit_i(circuit_d(query.clone())).apply(input.clone());
    let incremental_output = incrementalize(query.clone()).apply(differentiate(&input));

    let lowered_output = lower_scalar(table.clone(), "semantic_scalar_query", &output)
        .await
        .expect("lower scalar query");
    let lowered_delta = lower_scalar(table.clone(), "semantic_scalar_delta", &delta_output)
        .await
        .expect("lower scalar delta query");
    let lowered_reintegrated = lower_scalar(
        table.clone(),
        "semantic_scalar_reintegrated",
        &reintegrated_output,
    )
    .await
    .expect("lower scalar reintegrated query");
    let lowered_incremental = lower_scalar(
        table.clone(),
        "semantic_scalar_incremental",
        &incremental_output,
    )
    .await
    .expect("lower scalar incremental query");

    assert_eq!(
        lowered_output
            .collect_prefix(8)
            .await
            .expect("read lowered scalar query"),
        observe(&output, 8)
    );
    assert_eq!(
        lowered_delta
            .collect_prefix(8)
            .await
            .expect("read lowered scalar delta query"),
        observe(&delta_output, 8)
    );
    assert_eq!(
        lowered_reintegrated
            .collect_prefix(8)
            .await
            .expect("read lowered scalar reintegrated query"),
        observe(&reintegrated_output, 8)
    );
    assert_eq!(
        lowered_incremental
            .collect_prefix(8)
            .await
            .expect("read lowered scalar incremental query"),
        observe(&incremental_output, 8)
    );

    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntGroup);
    let mut reopened =
        RuntimeStream::with_table(table, lowered_output.namespace().to_string(), group)
            .await
            .expect("reopen scalar query stream");
    assert_eq!(
        collect_runtime_scalar_prefix(&mut reopened, 8)
            .await
            .expect("read reopened scalar query"),
        observe(&output, 8)
    );
}

#[tokio::test]
async fn lowered_guarded_feedback_execution_matches_reference_and_survives_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let recursive = feedback("guarded_toggle", |_state| Stream::constant(1_i64));
    let guarded = feedback("guarded_feedback", move |state| {
        subtract(&recursive, &delay(&state))
    });

    let lowered = lower_scalar(table.clone(), "semantic_guarded_feedback", &guarded)
        .await
        .expect("lower guarded feedback");
    assert_eq!(
        lowered
            .collect_prefix(8)
            .await
            .expect("read lowered guarded feedback"),
        observe(&guarded, 8)
    );

    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntGroup);
    let mut reopened = RuntimeStream::with_table(table, lowered.namespace().to_string(), group)
        .await
        .expect("reopen guarded feedback stream");
    assert_eq!(
        collect_runtime_scalar_prefix(&mut reopened, 8)
            .await
            .expect("read reopened guarded feedback"),
        observe(&guarded, 8)
    );
}

#[tokio::test]
async fn lowered_zset_execution_matches_reference_delta_versions_and_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let snapshots = Stream::from_prefix(
        vec![
            zset([(1_i64, 1)]),
            zset([(1_i64, 1)]),
            zset([(1_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(2_i64, 1)]),
        ],
        zset([(2_i64, 1)]),
    );
    let output = aggregate_zset(
        &snapshots,
        |value| Some(value % 2),
        |_key, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
    );
    let delta_output = differentiate(&output);

    let lowered = lower_zset(table.clone(), "semantic_zset", &output)
        .await
        .expect("lower zset execution");
    assert_eq!(
        lowered
            .collect_snapshot_prefix(6)
            .await
            .expect("read lowered zset snapshot prefix"),
        observe(&output, 6)
    );
    assert_eq!(
        lowered
            .collect_delta_prefix(6)
            .await
            .expect("read lowered zset delta prefix"),
        observe(&delta_output, 6)
    );

    let mut snapshot_stream = lowered.snapshot_stream().stream();
    let mut delta_stream = lowered.delta_stream().stream();
    assert_eq!(
        handle_versions(&mut snapshot_stream, 6).await,
        vec![1, 1, 1, 2, 2, 3]
    );
    assert_eq!(
        handle_versions(&mut delta_stream, 6).await,
        vec![1, 0, 0, 2, 0, 3]
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_zset".to_string(),
            version: 0,
        },
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_zset/delta".to_string(),
            version: 0,
        },
    });
    let mut reopened_snapshot =
        RuntimeStream::with_table(table.clone(), "semantic_zset".to_string(), snapshot_group)
            .await
            .expect("reopen zset snapshot stream");
    let mut reopened_delta =
        RuntimeStream::with_table(table, "semantic_zset/delta".to_string(), delta_group)
            .await
            .expect("reopen zset delta stream");

    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(
            build_table(db.clone()),
            &mut reopened_snapshot,
            6
        )
        .await
        .expect("read reopened zset snapshot prefix"),
        observe(&output, 6)
    );
    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(build_table(db), &mut reopened_delta, 6)
            .await
            .expect("read reopened zset delta prefix"),
        observe(&delta_output, 6)
    );
}

#[tokio::test]
async fn lowered_relational_execution_matches_reference() {
    let db = build_db().await;
    let table = build_table(db);
    let left = Stream::from_prefix(
        vec![
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 2), (2_i64, 1), (3_i64, -1)]),
            zset([(1_i64, 2), (2_i64, 1), (3_i64, -1)]),
            zset([(2_i64, 1), (3_i64, -1), (4_i64, 1)]),
            zset([(2_i64, 1), (3_i64, -1), (4_i64, 1)]),
            zset([(4_i64, 1)]),
            zset([(4_i64, 1)]),
        ],
        zset([(4_i64, 1)]),
    );
    let right = Stream::from_prefix(
        vec![
            zset([(1_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1), (3_i64, 1)]),
            zset([(2_i64, 1), (4_i64, 1)]),
            zset([(2_i64, 1), (4_i64, 1)]),
            zset([(4_i64, 1)]),
            zset([(4_i64, 1)]),
        ],
        zset([(4_i64, 1)]),
    );

    let bag_union = union_zset(&left, &right);
    let bag_map_filter = map_zset(&filter_zset(&left, |value| *value >= 2), |value| value * 10);
    let bag_join = join_zset(
        &left,
        &right,
        |left| Some(left % 2),
        |right| Some(right % 2),
        |left, right| left <= right,
        |left, right| (*left, *right),
    );

    let left_set = distinct_zset(&left);
    let right_set = distinct_zset(&right);
    let set_union = union_set(&left_set, &right_set);
    let set_map_filter = map_set(&filter_set(&left_set, |value| *value >= 2), |value| {
        value * 100
    });
    let set_joined = join_set(
        &left_set,
        &right_set,
        |left| Some(left % 2),
        |right| Some(right % 2),
        |left, right| left <= right,
        |left, right| (*left, *right),
    );

    let lowered_bag_union = lower_zset(table.clone(), "semantic_bag_union", &bag_union)
        .await
        .expect("lower bag union");
    let lowered_bag_map_filter =
        lower_zset(table.clone(), "semantic_bag_map_filter", &bag_map_filter)
            .await
            .expect("lower bag map/filter");
    let lowered_bag_join = lower_zset(table.clone(), "semantic_bag_join", &bag_join)
        .await
        .expect("lower bag join");
    let lowered_set_union = lower_set(table.clone(), "semantic_set_union", &set_union)
        .await
        .expect("lower set union");
    let lowered_set_map_filter =
        lower_set(table.clone(), "semantic_set_map_filter", &set_map_filter)
            .await
            .expect("lower set map/filter");
    let lowered_set_join = lower_set(table, "semantic_set_join", &set_joined)
        .await
        .expect("lower set join");

    assert_eq!(
        lowered_bag_union
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered bag union"),
        observe(&bag_union, 8)
    );
    assert_eq!(
        lowered_bag_map_filter
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered bag map/filter"),
        observe(&bag_map_filter, 8)
    );
    assert_eq!(
        lowered_bag_join
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered bag join"),
        observe(&bag_join, 8)
    );
    assert_eq!(
        lowered_set_union
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered set union"),
        observe(
            &set_union.lift("set_union_to_zset", |value| value.to_zset()),
            8
        )
    );
    assert_eq!(
        lowered_set_map_filter
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered set map/filter"),
        observe(
            &set_map_filter.lift("set_map_filter_to_zset", |value| value.to_zset()),
            8,
        )
    );
    assert_eq!(
        lowered_set_join
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered set join"),
        observe(
            &set_joined.lift("set_join_to_zset", |value| value.to_zset()),
            8
        )
    );
}

#[tokio::test]
async fn lowered_set_and_indexed_execution_matches_reference() {
    let db = build_db().await;
    let table = build_table(db);
    let bag = Stream::from_prefix(
        vec![
            zset([(1_i64, 2), (2_i64, 1)]),
            zset([(1_i64, 2), (2_i64, 1)]),
            zset([(2_i64, 1), (3_i64, 1)]),
            zset([(2_i64, 1), (3_i64, 1)]),
        ],
        zset([(2_i64, 1), (3_i64, 1)]),
    );
    let other = Stream::from_prefix(
        vec![
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (3_i64, 1)]),
        ],
        zset([(1_i64, 1), (3_i64, 1)]),
    );
    let set_stream = distinct_zset(&bag);
    let indexed_stream = arrange_by(&bag, |value| Some(value % 2));
    let lookup_stream = lookup_index(&indexed_stream, 1_i64);
    let indexed_joined = join_indexed(
        &indexed_stream,
        &arrange_by(&other, |value| Some(value % 2)),
        |key, left, right| (*key, *left, *right),
    );

    let lowered_set = lower_set(table.clone(), "semantic_set", &set_stream)
        .await
        .expect("lower set execution");
    let lowered_indexed = lower_indexed(table.clone(), "semantic_indexed", &indexed_stream)
        .await
        .expect("lower indexed execution");
    let lowered_lookup = lower_zset(table.clone(), "semantic_lookup", &lookup_stream)
        .await
        .expect("lower indexed lookup");
    let lowered_indexed_join = lower_zset(table, "semantic_indexed_join", &indexed_joined)
        .await
        .expect("lower indexed join");

    assert_eq!(
        lowered_set
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered set execution"),
        observe(&set_stream.lift("set_to_zset", |value| value.to_zset()), 4)
    );
    assert_eq!(
        lowered_indexed
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered indexed execution"),
        observe(
            &indexed_stream.lift("indexed_to_pairs", |value| value.as_pairs()),
            4,
        )
    );
    assert_eq!(
        lowered_lookup
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered indexed lookup"),
        observe(&lookup_stream, 4)
    );
    assert_eq!(
        lowered_indexed_join
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered indexed join"),
        observe(&indexed_joined, 4)
    );
}

#[tokio::test]
async fn lowered_nested_flat_map_unnest_and_window_execution_match_reference() {
    let db = build_db().await;
    let table = build_table(db.clone());

    let nested = Stream::from_prefix(
        vec![
            zset::<(i64, ZSet<i64>)>([]),
            zset([((1_i64, zset([(10_i64, 1)])), 1)]),
            zset([
                ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 1),
                ((2_i64, zset([(30_i64, 2)])), 1),
            ]),
            zset([
                ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 1),
                ((2_i64, zset([(30_i64, 2)])), 1),
            ]),
        ],
        zset([
            ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 1),
            ((2_i64, zset([(30_i64, 2)])), 1),
        ]),
    );
    let flat_mapped = flat_map_zset(
        &Stream::from_prefix(
            vec![
                zset([(1_i64, 1)]),
                zset([(1_i64, 1), (2_i64, 1)]),
                zset([(2_i64, 1)]),
            ],
            zset([(2_i64, 1)]),
        ),
        |value| [(*value * 10, 1), (*value * 10 + 1, -1)],
    );
    let unnested = unnest_zset(&nested);

    let lowered_nested = lower_zset(table.clone(), "semantic_nested", &nested)
        .await
        .expect("lower nested execution");
    let lowered_flat_map = lower_zset(table.clone(), "semantic_flat_map", &flat_mapped)
        .await
        .expect("lower flat_map execution");
    let lowered_unnest = lower_zset(table.clone(), "semantic_unnest", &unnested)
        .await
        .expect("lower unnest execution");

    assert_eq!(
        lowered_nested
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered nested execution"),
        observe(&nested, 4)
    );
    assert_eq!(
        lowered_flat_map
            .collect_snapshot_prefix(3)
            .await
            .expect("read lowered flat_map execution"),
        observe(&flat_mapped, 3)
    );
    assert_eq!(
        lowered_unnest
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered unnest execution"),
        observe(&unnested, 4)
    );

    let window_input = Stream::from_prefix(
        vec![
            zset::<Event>([]),
            zset([(event(1, 0, 5), 1)]),
            zset([(event(1, 0, 5), 1), (event(1, 4, 7), 1)]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
            ]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
            ]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
            ]),
        ],
        zset([
            (event(1, 0, 5), 1),
            (event(1, 4, 7), 1),
            (event(1, 10, 9), 1),
        ]),
    );
    let windows = sliding_window_aggregate(
        &window_input,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
        5,
    );
    let delta_windows = differentiate(&windows);
    let lowered_windows = lower_zset(table.clone(), "semantic_windows", &windows)
        .await
        .expect("lower window execution");

    assert_eq!(
        lowered_windows
            .collect_snapshot_prefix(6)
            .await
            .expect("read lowered window snapshot execution"),
        observe(&windows, 6)
    );
    assert_eq!(
        lowered_windows
            .collect_delta_prefix(6)
            .await
            .expect("read lowered window delta execution"),
        observe(&delta_windows, 6)
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_windows".to_string(),
            version: 0,
        },
    });
    let mut reopened =
        RuntimeStream::with_table(table, "semantic_windows".to_string(), snapshot_group)
            .await
            .expect("reopen window snapshot stream");
    assert_eq!(
        collect_runtime_zset_prefix::<(Window<i64>, i64)>(build_table(db), &mut reopened, 6)
            .await
            .expect("read reopened window snapshot execution"),
        observe(&windows, 6)
    );
}
