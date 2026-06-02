use super::*;

#[tokio::test]
async fn lowered_guarded_monotonic_collection_recursion_matches_reference_and_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let reachability = reachability_stream();
    let delta_reachability = differentiate(&reachability);

    let lowered = lower_zset(table.clone(), "semantic_reachability", &reachability)
        .await
        .expect("lower guarded monotonic collection recursion");
    assert_eq!(
        lowered
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered monotonic recursion snapshot prefix"),
        observe(&reachability, 4)
    );
    assert_eq!(
        lowered
            .collect_delta_prefix(4)
            .await
            .expect("read lowered monotonic recursion delta prefix"),
        observe(&delta_reachability, 4)
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_reachability".to_string(),
            version: 0,
        },
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_reachability/delta".to_string(),
            version: 0,
        },
    });
    let mut reopened_snapshot = RuntimeStream::with_table(
        table.clone(),
        "semantic_reachability".to_string(),
        snapshot_group,
    )
    .await
    .expect("reopen monotonic recursion snapshot stream");
    let mut reopened_delta = RuntimeStream::with_table(
        table,
        "semantic_reachability/delta".to_string(),
        delta_group,
    )
    .await
    .expect("reopen monotonic recursion delta stream");

    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(
            build_table(db.clone()),
            &mut reopened_snapshot,
            4,
        )
        .await
        .expect("read reopened monotonic recursion snapshot prefix"),
        observe(&reachability, 4)
    );
    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(build_table(db), &mut reopened_delta, 4)
            .await
            .expect("read reopened monotonic recursion delta prefix"),
        observe(&delta_reachability, 4)
    );
}

#[tokio::test]
async fn lowered_guarded_non_monotonic_collection_recursion_matches_reference_and_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let oscillating = oscillating_collection_stream();
    let delta_oscillating = differentiate(&oscillating);

    let lowered = lower_zset(table.clone(), "semantic_oscillating", &oscillating)
        .await
        .expect("lower guarded non-monotonic collection recursion");
    assert_eq!(
        lowered
            .collect_snapshot_prefix(6)
            .await
            .expect("read lowered non-monotonic recursion snapshot prefix"),
        observe(&oscillating, 6)
    );
    assert_eq!(
        lowered
            .collect_delta_prefix(6)
            .await
            .expect("read lowered non-monotonic recursion delta prefix"),
        observe(&delta_oscillating, 6)
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_oscillating".to_string(),
            version: 0,
        },
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_oscillating/delta".to_string(),
            version: 0,
        },
    });
    let mut reopened_snapshot = RuntimeStream::with_table(
        table.clone(),
        "semantic_oscillating".to_string(),
        snapshot_group,
    )
    .await
    .expect("reopen non-monotonic recursion snapshot stream");
    let mut reopened_delta =
        RuntimeStream::with_table(table, "semantic_oscillating/delta".to_string(), delta_group)
            .await
            .expect("reopen non-monotonic recursion delta stream");

    assert_eq!(
        collect_runtime_zset_prefix::<String>(build_table(db.clone()), &mut reopened_snapshot, 6)
            .await
            .expect("read reopened non-monotonic recursion snapshot prefix"),
        observe(&oscillating, 6)
    );
    assert_eq!(
        collect_runtime_zset_prefix::<String>(build_table(db), &mut reopened_delta, 6)
            .await
            .expect("read reopened non-monotonic recursion delta prefix"),
        observe(&delta_oscillating, 6)
    );
}
