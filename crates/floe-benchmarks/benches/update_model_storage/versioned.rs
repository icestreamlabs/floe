use super::*;

pub(super) fn bench_update_model_versioned(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = delta_schema();
    let mut group = c.benchmark_group("update_model_versioned");

    for &batch_size in MODEL_BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let key_cardinality = (batch_size / KEY_CARDINALITY_DIVISOR).max(1) as i64;
        let initial = strip_keyed_updates(&initial_keyed_updates(batch_size, key_cardinality));
        let updates_0_to_1 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 0, 1));
        let updates_1_to_0 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 1, 0));
        let versioned_metrics_table =
            open_table(&runtime, next_db_name("bench-versioned-growth", batch_size));
        let versioned_metrics_namespace = next_namespace("versioned_growth");
        let ledger_metrics_table =
            open_table(&runtime, next_db_name("bench-ledger-growth", batch_size));
        let ledger_metrics_prefix = format!(
            "bench/ledger-growth/{batch_size}/{}",
            NS_ID.fetch_add(1, Ordering::Relaxed)
        );
        let (versioned_growth_bytes, ledger_growth_bytes) = runtime.block_on(async {
            let dict = Arc::new(
                Dictionary::<BenchRow>::with_table(
                    versioned_metrics_table.clone(),
                    versioned_metrics_namespace.clone(),
                    None,
                )
                .await
                .expect("create dictionary for growth metrics"),
            );
            let mut versioned = VersionedZSet::new(
                dict.clone(),
                versioned_metrics_table.clone(),
                versioned_metrics_namespace.clone(),
            )
            .await
            .expect("create versioned zset for growth metrics");
            apply_versioned_deltas(&mut versioned, dict.as_ref(), &initial).await;
            apply_versioned_deltas(&mut versioned, dict.as_ref(), &updates_0_to_1).await;
            let versioned_growth_bytes =
                versioned_storage_bytes(versioned_metrics_table, &versioned_metrics_namespace)
                    .await;

            ledger_append(
                ledger_metrics_table.clone(),
                Arc::clone(&schema),
                &ledger_metrics_prefix,
                1,
                &initial,
            )
            .await;
            ledger_append(
                ledger_metrics_table.clone(),
                Arc::clone(&schema),
                &ledger_metrics_prefix,
                2,
                &updates_0_to_1,
            )
            .await;
            let ledger_growth_bytes =
                prefix_total_bytes(ledger_metrics_table, ledger_metrics_prefix.as_bytes()).await;
            (versioned_growth_bytes, ledger_growth_bytes)
        });
        println!(
            "update_model_growth_report,batch_size={batch_size},versioned_growth_bytes={versioned_growth_bytes},ledger_growth_bytes={ledger_growth_bytes}"
        );

        group.bench_function(
            BenchmarkId::new("versioned_zset_write_materialize_toggle", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-versioned-zset", batch_size));
                let namespace = next_namespace("versioned_zset");
                let dict = runtime.block_on(async {
                    Arc::new(
                        Dictionary::<BenchRow>::with_table(table.clone(), namespace.clone(), None)
                            .await
                            .expect("create dictionary"),
                    )
                });
                let mut versioned = runtime.block_on(async {
                    VersionedZSet::new(dict.clone(), table.clone(), namespace)
                        .await
                        .expect("create versioned zset")
                });
                runtime.block_on(apply_versioned_deltas(
                    &mut versioned,
                    dict.as_ref(),
                    &initial,
                ));

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        apply_versioned_deltas(&mut versioned, dict.as_ref(), updates).await;
                        let materialized = versioned.materialize().await.expect("materialize");
                        black_box(materialized);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new(
                "versioned_zset_write_materialize_toggle_compacted",
                batch_size,
            ),
            |b| {
                let table = open_table(
                    &runtime,
                    next_db_name("bench-versioned-zset-compacted", batch_size),
                );
                let namespace = next_namespace("versioned_zset_compacted");
                let dict = runtime.block_on(async {
                    Arc::new(
                        Dictionary::<BenchRow>::with_table(table.clone(), namespace.clone(), None)
                            .await
                            .expect("create dictionary"),
                    )
                });
                let mut versioned = runtime.block_on(async {
                    VersionedZSet::new(dict.clone(), table.clone(), namespace)
                        .await
                        .expect("create versioned zset")
                });
                runtime.block_on(apply_versioned_deltas(
                    &mut versioned,
                    dict.as_ref(),
                    &initial,
                ));

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        apply_versioned_deltas(&mut versioned, dict.as_ref(), updates).await;
                        if versioned.current_handle().is_some() {
                            let _ = versioned.compact_current().await.expect("compact current");
                        }
                        let materialized = versioned.materialize().await.expect("materialize");
                        black_box(materialized);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_ledger_write_materialize_toggle", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-arrow-ledger", batch_size));
                let prefix = format!(
                    "bench/ledger/{batch_size}/{}",
                    NS_ID.fetch_add(1, Ordering::Relaxed)
                );
                let mut version = 1_u64;
                runtime.block_on(ledger_append(
                    table.clone(),
                    Arc::clone(&schema),
                    &prefix,
                    version,
                    &initial,
                ));

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    version += 1;
                    runtime.block_on(ledger_append(
                        table.clone(),
                        Arc::clone(&schema),
                        &prefix,
                        version,
                        updates,
                    ));
                    runtime.block_on(async {
                        let materialized = ledger_materialize(table.clone(), &prefix).await;
                        black_box(materialized);
                    });
                });
            },
        );
    }

    group.finish();
}
