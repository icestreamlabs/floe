use super::*;

pub(super) fn bench_update_model_gc_pressure(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = delta_schema();
    let mut group = c.benchmark_group("update_model_gc_pressure");

    for &batch_size in MODEL_BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let key_cardinality = (batch_size / KEY_CARDINALITY_DIVISOR).max(1) as i64;
        let initial = strip_keyed_updates(&initial_keyed_updates(batch_size, key_cardinality));
        let updates_0_to_1 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 0, 1));
        let updates_1_to_0 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 1, 0));

        let metric_table = open_table(&runtime, next_db_name("bench-gc-metric", batch_size));
        let metric_namespace = next_namespace("gc_metric");
        let metric_segment_store =
            ArrowSegmentStore::new(metric_table.clone(), metric_namespace.clone());
        let metric_manifest_store =
            ManifestStore::<DataManifest>::data(metric_table.clone(), metric_namespace.clone());
        let metric_gc = GcService::new(
            metric_table.clone(),
            metric_namespace.clone(),
            GcPolicy {
                grace_period: Duration::ZERO,
            },
        );
        let metric_deltas = updates_0_to_1.clone();
        let metric_sweep = runtime.block_on(async {
            let batch = deltas_to_record_batch(Arc::clone(&schema), &initial);
            metric_segment_store
                .write_segment(
                    1,
                    Arc::clone(&schema),
                    &[batch],
                    segment_stats_for_deltas(&initial),
                )
                .await
                .expect("write metric seed segment");
            metric_manifest_store
                .publish_manifest(&DataManifest {
                    version: 1,
                    base: None,
                    reference_count: 1,
                    statistics: manifest_stats(1, initial.len(), 0),
                    segments: vec![1],
                })
                .await
                .expect("publish metric seed manifest");

            let churn_batch = deltas_to_record_batch(Arc::clone(&schema), &metric_deltas);
            let churn_payload = encode_arrow_ipc_delta_batch(Arc::clone(&schema), &metric_deltas);
            metric_segment_store
                .write_segment(
                    2,
                    Arc::clone(&schema),
                    &[churn_batch],
                    segment_stats_for_deltas(&metric_deltas),
                )
                .await
                .expect("write metric churn segment");
            metric_manifest_store
                .publish_manifest(&DataManifest {
                    version: 2,
                    base: None,
                    reference_count: 1,
                    statistics: manifest_stats(1, metric_deltas.len(), churn_payload.len() as u64),
                    segments: vec![2],
                })
                .await
                .expect("publish metric churn manifest");
            metric_gc.sweep_once().await.expect("metric GC sweep")
        });
        println!(
            "update_model_gc_report,batch_size={batch_size},gc_marked={},gc_deleted={}",
            metric_sweep.marked, metric_sweep.deleted
        );

        group.bench_function(
            BenchmarkId::new("gc_manifest_churn_sweep", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-gc-churn", batch_size));
                let namespace = next_namespace("gc_churn");
                let segment_store = ArrowSegmentStore::new(table.clone(), namespace.clone());
                let manifest_store =
                    ManifestStore::<DataManifest>::data(table.clone(), namespace.clone());
                let gc = GcService::new(
                    table,
                    namespace,
                    GcPolicy {
                        grace_period: Duration::ZERO,
                    },
                );
                let mut segment_id = 1_u64;
                let mut version = 1_u64;
                runtime.block_on(async {
                    let seed_batch = deltas_to_record_batch(Arc::clone(&schema), &initial);
                    segment_store
                        .write_segment(
                            segment_id,
                            Arc::clone(&schema),
                            &[seed_batch],
                            segment_stats_for_deltas(&initial),
                        )
                        .await
                        .expect("write churn seed segment");
                    manifest_store
                        .publish_manifest(&DataManifest {
                            version,
                            base: None,
                            reference_count: 1,
                            statistics: manifest_stats(1, initial.len(), 0),
                            segments: vec![segment_id],
                        })
                        .await
                        .expect("publish churn seed manifest");
                });

                let mut flip = false;
                b.iter(|| {
                    let deltas = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    segment_id = segment_id.saturating_add(1);
                    version = version.saturating_add(1);
                    runtime.block_on(async {
                        let churn_batch = deltas_to_record_batch(Arc::clone(&schema), deltas);
                        let churn_payload =
                            encode_arrow_ipc_delta_batch(Arc::clone(&schema), deltas);
                        segment_store
                            .write_segment(
                                segment_id,
                                Arc::clone(&schema),
                                &[churn_batch],
                                segment_stats_for_deltas(deltas),
                            )
                            .await
                            .expect("write churn segment");
                        manifest_store
                            .publish_manifest(&DataManifest {
                                version,
                                base: None,
                                reference_count: 1,
                                statistics: manifest_stats(
                                    1,
                                    deltas.len(),
                                    churn_payload.len() as u64,
                                ),
                                segments: vec![segment_id],
                            })
                            .await
                            .expect("publish churn manifest");
                        let sweep = gc.sweep_once().await.expect("run churn sweep");
                        black_box(sweep.deleted);
                    });
                });
            },
        );
    }

    group.finish();
}
