use super::*;

pub(super) fn bench_update_model_indexed(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = delta_schema();
    let mut group = c.benchmark_group("update_model_indexed");

    for &batch_size in MODEL_BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let key_cardinality = (batch_size / KEY_CARDINALITY_DIVISOR).max(1) as i64;
        let initial = initial_keyed_updates(batch_size, key_cardinality);
        let updates_0_to_1 = transition_keyed_updates(batch_size, key_cardinality, 0, 1);
        let updates_1_to_0 = transition_keyed_updates(batch_size, key_cardinality, 1, 0);
        let coalescing_probe = coalescing_probe_updates(batch_size, key_cardinality);
        let deltas_0_to_1 = strip_keyed_updates(&updates_0_to_1);
        let arrow_delta_bytes = encode_arrow_ipc_delta_batch(Arc::clone(&schema), &deltas_0_to_1);

        println!(
            "update_model_size_report,batch_size={batch_size},arrow_delta_total_bytes={}",
            arrow_delta_bytes.len()
        );
        let indexed_amp_table = open_table(
            &runtime,
            next_db_name("bench-arrow-indexed-amp", batch_size),
        );
        let indexed_amp_namespace = next_namespace("arrow_indexed_amp");
        let indexed_put_table = open_table(
            &runtime,
            next_db_name("bench-arrow-indexed-put", batch_size),
        );
        let indexed_put_namespace = next_namespace("arrow_indexed_put");
        let overlay_amp_table = open_table(&runtime, next_db_name("bench-overlay-amp", batch_size));
        let overlay_amp_prefix = format!(
            "bench/overlay-amp/{batch_size}/{}",
            NS_ID.fetch_add(1, Ordering::Relaxed)
        );
        let (
            arrow_indexed_read_amp,
            arrow_indexed_put_metrics,
            overlay_read_amp,
            cold_lookup,
            warm_lookup,
        ) = runtime.block_on(async {
            let put_index = IndexedBatchZSet::new(indexed_put_table, indexed_put_namespace);
            let indexed_put_metrics = put_index
                .apply_deltas_with_stats(coalescing_probe.iter().cloned())
                .await
                .expect("apply coalescing probe updates");

            let index = IndexedBatchZSet::new(indexed_amp_table, indexed_amp_namespace);
            index
                .apply_deltas(initial.iter().cloned())
                .await
                .expect("seed Arrow-indexed zset for read amplification");
            index
                .apply_deltas(updates_0_to_1.iter().cloned())
                .await
                .expect("apply Arrow-indexed zset updates for read amplification");
            let indexed_read_amp = index
                .estimated_read_amplification_for_key(&0_i64)
                .await
                .expect("estimate Arrow-indexed read amplification");

            overlay_append(
                overlay_amp_table.clone(),
                Arc::clone(&schema),
                &overlay_amp_prefix,
                1,
                &initial,
            )
            .await;
            overlay_append(
                overlay_amp_table.clone(),
                Arc::clone(&schema),
                &overlay_amp_prefix,
                2,
                &updates_0_to_1,
            )
            .await;
            let mut decode_cache = OverlayDecodeCache::new(OVERLAY_DECODE_CACHE_CAPACITY);
            let cold_lookup = overlay_lookup_with_cache(
                overlay_amp_table.clone(),
                &overlay_amp_prefix,
                0,
                &mut decode_cache,
            )
            .await;
            let warm_lookup = overlay_lookup_with_cache(
                overlay_amp_table.clone(),
                &overlay_amp_prefix,
                0,
                &mut decode_cache,
            )
            .await;
            let overlay_read_amp =
                overlay_read_amplification(overlay_amp_table, &overlay_amp_prefix, 0).await;
            (
                indexed_read_amp,
                indexed_put_metrics,
                overlay_read_amp,
                cold_lookup.metrics,
                warm_lookup.metrics,
            )
        });
        println!(
            "update_model_read_amp_report,batch_size={batch_size},arrow_indexed_read_amp={arrow_indexed_read_amp},overlay_read_amp={overlay_read_amp}"
        );
        println!(
            "update_model_put_report,batch_size={batch_size},input_non_zero={},persisted_records={},write_reduction={:.2}",
            arrow_indexed_put_metrics.non_zero_input_records,
            arrow_indexed_put_metrics.persisted_records,
            if arrow_indexed_put_metrics.non_zero_input_records == 0 {
                0.0
            } else {
                arrow_indexed_put_metrics.persisted_records as f64
                    / arrow_indexed_put_metrics.non_zero_input_records as f64
            }
        );
        println!(
            "update_model_overlay_decode_report,batch_size={batch_size},index_entries_cold={},segments_cold={},decoded_rows_cold={},cache_hits_warm={},cache_misses_warm={},decoded_rows_warm={},index_entries_warm={},segments_warm={}",
            cold_lookup.index_entries_scanned,
            cold_lookup.unique_segments,
            cold_lookup.decoded_rows,
            warm_lookup.cache_hits,
            warm_lookup.cache_misses,
            warm_lookup.decoded_rows,
            warm_lookup.index_entries_scanned,
            warm_lookup.unique_segments,
        );

        group.bench_function(
            BenchmarkId::new("arrow_indexed_zset_apply_toggle", batch_size),
            |b| {
                let table = open_table(
                    &runtime,
                    next_db_name("bench-arrow-indexed-zset", batch_size),
                );
                let namespace = next_namespace("arrow_indexed_zset");
                let index = IndexedBatchZSet::new(table, namespace);
                runtime.block_on(async {
                    index
                        .apply_deltas(initial.iter().cloned())
                        .await
                        .expect("seed Arrow-indexed zset");
                });

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        index
                            .apply_deltas(updates.iter().cloned())
                            .await
                            .expect("apply Arrow-indexed zset toggle");
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_indexed_zset_apply_lookup_hot_key", batch_size),
            |b| {
                let table = open_table(
                    &runtime,
                    next_db_name("bench-arrow-indexed-hot", batch_size),
                );
                let namespace = next_namespace("arrow_indexed_zset_hot");
                let index = IndexedBatchZSet::new(table, namespace);
                runtime.block_on(async {
                    index
                        .apply_deltas(initial.iter().cloned())
                        .await
                        .expect("seed Arrow-indexed zset");
                });

                let lookup_key = 0_i64;
                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        index
                            .apply_deltas(updates.iter().cloned())
                            .await
                            .expect("apply Arrow-indexed zset toggle");
                        let values = index
                            .values_for_key(&lookup_key)
                            .await
                            .expect("lookup Arrow-indexed zset key");
                        black_box(values);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_overlay_append_toggle", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-overlay", batch_size));
                let prefix = format!(
                    "bench/overlay/{batch_size}/{}",
                    NS_ID.fetch_add(1, Ordering::Relaxed)
                );
                let mut next_segment_id = 1_u64;
                runtime.block_on(overlay_append(
                    table.clone(),
                    Arc::clone(&schema),
                    &prefix,
                    next_segment_id,
                    &initial,
                ));
                next_segment_id += 1;

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(overlay_append(
                        table.clone(),
                        Arc::clone(&schema),
                        &prefix,
                        next_segment_id,
                        updates,
                    ));
                    next_segment_id += 1;
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_overlay_append_lookup_hot_key", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-overlay-hot", batch_size));
                let prefix = format!(
                    "bench/overlay-hot/{batch_size}/{}",
                    NS_ID.fetch_add(1, Ordering::Relaxed)
                );
                let mut next_segment_id = 1_u64;
                runtime.block_on(overlay_append(
                    table.clone(),
                    Arc::clone(&schema),
                    &prefix,
                    next_segment_id,
                    &initial,
                ));
                next_segment_id += 1;

                let lookup_key = 0_i64;
                let mut flip = false;
                let mut segment_cache = OverlayDecodeCache::new(OVERLAY_DECODE_CACHE_CAPACITY);
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    segment_cache.invalidate(next_segment_id);
                    runtime.block_on(overlay_append(
                        table.clone(),
                        Arc::clone(&schema),
                        &prefix,
                        next_segment_id,
                        updates,
                    ));
                    next_segment_id += 1;
                    runtime.block_on(async {
                        let result = overlay_lookup_with_cache(
                            table.clone(),
                            &prefix,
                            lookup_key,
                            &mut segment_cache,
                        )
                        .await;
                        black_box(result.values);
                    });
                });
            },
        );
    }

    group.finish();
}
