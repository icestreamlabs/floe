use super::*;

pub(super) fn bench_update_model_dictionary(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("update_model_dictionary");

    for &batch_size in MODEL_BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let baseline_rows: Vec<BenchRow> = (0..batch_size)
            .map(|entity| BenchRow::for_entity(entity as i64, 0))
            .collect();

        group.bench_function(
            BenchmarkId::new("dictionary_intern_existing_rows", batch_size),
            |b| {
                let table = open_table(
                    &runtime,
                    next_db_name("bench-dictionary-existing", batch_size),
                );
                let namespace = next_namespace("dictionary_existing");
                let dict = runtime.block_on(async {
                    Arc::new(
                        Dictionary::<BenchRow>::with_table(table, namespace, None)
                            .await
                            .expect("create dictionary"),
                    )
                });
                runtime.block_on(async {
                    let mut batch = dict.batch();
                    for row in &baseline_rows {
                        batch.intern(row).await.expect("seed dictionary row");
                    }
                });

                b.iter(|| {
                    runtime.block_on(async {
                        let mut batch = dict.batch();
                        for row in &baseline_rows {
                            let id = batch.intern(row).await.expect("intern existing row");
                            black_box(id);
                        }
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("dictionary_intern_new_rows", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-dictionary-new", batch_size));
                let namespace = next_namespace("dictionary_new");
                let dict = runtime.block_on(async {
                    Arc::new(
                        Dictionary::<BenchRow>::with_table(table, namespace, None)
                            .await
                            .expect("create dictionary"),
                    )
                });
                let mut epoch = 0_i64;

                b.iter(|| {
                    epoch += 1;
                    runtime.block_on(async {
                        let mut batch = dict.batch();
                        for entity in 0..batch_size {
                            let row = BenchRow::for_entity(entity as i64, epoch);
                            let id = batch.intern(&row).await.expect("intern new row");
                            black_box(id);
                        }
                    });
                });
            },
        );
    }

    group.finish();
}
