use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use object_store::memory::InMemory;
use slatedb::Db;

use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::util::{compute_delta, materialize_zset_handle};
use dbsp::stream::{StreamRetention, ZSetStream};

struct BenchState {
    zset: ZSetStream<Vec<u8>>,
    table: Arc<dyn KeyValueTable>,
    dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
    keys: Vec<Vec<u8>>,
}

async fn build_state(name: &str, ticks: usize) -> BenchState {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, store).await.expect("open slate db"));
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    let namespace = format!("{name}/stream");
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .expect("build dictionary"),
    );
    let zset = ZSetStream::new(
        dict,
        table.clone(),
        namespace,
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("create zset stream");
    let keys = (0..ticks)
        .map(|idx| format!("key_{idx}").into_bytes())
        .collect();
    BenchState {
        zset,
        table,
        dict_cache: HashMap::new(),
        keys,
    }
}

async fn run_materialize_diff(mut state: BenchState) {
    let mut prev: HashMap<Vec<u8>, i64> = HashMap::new();
    for key in &state.keys {
        state.zset.add_delta(key.clone(), 1);
        let snapshot = state.zset.flush().await.expect("flush snapshot");
        let current = materialize_zset_handle::<Vec<u8>>(
            state.table.clone(),
            &mut state.dict_cache,
            &snapshot,
        )
        .await
        .expect("materialize snapshot");
        let _ = compute_delta(&prev, &current);
        prev = current;
    }
}

async fn run_overlay_delta(mut state: BenchState) {
    for key in &state.keys {
        state.zset.add_delta(key.clone(), 1);
        let (_snapshot, delta_handle) = state.zset.flush_with_delta().await.expect("flush delta");
        let _ = materialize_zset_handle::<Vec<u8>>(
            state.table.clone(),
            &mut state.dict_cache,
            &delta_handle,
        )
        .await
        .expect("materialize delta");
    }
}

fn bench_delta_emission(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    let mut group = c.benchmark_group("delta_emission");
    for ticks in [100_usize, 1_000] {
        group.bench_function(BenchmarkId::new("materialize_diff", ticks), |b| {
            b.iter_batched(
                || runtime.block_on(build_state("bench_materialize", ticks)),
                |state| runtime.block_on(run_materialize_diff(state)),
                BatchSize::SmallInput,
            );
        });
        group.bench_function(BenchmarkId::new("overlay_delta", ticks), |b| {
            b.iter_batched(
                || runtime.block_on(build_state("bench_overlay", ticks)),
                |state| runtime.block_on(run_overlay_delta(state)),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_delta_emission);
criterion_main!(benches);
