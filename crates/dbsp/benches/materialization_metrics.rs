use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::memory::InMemory;
use slatedb::Db;

use dbsp::collections::zset::CompactionPolicy;
use dbsp::handles::ZSetHandle;
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::util::materialize_zset_handle;
use dbsp::stream::{StreamRetention, ZSetStream};

struct MaterializeState {
    table: Arc<dyn KeyValueTable>,
    dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
    handle: ZSetHandle,
}

struct GrowthState {
    zset: ZSetStream<Vec<u8>>,
}

struct RestartState {
    table: Arc<dyn KeyValueTable>,
    namespace: String,
}

async fn build_stream(name: &str) -> (ZSetStream<Vec<u8>>, Arc<dyn KeyValueTable>) {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, store).await.expect("open slate db"));
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    let namespace = format!("{name}/stream");
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .expect("build dictionary"),
    );
    let mut zset = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .expect("create zset stream");
    zset.set_compaction_policy(CompactionPolicy::disabled());
    (zset, table)
}

async fn build_materialize_state(name: &str, ticks: usize) -> MaterializeState {
    let (mut zset, table) = build_stream(&format!("{name}/materialize_{ticks}")).await;
    let mut handle = zset.current_handle().clone();
    for tick in 0..ticks {
        let key = format!("key_{tick}").into_bytes();
        zset.add_delta(key, 1);
        handle = zset.flush().await.expect("flush snapshot");
    }
    MaterializeState {
        table,
        dict_cache: HashMap::new(),
        handle,
    }
}

async fn build_growth_state(name: &str, ticks: usize) -> GrowthState {
    let (mut zset, _table) = build_stream(&format!("{name}/growth_{ticks}")).await;
    for tick in 0..ticks {
        let key = format!("key_{tick}").into_bytes();
        zset.add_delta(key, 1);
        zset.flush().await.expect("flush snapshot");
    }
    GrowthState { zset }
}

async fn build_restart_state(name: &str, ticks: usize) -> RestartState {
    let namespace = format!("{name}/restart_{ticks}");
    let (mut zset, table) = build_stream(&namespace).await;
    for tick in 0..ticks {
        let key = format!("key_{tick}").into_bytes();
        zset.add_delta(key, 1);
        zset.flush().await.expect("flush snapshot");
    }
    RestartState { table, namespace }
}

async fn run_materialize(mut state: MaterializeState) {
    let materialized = materialize_zset_handle::<Vec<u8>>(
        state.table.clone(),
        &mut state.dict_cache,
        &state.handle,
    )
    .await
    .expect("materialize handle");
    criterion::black_box(materialized.len());
}

async fn run_chain_stats(state: &mut GrowthState) {
    let stats = state
        .zset
        .versioned()
        .chain_stats()
        .await
        .expect("read chain stats");
    criterion::black_box((
        stats.version_count,
        stats.segment_count,
        stats.max_bucket_segment_count,
    ));
}

async fn run_restart_recovery(state: RestartState) {
    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(state.table.clone(), state.namespace.clone(), None)
            .await
            .expect("reopen dictionary"),
    );
    let zset = ZSetStream::new(
        dict,
        state.table.clone(),
        state.namespace.clone(),
        StreamRetention::None,
    )
    .await
    .expect("reopen zset stream");
    let restored = zset
        .latest_view()
        .materialize()
        .await
        .expect("materialize reopened state");
    criterion::black_box(restored.len());
}

fn bench_materialization_latency(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    let mut group = c.benchmark_group("materialization_latency");
    for ticks in [100_usize, 1_000, 5_000] {
        group.throughput(Throughput::Elements(ticks as u64));
        group.bench_function(BenchmarkId::new("materialize_latest", ticks), |b| {
            b.iter_batched(
                || runtime.block_on(build_materialize_state("bench_materialize", ticks)),
                |state| runtime.block_on(run_materialize(state)),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_storage_growth(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    let mut group = c.benchmark_group("storage_growth");
    for ticks in [100_usize, 1_000, 5_000] {
        group.throughput(Throughput::Elements(ticks as u64));
        group.bench_function(BenchmarkId::new("chain_stats", ticks), |b| {
            b.iter_batched(
                || runtime.block_on(build_growth_state("bench_growth", ticks)),
                |mut state| runtime.block_on(run_chain_stats(&mut state)),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_restart_recovery(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    let mut group = c.benchmark_group("restart_recovery_latency");
    for ticks in [100_usize, 1_000, 5_000] {
        group.throughput(Throughput::Elements(ticks as u64));
        group.bench_function(BenchmarkId::new("reopen_and_materialize", ticks), |b| {
            b.iter_batched(
                || runtime.block_on(build_restart_state("bench_restart", ticks)),
                |state| runtime.block_on(run_restart_recovery(state)),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_materialization_latency,
    bench_storage_growth,
    bench_restart_recovery
);
criterion_main!(benches);
