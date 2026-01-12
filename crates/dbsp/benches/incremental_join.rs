use std::collections::BTreeMap;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use object_store::memory::InMemory;
use slatedb::Db;

use dbsp::collections::IndexedZSet;
use dbsp::collections::zset::{SegmentRecord, VersionedZSet};
use dbsp::handles::ZSetHandle;
use dbsp::operators::join::JoinOp;
use dbsp::relation_state::RelationState;
use dbsp::stream::runtime::DeltaOperator;
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::{KeyValueTable, SlateTable};

struct JoinBenchState {
    op: JoinOp<i64, i64, i64, i64>,
    left_delta: ZSetHandle,
    right_delta: ZSetHandle,
}

async fn build_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open slate db"))
}

async fn stage_version<K>(
    dict: Arc<Dictionary<K>>,
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
    deltas: &[(K, i64)],
) -> ZSetHandle
where
    K: rkyv::Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'rk> rkyv::Serialize<dbsp::storage::encoding::RkyvSerializer<'rk>>,
    K::Archived: rkyv::Deserialize<K, dbsp::storage::encoding::RkyvDeserializer>
        + for<'a> rkyv::bytecheck::CheckBytes<dbsp::storage::encoding::RkyvValidator<'a>>,
{
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        let id = dict_batch.intern(key).await.expect("intern key for join");
        buckets.entry(bucket_for(id)).or_default().push((id, *delta));
    }
    drop(dict_batch);

    let mut segments = Vec::new();
    for (bucket, mut bucket_deltas) in buckets {
        bucket_deltas.retain(|(_, delta)| *delta != 0);
        if bucket_deltas.is_empty() {
            continue;
        }
        bucket_deltas.sort_by_key(|(id, _)| *id);
        segments.push(SegmentRecord {
            id: 0,
            bucket,
            deltas: bucket_deltas,
        });
    }

    let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
        .await
        .expect("build versioned");
    let version = versioned
        .create_version_with_base(segments, None)
        .await
        .expect("create version");
    versioned.handle_for_version(version)
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

async fn build_state(base_size: usize, delta_size: usize) -> JoinBenchState {
    let db = build_db(&format!("join-bench-{base_size}")).await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "bench_left_stream", None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "bench_right_stream", None)
            .await
            .expect("right dict"),
    );

    let left_state = RelationState::empty(table.clone(), "bench_left_state".to_string())
        .await
        .expect("left state");
    let right_state = RelationState::empty(table.clone(), "bench_right_state".to_string())
        .await
        .expect("right state");
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), "bench_output", None)
            .await
            .expect("output dict"),
    );
    let output = VersionedZSet::new(output_dict, table.clone(), "bench_output".to_string())
        .await
        .expect("output zset");
    let left_index = IndexedZSet::new(table.clone(), "bench_left_index");
    let right_index = IndexedZSet::new(table.clone(), "bench_right_index");

    let mut op = JoinOp::new(
        left_state,
        right_state,
        left_index,
        right_index,
        Arc::new(|value: &i64| Some(*value)),
        Arc::new(|value: &i64| Some(*value)),
        Arc::new(|l: &i64, r: &i64| l == r),
        Arc::new(|l: &i64, r: &i64| l + r),
        table.clone(),
        output,
        None,
    );

    let base_left: Vec<(i64, i64)> = (0..base_size as i64).map(|idx| (idx, 1)).collect();
    let base_right: Vec<(i64, i64)> = (0..base_size as i64).map(|idx| (idx, 1)).collect();
    let left_base = stage_version(
        left_dict.clone(),
        table.clone(),
        "bench_left_stream",
        &base_left,
    )
    .await;
    let right_base = stage_version(
        right_dict.clone(),
        table.clone(),
        "bench_right_stream",
        &base_right,
    )
    .await;
    let _ = op
        .on_step(0, &[left_base, right_base])
        .await
        .expect("bootstrap join");

    let delta_left: Vec<(i64, i64)> = (0..delta_size as i64).map(|idx| (idx, 1)).collect();
    let left_delta = stage_version(
        left_dict.clone(),
        table.clone(),
        "bench_left_stream",
        &delta_left,
    )
    .await;
    let right_delta = ZSetHandle {
        ns: "bench_right_stream".to_string(),
        version: 0,
    };

    JoinBenchState {
        op,
        left_delta,
        right_delta,
    }
}

async fn run_incremental_join(mut state: JoinBenchState) {
    let _ = state
        .op
        .on_step(1, &[state.left_delta, state.right_delta])
        .await
        .expect("join step");
}

fn bench_incremental_join(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    let mut group = c.benchmark_group("incremental_join");
    for base_size in [1_000_usize, 10_000] {
        let delta_size = 100;
        group.bench_function(
            BenchmarkId::new("delta_left", base_size),
            |b| {
                b.iter_batched(
                    || runtime.block_on(build_state(base_size, delta_size)),
                    |state| runtime.block_on(run_incremental_join(state)),
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_incremental_join);
criterion_main!(benches);
