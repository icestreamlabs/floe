use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use object_store::memory::InMemory;
use slatedb::Db;

use dbsp::LogicalWorkSnapshot;
use dbsp::collections::IndexedBatchZSet;
use dbsp::collections::zset::{SegmentRecord, VersionedZSet};
use dbsp::handles::ZSetHandle;
use dbsp::operators::join::JoinOp;
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::runtime::DeltaOperator;

static BENCH_COUNTER: AtomicU64 = AtomicU64::new(1);

struct JoinEvidenceState {
    op: JoinOp<i64, i64, i64, i64>,
    left_delta: ZSetHandle,
    right_empty: ZSetHandle,
}

type RowKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;
type BatchJoinKeyExtractor<T, K> = Arc<dyn Fn(&[(T, i64)]) -> Vec<(K, T, i64)> + Send + Sync>;

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
        let id = dict_batch
            .intern(key)
            .await
            .expect("intern key for evidence bench");
        buckets
            .entry(bucket_for(id))
            .or_default()
            .push((id, *delta));
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

fn join_key(row: &i64) -> Option<i64> {
    Some(*row / 1_000)
}

fn batch_join_key<T, K>(key_extractor: RowKeyExtractor<T, K>) -> BatchJoinKeyExtractor<T, K>
where
    T: Clone + 'static,
    K: 'static,
{
    Arc::new(move |deltas: &[(T, i64)]| {
        deltas
            .iter()
            .filter_map(|(row, weight)| key_extractor(row).map(|key| (key, row.clone(), *weight)))
            .collect()
    })
}

async fn build_join_state(unrelated_history: usize, affected_fanout: usize) -> JoinEvidenceState {
    let id = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let prefix = format!("incrementality_evidence_{unrelated_history}_{affected_fanout}_{id}");
    let db = build_db(&prefix).await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    let left_ns = format!("{prefix}_left_stream");
    let right_ns = format!("{prefix}_right_stream");
    let output_ns = format!("{prefix}_output");

    let left_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), left_ns.clone(), None)
            .await
            .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), right_ns.clone(), None)
            .await
            .expect("right dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<i64>::with_table(table.clone(), output_ns.clone(), None)
            .await
            .expect("output dict"),
    );
    let output = VersionedZSet::new(output_dict, table.clone(), output_ns)
        .await
        .expect("output zset");

    let mut op = JoinOp::new_batch(
        IndexedBatchZSet::new(table.clone(), format!("{prefix}_left_index")),
        IndexedBatchZSet::new(table.clone(), format!("{prefix}_right_index")),
        batch_join_key(Arc::new(join_key)),
        batch_join_key(Arc::new(join_key)),
        Arc::new(|left: &i64, right: &i64| join_key(left) == join_key(right)),
        Arc::new(|left: &i64, right: &i64| left + right),
        table.clone(),
        output,
        None,
    );

    let mut right_seed = (0..unrelated_history)
        .map(|idx| (1_000_000_000 + (idx as i64 * 1_000), 1))
        .collect::<Vec<_>>();
    right_seed.extend((0..affected_fanout).map(|idx| (7_000 + idx as i64, 1)));
    let right_seed = stage_version(right_dict.clone(), table.clone(), &right_ns, &right_seed).await;
    let left_empty = ZSetHandle {
        ns: left_ns.clone(),
        version: 0,
    };
    op.on_step(0, &[left_empty, right_seed])
        .await
        .expect("seed join");

    let left_delta = stage_version(left_dict, table, &left_ns, &[(7_999, 1)]).await;
    let right_empty = ZSetHandle {
        ns: right_ns,
        version: 0,
    };
    JoinEvidenceState {
        op,
        left_delta,
        right_empty,
    }
}

async fn run_join_evidence(mut state: JoinEvidenceState) -> LogicalWorkSnapshot {
    state
        .op
        .on_step(1, &[state.left_delta, state.right_empty])
        .await
        .expect("join evidence step");
    state.op.logical_work().expect("join evidence logical work")
}

fn print_join_evidence(label: &str, history: usize, fanout: usize, work: LogicalWorkSnapshot) {
    eprintln!(
        "incrementality_evidence {label} history={history} fanout={fanout} \
         input_delta_rows={} right_state_rows_examined={} output_delta_rows={} \
         state_lookup_keys={} index_segments_examined={} index_postings_examined={} \
         state_full_scan_count={} cache_rebuild_rows={}",
        work.input_delta_rows,
        work.right_state_rows_examined,
        work.output_delta_rows,
        work.state_lookup_keys,
        work.index_segments_examined,
        work.index_postings_examined,
        work.state_full_scan_count,
        work.cache_rebuild_rows,
    );
}

fn bench_incrementality_evidence(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    let mut group = c.benchmark_group("incrementality_evidence_join");

    for history in [1_000_usize, 10_000] {
        let fanout = 4;
        let sample = runtime.block_on(run_join_evidence(
            runtime.block_on(build_join_state(history, fanout)),
        ));
        print_join_evidence("fixed_fanout", history, fanout, sample);
        group.bench_function(BenchmarkId::new("history_fixed_fanout", history), |b| {
            b.iter_batched(
                || runtime.block_on(build_join_state(history, fanout)),
                |state| runtime.block_on(run_join_evidence(state)),
                BatchSize::SmallInput,
            );
        });
    }

    for fanout in [1_usize, 8, 64] {
        let history = 1_000;
        let sample = runtime.block_on(run_join_evidence(
            runtime.block_on(build_join_state(history, fanout)),
        ));
        print_join_evidence("fixed_history", history, fanout, sample);
        group.bench_function(BenchmarkId::new("fanout_fixed_history", fanout), |b| {
            b.iter_batched(
                || runtime.block_on(build_join_state(history, fanout)),
                |state| runtime.block_on(run_join_evidence(state)),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_incrementality_evidence);
criterion_main!(benches);
