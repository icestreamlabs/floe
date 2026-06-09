use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use object_store::memory::InMemory;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::Db;

use dbsp::collections::ColumnarI64ZSet;
use dbsp::collections::zset::{SegmentRecord, VersionedZSet};
use dbsp::handles::ZSetHandle;
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::runtime::DeltaOperator;
use dbsp::stream::util::materialize_zset_handle;
use dbsp::{
    CountAggregateOp, CountAggregateRow, CountAggregateSlotKind, CountAggregateSlotUpdate,
    GroupedCountState, RelationState, SlateBackedColumnarCountByKeyOp,
};

static BENCH_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct TickWorkload {
    rows: usize,
    groups: usize,
    ticks: usize,
    seed_multiplier: usize,
}

struct ExistingCountState {
    op: CountAggregateOp<i64, i64, ()>,
    input_dict: Arc<Dictionary<i64>>,
    table: Arc<dyn KeyValueTable>,
    input_ns: String,
    seed_output: Option<ZSetHandle>,
    ticks: Vec<Vec<(i64, i64)>>,
}

struct ColumnarCountState {
    op: SlateBackedColumnarCountByKeyOp,
    input_zset: dbsp::collections::SlateBackedColumnarI64ZSet,
    seed_output: Option<ZSetHandle>,
    ticks: Vec<ColumnarI64ZSet>,
}

async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
    Arc::new(SlateTable::new(db))
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

async fn stage_version<T>(
    dict: Arc<Dictionary<T>>,
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
    deltas: &[(T, i64)],
) -> ZSetHandle
where
    T: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        if *delta == 0 {
            continue;
        }
        let id = dict_batch.intern(key).await.expect("intern input key");
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

    if segments.is_empty() {
        return ZSetHandle {
            ns: namespace.to_string(),
            version: 0,
        };
    }

    let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
        .await
        .expect("build input zset");
    let version = versioned
        .create_version_with_base(segments, None)
        .await
        .expect("create input version");
    versioned.handle_for_version(version)
}

fn seed_tick(workload: &TickWorkload) -> Vec<(i64, i64)> {
    (0..(workload.rows * workload.seed_multiplier))
        .map(|idx| ((idx % workload.groups) as i64, 1))
        .collect()
}

fn semantic_tick(workload: &TickWorkload, tick: usize) -> Vec<(i64, i64)> {
    (0..workload.rows)
        .map(|idx| {
            let key = ((idx + tick * 17) % workload.groups) as i64;
            let weight = match (tick + idx) % 4 {
                0 | 1 => 1,
                _ => -1,
            };
            (key, weight)
        })
        .collect()
}

fn columnar_from_rows(rows: &[(i64, i64)]) -> ColumnarI64ZSet {
    let keys = rows.iter().map(|(key, _)| *key).collect::<Vec<_>>();
    let weights = rows.iter().map(|(_, weight)| *weight).collect::<Vec<_>>();
    ColumnarI64ZSet::from_i64_columns(&["key"], &[keys], weights).expect("columnar input zset")
}

async fn build_existing_count_state(workload: &TickWorkload) -> ExistingCountState {
    let id = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let prefix = format!(
        "existing_count_semantics_{}_{}_{}",
        workload.rows, workload.groups, id
    );
    let table = build_table(&prefix).await;
    let input_ns = format!("{prefix}/input");
    let input_dict = Arc::new(
        Dictionary::<i64>::with_table(Arc::clone(&table), input_ns.clone(), None)
            .await
            .expect("create input dictionary"),
    );
    let state =
        RelationState::<(i64, GroupedCountState)>::empty(table.clone(), format!("{prefix}/state"))
            .await
            .expect("create existing count state");
    let output_dict = Arc::new(
        Dictionary::<(i64, Vec<i64>)>::with_table(table.clone(), format!("{prefix}/output"), None)
            .await
            .expect("create output dictionary"),
    );
    let output = VersionedZSet::new(output_dict, table.clone(), format!("{prefix}/output"))
        .await
        .expect("create output zset");
    let row_evaluator = Arc::new(|deltas: &[(i64, i64)]| {
        deltas
            .iter()
            .map(|(key, weight)| {
                (
                    CountAggregateRow {
                        key: *key,
                        slots: vec![CountAggregateSlotUpdate::Linear(1)],
                    },
                    *weight,
                )
            })
            .collect()
    });
    let mut op = CountAggregateOp::new_batch(
        state,
        table.clone(),
        row_evaluator,
        output,
        vec![CountAggregateSlotKind::Linear],
        None,
    );

    let seed = stage_version(
        Arc::clone(&input_dict),
        Arc::clone(&table),
        &input_ns,
        &seed_tick(workload),
    )
    .await;
    let seed_output = op.on_step(0, &[seed]).await.expect("seed existing count");

    let ticks = (0..workload.ticks)
        .map(|tick| semantic_tick(workload, tick))
        .collect::<Vec<_>>();
    ExistingCountState {
        op,
        input_dict,
        table,
        input_ns,
        seed_output,
        ticks,
    }
}

async fn build_columnar_count_state(workload: &TickWorkload) -> ColumnarCountState {
    let id = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let prefix = format!(
        "columnar_count_semantics_{}_{}_{}",
        workload.rows, workload.groups, id
    );
    let table = build_table(&prefix).await;
    let mut input_zset = dbsp::collections::SlateBackedColumnarI64ZSet::new(
        table.clone(),
        format!("{prefix}/input"),
        &["key"],
    )
    .await
    .expect("create columnar input zset");
    let mut op = SlateBackedColumnarCountByKeyOp::new(table, format!("{prefix}/count"))
        .await
        .expect("create columnar count op");

    let seed = columnar_from_rows(&seed_tick(workload));
    let seed_handle = input_zset
        .create_version(&seed, None)
        .await
        .expect("stage columnar seed")
        .expect("seed handle");
    let seed_read = input_zset
        .read_delta(&seed_handle)
        .await
        .expect("read columnar seed");
    op.apply_delta(&seed_read)
        .await
        .expect("seed columnar count");
    let seed_output = op.last_output_handle().cloned();

    let ticks = (0..workload.ticks)
        .map(|tick| columnar_from_rows(&semantic_tick(workload, tick)))
        .collect::<Vec<_>>();
    ColumnarCountState {
        op,
        input_zset,
        seed_output,
        ticks,
    }
}

async fn run_existing_count(mut state: ExistingCountState) -> usize {
    let mut output_rows = 0usize;
    for (idx, tick) in state.ticks.iter().enumerate() {
        let input = stage_version(
            Arc::clone(&state.input_dict),
            Arc::clone(&state.table),
            &state.input_ns,
            tick,
        )
        .await;
        if let Some(output) = state
            .op
            .on_step((idx + 1) as i64, &[input])
            .await
            .expect("run existing count")
        {
            output_rows = output_rows.saturating_add(output.version as usize);
        }
    }
    output_rows
}

async fn run_columnar_count(mut state: ColumnarCountState) -> usize {
    let mut output_rows = 0usize;
    for tick in &state.ticks {
        let Some(input) = state
            .input_zset
            .create_version(tick, None)
            .await
            .expect("stage columnar input tick")
        else {
            continue;
        };
        let delta = state
            .input_zset
            .read_delta(&input)
            .await
            .expect("read columnar input tick");
        let _output = state
            .op
            .apply_delta(&delta)
            .await
            .expect("run columnar count");
        if let Some(handle) = state.op.last_output_handle() {
            output_rows = output_rows.saturating_add(handle.version as usize);
        }
    }
    output_rows
}

async fn collect_existing_final_counts(mut state: ExistingCountState) -> HashMap<i64, i64> {
    let mut cache = HashMap::new();
    let mut relation: HashMap<(i64, Vec<i64>), i64> = HashMap::new();
    if let Some(seed_output) = state.seed_output.as_ref() {
        let seed_delta = materialize_zset_handle::<(i64, Vec<i64>)>(
            state.table.clone(),
            &mut cache,
            seed_output,
        )
        .await
        .expect("materialize existing seed output");
        for (row, weight) in seed_delta {
            if weight != 0 {
                relation.insert(row, weight);
            }
        }
    }
    for (idx, tick) in state.ticks.iter().enumerate() {
        let input = stage_version(
            Arc::clone(&state.input_dict),
            Arc::clone(&state.table),
            &state.input_ns,
            tick,
        )
        .await;
        let Some(output) = state
            .op
            .on_step((idx + 1) as i64, &[input])
            .await
            .expect("run existing count")
        else {
            continue;
        };
        let delta =
            materialize_zset_handle::<(i64, Vec<i64>)>(state.table.clone(), &mut cache, &output)
                .await
                .expect("materialize existing output delta");
        for (row, weight) in delta {
            let next = relation
                .get(&row)
                .copied()
                .unwrap_or(0_i64)
                .saturating_add(weight);
            if next == 0 {
                relation.remove(&row);
            } else {
                relation.insert(row, next);
            }
        }
    }

    let mut final_counts = HashMap::new();
    for ((key, counts), weight) in relation {
        if weight == 0 {
            continue;
        }
        assert_eq!(weight, 1);
        assert_eq!(counts.len(), 1);
        final_counts.insert(key, counts[0]);
    }
    final_counts
}

async fn collect_columnar_final_counts(mut state: ColumnarCountState) -> HashMap<i64, i64> {
    let mut relation: HashMap<Vec<i64>, i64> = HashMap::new();
    if let Some(seed_output) = state.seed_output.as_ref() {
        let seed_delta = state
            .op
            .read_output_delta(seed_output)
            .await
            .expect("materialize columnar seed output");
        apply_columnar_output_delta(&mut relation, seed_delta);
    }
    for tick in &state.ticks {
        let Some(input) = state
            .input_zset
            .create_version(tick, None)
            .await
            .expect("stage columnar input tick")
        else {
            continue;
        };
        let delta = state
            .input_zset
            .read_delta(&input)
            .await
            .expect("read columnar input tick");
        state
            .op
            .apply_delta(&delta)
            .await
            .expect("run columnar count");
        let Some(output_handle) = state.op.last_output_handle().cloned() else {
            continue;
        };
        let output_delta = state
            .op
            .read_output_delta(&output_handle)
            .await
            .expect("materialize columnar output delta");
        apply_columnar_output_delta(&mut relation, output_delta);
    }

    let final_counts = relation_to_counts(relation);
    assert_eq!(
        final_counts,
        columnar_state_snapshot_counts(state.op.state_snapshot())
    );
    final_counts
}

fn apply_columnar_output_delta(relation: &mut HashMap<Vec<i64>, i64>, delta: ColumnarI64ZSet) {
    for (row, weight) in delta.materialize().expect("materialize columnar output") {
        let next = relation
            .get(&row)
            .copied()
            .unwrap_or(0_i64)
            .saturating_add(weight);
        if next == 0 {
            relation.remove(&row);
        } else {
            relation.insert(row, next);
        }
    }
}

fn relation_to_counts(relation: HashMap<Vec<i64>, i64>) -> HashMap<i64, i64> {
    let mut final_counts = HashMap::new();
    for (row, weight) in relation {
        if weight == 0 {
            continue;
        }
        assert_eq!(weight, 1);
        assert_eq!(row.len(), 2);
        final_counts.insert(row[0], row[1]);
    }
    final_counts
}

fn columnar_state_snapshot_counts(snapshot: &ColumnarI64ZSet) -> HashMap<i64, i64> {
    relation_to_counts(snapshot.materialize().expect("materialize columnar state"))
}

async fn verify_semantic_workload(workload: &TickWorkload) {
    let existing = build_existing_count_state(workload).await;
    let columnar = build_columnar_count_state(workload).await;
    let existing_counts = collect_existing_final_counts(existing).await;
    let columnar_counts = collect_columnar_final_counts(columnar).await;
    assert_eq!(existing_counts, columnar_counts);
}

fn bench_columnar_count_zset(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    let mut group = c.benchmark_group("columnar_count_zset_semantics");

    for workload in [
        TickWorkload {
            rows: 8_192,
            groups: 1_024,
            ticks: 8,
            seed_multiplier: 4,
        },
        TickWorkload {
            rows: 32_768,
            groups: 4_096,
            ticks: 8,
            seed_multiplier: 4,
        },
    ] {
        runtime.block_on(verify_semantic_workload(&workload));
        let label = format!(
            "rows{}_groups{}_ticks{}",
            workload.rows, workload.groups, workload.ticks
        );
        group.bench_function(BenchmarkId::new("existing_count_aggregate", &label), |b| {
            b.iter_batched(
                || runtime.block_on(build_existing_count_state(&workload)),
                |state| runtime.block_on(run_existing_count(state)),
                BatchSize::SmallInput,
            );
        });
        group.bench_function(BenchmarkId::new("columnar_count_by_key", &label), |b| {
            b.iter_batched(
                || runtime.block_on(build_columnar_count_state(&workload)),
                |state| runtime.block_on(run_columnar_count(state)),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_columnar_count_zset);
criterion_main!(benches);
