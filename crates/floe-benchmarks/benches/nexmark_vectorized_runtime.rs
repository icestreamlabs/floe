use std::sync::Arc;

use anyhow::{Context, Result};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::storage::{KeyValueTable, SlateTable};
use floe_executor::{
    MaterializedViewRegistry, VectorizedExecutionRuntime, VectorizedExecutionRuntimeOptions,
    VectorizedMaterializedViewPlan,
};
use floe_node_core::generator;
use floe_node_core::source::SourceRegistry;
use object_store::memory::InMemory;
use slatedb::Db;
use tokio::runtime::Runtime;

const SOURCE_NAME: &str = "nexmark_bid";
const ROWS_PER_TICK: usize = 8_192;
const TICKS: usize = 8;

#[derive(Clone, Copy)]
struct NexmarkRuntimeCase {
    id: &'static str,
    view_name: &'static str,
    query: &'static str,
    output_schema: fn() -> SchemaRef,
}

fn bench_nexmark_vectorized_runtime(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let cases = [
        NexmarkRuntimeCase {
            id: "q1",
            view_name: "mv_nexmark_q1",
            query: r#"SELECT auction, bidder, price * 89 / 100 AS converted_price, date_time AS "dateTime", extra FROM nexmark_bid"#,
            output_schema: q1_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q2",
            view_name: "mv_nexmark_q2",
            query: "SELECT auction, price FROM nexmark_bid WHERE auction % 123 = 0",
            output_schema: q2_output_schema,
        },
    ];

    let mut group = c.benchmark_group("nexmark_vectorized_runtime_columnar_stateless");
    for case in cases {
        group.throughput(Throughput::Elements((ROWS_PER_TICK * TICKS) as u64));
        group.bench_with_input(
            BenchmarkId::new(case.id, format!("{ROWS_PER_TICK}x{TICKS}")),
            &case,
            |b, case| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(build_runtime_case(case))
                            .expect("build runtime benchmark case")
                    },
                    |(mut execution, registry, batches)| {
                        runtime
                            .block_on(run_runtime_case(case, &mut execution, registry, batches))
                            .expect("run runtime benchmark case")
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

async fn build_runtime_case(
    case: &NexmarkRuntimeCase,
) -> Result<(
    VectorizedExecutionRuntime,
    Arc<MaterializedViewRegistry>,
    Vec<RecordBatch>,
)> {
    let mut sources = SourceRegistry::new();
    sources.extend(generator::definitions()?);
    let source_schema = generator::definitions()?
        .into_iter()
        .find(|definition| definition.name() == SOURCE_NAME)
        .ok_or_else(|| anyhow::anyhow!("missing {SOURCE_NAME} definition"))?
        .to_arrow_schema();
    let output_schema = (case.output_schema)();
    let table = build_operator_state_table(case.id).await?;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let execution = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            case.view_name,
            case.query,
            output_schema,
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .with_context(|| format!("build vectorized runtime for {}", case.id))?;
    let batches = (0..TICKS)
        .map(|tick| {
            bid_batch(
                Arc::clone(&source_schema),
                tick * ROWS_PER_TICK,
                ROWS_PER_TICK,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((execution, registry, batches))
}

async fn run_runtime_case(
    case: &NexmarkRuntimeCase,
    execution: &mut VectorizedExecutionRuntime,
    registry: Arc<MaterializedViewRegistry>,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    for (idx, batch) in batches.into_iter().enumerate() {
        execution
            .append_source_batches_for_execution_and_query(SOURCE_NAME, vec![batch], Vec::new())
            .await?;
        execution.run_tick((idx + 1) as i64).await?;
    }
    let handle = registry
        .get(case.view_name)
        .ok_or_else(|| anyhow::anyhow!("missing materialized view {}", case.view_name))?;
    let snapshot = handle
        .arrow_snapshot_for(TICKS as i64)
        .ok_or_else(|| anyhow::anyhow!("missing final snapshot for {}", case.view_name))?;
    let rows = snapshot.iter().map(RecordBatch::num_rows).sum::<usize>();
    black_box(rows);
    Ok(())
}

async fn build_operator_state_table(name: &str) -> Result<Arc<dyn KeyValueTable>> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(format!("nexmark-vectorized-runtime-{name}"), store).await?);
    Ok(Arc::new(SlateTable::new(db)))
}

fn bid_batch(schema: SchemaRef, start: usize, rows: usize) -> Result<RecordBatch> {
    let mut auctions = Vec::with_capacity(rows);
    let mut bidders = Vec::with_capacity(rows);
    let mut prices = Vec::with_capacity(rows);
    let mut channels = Vec::with_capacity(rows);
    let mut urls = Vec::with_capacity(rows);
    let mut date_times = Vec::with_capacity(rows);
    let mut extras = Vec::with_capacity(rows);

    for offset in 0..rows {
        let seq = start + offset;
        auctions.push((seq % 10_000) as i64);
        bidders.push((seq % 50_000) as i64);
        prices.push(1_000 + (seq % 1_000_000) as i64);
        channels.push(match seq % 4 {
            0 => "apple".to_string(),
            1 => "google".to_string(),
            2 => "facebook".to_string(),
            _ => "baidu".to_string(),
        });
        urls.push(format!(
            "https://example.test/path/{seq}?channel_id={}",
            seq % 16
        ));
        date_times.push(1_700_000_000_000_i64 + seq as i64);
        extras.push(format!("extra-{seq}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auctions)) as ArrayRef,
            Arc::new(Int64Array::from(bidders)) as ArrayRef,
            Arc::new(Int64Array::from(prices)) as ArrayRef,
            Arc::new(StringArray::from(channels)) as ArrayRef,
            Arc::new(StringArray::from(urls)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(date_times)) as ArrayRef,
            Arc::new(StringArray::from(extras)) as ArrayRef,
        ],
    )
    .context("build nexmark bid batch")
}

fn q1_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("converted_price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("extra", DataType::Utf8, true),
    ]))
}

fn q2_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]))
}

criterion_group!(benches, bench_nexmark_vectorized_runtime);
criterion_main!(benches);
