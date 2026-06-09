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
use floe_node_core::planner::planner_udfs;
use floe_node_core::source::SourceRegistry;
use object_store::memory::InMemory;
use slatedb::Db;
use tokio::runtime::Runtime;

const ROWS_PER_TICK: usize = 8_192;
const TICKS: usize = 8;

#[derive(Clone, Copy)]
struct NexmarkRuntimeCase {
    id: &'static str,
    source_name: &'static str,
    view_name: &'static str,
    query: &'static str,
    output_schema: fn() -> SchemaRef,
    batch: fn(SchemaRef, usize, usize) -> Result<RecordBatch>,
}

fn bench_nexmark_vectorized_runtime(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let cases = [
        NexmarkRuntimeCase {
            id: "q1",
            source_name: "nexmark_bid",
            view_name: "mv_nexmark_q1",
            query: r#"SELECT auction, bidder, price * 89 / 100 AS converted_price, date_time AS "dateTime", extra FROM nexmark_bid"#,
            output_schema: q1_output_schema,
            batch: bid_batch,
        },
        NexmarkRuntimeCase {
            id: "q2",
            source_name: "nexmark_bid",
            view_name: "mv_nexmark_q2",
            query: "SELECT auction, price FROM nexmark_bid WHERE auction % 123 = 0",
            output_schema: q2_output_schema,
            batch: bid_batch,
        },
        NexmarkRuntimeCase {
            id: "q5",
            source_name: "nexmark_bid",
            view_name: "mv_nexmark_q5",
            query: r#"SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP("dateTime", 2000, 10000)"#,
            output_schema: q5_output_schema,
            batch: bid_batch,
        },
        NexmarkRuntimeCase {
            id: "q7",
            source_name: "nexmark_bid",
            view_name: "mv_nexmark_q7",
            query: r#"SELECT MAX(price) AS maxprice FROM bid GROUP BY TUMBLE("dateTime", 10000)"#,
            output_schema: q7_output_schema,
            batch: bid_batch,
        },
        NexmarkRuntimeCase {
            id: "q8",
            source_name: "nexmark_person",
            view_name: "mv_nexmark_q8",
            query: r#"SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, TUMBLE("dateTime", 10000)"#,
            output_schema: q8_output_schema,
            batch: person_batch,
        },
        NexmarkRuntimeCase {
            id: "q12",
            source_name: "nexmark_bid",
            view_name: "mv_nexmark_q12",
            query: r#"SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE("dateTime", 10000)"#,
            output_schema: q12_output_schema,
            batch: bid_batch,
        },
    ];

    let mut group = c.benchmark_group("nexmark_vectorized_runtime_columnar");
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
        .find(|definition| definition.name() == case.source_name)
        .ok_or_else(|| anyhow::anyhow!("missing {} definition", case.source_name))?
        .to_arrow_schema();
    let output_schema = (case.output_schema)();
    let table = build_operator_state_table(case.id).await?;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let execution = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            case.view_name,
            case.query,
            output_schema,
        )],
        Arc::clone(&registry),
        planner_udfs(),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .with_context(|| format!("build vectorized runtime for {}", case.id))?;
    let batches = (0..TICKS)
        .map(|tick| {
            (case.batch)(
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
            .append_source_batches_for_execution_and_query(
                case.source_name,
                vec![batch],
                Vec::new(),
            )
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

fn person_batch(schema: SchemaRef, start: usize, rows: usize) -> Result<RecordBatch> {
    let mut ids = Vec::with_capacity(rows);
    let mut names = Vec::with_capacity(rows);
    let mut emails = Vec::with_capacity(rows);
    let mut credit_cards = Vec::with_capacity(rows);
    let mut cities = Vec::with_capacity(rows);
    let mut states = Vec::with_capacity(rows);
    let mut date_times = Vec::with_capacity(rows);
    let mut extras = Vec::with_capacity(rows);

    for offset in 0..rows {
        let seq = start + offset;
        ids.push((seq % 50_000) as i64);
        names.push(format!("person-{seq}"));
        emails.push(format!("person-{seq}@example.test"));
        credit_cards.push(format!("411111111111{:04}", seq % 10_000));
        cities.push(match seq % 4 {
            0 => "portland".to_string(),
            1 => "boise".to_string(),
            2 => "san francisco".to_string(),
            _ => "seattle".to_string(),
        });
        states.push(match seq % 4 {
            0 => "or".to_string(),
            1 => "id".to_string(),
            2 => "ca".to_string(),
            _ => "wa".to_string(),
        });
        date_times.push(1_700_000_000_000_i64 + seq as i64);
        extras.push(format!("extra-{seq}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(emails)) as ArrayRef,
            Arc::new(StringArray::from(credit_cards)) as ArrayRef,
            Arc::new(StringArray::from(cities)) as ArrayRef,
            Arc::new(StringArray::from(states)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(date_times)) as ArrayRef,
            Arc::new(StringArray::from(extras)) as ArrayRef,
        ],
    )
    .context("build nexmark person batch")
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

fn q5_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("num", DataType::Int64, false),
    ]))
}

fn q7_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "maxprice",
        DataType::Int64,
        true,
    )]))
}

fn q8_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("person_count", DataType::Int64, false),
    ]))
}

fn q12_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("bid_count", DataType::Int64, false),
    ]))
}

criterion_group!(benches, bench_nexmark_vectorized_runtime);
criterion_main!(benches);
