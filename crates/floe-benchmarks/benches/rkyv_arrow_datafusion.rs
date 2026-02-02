use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::{Expr, TableType, col};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use dbsp::storage::encoding::{decode, encode};
use parking_lot::RwLock;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::runtime::Runtime;

const BATCH_SIZES: &[usize] = &[64, 256, 1024, 4096, 16384];
const TEXT_LEN: usize = "value-00000000".len();

#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
struct BenchRow {
    id: i64,
    value: i64,
    flag: bool,
    text: String,
}

impl BenchRow {
    fn new(idx: i64) -> Self {
        Self {
            id: idx,
            value: idx.wrapping_mul(3),
            flag: idx % 2 == 0,
            text: format!("value-{idx:08}"),
        }
    }
}

#[derive(Debug)]
struct BenchBatchState {
    batch: Option<RecordBatch>,
    yielded: bool,
}

impl BenchBatchState {
    fn new() -> Self {
        Self {
            batch: None,
            yielded: true,
        }
    }

    fn set_batch(&mut self, batch: RecordBatch) {
        self.batch = Some(batch);
        self.yielded = false;
    }
}

#[derive(Debug)]
struct BenchBatchGenerator {
    state: Arc<RwLock<BenchBatchState>>,
}

impl BenchBatchGenerator {
    fn new(state: Arc<RwLock<BenchBatchState>>) -> Self {
        Self { state }
    }
}

impl fmt::Display for BenchBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BenchBatchGenerator")
    }
}

impl LazyBatchGenerator for BenchBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        let mut state = self.state.write();
        if state.yielded {
            return Ok(None);
        }
        state.yielded = true;
        Ok(state.batch.clone())
    }
}

#[derive(Debug)]
struct BenchTableProvider {
    schema: SchemaRef,
    state: Arc<RwLock<BenchBatchState>>,
}

impl BenchTableProvider {
    fn new(schema: SchemaRef, state: Arc<RwLock<BenchBatchState>>) -> Self {
        Self { schema, state }
    }
}

#[async_trait]
impl TableProvider for BenchTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let generator = BenchBatchGenerator::new(Arc::clone(&self.state));
        let exec = Arc::new(LazyMemoryExec::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(RwLock::new(generator))],
        )?);

        let Some(projection) = projection else {
            return Ok(exec);
        };

        let exprs = projection
            .iter()
            .map(|index| {
                let field = self.schema.field(*index);
                ProjectionExpr {
                    expr: Arc::new(Column::new(field.name(), *index)),
                    alias: field.name().to_string(),
                }
            })
            .collect::<Vec<_>>();
        let projected = ProjectionExec::try_new(exprs, exec)?;
        Ok(Arc::new(projected))
    }
}

fn bench_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

fn encode_rows(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|idx| encode(&BenchRow::new(idx as i64)).expect("encode row"))
        .collect()
}

fn decode_to_batch(schema: SchemaRef, encoded: &[Vec<u8>]) -> RecordBatch {
    let mut id_builder = Int64Builder::with_capacity(encoded.len());
    let mut value_builder = Int64Builder::with_capacity(encoded.len());
    let mut flag_builder = BooleanBuilder::with_capacity(encoded.len());
    let mut text_builder = StringBuilder::with_capacity(encoded.len(), encoded.len() * TEXT_LEN);

    for bytes in encoded {
        let row: BenchRow = decode(bytes).expect("decode row");
        id_builder.append_value(row.id);
        value_builder.append_value(row.value);
        flag_builder.append_value(row.flag);
        text_builder.append_value(&row.text);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(value_builder.finish()),
        Arc::new(flag_builder.finish()),
        Arc::new(text_builder.finish()),
    ];

    RecordBatch::try_new(schema, arrays).expect("record batch")
}

type Row = Vec<ScalarValue>;

fn decode_to_rows(encoded: &[Vec<u8>]) -> Vec<Row> {
    let mut rows = Vec::with_capacity(encoded.len());
    for bytes in encoded {
        let row: BenchRow = decode(bytes).expect("decode row");
        rows.push(vec![
            ScalarValue::Int64(Some(row.id)),
            ScalarValue::Int64(Some(row.value)),
            ScalarValue::Boolean(Some(row.flag)),
            ScalarValue::Utf8(Some(row.text)),
        ]);
    }
    rows
}

fn scalar_eval(rows: &[Row]) -> i64 {
    let mut sum = 0i64;
    for row in rows {
        let flag = match row.get(2) {
            Some(ScalarValue::Boolean(Some(value))) => *value,
            Some(ScalarValue::Boolean(None) | ScalarValue::Null) => false,
            other => panic!("unexpected flag value: {other:?}"),
        };

        if !flag {
            continue;
        }

        match row.get(1) {
            Some(ScalarValue::Int64(Some(value))) => sum += value,
            Some(ScalarValue::Int64(None) | ScalarValue::Null) => {}
            other => panic!("unexpected value column: {other:?}"),
        }
    }
    sum
}

async fn run_query(
    ctx: &SessionContext,
    batch: RecordBatch,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let df = ctx.read_batch(batch)?;
    let df = df.filter(col("flag"))?;
    let df = df.aggregate(vec![], vec![sum(col("value"))])?;
    df.collect().await
}

fn bench_vectorized_batch_sizes(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = bench_schema();
    let mut group = c.benchmark_group("rkyv_arrow_datafusion");

    for &batch_size in BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let encoded = encode_rows(batch_size);

        group.bench_function(BenchmarkId::new("decode_to_arrow", batch_size), |b| {
            b.iter(|| {
                let batch = decode_to_batch(schema.clone(), &encoded);
                black_box(batch);
            });
        });

        group.bench_function(BenchmarkId::new("decode_to_scalar", batch_size), |b| {
            b.iter(|| {
                let rows = decode_to_rows(&encoded);
                black_box(rows);
            });
        });

        let rows = decode_to_rows(&encoded);
        group.bench_function(BenchmarkId::new("scalar_eval", batch_size), |b| {
            let rows = &rows;
            b.iter(|| {
                let result = scalar_eval(rows);
                black_box(result);
            });
        });

        let reuse_ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let reuse_state = Arc::new(RwLock::new(BenchBatchState::new()));
        reuse_ctx
            .register_table(
                "bench",
                Arc::new(BenchTableProvider::new(
                    schema.clone(),
                    Arc::clone(&reuse_state),
                )),
            )
            .expect("register table");
        let reuse_plan = runtime
            .block_on(async {
                let df = reuse_ctx.table("bench").await?;
                let df = df.filter(col("flag"))?;
                let df = df.aggregate(vec![], vec![sum(col("value"))])?;
                df.create_physical_plan().await
            })
            .expect("create reuse plan");
        let reuse_task_ctx = reuse_ctx.task_ctx();
        let reuse_batch = decode_to_batch(schema.clone(), &encoded);

        group.bench_function(BenchmarkId::new("datafusion_eval", batch_size), |b| {
            let plan = reuse_plan.clone();
            let task_ctx = reuse_task_ctx.clone();
            let state = Arc::clone(&reuse_state);
            let batch = reuse_batch.clone();
            b.iter(|| {
                state.write().set_batch(batch.clone());
                runtime
                    .block_on(async {
                        let result =
                            datafusion::physical_plan::collect(plan.clone(), task_ctx.clone())
                                .await?;
                        Ok::<_, datafusion::error::DataFusionError>(result)
                    })
                    .map(|result| black_box(result))
                    .expect("datafusion collect");
            });
        });

        group.bench_function(BenchmarkId::new("vectorized_reuse_plan", batch_size), |b| {
            let plan = reuse_plan.clone();
            let task_ctx = reuse_task_ctx.clone();
            let state = Arc::clone(&reuse_state);
            b.iter(|| {
                let decoded = decode_to_batch(schema.clone(), &encoded);
                state.write().set_batch(decoded);
                runtime
                    .block_on(async {
                        let result =
                            datafusion::physical_plan::collect(plan.clone(), task_ctx.clone())
                                .await?;
                        Ok::<_, datafusion::error::DataFusionError>(result)
                    })
                    .map(|result| black_box(result))
                    .expect("datafusion collect");
            });
        });

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        group.bench_function(BenchmarkId::new("end_to_end", batch_size), |b| {
            let ctx = ctx.clone();
            b.iter(|| {
                let batch = decode_to_batch(schema.clone(), &encoded);
                runtime
                    .block_on(async {
                        let result = run_query(&ctx, batch).await?;
                        Ok::<_, datafusion::error::DataFusionError>(result)
                    })
                    .map(|result| black_box(result))
                    .expect("datafusion query");
            });
        });

        group.bench_function(BenchmarkId::new("scalar_end_to_end", batch_size), |b| {
            b.iter(|| {
                let rows = decode_to_rows(&encoded);
                let result = scalar_eval(&rows);
                black_box(result);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_vectorized_batch_sizes);
criterion_main!(benches);
