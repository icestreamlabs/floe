use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use datafusion::arrow::array::{
    BooleanArray, BooleanBuilder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::storage::encoding::{decode, encode};
use dbsp::storage::{KeyValueTable, SlateTable};
use object_store::memory::InMemory;
use rkyv::{Archive, Deserialize, Serialize};
use slatedb::{Db, WriteBatch};
use tokio::runtime::Runtime;

const BATCH_SIZES: &[usize] = &[64, 256, 1024, 4096, 16384];

static DB_ID: AtomicU64 = AtomicU64::new(0);

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

fn build_rows(count: usize) -> Vec<BenchRow> {
    (0..count).map(|idx| BenchRow::new(idx as i64)).collect()
}

fn row_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

fn rows_to_record_batch(schema: SchemaRef, rows: &[BenchRow]) -> RecordBatch {
    let mut id_builder = Int64Builder::with_capacity(rows.len());
    let mut value_builder = Int64Builder::with_capacity(rows.len());
    let mut flag_builder = BooleanBuilder::with_capacity(rows.len());
    let mut text_builder =
        StringBuilder::with_capacity(rows.len(), rows.len() * "value-00000000".len());

    for row in rows {
        id_builder.append_value(row.id);
        value_builder.append_value(row.value);
        flag_builder.append_value(row.flag);
        text_builder.append_value(&row.text);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(value_builder.finish()),
            Arc::new(flag_builder.finish()),
            Arc::new(text_builder.finish()),
        ],
    )
    .expect("build Arrow record batch")
}

fn record_batch_to_rows(batch: &RecordBatch) -> Vec<BenchRow> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");
    let flags = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("flag column");
    let texts = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("text column");

    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        rows.push(BenchRow {
            id: ids.value(idx),
            value: values.value(idx),
            flag: flags.value(idx),
            text: texts.value(idx).to_string(),
        });
    }
    rows
}

fn encode_rkyv_rows(rows: &[BenchRow]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| encode(row).expect("encode row with rkyv"))
        .collect()
}

fn decode_rkyv_rows(encoded: &[Vec<u8>]) -> Vec<BenchRow> {
    encoded
        .iter()
        .map(|bytes| decode(bytes).expect("decode row with rkyv"))
        .collect()
}

fn encode_rkyv_batch(rows: &[BenchRow]) -> Vec<u8> {
    encode(&rows.to_vec()).expect("encode row batch with rkyv")
}

fn decode_rkyv_batch(encoded: &[u8]) -> Vec<BenchRow> {
    decode(encoded).expect("decode row batch with rkyv")
}

fn encode_arrow_ipc_batch(schema: SchemaRef, rows: &[BenchRow]) -> Vec<u8> {
    let batch = rows_to_record_batch(Arc::clone(&schema), rows);
    let mut payload = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut payload, schema.as_ref()).expect("create Arrow IPC writer");
        writer.write(&batch).expect("write Arrow IPC batch");
        writer.finish().expect("finish Arrow IPC writer");
    }
    payload
}

fn encode_arrow_ipc_rows(schema: SchemaRef, rows: &[BenchRow]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| encode_arrow_ipc_batch(Arc::clone(&schema), std::slice::from_ref(row)))
        .collect()
}

fn decode_arrow_ipc_batch(bytes: &[u8]) -> Vec<BenchRow> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).expect("create Arrow IPC reader");
    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch.expect("read Arrow IPC batch");
        rows.extend(record_batch_to_rows(&batch));
    }
    rows
}

fn decode_arrow_ipc_row(bytes: &[u8]) -> BenchRow {
    let mut rows = decode_arrow_ipc_batch(bytes);
    assert_eq!(rows.len(), 1, "expected exactly one row in Arrow IPC row");
    rows.remove(0)
}

fn decode_arrow_ipc_rows(encoded: &[Vec<u8>]) -> Vec<BenchRow> {
    encoded
        .iter()
        .map(|bytes| decode_arrow_ipc_row(bytes))
        .collect()
}

fn total_bytes(buffers: &[Vec<u8>]) -> usize {
    buffers.iter().map(Vec::len).sum::<usize>()
}

fn next_db_name(prefix: &str, batch_size: usize) -> String {
    let id = DB_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{batch_size}-{id}")
}

fn open_table(runtime: &Runtime, name: String) -> Arc<dyn KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = runtime
        .block_on(Db::open(name, store))
        .expect("open SlateDB for bench");
    Arc::new(SlateTable::new(Arc::new(db)))
}

fn row_key(prefix: &str, iteration: u64, index: usize) -> Vec<u8> {
    format!("{prefix}/{iteration}/{index:08}").into_bytes()
}

fn batch_key(prefix: &str, iteration: u64) -> Vec<u8> {
    format!("{prefix}/{iteration}").into_bytes()
}

fn bench_rkyv_arrow_ipc_storage(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = row_schema();
    let mut group = c.benchmark_group("rkyv_arrow_ipc_storage");

    for &batch_size in BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let rows = build_rows(batch_size);

        let rkyv_row_encoded = encode_rkyv_rows(&rows);
        let rkyv_batch_encoded = encode_rkyv_batch(&rows);
        let arrow_row_encoded = encode_arrow_ipc_rows(Arc::clone(&schema), &rows);
        let arrow_batch_encoded = encode_arrow_ipc_batch(Arc::clone(&schema), &rows);

        let rkyv_row_bytes = total_bytes(&rkyv_row_encoded);
        let arrow_row_bytes = total_bytes(&arrow_row_encoded);
        let rkyv_batch_bytes = rkyv_batch_encoded.len();
        let arrow_batch_bytes = arrow_batch_encoded.len();
        println!(
            "size_report,batch_size={batch_size},rkyv_row_total_bytes={rkyv_row_bytes},arrow_row_total_bytes={arrow_row_bytes},rkyv_batch_total_bytes={rkyv_batch_bytes},arrow_batch_total_bytes={arrow_batch_bytes},rkyv_row_bytes_per_row={:.2},arrow_row_bytes_per_row={:.2},rkyv_batch_bytes_per_row={:.2},arrow_batch_bytes_per_row={:.2},arrow_row_over_rkyv_row={:.2},arrow_batch_over_rkyv_batch={:.2}",
            rkyv_row_bytes as f64 / batch_size as f64,
            arrow_row_bytes as f64 / batch_size as f64,
            rkyv_batch_bytes as f64 / batch_size as f64,
            arrow_batch_bytes as f64 / batch_size as f64,
            arrow_row_bytes as f64 / rkyv_row_bytes as f64,
            arrow_batch_bytes as f64 / rkyv_batch_bytes as f64
        );

        group.bench_function(BenchmarkId::new("encode_rkyv_rows", batch_size), |b| {
            b.iter(|| {
                let encoded = encode_rkyv_rows(black_box(&rows));
                black_box(encoded);
            });
        });

        group.bench_function(BenchmarkId::new("decode_rkyv_rows", batch_size), |b| {
            b.iter(|| {
                let decoded = decode_rkyv_rows(black_box(&rkyv_row_encoded));
                black_box(decoded);
            });
        });

        group.bench_function(BenchmarkId::new("encode_rkyv_batch", batch_size), |b| {
            b.iter(|| {
                let encoded = encode_rkyv_batch(black_box(&rows));
                black_box(encoded);
            });
        });

        group.bench_function(BenchmarkId::new("decode_rkyv_batch", batch_size), |b| {
            b.iter(|| {
                let decoded = decode_rkyv_batch(black_box(&rkyv_batch_encoded));
                black_box(decoded);
            });
        });

        group.bench_function(
            BenchmarkId::new("encode_arrow_ipc_batch", batch_size),
            |b| {
                let schema = Arc::clone(&schema);
                b.iter(|| {
                    let encoded = encode_arrow_ipc_batch(Arc::clone(&schema), black_box(&rows));
                    black_box(encoded);
                });
            },
        );

        group.bench_function(BenchmarkId::new("encode_arrow_ipc_rows", batch_size), |b| {
            let schema = Arc::clone(&schema);
            b.iter(|| {
                let encoded = encode_arrow_ipc_rows(Arc::clone(&schema), black_box(&rows));
                black_box(encoded);
            });
        });

        group.bench_function(
            BenchmarkId::new("decode_arrow_ipc_batch", batch_size),
            |b| {
                b.iter(|| {
                    let decoded = decode_arrow_ipc_batch(black_box(&arrow_batch_encoded));
                    black_box(decoded);
                });
            },
        );

        group.bench_function(BenchmarkId::new("decode_arrow_ipc_rows", batch_size), |b| {
            b.iter(|| {
                let decoded = decode_arrow_ipc_rows(black_box(&arrow_row_encoded));
                black_box(decoded);
            });
        });

        group.bench_function(
            BenchmarkId::new("slatedb_e2e_write_read_rkyv_row_per_key", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-rkyv", batch_size));
                let key_prefix = format!("bench/rkyv/{batch_size}");
                let iteration = AtomicU64::new(0);

                b.iter(|| {
                    let iter_id = iteration.fetch_add(1, Ordering::Relaxed);
                    runtime.block_on(async {
                        let mut batch = WriteBatch::new();
                        let mut keys = Vec::with_capacity(rkyv_row_encoded.len());
                        for (idx, payload) in rkyv_row_encoded.iter().enumerate() {
                            let key = row_key(&key_prefix, iter_id, idx);
                            batch.put(key.clone(), payload.clone());
                            keys.push(key);
                        }
                        table.write_batch(batch).await.expect("write rkyv rows");

                        let mut decoded_rows = Vec::with_capacity(keys.len());
                        for key in keys {
                            let bytes = table
                                .get(&key)
                                .await
                                .expect("get rkyv row")
                                .expect("missing rkyv row");
                            let row: BenchRow = decode(&bytes).expect("decode rkyv row");
                            decoded_rows.push(row);
                        }

                        black_box(decoded_rows);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("slatedb_e2e_write_read_arrow_ipc_row_per_key", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-arrow-row", batch_size));
                let key_prefix = format!("bench/arrow-row/{batch_size}");
                let iteration = AtomicU64::new(0);

                b.iter(|| {
                    let iter_id = iteration.fetch_add(1, Ordering::Relaxed);
                    runtime.block_on(async {
                        let mut batch = WriteBatch::new();
                        let mut keys = Vec::with_capacity(arrow_row_encoded.len());
                        for (idx, payload) in arrow_row_encoded.iter().enumerate() {
                            let key = row_key(&key_prefix, iter_id, idx);
                            batch.put(key.clone(), payload.clone());
                            keys.push(key);
                        }
                        table.write_batch(batch).await.expect("write arrow rows");

                        let mut decoded_rows = Vec::with_capacity(keys.len());
                        for key in keys {
                            let bytes = table
                                .get(&key)
                                .await
                                .expect("get arrow row")
                                .expect("missing arrow row");
                            decoded_rows.push(decode_arrow_ipc_row(&bytes));
                        }
                        black_box(decoded_rows);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("slatedb_e2e_write_read_rkyv_batch_per_key", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-rkyv-batch", batch_size));
                let key_prefix = format!("bench/rkyv-batch/{batch_size}");
                let iteration = AtomicU64::new(0);

                b.iter(|| {
                    let iter_id = iteration.fetch_add(1, Ordering::Relaxed);
                    runtime.block_on(async {
                        let key = batch_key(&key_prefix, iter_id);
                        table
                            .put(&key, &rkyv_batch_encoded)
                            .await
                            .expect("write rkyv batch");
                        let bytes = table
                            .get(&key)
                            .await
                            .expect("get rkyv batch")
                            .expect("missing rkyv batch");
                        let decoded_rows = decode_rkyv_batch(&bytes);
                        black_box(decoded_rows);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("slatedb_e2e_write_read_arrow_ipc_batch_per_key", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-arrow", batch_size));
                let key_prefix = format!("bench/arrow/{batch_size}");
                let iteration = AtomicU64::new(0);

                b.iter(|| {
                    let iter_id = iteration.fetch_add(1, Ordering::Relaxed);
                    runtime.block_on(async {
                        let key = batch_key(&key_prefix, iter_id);
                        table
                            .put(&key, &arrow_batch_encoded)
                            .await
                            .expect("write arrow ipc batch");
                        let bytes = table
                            .get(&key)
                            .await
                            .expect("get arrow ipc batch")
                            .expect("missing arrow ipc batch");
                        let decoded_rows = decode_arrow_ipc_batch(&bytes);
                        black_box(decoded_rows);
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_rkyv_arrow_ipc_storage);
criterion_main!(benches);
