use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use datafusion::arrow::array::{
    BooleanArray, BooleanBuilder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::collections::IndexedBatchZSet;
use dbsp::collections::zset::{SegmentRecord, VersionedZSet};
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::gc::{GcPolicy, GcService};
use dbsp::storage::manifest::{DataManifest, ManifestStatistics, ManifestStore};
use dbsp::storage::segment::{ArrowSegmentStore, SegmentWriteStats};
use dbsp::storage::{KeyValueTable, SlateTable};
use object_store::memory::InMemory;
use rkyv::{Archive, Deserialize, Serialize};
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};
use tokio::runtime::Runtime;

const MODEL_BATCH_SIZES: &[usize] = &[64, 256, 1024];
const KEY_CARDINALITY_DIVISOR: usize = 8;
const OVERLAY_DECODE_CACHE_CAPACITY: usize = 256;

static DB_ID: AtomicU64 = AtomicU64::new(0);
static NS_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(all(feature = "allocator-mimalloc", feature = "allocator-jemalloc"))]
compile_error!("enable at most one allocator feature");

#[cfg(feature = "allocator-mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "allocator-jemalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
struct BenchRow {
    id: i64,
    value: i64,
    flag: bool,
    text: String,
}

impl BenchRow {
    fn for_entity(entity: i64, phase: i64) -> Self {
        Self {
            id: entity,
            value: entity.wrapping_mul(17).wrapping_add(phase),
            flag: phase % 2 == 0,
            text: format!("entity-{entity:08}-p{phase}"),
        }
    }
}

fn next_db_name(prefix: &str, batch_size: usize) -> String {
    let id = DB_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{batch_size}-{id}")
}

fn next_namespace(prefix: &str) -> String {
    let id = NS_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

fn open_table(runtime: &Runtime, name: String) -> Arc<dyn KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = runtime
        .block_on(Db::open(name, store))
        .expect("open SlateDB for benchmark");
    Arc::new(SlateTable::new(Arc::new(db)))
}

fn initial_keyed_updates(batch_size: usize, key_cardinality: i64) -> Vec<(i64, BenchRow, i64)> {
    (0..batch_size)
        .map(|entity| {
            let entity = entity as i64;
            let key = entity % key_cardinality;
            (key, BenchRow::for_entity(entity, 0), 1)
        })
        .collect()
}

fn transition_keyed_updates(
    batch_size: usize,
    key_cardinality: i64,
    from_phase: i64,
    to_phase: i64,
) -> Vec<(i64, BenchRow, i64)> {
    let mut updates = Vec::with_capacity(batch_size * 2);
    for entity in 0..batch_size {
        let entity = entity as i64;
        let key = entity % key_cardinality;
        updates.push((key, BenchRow::for_entity(entity, from_phase), -1));
        updates.push((key, BenchRow::for_entity(entity, to_phase), 1));
    }
    updates
}

fn coalescing_probe_updates(batch_size: usize, key_cardinality: i64) -> Vec<(i64, BenchRow, i64)> {
    let mut updates = Vec::with_capacity(batch_size * 3);
    for entity in 0..batch_size {
        let entity = entity as i64;
        let key = entity % key_cardinality;
        let row = BenchRow::for_entity(entity, 0);
        updates.push((key, row.clone(), 1));
        updates.push((key, row.clone(), -1));
        updates.push((key, row, 1));
    }
    updates
}

fn strip_keyed_updates(updates: &[(i64, BenchRow, i64)]) -> Vec<(BenchRow, i64)> {
    updates
        .iter()
        .map(|(_, row, delta)| (row.clone(), *delta))
        .collect()
}

fn delta_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]))
}

fn deltas_to_record_batch(schema: SchemaRef, deltas: &[(BenchRow, i64)]) -> RecordBatch {
    let mut id_builder = Int64Builder::with_capacity(deltas.len());
    let mut value_builder = Int64Builder::with_capacity(deltas.len());
    let mut flag_builder = BooleanBuilder::with_capacity(deltas.len());
    let mut text_builder =
        StringBuilder::with_capacity(deltas.len(), deltas.len() * "entity-00000000-p0".len());
    let mut weight_builder = Int64Builder::with_capacity(deltas.len());

    for (row, weight) in deltas {
        id_builder.append_value(row.id);
        value_builder.append_value(row.value);
        flag_builder.append_value(row.flag);
        text_builder.append_value(&row.text);
        weight_builder.append_value(*weight);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(value_builder.finish()),
            Arc::new(flag_builder.finish()),
            Arc::new(text_builder.finish()),
            Arc::new(weight_builder.finish()),
        ],
    )
    .expect("build Arrow delta batch")
}

fn record_batch_to_deltas(batch: &RecordBatch) -> Vec<(BenchRow, i64)> {
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
    let weights = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("weight column");

    let mut deltas = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        deltas.push((
            BenchRow {
                id: ids.value(idx),
                value: values.value(idx),
                flag: flags.value(idx),
                text: texts.value(idx).to_string(),
            },
            weights.value(idx),
        ));
    }
    deltas
}

fn encode_arrow_ipc_delta_batch(schema: SchemaRef, deltas: &[(BenchRow, i64)]) -> Vec<u8> {
    let batch = deltas_to_record_batch(Arc::clone(&schema), deltas);
    let mut payload = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut payload, schema.as_ref()).expect("create Arrow IPC writer");
        writer.write(&batch).expect("write Arrow IPC delta batch");
        writer.finish().expect("finish Arrow IPC delta writer");
    }
    payload
}

fn decode_arrow_ipc_delta_batch(bytes: &[u8]) -> Vec<(BenchRow, i64)> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).expect("create Arrow IPC reader");
    let mut deltas = Vec::new();
    for batch in &mut reader {
        let batch = batch.expect("read Arrow IPC delta batch");
        deltas.extend(record_batch_to_deltas(&batch));
    }
    deltas
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

async fn build_versioned_segments(
    dict: &Dictionary<BenchRow>,
    deltas: &[(BenchRow, i64)],
) -> Vec<SegmentRecord> {
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (row, delta) in deltas {
        if *delta == 0 {
            continue;
        }
        let id = dict_batch.intern(row).await.expect("intern row");
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
    segments
}

async fn apply_versioned_deltas(
    versioned: &mut VersionedZSet<BenchRow>,
    dict: &Dictionary<BenchRow>,
    deltas: &[(BenchRow, i64)],
) {
    let segments = build_versioned_segments(dict, deltas).await;
    if segments.is_empty() {
        return;
    }
    let base = versioned.current_handle().map(|handle| handle.version);
    versioned
        .create_version_with_base(segments, base)
        .await
        .expect("create version");
}

fn overlay_segment_key(prefix: &str, segment_id: u64) -> Vec<u8> {
    format!("{prefix}/segments/{segment_id:020}").into_bytes()
}

fn overlay_index_prefix(prefix: &str, index_key: i64) -> Vec<u8> {
    format!("{prefix}/index/{index_key:020}/").into_bytes()
}

fn overlay_index_key(prefix: &str, index_key: i64, segment_id: u64, row_index: usize) -> Vec<u8> {
    format!("{prefix}/index/{index_key:020}/{segment_id:020}/{row_index:08}").into_bytes()
}

fn encode_row_ref_delta(segment_id: u64, row_index: u32, delta: i64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(20);
    payload.extend_from_slice(&segment_id.to_be_bytes());
    payload.extend_from_slice(&row_index.to_be_bytes());
    payload.extend_from_slice(&delta.to_be_bytes());
    payload
}

fn decode_row_ref_delta(payload: &[u8]) -> (u64, u32, i64) {
    assert_eq!(payload.len(), 20, "invalid row-ref payload size");
    let mut segment_bytes = [0_u8; 8];
    segment_bytes.copy_from_slice(&payload[..8]);
    let mut row_bytes = [0_u8; 4];
    row_bytes.copy_from_slice(&payload[8..12]);
    let mut delta_bytes = [0_u8; 8];
    delta_bytes.copy_from_slice(&payload[12..20]);
    (
        u64::from_be_bytes(segment_bytes),
        u32::from_be_bytes(row_bytes),
        i64::from_be_bytes(delta_bytes),
    )
}

#[derive(Debug, Default, Clone, Copy)]
struct OverlayLookupMetrics {
    index_entries_scanned: usize,
    unique_segments: usize,
    cache_hits: usize,
    cache_misses: usize,
    decoded_rows: usize,
}

#[derive(Debug)]
struct OverlayLookupResult {
    values: Vec<(BenchRow, i64)>,
    metrics: OverlayLookupMetrics,
}

async fn overlay_append(
    table: Arc<dyn KeyValueTable>,
    schema: SchemaRef,
    prefix: &str,
    segment_id: u64,
    updates: &[(i64, BenchRow, i64)],
) {
    let segment_deltas = strip_keyed_updates(updates);
    let segment_payload = encode_arrow_ipc_delta_batch(schema, &segment_deltas);
    let segment_key = overlay_segment_key(prefix, segment_id);

    let mut batch = WriteBatch::new();
    batch.put(segment_key, segment_payload);
    for (row_index, (index_key, _, delta)) in updates.iter().enumerate() {
        let key = overlay_index_key(prefix, *index_key, segment_id, row_index);
        let value = encode_row_ref_delta(segment_id, row_index as u32, *delta);
        batch.put(key, value);
    }
    table
        .write_batch(batch)
        .await
        .expect("write overlay segment");
}

async fn overlay_lookup_with_cache(
    table: Arc<dyn KeyValueTable>,
    prefix: &str,
    index_key: i64,
    segment_cache: &mut OverlayDecodeCache<Vec<(BenchRow, i64)>>,
) -> OverlayLookupResult {
    let entries = table
        .scan_prefix(
            &overlay_index_prefix(prefix, index_key),
            &ScanOptions::default(),
        )
        .await
        .expect("scan overlay index");
    let mut refs_by_segment: HashMap<u64, Vec<(u32, i64)>> = HashMap::new();
    for (_, payload) in entries {
        let (segment_id, row_index, delta) = decode_row_ref_delta(&payload);
        refs_by_segment
            .entry(segment_id)
            .or_default()
            .push((row_index, delta));
    }

    let mut metrics = OverlayLookupMetrics {
        index_entries_scanned: refs_by_segment.values().map(Vec::len).sum(),
        unique_segments: refs_by_segment.len(),
        ..OverlayLookupMetrics::default()
    };
    let mut aggregate: HashMap<BenchRow, i64> = HashMap::new();
    for (segment_id, refs) in refs_by_segment {
        let rows = if let Some(cached_rows) = segment_cache.get(segment_id) {
            metrics.cache_hits = metrics.cache_hits.saturating_add(1);
            cached_rows
        } else {
            metrics.cache_misses = metrics.cache_misses.saturating_add(1);
            let bytes = table
                .get(&overlay_segment_key(prefix, segment_id))
                .await
                .expect("get overlay segment")
                .expect("missing overlay segment");
            let decoded = decode_arrow_ipc_delta_batch(&bytes);
            metrics.decoded_rows = metrics.decoded_rows.saturating_add(decoded.len());
            segment_cache.insert_miss(segment_id, decoded)
        };
        for (row_index, delta) in refs {
            let (row, _) = rows
                .get(row_index as usize)
                .expect("invalid row index in overlay");
            *aggregate.entry(row.clone()).or_insert(0) += delta;
        }
    }
    aggregate.retain(|_, weight| *weight != 0);
    OverlayLookupResult {
        values: aggregate.into_iter().collect(),
        metrics,
    }
}

async fn overlay_read_amplification(
    table: Arc<dyn KeyValueTable>,
    prefix: &str,
    index_key: i64,
) -> usize {
    let entries = table
        .scan_prefix(
            &overlay_index_prefix(prefix, index_key),
            &ScanOptions::default(),
        )
        .await
        .expect("scan overlay index");
    let mut segment_ids = HashSet::new();
    for (_, payload) in entries {
        let (segment_id, _, _) = decode_row_ref_delta(&payload);
        segment_ids.insert(segment_id);
    }
    segment_ids.len().saturating_add(
        table
            .scan_prefix(
                &overlay_index_prefix(prefix, index_key),
                &ScanOptions::default(),
            )
            .await
            .expect("scan overlay index for entry count")
            .len(),
    )
}

async fn prefix_total_bytes(table: Arc<dyn KeyValueTable>, prefix: &[u8]) -> u64 {
    let entries = table
        .scan_prefix(prefix, &ScanOptions::default())
        .await
        .expect("scan prefix for byte accounting");
    entries
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum()
}

async fn versioned_storage_bytes(table: Arc<dyn KeyValueTable>, namespace: &str) -> u64 {
    let zset_prefix = format!("zset/{namespace}/").into_bytes();
    let dict_prefix = format!("dict/{namespace}/").into_bytes();
    prefix_total_bytes(table.clone(), &zset_prefix).await
        + prefix_total_bytes(table, &dict_prefix).await
}

fn ledger_segment_key(prefix: &str, segment_id: u64) -> Vec<u8> {
    format!("{prefix}/segments/{segment_id:020}").into_bytes()
}

fn ledger_version_prefix(prefix: &str) -> Vec<u8> {
    format!("{prefix}/versions/").into_bytes()
}

fn ledger_version_key(prefix: &str, version: u64) -> Vec<u8> {
    format!("{prefix}/versions/{version:020}").into_bytes()
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(bytes: &[u8]) -> u64 {
    assert_eq!(bytes.len(), 8, "invalid u64 payload width");
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(bytes);
    u64::from_be_bytes(raw)
}

async fn ledger_append(
    table: Arc<dyn KeyValueTable>,
    schema: SchemaRef,
    prefix: &str,
    version: u64,
    deltas: &[(BenchRow, i64)],
) {
    let mut batch = WriteBatch::new();
    let payload = encode_arrow_ipc_delta_batch(schema, deltas);
    batch.put(ledger_segment_key(prefix, version), payload);
    batch.put(ledger_version_key(prefix, version), encode_u64(version));
    table
        .write_batch(batch)
        .await
        .expect("append ledger version");
}

async fn ledger_materialize(table: Arc<dyn KeyValueTable>, prefix: &str) -> HashMap<BenchRow, i64> {
    let version_entries = table
        .scan_prefix(&ledger_version_prefix(prefix), &ScanOptions::default())
        .await
        .expect("scan ledger versions");
    let mut aggregate: HashMap<BenchRow, i64> = HashMap::new();
    for (_, payload) in version_entries {
        let segment_id = decode_u64(&payload);
        let bytes = table
            .get(&ledger_segment_key(prefix, segment_id))
            .await
            .expect("get ledger segment")
            .expect("missing ledger segment");
        for (row, delta) in decode_arrow_ipc_delta_batch(&bytes) {
            *aggregate.entry(row).or_insert(0) += delta;
        }
    }
    aggregate.retain(|_, weight| *weight != 0);
    aggregate
}

fn segment_stats_for_deltas(deltas: &[(BenchRow, i64)]) -> SegmentWriteStats {
    let mut min_hash = u64::MAX;
    let mut max_hash = 0_u64;
    let mut tombstones = 0_usize;
    for (row, delta) in deltas {
        let hash = row.id as u64;
        min_hash = min_hash.min(hash);
        max_hash = max_hash.max(hash);
        if *delta < 0 {
            tombstones += 1;
        }
    }
    if deltas.is_empty() {
        min_hash = 0;
    }
    let tombstone_ratio = if deltas.is_empty() {
        0.0
    } else {
        tombstones as f64 / deltas.len() as f64
    };
    SegmentWriteStats::new(min_hash, max_hash, tombstone_ratio).expect("segment stats")
}

fn manifest_stats(segment_count: usize, row_count: usize, total_bytes: u64) -> ManifestStatistics {
    ManifestStatistics::new(segment_count as u64, row_count as u64, total_bytes, 0.0)
        .expect("manifest stats")
}

fn bench_update_model_indexed(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = delta_schema();
    let mut group = c.benchmark_group("update_model_indexed");

    for &batch_size in MODEL_BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let key_cardinality = (batch_size / KEY_CARDINALITY_DIVISOR).max(1) as i64;
        let initial = initial_keyed_updates(batch_size, key_cardinality);
        let updates_0_to_1 = transition_keyed_updates(batch_size, key_cardinality, 0, 1);
        let updates_1_to_0 = transition_keyed_updates(batch_size, key_cardinality, 1, 0);
        let coalescing_probe = coalescing_probe_updates(batch_size, key_cardinality);
        let deltas_0_to_1 = strip_keyed_updates(&updates_0_to_1);
        let arrow_delta_bytes = encode_arrow_ipc_delta_batch(Arc::clone(&schema), &deltas_0_to_1);

        println!(
            "update_model_size_report,batch_size={batch_size},arrow_delta_total_bytes={}",
            arrow_delta_bytes.len()
        );
        let indexed_amp_table = open_table(
            &runtime,
            next_db_name("bench-arrow-indexed-amp", batch_size),
        );
        let indexed_amp_namespace = next_namespace("arrow_indexed_amp");
        let indexed_put_table = open_table(
            &runtime,
            next_db_name("bench-arrow-indexed-put", batch_size),
        );
        let indexed_put_namespace = next_namespace("arrow_indexed_put");
        let overlay_amp_table = open_table(&runtime, next_db_name("bench-overlay-amp", batch_size));
        let overlay_amp_prefix = format!(
            "bench/overlay-amp/{batch_size}/{}",
            NS_ID.fetch_add(1, Ordering::Relaxed)
        );
        let (
            arrow_indexed_read_amp,
            arrow_indexed_put_metrics,
            overlay_read_amp,
            cold_lookup,
            warm_lookup,
        ) = runtime.block_on(async {
            let put_index = IndexedBatchZSet::new(indexed_put_table, indexed_put_namespace);
            let indexed_put_metrics = put_index
                .apply_deltas_with_stats(coalescing_probe.iter().cloned())
                .await
                .expect("apply coalescing probe updates");

            let index = IndexedBatchZSet::new(indexed_amp_table, indexed_amp_namespace);
            index
                .apply_deltas(initial.iter().cloned())
                .await
                .expect("seed Arrow-indexed zset for read amplification");
            index
                .apply_deltas(updates_0_to_1.iter().cloned())
                .await
                .expect("apply Arrow-indexed zset updates for read amplification");
            let indexed_read_amp = index
                .estimated_read_amplification_for_key(&0_i64)
                .await
                .expect("estimate Arrow-indexed read amplification");

            overlay_append(
                overlay_amp_table.clone(),
                Arc::clone(&schema),
                &overlay_amp_prefix,
                1,
                &initial,
            )
            .await;
            overlay_append(
                overlay_amp_table.clone(),
                Arc::clone(&schema),
                &overlay_amp_prefix,
                2,
                &updates_0_to_1,
            )
            .await;
            let mut decode_cache = OverlayDecodeCache::new(OVERLAY_DECODE_CACHE_CAPACITY);
            let cold_lookup = overlay_lookup_with_cache(
                overlay_amp_table.clone(),
                &overlay_amp_prefix,
                0,
                &mut decode_cache,
            )
            .await;
            let warm_lookup = overlay_lookup_with_cache(
                overlay_amp_table.clone(),
                &overlay_amp_prefix,
                0,
                &mut decode_cache,
            )
            .await;
            let overlay_read_amp =
                overlay_read_amplification(overlay_amp_table, &overlay_amp_prefix, 0).await;
            (
                indexed_read_amp,
                indexed_put_metrics,
                overlay_read_amp,
                cold_lookup.metrics,
                warm_lookup.metrics,
            )
        });
        println!(
            "update_model_read_amp_report,batch_size={batch_size},arrow_indexed_read_amp={arrow_indexed_read_amp},overlay_read_amp={overlay_read_amp}"
        );
        println!(
            "update_model_put_report,batch_size={batch_size},input_non_zero={},persisted_records={},write_reduction={:.2}",
            arrow_indexed_put_metrics.non_zero_input_records,
            arrow_indexed_put_metrics.persisted_records,
            if arrow_indexed_put_metrics.non_zero_input_records == 0 {
                0.0
            } else {
                arrow_indexed_put_metrics.persisted_records as f64
                    / arrow_indexed_put_metrics.non_zero_input_records as f64
            }
        );
        println!(
            "update_model_overlay_decode_report,batch_size={batch_size},index_entries_cold={},segments_cold={},decoded_rows_cold={},cache_hits_warm={},cache_misses_warm={},decoded_rows_warm={},index_entries_warm={},segments_warm={}",
            cold_lookup.index_entries_scanned,
            cold_lookup.unique_segments,
            cold_lookup.decoded_rows,
            warm_lookup.cache_hits,
            warm_lookup.cache_misses,
            warm_lookup.decoded_rows,
            warm_lookup.index_entries_scanned,
            warm_lookup.unique_segments,
        );

        group.bench_function(
            BenchmarkId::new("arrow_indexed_zset_apply_toggle", batch_size),
            |b| {
                let table = open_table(
                    &runtime,
                    next_db_name("bench-arrow-indexed-zset", batch_size),
                );
                let namespace = next_namespace("arrow_indexed_zset");
                let index = IndexedBatchZSet::new(table, namespace);
                runtime.block_on(async {
                    index
                        .apply_deltas(initial.iter().cloned())
                        .await
                        .expect("seed Arrow-indexed zset");
                });

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        index
                            .apply_deltas(updates.iter().cloned())
                            .await
                            .expect("apply Arrow-indexed zset toggle");
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_indexed_zset_apply_lookup_hot_key", batch_size),
            |b| {
                let table = open_table(
                    &runtime,
                    next_db_name("bench-arrow-indexed-hot", batch_size),
                );
                let namespace = next_namespace("arrow_indexed_zset_hot");
                let index = IndexedBatchZSet::new(table, namespace);
                runtime.block_on(async {
                    index
                        .apply_deltas(initial.iter().cloned())
                        .await
                        .expect("seed Arrow-indexed zset");
                });

                let lookup_key = 0_i64;
                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        index
                            .apply_deltas(updates.iter().cloned())
                            .await
                            .expect("apply Arrow-indexed zset toggle");
                        let values = index
                            .values_for_key(&lookup_key)
                            .await
                            .expect("lookup Arrow-indexed zset key");
                        black_box(values);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_overlay_append_toggle", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-overlay", batch_size));
                let prefix = format!(
                    "bench/overlay/{batch_size}/{}",
                    NS_ID.fetch_add(1, Ordering::Relaxed)
                );
                let mut next_segment_id = 1_u64;
                runtime.block_on(overlay_append(
                    table.clone(),
                    Arc::clone(&schema),
                    &prefix,
                    next_segment_id,
                    &initial,
                ));
                next_segment_id += 1;

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(overlay_append(
                        table.clone(),
                        Arc::clone(&schema),
                        &prefix,
                        next_segment_id,
                        updates,
                    ));
                    next_segment_id += 1;
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_overlay_append_lookup_hot_key", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-overlay-hot", batch_size));
                let prefix = format!(
                    "bench/overlay-hot/{batch_size}/{}",
                    NS_ID.fetch_add(1, Ordering::Relaxed)
                );
                let mut next_segment_id = 1_u64;
                runtime.block_on(overlay_append(
                    table.clone(),
                    Arc::clone(&schema),
                    &prefix,
                    next_segment_id,
                    &initial,
                ));
                next_segment_id += 1;

                let lookup_key = 0_i64;
                let mut flip = false;
                let mut segment_cache = OverlayDecodeCache::new(OVERLAY_DECODE_CACHE_CAPACITY);
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    segment_cache.invalidate(next_segment_id);
                    runtime.block_on(overlay_append(
                        table.clone(),
                        Arc::clone(&schema),
                        &prefix,
                        next_segment_id,
                        updates,
                    ));
                    next_segment_id += 1;
                    runtime.block_on(async {
                        let result = overlay_lookup_with_cache(
                            table.clone(),
                            &prefix,
                            lookup_key,
                            &mut segment_cache,
                        )
                        .await;
                        black_box(result.values);
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_update_model_versioned(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = delta_schema();
    let mut group = c.benchmark_group("update_model_versioned");

    for &batch_size in MODEL_BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let key_cardinality = (batch_size / KEY_CARDINALITY_DIVISOR).max(1) as i64;
        let initial = strip_keyed_updates(&initial_keyed_updates(batch_size, key_cardinality));
        let updates_0_to_1 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 0, 1));
        let updates_1_to_0 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 1, 0));
        let versioned_metrics_table =
            open_table(&runtime, next_db_name("bench-versioned-growth", batch_size));
        let versioned_metrics_namespace = next_namespace("versioned_growth");
        let ledger_metrics_table =
            open_table(&runtime, next_db_name("bench-ledger-growth", batch_size));
        let ledger_metrics_prefix = format!(
            "bench/ledger-growth/{batch_size}/{}",
            NS_ID.fetch_add(1, Ordering::Relaxed)
        );
        let (versioned_growth_bytes, ledger_growth_bytes) = runtime.block_on(async {
            let dict = Arc::new(
                Dictionary::<BenchRow>::with_table(
                    versioned_metrics_table.clone(),
                    versioned_metrics_namespace.clone(),
                    None,
                )
                .await
                .expect("create dictionary for growth metrics"),
            );
            let mut versioned = VersionedZSet::new(
                dict.clone(),
                versioned_metrics_table.clone(),
                versioned_metrics_namespace.clone(),
            )
            .await
            .expect("create versioned zset for growth metrics");
            apply_versioned_deltas(&mut versioned, dict.as_ref(), &initial).await;
            apply_versioned_deltas(&mut versioned, dict.as_ref(), &updates_0_to_1).await;
            let versioned_growth_bytes =
                versioned_storage_bytes(versioned_metrics_table, &versioned_metrics_namespace)
                    .await;

            ledger_append(
                ledger_metrics_table.clone(),
                Arc::clone(&schema),
                &ledger_metrics_prefix,
                1,
                &initial,
            )
            .await;
            ledger_append(
                ledger_metrics_table.clone(),
                Arc::clone(&schema),
                &ledger_metrics_prefix,
                2,
                &updates_0_to_1,
            )
            .await;
            let ledger_growth_bytes =
                prefix_total_bytes(ledger_metrics_table, ledger_metrics_prefix.as_bytes()).await;
            (versioned_growth_bytes, ledger_growth_bytes)
        });
        println!(
            "update_model_growth_report,batch_size={batch_size},versioned_growth_bytes={versioned_growth_bytes},ledger_growth_bytes={ledger_growth_bytes}"
        );

        group.bench_function(
            BenchmarkId::new("versioned_zset_write_materialize_toggle", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-versioned-zset", batch_size));
                let namespace = next_namespace("versioned_zset");
                let dict = runtime.block_on(async {
                    Arc::new(
                        Dictionary::<BenchRow>::with_table(table.clone(), namespace.clone(), None)
                            .await
                            .expect("create dictionary"),
                    )
                });
                let mut versioned = runtime.block_on(async {
                    VersionedZSet::new(dict.clone(), table.clone(), namespace)
                        .await
                        .expect("create versioned zset")
                });
                runtime.block_on(apply_versioned_deltas(
                    &mut versioned,
                    dict.as_ref(),
                    &initial,
                ));

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        apply_versioned_deltas(&mut versioned, dict.as_ref(), updates).await;
                        let materialized = versioned.materialize().await.expect("materialize");
                        black_box(materialized);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new(
                "versioned_zset_write_materialize_toggle_compacted",
                batch_size,
            ),
            |b| {
                let table = open_table(
                    &runtime,
                    next_db_name("bench-versioned-zset-compacted", batch_size),
                );
                let namespace = next_namespace("versioned_zset_compacted");
                let dict = runtime.block_on(async {
                    Arc::new(
                        Dictionary::<BenchRow>::with_table(table.clone(), namespace.clone(), None)
                            .await
                            .expect("create dictionary"),
                    )
                });
                let mut versioned = runtime.block_on(async {
                    VersionedZSet::new(dict.clone(), table.clone(), namespace)
                        .await
                        .expect("create versioned zset")
                });
                runtime.block_on(apply_versioned_deltas(
                    &mut versioned,
                    dict.as_ref(),
                    &initial,
                ));

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    runtime.block_on(async {
                        apply_versioned_deltas(&mut versioned, dict.as_ref(), updates).await;
                        if versioned.current_handle().is_some() {
                            let _ = versioned.compact_current().await.expect("compact current");
                        }
                        let materialized = versioned.materialize().await.expect("materialize");
                        black_box(materialized);
                    });
                });
            },
        );

        group.bench_function(
            BenchmarkId::new("arrow_ledger_write_materialize_toggle", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-arrow-ledger", batch_size));
                let prefix = format!(
                    "bench/ledger/{batch_size}/{}",
                    NS_ID.fetch_add(1, Ordering::Relaxed)
                );
                let mut version = 1_u64;
                runtime.block_on(ledger_append(
                    table.clone(),
                    Arc::clone(&schema),
                    &prefix,
                    version,
                    &initial,
                ));

                let mut flip = false;
                b.iter(|| {
                    let updates = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    version += 1;
                    runtime.block_on(ledger_append(
                        table.clone(),
                        Arc::clone(&schema),
                        &prefix,
                        version,
                        updates,
                    ));
                    runtime.block_on(async {
                        let materialized = ledger_materialize(table.clone(), &prefix).await;
                        black_box(materialized);
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_update_model_gc_pressure(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = delta_schema();
    let mut group = c.benchmark_group("update_model_gc_pressure");

    for &batch_size in MODEL_BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        let key_cardinality = (batch_size / KEY_CARDINALITY_DIVISOR).max(1) as i64;
        let initial = strip_keyed_updates(&initial_keyed_updates(batch_size, key_cardinality));
        let updates_0_to_1 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 0, 1));
        let updates_1_to_0 =
            strip_keyed_updates(&transition_keyed_updates(batch_size, key_cardinality, 1, 0));

        let metric_table = open_table(&runtime, next_db_name("bench-gc-metric", batch_size));
        let metric_namespace = next_namespace("gc_metric");
        let metric_segment_store =
            ArrowSegmentStore::new(metric_table.clone(), metric_namespace.clone());
        let metric_manifest_store =
            ManifestStore::<DataManifest>::data(metric_table.clone(), metric_namespace.clone());
        let metric_gc = GcService::new(
            metric_table.clone(),
            metric_namespace.clone(),
            GcPolicy {
                grace_period: Duration::ZERO,
            },
        );
        let metric_deltas = updates_0_to_1.clone();
        let metric_sweep = runtime.block_on(async {
            let batch = deltas_to_record_batch(Arc::clone(&schema), &initial);
            metric_segment_store
                .write_segment(
                    1,
                    Arc::clone(&schema),
                    &[batch],
                    segment_stats_for_deltas(&initial),
                )
                .await
                .expect("write metric seed segment");
            metric_manifest_store
                .publish_manifest(&DataManifest {
                    version: 1,
                    base: None,
                    reference_count: 1,
                    statistics: manifest_stats(1, initial.len(), 0),
                    segments: vec![1],
                })
                .await
                .expect("publish metric seed manifest");

            let churn_batch = deltas_to_record_batch(Arc::clone(&schema), &metric_deltas);
            let churn_payload = encode_arrow_ipc_delta_batch(Arc::clone(&schema), &metric_deltas);
            metric_segment_store
                .write_segment(
                    2,
                    Arc::clone(&schema),
                    &[churn_batch],
                    segment_stats_for_deltas(&metric_deltas),
                )
                .await
                .expect("write metric churn segment");
            metric_manifest_store
                .publish_manifest(&DataManifest {
                    version: 2,
                    base: None,
                    reference_count: 1,
                    statistics: manifest_stats(1, metric_deltas.len(), churn_payload.len() as u64),
                    segments: vec![2],
                })
                .await
                .expect("publish metric churn manifest");
            metric_gc.sweep_once().await.expect("metric GC sweep")
        });
        println!(
            "update_model_gc_report,batch_size={batch_size},gc_marked={},gc_deleted={}",
            metric_sweep.marked, metric_sweep.deleted
        );

        group.bench_function(
            BenchmarkId::new("gc_manifest_churn_sweep", batch_size),
            |b| {
                let table = open_table(&runtime, next_db_name("bench-gc-churn", batch_size));
                let namespace = next_namespace("gc_churn");
                let segment_store = ArrowSegmentStore::new(table.clone(), namespace.clone());
                let manifest_store =
                    ManifestStore::<DataManifest>::data(table.clone(), namespace.clone());
                let gc = GcService::new(
                    table,
                    namespace,
                    GcPolicy {
                        grace_period: Duration::ZERO,
                    },
                );
                let mut segment_id = 1_u64;
                let mut version = 1_u64;
                runtime.block_on(async {
                    let seed_batch = deltas_to_record_batch(Arc::clone(&schema), &initial);
                    segment_store
                        .write_segment(
                            segment_id,
                            Arc::clone(&schema),
                            &[seed_batch],
                            segment_stats_for_deltas(&initial),
                        )
                        .await
                        .expect("write churn seed segment");
                    manifest_store
                        .publish_manifest(&DataManifest {
                            version,
                            base: None,
                            reference_count: 1,
                            statistics: manifest_stats(1, initial.len(), 0),
                            segments: vec![segment_id],
                        })
                        .await
                        .expect("publish churn seed manifest");
                });

                let mut flip = false;
                b.iter(|| {
                    let deltas = if flip {
                        &updates_1_to_0
                    } else {
                        &updates_0_to_1
                    };
                    flip = !flip;
                    segment_id = segment_id.saturating_add(1);
                    version = version.saturating_add(1);
                    runtime.block_on(async {
                        let churn_batch = deltas_to_record_batch(Arc::clone(&schema), deltas);
                        let churn_payload =
                            encode_arrow_ipc_delta_batch(Arc::clone(&schema), deltas);
                        segment_store
                            .write_segment(
                                segment_id,
                                Arc::clone(&schema),
                                &[churn_batch],
                                segment_stats_for_deltas(deltas),
                            )
                            .await
                            .expect("write churn segment");
                        manifest_store
                            .publish_manifest(&DataManifest {
                                version,
                                base: None,
                                reference_count: 1,
                                statistics: manifest_stats(
                                    1,
                                    deltas.len(),
                                    churn_payload.len() as u64,
                                ),
                                segments: vec![segment_id],
                            })
                            .await
                            .expect("publish churn manifest");
                        let sweep = gc.sweep_once().await.expect("run churn sweep");
                        black_box(sweep.deleted);
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_update_model_dictionary(c: &mut Criterion) {
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

criterion_group!(
    benches,
    bench_update_model_indexed,
    bench_update_model_versioned,
    bench_update_model_gc_pressure,
    bench_update_model_dictionary
);
criterion_main!(benches);
use floe_benchmarks::overlay_decode_cache::OverlayDecodeCache;
