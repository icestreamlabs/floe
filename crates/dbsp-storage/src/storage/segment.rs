use std::io::Cursor;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use rkyv::{Archive, Deserialize, Serialize};
use slatedb::config::ScanOptions;

use crate::storage::KeyValueTable;
use crate::storage::encoding;
use crate::storage::keyspace;

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub row_count: u64,
    pub byte_size: u64,
    pub min_key_hash: u64,
    pub max_key_hash: u64,
    pub tombstone_ratio: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentWriteStats {
    pub min_key_hash: u64,
    pub max_key_hash: u64,
    pub tombstone_ratio: f64,
}

impl SegmentWriteStats {
    pub fn new(min_key_hash: u64, max_key_hash: u64, tombstone_ratio: f64) -> Result<Self> {
        if min_key_hash > max_key_hash {
            bail!("min_key_hash must be less than or equal to max_key_hash");
        }
        if !(0.0..=1.0).contains(&tombstone_ratio) {
            bail!("tombstone_ratio must be between 0.0 and 1.0");
        }
        Ok(Self {
            min_key_hash,
            max_key_hash,
            tombstone_ratio,
        })
    }
}

#[derive(Debug)]
pub struct ArrowSegment {
    pub metadata: SegmentMetadata,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
struct SegmentEnvelope {
    metadata: SegmentMetadata,
    payload: Vec<u8>,
}

pub fn encode_record_batches(schema: SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    for batch in batches {
        if batch.schema().as_ref() != schema.as_ref() {
            bail!("record batch schema mismatch while encoding Arrow segment");
        }
    }

    let mut payload = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut payload, schema.as_ref()).context("create Arrow IPC writer")?;
    for batch in batches {
        writer
            .write(batch)
            .context("write record batch to Arrow IPC payload")?;
    }
    writer.finish().context("finalize Arrow IPC writer")?;
    drop(writer);

    Ok(payload)
}

pub fn decode_record_batches(payload: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(Cursor::new(payload), None)
        .context("create Arrow IPC reader for segment payload")?;
    let schema = reader.schema();
    let mut batches = Vec::new();

    for batch in reader {
        let batch = batch.context("read record batch from Arrow IPC payload")?;
        batches.push(batch);
    }

    Ok((schema, batches))
}

pub struct ArrowSegmentStore {
    table: Arc<dyn KeyValueTable>,
    segment_prefix: Vec<u8>,
}

pub fn encode_segment_envelope(
    schema: SchemaRef,
    batches: &[RecordBatch],
    stats: SegmentWriteStats,
) -> Result<(Vec<u8>, SegmentMetadata)> {
    let payload = encode_record_batches(schema, batches).context("encode Arrow segment data")?;
    let row_count = row_count(batches)?;
    let metadata = SegmentMetadata {
        row_count,
        byte_size: usize_to_u64(payload.len())?,
        min_key_hash: stats.min_key_hash,
        max_key_hash: stats.max_key_hash,
        tombstone_ratio: stats.tombstone_ratio,
    };
    let envelope = SegmentEnvelope { metadata, payload };
    let encoded = encoding::encode(&envelope).context("encode Arrow segment envelope")?;
    Ok((encoded, envelope.metadata))
}

impl ArrowSegmentStore {
    pub fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let segment_prefix = keyspace::segment_data_prefix(&namespace);
        Self {
            table,
            segment_prefix,
        }
    }

    pub async fn write_segment(
        &self,
        segment_id: u64,
        schema: SchemaRef,
        batches: &[RecordBatch],
        stats: SegmentWriteStats,
    ) -> Result<SegmentMetadata> {
        let (encoded, metadata) = encode_segment_envelope(schema, batches, stats)?;
        self.table
            .put(&self.segment_key(segment_id), &encoded)
            .await
            .with_context(|| format!("persist Arrow segment {segment_id}"))?;
        Ok(metadata)
    }

    pub async fn read_segment(&self, segment_id: u64) -> Result<Option<ArrowSegment>> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.segment_key(segment_id))
            .await
            .with_context(|| format!("read Arrow segment {segment_id}"))?
        else {
            return Ok(None);
        };

        let envelope: SegmentEnvelope =
            encoding::decode(bytes.as_ref()).context("decode Arrow segment envelope")?;
        let (schema, batches) =
            decode_record_batches(&envelope.payload).context("decode Arrow segment payload")?;

        let decoded_row_count = row_count(&batches)?;
        if envelope.metadata.row_count != decoded_row_count {
            bail!(
                "Arrow segment row_count metadata mismatch for segment {segment_id}: expected {}, decoded {}",
                envelope.metadata.row_count,
                decoded_row_count
            );
        }

        let decoded_byte_size = usize_to_u64(envelope.payload.len())?;
        if envelope.metadata.byte_size != decoded_byte_size {
            bail!(
                "Arrow segment byte_size metadata mismatch for segment {segment_id}: expected {}, decoded {}",
                envelope.metadata.byte_size,
                decoded_byte_size
            );
        }

        Ok(Some(ArrowSegment {
            metadata: envelope.metadata,
            schema,
            batches,
        }))
    }

    pub async fn list_segment_ids(&self) -> Result<Vec<u64>> {
        let entries = self
            .table
            .scan_prefix(&self.segment_prefix, &ScanOptions::default())
            .await
            .context("scan Arrow segment prefix")?;

        entries
            .into_iter()
            .map(|(key, _)| self.segment_id_from_key(&key))
            .collect()
    }

    pub fn key_for_segment(&self, segment_id: u64) -> Vec<u8> {
        self.segment_key(segment_id)
    }

    fn segment_key(&self, segment_id: u64) -> Vec<u8> {
        keyspace::key_with_u64(&self.segment_prefix, segment_id)
    }

    fn segment_id_from_key(&self, key: &[u8]) -> Result<u64> {
        keyspace::parse_u64_key_suffix(&self.segment_prefix, key)
            .ok_or_else(|| anyhow!("invalid Arrow segment key suffix"))
    }
}

fn row_count(batches: &[RecordBatch]) -> Result<u64> {
    batches.iter().try_fold(0_u64, |count, batch| {
        let rows = usize_to_u64(batch.num_rows())?;
        count
            .checked_add(rows)
            .ok_or_else(|| anyhow!("segment row count overflow"))
    })
}

fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("value does not fit into u64"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use object_store::memory::InMemory;
    use slatedb::Db;

    use crate::storage::SlateTable;

    use super::{
        ArrowSegmentStore, SegmentWriteStats, decode_record_batches, encode_record_batches,
    };

    async fn build_store(namespace: &str) -> ArrowSegmentStore {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(namespace, store).await.expect("open SlateDB"));
        ArrowSegmentStore::new(Arc::new(SlateTable::new(db)), namespace)
    }

    fn row_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key_hash", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ]))
    }

    fn row_batch(schema: SchemaRef, rows: &[(u64, i64, i64)]) -> RecordBatch {
        let key_hashes: Vec<u64> = rows.iter().map(|(hash, _, _)| *hash).collect();
        let values: Vec<i64> = rows.iter().map(|(_, value, _)| *value).collect();
        let deltas: Vec<i64> = rows.iter().map(|(_, _, delta)| *delta).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(key_hashes)) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
                Arc::new(Int64Array::from(deltas)) as ArrayRef,
            ],
        )
        .expect("build record batch")
    }

    fn read_rows(batch: &RecordBatch) -> Vec<(u64, i64, i64)> {
        let key_hashes = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("key_hashes");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("values");
        let deltas = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("deltas");

        (0..batch.num_rows())
            .map(|idx| (key_hashes.value(idx), values.value(idx), deltas.value(idx)))
            .collect()
    }

    #[test]
    fn arrow_ipc_encode_decode_round_trip() {
        let schema = row_schema();
        let batches = vec![
            row_batch(Arc::clone(&schema), &[(3, 10, 1), (8, 11, -1)]),
            row_batch(Arc::clone(&schema), &[(20, 20, 1)]),
        ];

        let payload = encode_record_batches(Arc::clone(&schema), &batches).expect("encode batches");
        let (decoded_schema, decoded_batches) =
            decode_record_batches(&payload).expect("decode batches");

        assert_eq!(decoded_schema.as_ref(), schema.as_ref());
        assert_eq!(decoded_batches.len(), batches.len());
        assert_eq!(read_rows(&decoded_batches[0]), read_rows(&batches[0]));
        assert_eq!(read_rows(&decoded_batches[1]), read_rows(&batches[1]));
    }

    #[tokio::test]
    async fn segment_store_write_read_round_trip() {
        let schema = row_schema();
        let store = build_store("segment-write-read").await;
        let batches = vec![
            row_batch(Arc::clone(&schema), &[(3, 10, 1), (8, 11, -1)]),
            row_batch(Arc::clone(&schema), &[(20, 20, 1)]),
        ];
        let stats = SegmentWriteStats::new(3, 20, 1.0 / 3.0).expect("segment stats");

        let metadata = store
            .write_segment(7, Arc::clone(&schema), &batches, stats)
            .await
            .expect("write segment");

        assert_eq!(metadata.row_count, 3);
        assert!(metadata.byte_size > 0);
        assert_eq!(metadata.min_key_hash, 3);
        assert_eq!(metadata.max_key_hash, 20);

        let segment = store
            .read_segment(7)
            .await
            .expect("read segment")
            .expect("segment exists");

        assert_eq!(segment.metadata, metadata);
        assert_eq!(segment.schema.as_ref(), schema.as_ref());
        assert_eq!(segment.batches.len(), 2);
        assert_eq!(
            read_rows(&segment.batches[0]),
            vec![(3, 10, 1), (8, 11, -1)]
        );
        assert_eq!(read_rows(&segment.batches[1]), vec![(20, 20, 1)]);
    }

    #[tokio::test]
    async fn list_segment_ids_scans_in_key_order() {
        let schema = row_schema();
        let store = build_store("segment-scan-order").await;
        let stats = SegmentWriteStats::new(1, 1, 0.0).expect("segment stats");
        let ids = [9_u64, 2, 42, 3];

        for segment_id in ids {
            let batch = row_batch(Arc::clone(&schema), &[(1, segment_id as i64, 1)]);
            store
                .write_segment(segment_id, Arc::clone(&schema), &[batch], stats.clone())
                .await
                .expect("write segment");
        }

        let segment_ids = store.list_segment_ids().await.expect("list segment ids");
        assert_eq!(segment_ids, vec![2, 3, 9, 42]);
    }
}
