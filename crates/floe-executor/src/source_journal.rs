use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::storage::{KeyValueTable, prefix_bounds};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

const KAFKA_SOURCE_JOURNAL_PREFIX: &str = "kafka_source_journal";
const VECTORIZED_SOURCE_BATCH_JOURNAL_PREFIX: &str = "vectorized_source_journal";
const VECTORIZED_SOURCE_BATCH_JOURNAL_ARROW_MAGIC: &[u8] = b"FLOE_VECTORIZED_SOURCE_BATCH_ARROW_V1";
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Clone, PartialEq)]
pub struct VectorizedSourceBatchJournalEntry {
    pub source: String,
    pub tick_id: u64,
    pub max_event_time_ms: Option<i64>,
    pub batches: Vec<RecordBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaSourceJournalRange {
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
    pub row_count: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaSourceJournalEntry {
    pub source: String,
    pub tick_id: u64,
    pub max_event_time_ms: Option<i64>,
    pub ranges: Vec<KafkaSourceJournalRange>,
}

#[derive(Clone)]
pub struct VectorizedSourceBatchJournal {
    table: Arc<dyn KeyValueTable>,
}

#[derive(Clone)]
pub struct KafkaSourceJournal {
    table: Arc<dyn KeyValueTable>,
}

impl VectorizedSourceBatchJournal {
    pub fn new(table: Arc<dyn KeyValueTable>) -> Self {
        Self { table }
    }

    pub async fn append(
        &self,
        source: &str,
        tick_id: u64,
        max_event_time_ms: Option<i64>,
        batches: &[RecordBatch],
    ) -> Result<usize> {
        let mut batch = WriteBatch::new();
        let encoded_len = append_vectorized_entry_to_batch(
            &mut batch,
            source,
            tick_id,
            max_event_time_ms,
            batches,
        )?;
        if encoded_len == 0 {
            return Ok(0);
        }
        self.table.write_batch(batch).await.with_context(|| {
            format!(
                "persist vectorized source batch journal entry for '{source}' at tick {tick_id}"
            )
        })?;
        Ok(encoded_len)
    }

    pub async fn load_committed_entries_up_to(
        &self,
        max_tick_id: u64,
        allowed_sources: &BTreeSet<String>,
    ) -> Result<Vec<VectorizedSourceBatchJournalEntry>> {
        let prefix = vectorized_entry_prefix();
        let mut should_continue = |key: &[u8], _value: &[u8]| -> Result<bool> {
            let (tick_id, _) = parse_vectorized_entry_key(key)?;
            Ok(tick_id <= max_tick_id)
        };
        let entries = self
            .table
            .scan_range_bytes_until(
                prefix_bounds(&prefix),
                &ScanOptions::default(),
                &mut should_continue,
            )
            .await
            .context("scan vectorized source batch journal")?;
        let mut recovered = Vec::new();
        for (key, value) in entries {
            let (tick_id, source) = parse_vectorized_entry_key(key.as_ref())?;
            if tick_id > max_tick_id {
                break;
            }
            if !allowed_sources.is_empty() && !allowed_sources.contains(&source) {
                continue;
            }
            let (max_event_time_ms, batches) =
                decode_vectorized_entry(value.as_ref()).with_context(|| {
                    format!(
                        "decode vectorized source batch journal entry for '{source}' at tick {tick_id}"
                    )
                })?;
            recovered.push(VectorizedSourceBatchJournalEntry {
                source,
                tick_id,
                max_event_time_ms,
                batches,
            });
        }
        Ok(recovered)
    }
}

impl KafkaSourceJournal {
    pub fn new(table: Arc<dyn KeyValueTable>) -> Self {
        Self { table }
    }

    pub async fn append(
        &self,
        source: &str,
        tick_id: u64,
        max_event_time_ms: Option<i64>,
        ranges: &[KafkaSourceJournalRange],
    ) -> Result<usize> {
        let mut batch = WriteBatch::new();
        let encoded_len = append_kafka_source_metadata_entry_to_batch(
            &mut batch,
            source,
            tick_id,
            max_event_time_ms,
            ranges,
        )?;
        if encoded_len == 0 {
            return Ok(0);
        }
        self.table.write_batch(batch).await.with_context(|| {
            format!("persist kafka source journal metadata for '{source}' at tick {tick_id}")
        })?;
        Ok(encoded_len)
    }

    pub async fn load_committed_entries_up_to(
        &self,
        max_tick_id: u64,
        allowed_sources: &BTreeSet<String>,
    ) -> Result<Vec<KafkaSourceJournalEntry>> {
        let prefix = kafka_entry_prefix();
        let mut should_continue = |key: &[u8], _value: &[u8]| -> Result<bool> {
            let (tick_id, _) = parse_kafka_entry_key(key)?;
            Ok(tick_id <= max_tick_id)
        };
        let entries = self
            .table
            .scan_range_bytes_until(
                prefix_bounds(&prefix),
                &ScanOptions::default(),
                &mut should_continue,
            )
            .await
            .context("scan kafka source journal metadata")?;
        let mut recovered = Vec::new();
        for (key, value) in entries {
            let (tick_id, source) = parse_kafka_entry_key(key.as_ref())?;
            if tick_id > max_tick_id {
                break;
            }
            if !allowed_sources.is_empty() && !allowed_sources.contains(&source) {
                continue;
            }
            let (max_event_time_ms, ranges) =
                decode_kafka_entry(value.as_ref()).with_context(|| {
                    format!("decode kafka source journal metadata for '{source}' at tick {tick_id}")
                })?;
            recovered.push(KafkaSourceJournalEntry {
                source,
                tick_id,
                max_event_time_ms,
                ranges,
            });
        }
        Ok(recovered)
    }
}

pub(crate) fn append_kafka_source_metadata_entry_to_batch(
    batch: &mut WriteBatch,
    source: &str,
    tick_id: u64,
    max_event_time_ms: Option<i64>,
    ranges: &[KafkaSourceJournalRange],
) -> Result<usize> {
    if ranges.is_empty() {
        return Ok(0);
    }
    let encoded = encode_kafka_entry(max_event_time_ms, ranges)?;
    let encoded_len = encoded.len();
    batch.put(kafka_entry_key(source, tick_id)?, encoded);
    Ok(encoded_len)
}

pub fn append_vectorized_entry_to_batch(
    batch: &mut WriteBatch,
    source: &str,
    tick_id: u64,
    max_event_time_ms: Option<i64>,
    batches: &[RecordBatch],
) -> Result<usize> {
    let encoded = encode_vectorized_entry(max_event_time_ms, batches)?;
    if encoded.is_empty() {
        return Ok(0);
    }
    let encoded_len = encoded.len();
    batch.put(vectorized_entry_key(source, tick_id)?, encoded);
    Ok(encoded_len)
}

pub fn kafka_source_journal_initial_checksum() -> u64 {
    FNV_OFFSET_BASIS
}

pub fn update_kafka_source_journal_checksum(checksum: &mut u64, offset: i64, row: &[u8]) {
    update_fnv64(checksum, &offset.to_le_bytes());
    update_fnv64(
        checksum,
        &(u64::try_from(row.len()).unwrap_or(u64::MAX)).to_le_bytes(),
    );
    update_fnv64(checksum, row);
}

fn vectorized_entry_prefix() -> Vec<u8> {
    format!("{VECTORIZED_SOURCE_BATCH_JOURNAL_PREFIX}/entries/").into_bytes()
}

fn vectorized_entry_key(source: &str, tick_id: u64) -> Result<Vec<u8>> {
    ensure!(
        !source.is_empty() && !source.contains('/'),
        "invalid vectorized source batch journal source '{source}'"
    );
    Ok(
        format!("{VECTORIZED_SOURCE_BATCH_JOURNAL_PREFIX}/entries/{tick_id:020}/{source}")
            .into_bytes(),
    )
}

fn parse_vectorized_entry_key(key: &[u8]) -> Result<(u64, String)> {
    let key_str =
        std::str::from_utf8(key).context("vectorized source batch journal key must be utf8")?;
    let mut parts = key_str.split('/');
    let prefix = parts.next().unwrap_or_default();
    let section = parts.next().unwrap_or_default();
    let tick_id = parts.next().unwrap_or_default();
    let source = parts.next().unwrap_or_default();
    if prefix != VECTORIZED_SOURCE_BATCH_JOURNAL_PREFIX || section != "entries" || source.is_empty()
    {
        return Err(anyhow!(
            "invalid vectorized source batch journal key '{key_str}'"
        ));
    }
    let tick_id = tick_id
        .parse::<u64>()
        .with_context(|| format!("parse vectorized source batch journal tick from '{key_str}'"))?;
    Ok((tick_id, source.to_string()))
}

fn kafka_entry_prefix() -> Vec<u8> {
    format!("{KAFKA_SOURCE_JOURNAL_PREFIX}/entries/").into_bytes()
}

fn kafka_entry_key(source: &str, tick_id: u64) -> Result<Vec<u8>> {
    ensure!(
        !source.is_empty() && !source.contains('/'),
        "invalid kafka source journal source '{source}'"
    );
    Ok(format!("{KAFKA_SOURCE_JOURNAL_PREFIX}/entries/{tick_id:020}/{source}").into_bytes())
}

fn parse_kafka_entry_key(key: &[u8]) -> Result<(u64, String)> {
    let key_str = std::str::from_utf8(key).context("kafka source journal key must be utf8")?;
    let mut parts = key_str.split('/');
    let prefix = parts.next().unwrap_or_default();
    let section = parts.next().unwrap_or_default();
    let tick_id = parts.next().unwrap_or_default();
    let source = parts.next().unwrap_or_default();
    if prefix != KAFKA_SOURCE_JOURNAL_PREFIX || section != "entries" || source.is_empty() {
        return Err(anyhow!("invalid kafka source journal key '{key_str}'"));
    }
    let tick_id = tick_id
        .parse::<u64>()
        .with_context(|| format!("parse kafka source journal tick from '{key_str}'"))?;
    Ok((tick_id, source.to_string()))
}

fn encode_vectorized_entry(
    max_event_time_ms: Option<i64>,
    batches: &[RecordBatch],
) -> Result<Vec<u8>> {
    let mut non_empty = batches.iter().filter(|batch| batch.num_rows() > 0);
    let Some(first) = non_empty.next() else {
        return Ok(Vec::new());
    };
    let schema = first.schema();
    let mut encoded = Vec::new();
    encoded.extend_from_slice(VECTORIZED_SOURCE_BATCH_JOURNAL_ARROW_MAGIC);
    encoded.extend_from_slice(&max_event_time_ms.unwrap_or(-1).to_le_bytes());
    {
        let mut writer = StreamWriter::try_new(&mut encoded, schema.as_ref())
            .context("create vectorized source batch journal Arrow writer")?;
        writer
            .write(first)
            .context("write vectorized source batch journal Arrow batch")?;
        for batch in non_empty {
            if batch.schema().as_ref() != schema.as_ref() {
                bail!("vectorized source batch journal entry contains mixed schemas");
            }
            writer
                .write(batch)
                .context("write vectorized source batch journal Arrow batch")?;
        }
        writer
            .finish()
            .context("finalize vectorized source batch journal Arrow writer")?;
    }
    Ok(encoded)
}

fn decode_vectorized_entry(value: &[u8]) -> Result<(Option<i64>, Vec<RecordBatch>)> {
    if !value.starts_with(VECTORIZED_SOURCE_BATCH_JOURNAL_ARROW_MAGIC) {
        bail!("vectorized source batch journal entry missing Arrow header");
    }
    let mut cursor = VECTORIZED_SOURCE_BATCH_JOURNAL_ARROW_MAGIC.len();
    if value.len() < cursor + 8 {
        bail!("vectorized source batch journal Arrow entry missing metadata header");
    }
    let max_event_time_ms = i64::from_le_bytes(
        value[cursor..cursor + 8]
            .try_into()
            .expect("slice width already checked"),
    );
    cursor += 8;

    let reader = StreamReader::try_new(Cursor::new(&value[cursor..]), None)
        .context("create vectorized source batch journal Arrow reader")?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context("read vectorized source batch journal Arrow batch")?);
    }
    Ok((
        (max_event_time_ms >= 0).then_some(max_event_time_ms),
        batches,
    ))
}

pub(crate) fn encode_arrow_payload_row(
    batch: &RecordBatch,
    payload_width: usize,
    row_idx: usize,
) -> Result<Vec<u8>> {
    let count = u32::try_from(payload_width).context("too many vectorized output columns")?;
    let mut encoded = Vec::with_capacity(4 + payload_width.saturating_mul(16));
    encoded.extend_from_slice(&count.to_le_bytes());
    for column_idx in 0..payload_width {
        append_arrow_value(batch.column(column_idx).as_ref(), row_idx, &mut encoded)?;
    }
    Ok(encoded)
}

fn append_arrow_value(array: &dyn Array, row_idx: usize, encoded: &mut Vec<u8>) -> Result<()> {
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x05);
            } else {
                encoded.push(0x01);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x06);
            } else {
                encoded.push(0x02);
                let bytes = values.value(row_idx).as_bytes();
                let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x07);
            } else {
                encoded.push(0x03);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x08);
            } else {
                encoded.push(0x04);
                encoded.push(u8::from(values.value(row_idx)));
            }
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("expected Date32 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0A);
            } else {
                encoded.push(0x09);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Decimal128(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("expected Decimal128 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0C);
            } else {
                encoded.push(0x0B);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        other => {
            return Err(anyhow!(
                "unsupported vectorized output Arrow type for encoded boundary: {other:?}"
            ));
        }
    }
    Ok(())
}

fn encode_kafka_entry(
    max_event_time_ms: Option<i64>,
    ranges: &[KafkaSourceJournalRange],
) -> Result<Vec<u8>> {
    let count =
        u32::try_from(ranges.len()).context("too many ranges in kafka source journal metadata")?;
    let mut encoded = Vec::with_capacity(
        8 + 4
            + ranges
                .iter()
                .map(|range| 4 + range.topic.len() + 4 + 8 + 8 + 8 + 8)
                .sum::<usize>(),
    );
    encoded.extend_from_slice(&max_event_time_ms.unwrap_or(-1).to_le_bytes());
    encoded.extend_from_slice(&count.to_le_bytes());
    for range in ranges {
        ensure!(
            range.start_offset <= range.end_offset,
            "invalid kafka source journal offset range {}[{}] {}..{}",
            range.topic,
            range.partition,
            range.start_offset,
            range.end_offset
        );
        let topic_len =
            u32::try_from(range.topic.len()).context("kafka source journal topic too large")?;
        encoded.extend_from_slice(&topic_len.to_le_bytes());
        encoded.extend_from_slice(range.topic.as_bytes());
        encoded.extend_from_slice(&range.partition.to_le_bytes());
        encoded.extend_from_slice(&range.start_offset.to_le_bytes());
        encoded.extend_from_slice(&range.end_offset.to_le_bytes());
        encoded.extend_from_slice(&range.row_count.to_le_bytes());
        encoded.extend_from_slice(&range.checksum.to_le_bytes());
    }
    Ok(encoded)
}

fn decode_kafka_entry(value: &[u8]) -> Result<(Option<i64>, Vec<KafkaSourceJournalRange>)> {
    if value.len() < 12 {
        bail!("kafka source journal entry missing header");
    }
    let mut cursor = 0usize;
    let max_event_time_ms = i64::from_le_bytes(
        value[cursor..cursor + 8]
            .try_into()
            .expect("slice width already checked"),
    );
    cursor += 8;
    let count = u32::from_le_bytes(
        value[cursor..cursor + 4]
            .try_into()
            .expect("slice width already checked"),
    ) as usize;
    cursor += 4;

    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor + 4 > value.len() {
            bail!("kafka source journal entry truncated before topic length");
        }
        let topic_len = u32::from_le_bytes(
            value[cursor..cursor + 4]
                .try_into()
                .expect("slice width already checked"),
        ) as usize;
        cursor += 4;
        if cursor + topic_len + 36 > value.len() {
            bail!("kafka source journal entry truncated while decoding range");
        }
        let topic = std::str::from_utf8(&value[cursor..cursor + topic_len])
            .context("kafka source journal topic must be utf8")?
            .to_string();
        cursor += topic_len;
        let partition = i32::from_le_bytes(
            value[cursor..cursor + 4]
                .try_into()
                .expect("slice width already checked"),
        );
        cursor += 4;
        let start_offset = i64::from_le_bytes(
            value[cursor..cursor + 8]
                .try_into()
                .expect("slice width already checked"),
        );
        cursor += 8;
        let end_offset = i64::from_le_bytes(
            value[cursor..cursor + 8]
                .try_into()
                .expect("slice width already checked"),
        );
        cursor += 8;
        let row_count = u64::from_le_bytes(
            value[cursor..cursor + 8]
                .try_into()
                .expect("slice width already checked"),
        );
        cursor += 8;
        let checksum = u64::from_le_bytes(
            value[cursor..cursor + 8]
                .try_into()
                .expect("slice width already checked"),
        );
        cursor += 8;
        if start_offset > end_offset {
            bail!(
                "kafka source journal entry has invalid range {topic}[{partition}] {start_offset}..{end_offset}"
            );
        }
        ranges.push(KafkaSourceJournalRange {
            topic,
            partition,
            start_offset,
            end_offset,
            row_count,
            checksum,
        });
    }
    if cursor != value.len() {
        bail!("kafka source journal entry had trailing bytes");
    }
    Ok((
        (max_event_time_ms >= 0).then_some(max_event_time_ms),
        ranges,
    ))
}

fn update_fnv64(checksum: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *checksum ^= u64::from(*byte);
        *checksum = checksum.wrapping_mul(FNV_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::storage::SlateTable;
    use object_store::memory::InMemory;
    use slatedb::Db;

    async fn test_db(name: &str) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open(name, store).await.expect("open SlateDB"))
    }

    async fn test_table(name: &str) -> Arc<dyn KeyValueTable> {
        Arc::new(SlateTable::new(test_db(name).await))
    }

    #[tokio::test]
    async fn kafka_source_journal_roundtrips_metadata_entries() {
        let table = test_table("kafka-source-journal-roundtrip").await;
        let journal = KafkaSourceJournal::new(table);
        let mut checksum = kafka_source_journal_initial_checksum();
        update_kafka_source_journal_checksum(&mut checksum, 42, b"row-a");
        update_kafka_source_journal_checksum(&mut checksum, 43, b"row-b");
        journal
            .append(
                "nexmark_bid",
                7,
                Some(123),
                &[KafkaSourceJournalRange {
                    topic: "nexmark".to_string(),
                    partition: 0,
                    start_offset: 42,
                    end_offset: 43,
                    row_count: 2,
                    checksum,
                }],
            )
            .await
            .expect("append");

        let allowed = BTreeSet::from(["nexmark_bid".to_string()]);
        let entries = journal
            .load_committed_entries_up_to(7, &allowed)
            .await
            .expect("load");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].source, "nexmark_bid");
        assert_eq!(entries[0].tick_id, 7);
        assert_eq!(entries[0].max_event_time_ms, Some(123));
        assert_eq!(
            entries[0].ranges,
            vec![KafkaSourceJournalRange {
                topic: "nexmark".to_string(),
                partition: 0,
                start_offset: 42,
                end_offset: 43,
                row_count: 2,
                checksum,
            }]
        );
    }
}
