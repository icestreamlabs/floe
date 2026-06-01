use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use datafusion::arrow::array::{ArrayRef, BinaryArray, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::storage::KeyValueTable;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::outer_stream::OuterStreamRegistry;

const SOURCE_BATCH_JOURNAL_PREFIX: &str = "source_journal";
const KAFKA_SOURCE_JOURNAL_PREFIX: &str = "kafka_source_journal";
const VECTORIZED_SOURCE_BATCH_JOURNAL_PREFIX: &str = "vectorized_source_journal";
const SOURCE_BATCH_JOURNAL_ARROW_MAGIC: &[u8] = b"FLOE_SOURCE_BATCH_ARROW_V1";
const VECTORIZED_SOURCE_BATCH_JOURNAL_ARROW_MAGIC: &[u8] = b"FLOE_VECTORIZED_SOURCE_BATCH_ARROW_V1";
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceBatchJournalEntry {
    pub source: String,
    pub tick_id: u64,
    pub max_event_time_ms: Option<i64>,
    pub deltas: Vec<(Vec<u8>, i64)>,
}

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
pub struct SourceBatchJournal {
    table: Arc<dyn KeyValueTable>,
}

#[derive(Clone)]
pub struct VectorizedSourceBatchJournal {
    table: Arc<dyn KeyValueTable>,
}

#[derive(Clone)]
pub struct KafkaSourceJournal {
    table: Arc<dyn KeyValueTable>,
}

impl SourceBatchJournal {
    pub fn new(table: Arc<dyn KeyValueTable>) -> Self {
        Self { table }
    }

    pub async fn append(
        &self,
        source: &str,
        tick_id: u64,
        max_event_time_ms: Option<i64>,
        deltas: &[(Vec<u8>, i64)],
    ) -> Result<usize> {
        let mut batch = WriteBatch::new();
        let encoded_len =
            append_entry_to_batch(&mut batch, source, tick_id, max_event_time_ms, deltas)?;
        if encoded_len == 0 {
            return Ok(0);
        }
        self.table.write_batch(batch).await.with_context(|| {
            format!("persist source batch journal entry for '{source}' at tick {tick_id}")
        })?;
        Ok(encoded_len)
    }

    pub async fn load_committed_entries_up_to(
        &self,
        max_tick_id: u64,
        allowed_sources: &BTreeSet<String>,
    ) -> Result<Vec<SourceBatchJournalEntry>> {
        let entries = self
            .table
            .scan_prefix(&entry_prefix(), &ScanOptions::default())
            .await
            .context("scan source batch journal")?;
        let mut recovered = Vec::new();
        for (key, value) in entries {
            let (tick_id, source) = parse_entry_key(&key)?;
            if tick_id > max_tick_id {
                break;
            }
            if !allowed_sources.is_empty() && !allowed_sources.contains(&source) {
                continue;
            }
            let (max_event_time_ms, deltas) = decode_entry(&value).with_context(|| {
                format!("decode source batch journal entry for '{source}' at tick {tick_id}")
            })?;
            recovered.push(SourceBatchJournalEntry {
                source,
                tick_id,
                max_event_time_ms,
                deltas,
            });
        }
        Ok(recovered)
    }

    pub async fn replay_committed_entries_up_to(
        &self,
        registry: &mut OuterStreamRegistry,
        max_tick_id: u64,
        allowed_sources: &BTreeSet<String>,
    ) -> Result<usize> {
        let entries = self
            .load_committed_entries_up_to(max_tick_id, allowed_sources)
            .await?;
        let mut replayed = 0usize;
        if allowed_sources.is_empty() {
            for entry in entries {
                registry
                    .replay_transient_batch(
                        &entry.source,
                        i64::try_from(entry.tick_id).unwrap_or(i64::MAX),
                        entry.deltas,
                    )
                    .with_context(|| {
                        format!(
                            "replay source batch journal entry for '{}' at tick {}",
                            entry.source, entry.tick_id
                        )
                    })?;
                replayed = replayed.saturating_add(1);
            }
            return Ok(replayed);
        }

        let mut entry_by_tick_and_source = BTreeMap::new();
        for entry in entries {
            entry_by_tick_and_source.insert((entry.tick_id, entry.source.clone()), entry);
        }

        for tick_id in 1..=max_tick_id {
            for source in allowed_sources {
                let (replay_source, deltas) =
                    match entry_by_tick_and_source.remove(&(tick_id, source.clone())) {
                        Some(entry) => {
                            replayed = replayed.saturating_add(1);
                            (entry.source, entry.deltas)
                        }
                        None => (source.clone(), Vec::new()),
                    };
                registry
                    .replay_transient_batch(
                        &replay_source,
                        i64::try_from(tick_id).unwrap_or(i64::MAX),
                        deltas,
                    )
                    .with_context(|| {
                        format!(
                            "replay source batch journal entry for '{}' at tick {}",
                            replay_source, tick_id
                        )
                    })?;
            }
        }
        Ok(replayed)
    }

    pub async fn materialize_committed_source_up_to(
        &self,
        source: &str,
        max_tick_id: u64,
    ) -> Result<HashMap<Vec<u8>, i64>> {
        let allowed_sources = BTreeSet::from([source.to_string()]);
        let entries = self
            .load_committed_entries_up_to(max_tick_id, &allowed_sources)
            .await?;
        let mut snapshot = HashMap::new();
        for entry in entries {
            if entry.source != source {
                continue;
            }
            for (key, diff) in entry.deltas {
                let next = snapshot.get(&key).copied().unwrap_or(0) + diff;
                if next == 0 {
                    snapshot.remove(&key);
                } else {
                    snapshot.insert(key, next);
                }
            }
        }
        Ok(snapshot)
    }
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
        let entries = self
            .table
            .scan_prefix(&vectorized_entry_prefix(), &ScanOptions::default())
            .await
            .context("scan vectorized source batch journal")?;
        let mut recovered = Vec::new();
        for (key, value) in entries {
            let (tick_id, source) = parse_vectorized_entry_key(&key)?;
            if tick_id > max_tick_id {
                break;
            }
            if !allowed_sources.is_empty() && !allowed_sources.contains(&source) {
                continue;
            }
            let (max_event_time_ms, batches) =
                decode_vectorized_entry(&value).with_context(|| {
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
        let entries = self
            .table
            .scan_prefix(&kafka_entry_prefix(), &ScanOptions::default())
            .await
            .context("scan kafka source journal metadata")?;
        let mut recovered = Vec::new();
        for (key, value) in entries {
            let (tick_id, source) = parse_kafka_entry_key(&key)?;
            if tick_id > max_tick_id {
                break;
            }
            if !allowed_sources.is_empty() && !allowed_sources.contains(&source) {
                continue;
            }
            let (max_event_time_ms, ranges) = decode_kafka_entry(&value).with_context(|| {
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

pub(crate) fn append_entry_to_batch(
    batch: &mut WriteBatch,
    source: &str,
    tick_id: u64,
    max_event_time_ms: Option<i64>,
    deltas: &[(Vec<u8>, i64)],
) -> Result<usize> {
    if deltas.is_empty() {
        return Ok(0);
    }
    let encoded = encode_entry(max_event_time_ms, deltas)?;
    let encoded_len = encoded.len();
    batch.put(entry_key(source, tick_id)?, encoded);
    Ok(encoded_len)
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

fn entry_prefix() -> Vec<u8> {
    format!("{SOURCE_BATCH_JOURNAL_PREFIX}/entries/").into_bytes()
}

fn entry_key(source: &str, tick_id: u64) -> Result<Vec<u8>> {
    ensure!(
        !source.is_empty() && !source.contains('/'),
        "invalid source batch journal source '{source}'"
    );
    Ok(format!("{SOURCE_BATCH_JOURNAL_PREFIX}/entries/{tick_id:020}/{source}").into_bytes())
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

fn parse_entry_key(key: &[u8]) -> Result<(u64, String)> {
    let key_str = std::str::from_utf8(key).context("source batch journal key must be utf8")?;
    let mut parts = key_str.split('/');
    let prefix = parts.next().unwrap_or_default();
    let section = parts.next().unwrap_or_default();
    let tick_id = parts.next().unwrap_or_default();
    let source = parts.next().unwrap_or_default();
    if prefix != SOURCE_BATCH_JOURNAL_PREFIX || section != "entries" || source.is_empty() {
        return Err(anyhow!("invalid source batch journal key '{key_str}'"));
    }
    let tick_id = tick_id
        .parse::<u64>()
        .with_context(|| format!("parse source batch journal tick from '{key_str}'"))?;
    Ok((tick_id, source.to_string()))
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

fn encode_entry(max_event_time_ms: Option<i64>, deltas: &[(Vec<u8>, i64)]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row", DataType::Binary, false),
        Field::new("diff", DataType::Int64, false),
    ]));
    let row_array: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        deltas.iter().map(|(key, _)| key.as_slice()),
    ));
    let diff_array: ArrayRef = Arc::new(Int64Array::from_iter_values(
        deltas.iter().map(|(_, diff)| *diff),
    ));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![row_array, diff_array])
        .context("build source batch journal Arrow batch")?;

    let mut encoded = Vec::with_capacity(
        SOURCE_BATCH_JOURNAL_ARROW_MAGIC.len()
            + 8
            + deltas
                .iter()
                .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
                .sum::<usize>(),
    );
    encoded.extend_from_slice(SOURCE_BATCH_JOURNAL_ARROW_MAGIC);
    encoded.extend_from_slice(&max_event_time_ms.unwrap_or(-1).to_le_bytes());
    {
        let mut writer = StreamWriter::try_new(&mut encoded, schema.as_ref())
            .context("create source batch journal Arrow writer")?;
        writer
            .write(&batch)
            .context("write source batch journal Arrow batch")?;
        writer
            .finish()
            .context("finalize source batch journal Arrow writer")?;
    }
    Ok(encoded)
}

fn decode_entry(value: &[u8]) -> Result<(Option<i64>, Vec<(Vec<u8>, i64)>)> {
    if !value.starts_with(SOURCE_BATCH_JOURNAL_ARROW_MAGIC) {
        bail!("source batch journal entry missing Arrow header");
    }
    decode_arrow_entry(value)
}

fn decode_arrow_entry(value: &[u8]) -> Result<(Option<i64>, Vec<(Vec<u8>, i64)>)> {
    let mut cursor = SOURCE_BATCH_JOURNAL_ARROW_MAGIC.len();
    if value.len() < cursor + 8 {
        bail!("source batch journal Arrow entry missing header");
    }
    let max_event_time_ms = i64::from_le_bytes(
        value[cursor..cursor + 8]
            .try_into()
            .expect("slice width already checked"),
    );
    cursor += 8;

    let reader = StreamReader::try_new(Cursor::new(&value[cursor..]), None)
        .context("create source batch journal Arrow reader")?;
    let mut deltas = Vec::new();
    for batch in reader {
        let batch = batch.context("read source batch journal Arrow batch")?;
        let rows = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("source batch journal Arrow row column was not Binary"))?;
        let diffs = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("source batch journal Arrow diff column was not Int64"))?;
        for idx in 0..batch.num_rows() {
            deltas.push((rows.value(idx).to_vec(), diffs.value(idx)));
        }
    }
    Ok((
        (max_event_time_ms >= 0).then_some(max_event_time_ms),
        deltas,
    ))
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
    use crate::dbsp_bridge::DbspBridge;
    use dbsp::storage::SlateTable;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use tokio::time::{Duration, timeout};

    async fn test_db(name: &str) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open(name, store).await.expect("open SlateDB"))
    }

    async fn test_table(name: &str) -> Arc<dyn KeyValueTable> {
        Arc::new(SlateTable::new(test_db(name).await))
    }

    #[tokio::test]
    async fn source_batch_journal_roundtrips_entries() {
        let table = test_table("source-batch-journal-roundtrip").await;
        let journal = SourceBatchJournal::new(table);
        journal
            .append(
                "nexmark_bid",
                7,
                Some(123),
                &[(b"a".to_vec(), 1), (b"b".to_vec(), 1)],
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
        assert_eq!(entries[0].deltas.len(), 2);
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

    #[tokio::test]
    async fn source_batch_journal_replay_ignores_entries_after_commit_cutoff() {
        let db = test_db("source-batch-journal-cutoff").await;
        let journal = SourceBatchJournal::new(Arc::new(SlateTable::new(Arc::clone(&db))));
        journal
            .append("nexmark_bid", 1, None, &[(b"a".to_vec(), 1)])
            .await
            .expect("append committed entry");
        journal
            .append("nexmark_bid", 2, None, &[(b"b".to_vec(), 1)])
            .await
            .expect("append uncommitted entry");

        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let mut registry =
            OuterStreamRegistry::from_sources(vec!["nexmark_bid".to_string()], &mut bridge)
                .await
                .expect("outer streams");
        let mut rx = registry
            .transient_stream("nexmark_bid")
            .expect("transient stream")
            .subscribe();

        let allowed = BTreeSet::from(["nexmark_bid".to_string()]);
        let replayed = journal
            .replay_committed_entries_up_to(&mut registry, 1, &allowed)
            .await
            .expect("replay");
        assert_eq!(replayed, 1);

        let batch = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("replay timeout")
            .expect("transient batch");
        assert_eq!(batch.version, 1);
        assert_eq!(batch.deltas.as_slice(), &[(b"a".to_vec(), 1)]);
        assert!(
            timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
            "replay should stop at the committed tick boundary"
        );
    }

    #[tokio::test]
    async fn source_batch_journal_replay_synthesizes_empty_batches_for_missing_ticks() {
        let db = test_db("source-batch-journal-empty-replay").await;
        let journal = SourceBatchJournal::new(Arc::new(SlateTable::new(Arc::clone(&db))));
        journal
            .append("nexmark_bid", 1, None, &[(b"a".to_vec(), 1)])
            .await
            .expect("append bid entry");
        journal
            .append("nexmark_auction", 2, None, &[(b"z".to_vec(), 1)])
            .await
            .expect("append auction entry");

        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let mut registry = OuterStreamRegistry::from_sources(
            vec!["nexmark_bid".to_string(), "nexmark_auction".to_string()],
            &mut bridge,
        )
        .await
        .expect("outer streams");
        let mut bid_rx = registry
            .transient_stream("nexmark_bid")
            .expect("bid transient stream")
            .subscribe();
        let mut auction_rx = registry
            .transient_stream("nexmark_auction")
            .expect("auction transient stream")
            .subscribe();

        let allowed = BTreeSet::from(["nexmark_auction".to_string(), "nexmark_bid".to_string()]);
        let replayed = journal
            .replay_committed_entries_up_to(&mut registry, 2, &allowed)
            .await
            .expect("replay");
        assert_eq!(replayed, 2);

        let bid_tick_1 = timeout(Duration::from_secs(1), bid_rx.recv())
            .await
            .expect("bid tick 1 timeout")
            .expect("bid tick 1 batch");
        assert_eq!(bid_tick_1.version, 1);
        assert_eq!(bid_tick_1.deltas.as_slice(), &[(b"a".to_vec(), 1)]);

        let bid_tick_2 = timeout(Duration::from_secs(1), bid_rx.recv())
            .await
            .expect("bid tick 2 timeout")
            .expect("bid tick 2 batch");
        assert_eq!(bid_tick_2.version, 2);
        assert!(bid_tick_2.deltas.is_empty());

        let auction_tick_1 = timeout(Duration::from_secs(1), auction_rx.recv())
            .await
            .expect("auction tick 1 timeout")
            .expect("auction tick 1 batch");
        assert_eq!(auction_tick_1.version, 1);
        assert!(auction_tick_1.deltas.is_empty());

        let auction_tick_2 = timeout(Duration::from_secs(1), auction_rx.recv())
            .await
            .expect("auction tick 2 timeout")
            .expect("auction tick 2 batch");
        assert_eq!(auction_tick_2.version, 2);
        assert_eq!(auction_tick_2.deltas.as_slice(), &[(b"z".to_vec(), 1)]);
    }
}
