use super::*;

impl CdcBufferAppend {
    pub fn new(
        pipeline_name: impl Into<String>,
        source_name: impl Into<String>,
        table_id: impl Into<String>,
        source_position: CdcSourcePosition,
        transaction_id: Option<CdcTransactionId>,
        records: Vec<CdcBufferRecord>,
        buffered_at_unix_ms: u64,
    ) -> Result<Self> {
        let append = Self {
            pipeline_name: pipeline_name.into(),
            source_name: source_name.into(),
            table_id: table_id.into(),
            source_position,
            transaction_id,
            records,
            change_batches: Vec::new(),
            schema_versions: CdcSchemaVersionMap::new(),
            buffered_at_unix_ms,
        };
        append.validate()?;
        Ok(append)
    }

    pub fn new_change_batches(
        pipeline_name: impl Into<String>,
        source_name: impl Into<String>,
        table_id: impl Into<String>,
        source_position: CdcSourcePosition,
        transaction_id: Option<CdcTransactionId>,
        change_batches: Vec<ChangeBatch>,
        buffered_at_unix_ms: u64,
    ) -> Result<Self> {
        let append = Self {
            pipeline_name: pipeline_name.into(),
            source_name: source_name.into(),
            table_id: table_id.into(),
            source_position,
            transaction_id,
            records: Vec::new(),
            change_batches,
            schema_versions: CdcSchemaVersionMap::new(),
            buffered_at_unix_ms,
        };
        append.validate()?;
        Ok(append)
    }

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            !self.pipeline_name.trim().is_empty(),
            "CDC buffer pipeline name cannot be empty"
        );
        ensure!(
            !self.source_name.trim().is_empty(),
            "CDC buffer source name cannot be empty"
        );
        ensure!(
            !self.table_id.trim().is_empty(),
            "CDC buffer table id cannot be empty"
        );
        let has_records = !self.records.is_empty();
        let has_change_batches = !self.change_batches.is_empty();
        ensure!(
            has_records != has_change_batches,
            "CDC buffer append must contain exactly one payload kind"
        );
        Ok(())
    }

    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn source_position(&self) -> &CdcSourcePosition {
        &self.source_position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn records(&self) -> &[CdcBufferRecord] {
        &self.records
    }

    pub fn with_schema_versions(mut self, schema_versions: CdcSchemaVersionMap) -> Self {
        self.schema_versions = schema_versions;
        self
    }

    pub fn schema_versions(&self) -> &CdcSchemaVersionMap {
        &self.schema_versions
    }

    pub fn change_batches(&self) -> &[ChangeBatch] {
        &self.change_batches
    }

    pub fn record_count(&self) -> usize {
        match self.payload_format() {
            CdcBufferPayloadFormat::KafkaRecords => self.records.len(),
            CdcBufferPayloadFormat::ChangeBatches => self
                .change_batches
                .iter()
                .map(ChangeBatch::change_count)
                .sum(),
        }
    }

    pub fn payload_format(&self) -> CdcBufferPayloadFormat {
        if self.change_batches.is_empty() {
            CdcBufferPayloadFormat::KafkaRecords
        } else {
            CdcBufferPayloadFormat::ChangeBatches
        }
    }

    pub fn estimated_payload_bytes(&self) -> Result<usize> {
        Ok(self.encode_payload()?.len())
    }

    pub fn buffered_at_unix_ms(&self) -> u64 {
        self.buffered_at_unix_ms
    }

    pub(super) fn encode_payload(&self) -> Result<Vec<u8>> {
        match self.payload_format() {
            CdcBufferPayloadFormat::KafkaRecords => encode_payload_records(&self.records),
            CdcBufferPayloadFormat::ChangeBatches => {
                encode_payload_change_batches(&self.change_batches)
            }
        }
    }
}

impl CdcBufferRecord {
    pub fn new(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self {
            key,
            value,
            headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push(CdcBufferRecordHeader {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub fn headers(&self) -> &[CdcBufferRecordHeader] {
        &self.headers
    }

    pub fn byte_len(&self) -> usize {
        self.key.as_ref().map_or(0, Vec::len)
            + self.value.as_ref().map_or(0, Vec::len)
            + self
                .headers
                .iter()
                .map(|header| header.key.len() + header.value.len())
                .sum::<usize>()
    }
}

impl CdcBufferRecordHeader {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl CdcBufferedTransactionManifest {
    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn transaction_key(&self) -> &str {
        &self.transaction_key
    }

    pub fn source_position(&self) -> &CdcSourcePosition {
        &self.source_position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }

    pub fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub fn payload_storage(&self) -> CdcBufferPayloadStorage {
        self.payload_storage
    }

    pub fn payload_format(&self) -> CdcBufferPayloadFormat {
        self.payload_format
    }

    pub fn schema_versions(&self) -> &CdcSchemaVersionMap {
        &self.schema_versions
    }

    pub fn payload_object_key(&self) -> Option<&str> {
        self.payload_object_key.as_deref()
    }

    pub fn buffered_at_unix_ms(&self) -> u64 {
        self.buffered_at_unix_ms
    }

    pub fn delivered_at_unix_ms(&self) -> Option<u64> {
        self.delivered_at_unix_ms
    }

    pub(super) fn with_delivered_at(mut self, delivered_at_unix_ms: u64) -> Self {
        self.delivered_at_unix_ms = Some(delivered_at_unix_ms);
        self
    }
}

impl CdcBufferFrontier {
    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub fn source_position(&self) -> &CdcSourcePosition {
        &self.source_position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn updated_at_unix_ms(&self) -> u64 {
        self.updated_at_unix_ms
    }
}

impl CdcBufferCleanupPolicy {
    pub fn new(delivered_retention_ms: u64) -> Self {
        Self {
            delivered_retention_ms,
        }
    }

    pub fn delivered_retention_ms(&self) -> u64 {
        self.delivered_retention_ms
    }
}

impl CdcBufferStats {
    pub fn pending_transactions(&self) -> usize {
        self.pending_transactions
    }

    pub fn pending_objects(&self) -> usize {
        self.pending_objects
    }

    pub fn pending_records(&self) -> usize {
        self.pending_records
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub fn oldest_pending_age_ms(&self) -> Option<u64> {
        self.oldest_pending_age_ms
    }
}

impl CdcBufferCleanupSummary {
    pub fn deleted_transactions(&self) -> usize {
        self.deleted_transactions
    }

    pub fn deleted_records(&self) -> usize {
        self.deleted_records
    }

    pub fn deleted_bytes(&self) -> usize {
        self.deleted_bytes
    }
}

impl CdcBufferOrphanCleanupSummary {
    pub fn deleted_objects(&self) -> usize {
        self.deleted_objects
    }

    pub fn deleted_bytes(&self) -> usize {
        self.deleted_bytes
    }
}

impl CdcBufferIntegrityReport {
    pub fn pending_payload_objects(&self) -> usize {
        self.pending_payload_objects
    }

    pub fn delivered_payload_objects(&self) -> usize {
        self.delivered_payload_objects
    }

    pub fn missing_payload_objects(&self) -> usize {
        self.missing_payload_objects
    }

    pub fn orphan_payload_objects(&self) -> usize {
        self.orphan_payload_objects
    }

    pub fn orphan_payload_bytes(&self) -> usize {
        self.orphan_payload_bytes
    }
}

pub(super) async fn load_json<T: for<'de> Deserialize<'de>>(
    db: &Db,
    key: Vec<u8>,
    label: &str,
) -> Result<Option<T>> {
    let Some(value) = db.get(key).await.map_err(map_slate_err)? else {
        return Ok(None);
    };
    serde_json::from_slice(&value)
        .with_context(|| format!("decode {label}"))
        .map(Some)
}

pub(super) fn stage_pending_stats(
    batch: &mut WriteBatch,
    pipeline_name: &str,
    stats: &CdcBufferPendingStats,
) -> Result<()> {
    batch.put(
        pending_stats_key(pipeline_name),
        serde_json::to_vec(stats).context("encode CDC buffer pending stats")?,
    );
    Ok(())
}

pub(super) async fn write_batch(db: &Db, batch: WriteBatch, await_durable: bool) -> Result<()> {
    db.write_with_options(
        batch,
        &WriteOptions {
            await_durable,
            ..WriteOptions::default()
        },
    )
    .await
    .map(|_| ())
    .map_err(map_slate_err)
}

pub(super) async fn scan_prefix(db: &Db, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    scan_prefix_limit(db, prefix, usize::MAX).await
}

pub(super) async fn scan_prefix_limit(
    db: &Db,
    prefix: &[u8],
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut iter = db
        .scan_with_options(prefix_bounds(prefix), &ScanOptions::default())
        .await
        .map_err(map_slate_err)?;
    let mut values = Vec::new();
    while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
        values.push((kv.key.to_vec(), kv.value.to_vec()));
        if values.len() >= limit {
            break;
        }
    }
    Ok(values)
}

pub(super) fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

pub(super) fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}
