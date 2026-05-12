use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use floe_cdc_core::{CdcSourcePosition, CdcTransactionId};
use serde::{Deserialize, Serialize};
use slatedb::config::{ScanOptions, WriteOptions};
use slatedb::{Db, Error as SlateError, WriteBatch};

const CDC_BUFFER_PREFIX: &[u8] = b"floe_cdc_buffer/v1/";
#[derive(Clone)]
pub struct CdcBufferStore {
    db: Arc<Db>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcBufferAppend {
    pipeline_name: String,
    source_name: String,
    table_id: String,
    source_position: CdcSourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    records: Vec<CdcBufferRecord>,
    buffered_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcBufferRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcBufferedTransactionManifest {
    pipeline_name: String,
    source_name: String,
    table_id: String,
    transaction_key: String,
    source_position: CdcSourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    record_count: usize,
    payload_bytes: usize,
    buffered_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivered_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcBufferFrontier {
    pipeline_name: String,
    source_position: CdcSourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    updated_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdcBufferCleanupPolicy {
    delivered_retention_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcBufferStats {
    pending_transactions: usize,
    pending_records: usize,
    pending_bytes: usize,
    oldest_pending_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcBufferCleanupSummary {
    deleted_transactions: usize,
    deleted_records: usize,
    deleted_bytes: usize,
}

impl CdcBufferStore {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    pub async fn append_transaction(
        &self,
        append: CdcBufferAppend,
    ) -> Result<CdcBufferedTransactionManifest> {
        append.validate()?;
        let transaction_key =
            transaction_key(&append.source_position, append.transaction_id.as_ref())?;
        let manifest = CdcBufferedTransactionManifest {
            pipeline_name: append.pipeline_name.clone(),
            source_name: append.source_name.clone(),
            table_id: append.table_id.clone(),
            transaction_key: transaction_key.clone(),
            source_position: append.source_position.clone(),
            transaction_id: append.transaction_id.clone(),
            record_count: append.records.len(),
            payload_bytes: append.records.iter().map(CdcBufferRecord::byte_len).sum(),
            buffered_at_unix_ms: append.buffered_at_unix_ms,
            delivered_at_unix_ms: None,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            pending_manifest_key(&append.pipeline_name, &transaction_key),
            serde_json::to_vec(&manifest).context("encode CDC buffer transaction manifest")?,
        );
        for (idx, record) in append.records.iter().enumerate() {
            batch.put(
                payload_key(&append.pipeline_name, &transaction_key, idx),
                serde_json::to_vec(record).context("encode CDC buffer record")?,
            );
        }
        let frontier = CdcBufferFrontier {
            pipeline_name: append.pipeline_name,
            source_position: append.source_position,
            transaction_id: append.transaction_id,
            updated_at_unix_ms: append.buffered_at_unix_ms,
        };
        batch.put(
            source_frontier_key(&frontier.pipeline_name),
            serde_json::to_vec(&frontier).context("encode CDC buffer source frontier")?,
        );
        write_durable_batch(self.db.as_ref(), batch)
            .await
            .context("append CDC buffer transaction")?;
        Ok(manifest)
    }

    pub async fn pending_transactions(
        &self,
        pipeline_name: &str,
        limit: usize,
    ) -> Result<Vec<CdcBufferedTransactionManifest>> {
        let limit = limit.max(1);
        scan_prefix_limit(&self.db, &pending_manifest_prefix(pipeline_name), limit)
            .await?
            .into_iter()
            .map(|(_, value)| {
                serde_json::from_slice::<CdcBufferedTransactionManifest>(&value)
                    .context("decode CDC buffer pending manifest")
            })
            .collect()
    }

    pub async fn records(
        &self,
        manifest: &CdcBufferedTransactionManifest,
    ) -> Result<Vec<CdcBufferRecord>> {
        let records = scan_prefix(
            &self.db,
            &payload_prefix(manifest.pipeline_name(), manifest.transaction_key()),
        )
        .await?
        .into_iter()
        .map(|(_, value)| {
            serde_json::from_slice::<CdcBufferRecord>(&value)
                .context("decode CDC buffer payload record")
        })
        .collect::<Result<Vec<_>>>()?;
        ensure!(
            records.len() == manifest.record_count(),
            "CDC buffer transaction '{}' expected {} records, found {}",
            manifest.transaction_key(),
            manifest.record_count(),
            records.len()
        );
        Ok(records)
    }

    pub async fn mark_delivered(
        &self,
        manifest: &CdcBufferedTransactionManifest,
        delivered_at_unix_ms: u64,
    ) -> Result<CdcBufferedTransactionManifest> {
        let delivered = manifest.clone().with_delivered_at(delivered_at_unix_ms);
        let mut batch = WriteBatch::new();
        batch.delete(pending_manifest_key(
            manifest.pipeline_name(),
            manifest.transaction_key(),
        ));
        batch.put(
            delivered_manifest_key(
                manifest.pipeline_name(),
                delivered_at_unix_ms,
                manifest.transaction_key(),
            ),
            serde_json::to_vec(&delivered).context("encode delivered CDC buffer manifest")?,
        );
        let frontier = CdcBufferFrontier {
            pipeline_name: manifest.pipeline_name().to_string(),
            source_position: manifest.source_position().clone(),
            transaction_id: manifest.transaction_id().cloned(),
            updated_at_unix_ms: delivered_at_unix_ms,
        };
        batch.put(
            delivery_frontier_key(manifest.pipeline_name()),
            serde_json::to_vec(&frontier).context("encode CDC buffer delivery frontier")?,
        );
        write_durable_batch(self.db.as_ref(), batch)
            .await
            .context("mark CDC buffer transaction delivered")?;
        Ok(delivered)
    }

    pub async fn source_frontier(&self, pipeline_name: &str) -> Result<Option<CdcBufferFrontier>> {
        load_json(
            &self.db,
            source_frontier_key(pipeline_name),
            "CDC buffer source frontier",
        )
        .await
    }

    pub async fn delivery_frontier(
        &self,
        pipeline_name: &str,
    ) -> Result<Option<CdcBufferFrontier>> {
        load_json(
            &self.db,
            delivery_frontier_key(pipeline_name),
            "CDC buffer delivery frontier",
        )
        .await
    }

    pub async fn stats(&self, pipeline_name: &str, now_unix_ms: u64) -> Result<CdcBufferStats> {
        let manifests = self.pending_transactions(pipeline_name, usize::MAX).await?;
        let pending_transactions = manifests.len();
        let pending_records = manifests
            .iter()
            .map(CdcBufferedTransactionManifest::record_count)
            .sum();
        let pending_bytes = manifests
            .iter()
            .map(CdcBufferedTransactionManifest::payload_bytes)
            .sum();
        let oldest_pending_age_ms = manifests
            .iter()
            .map(|manifest| now_unix_ms.saturating_sub(manifest.buffered_at_unix_ms()))
            .max();
        Ok(CdcBufferStats {
            pending_transactions,
            pending_records,
            pending_bytes,
            oldest_pending_age_ms,
        })
    }

    pub async fn cleanup_delivered(
        &self,
        pipeline_name: &str,
        policy: CdcBufferCleanupPolicy,
        now_unix_ms: u64,
    ) -> Result<CdcBufferCleanupSummary> {
        let delivered = scan_prefix(&self.db, &delivered_manifest_prefix(pipeline_name)).await?;
        let mut batch = WriteBatch::new();
        let mut summary = CdcBufferCleanupSummary {
            deleted_transactions: 0,
            deleted_records: 0,
            deleted_bytes: 0,
        };

        for (key, value) in delivered {
            let manifest: CdcBufferedTransactionManifest = serde_json::from_slice(&value)
                .context("decode delivered CDC buffer manifest during cleanup")?;
            let Some(delivered_at) = manifest.delivered_at_unix_ms() else {
                continue;
            };
            if now_unix_ms.saturating_sub(delivered_at) < policy.delivered_retention_ms() {
                continue;
            }
            for (payload_key, payload_value) in scan_prefix(
                &self.db,
                &payload_prefix(manifest.pipeline_name(), manifest.transaction_key()),
            )
            .await?
            {
                batch.delete(payload_key);
                summary.deleted_records = summary.deleted_records.saturating_add(1);
                summary.deleted_bytes = summary.deleted_bytes.saturating_add(payload_value.len());
            }
            batch.delete(key);
            summary.deleted_transactions = summary.deleted_transactions.saturating_add(1);
        }

        if summary.deleted_transactions > 0 {
            write_durable_batch(self.db.as_ref(), batch)
                .await
                .context("cleanup delivered CDC buffer transactions")?;
        }
        Ok(summary)
    }
}

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
            buffered_at_unix_ms,
        };
        append.validate()?;
        Ok(append)
    }

    fn validate(&self) -> Result<()> {
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
        ensure!(
            !self.records.is_empty(),
            "CDC buffer append must contain at least one record"
        );
        Ok(())
    }
}

impl CdcBufferRecord {
    pub fn new(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub fn byte_len(&self) -> usize {
        self.key.as_ref().map_or(0, Vec::len) + self.value.as_ref().map_or(0, Vec::len)
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

    pub fn buffered_at_unix_ms(&self) -> u64 {
        self.buffered_at_unix_ms
    }

    pub fn delivered_at_unix_ms(&self) -> Option<u64> {
        self.delivered_at_unix_ms
    }

    fn with_delivered_at(mut self, delivered_at_unix_ms: u64) -> Self {
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

async fn load_json<T: for<'de> Deserialize<'de>>(
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

async fn write_durable_batch(db: &Db, batch: WriteBatch) -> Result<()> {
    db.write_with_options(
        batch,
        &WriteOptions {
            await_durable: true,
        },
    )
    .await
    .map(|_| ())
    .map_err(map_slate_err)
}

async fn scan_prefix(db: &Db, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    scan_prefix_limit(db, prefix, usize::MAX).await
}

async fn scan_prefix_limit(
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

fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

fn source_frontier_key(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"frontier/source");
    key
}

fn delivery_frontier_key(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"frontier/delivery");
    key
}

fn pending_manifest_prefix(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"pending/");
    key
}

fn pending_manifest_key(pipeline_name: &str, transaction_key: &str) -> Vec<u8> {
    let mut key = pending_manifest_prefix(pipeline_name);
    key.extend_from_slice(transaction_key.as_bytes());
    key
}

fn delivered_manifest_prefix(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"delivered/");
    key
}

fn delivered_manifest_key(
    pipeline_name: &str,
    delivered_at_unix_ms: u64,
    transaction_key: &str,
) -> Vec<u8> {
    let mut key = delivered_manifest_prefix(pipeline_name);
    key.extend_from_slice(format!("{delivered_at_unix_ms:020}/").as_bytes());
    key.extend_from_slice(transaction_key.as_bytes());
    key
}

fn payload_prefix(pipeline_name: &str, transaction_key: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"payload/");
    key.extend_from_slice(transaction_key.as_bytes());
    key.extend_from_slice(b"/");
    key
}

fn payload_key(pipeline_name: &str, transaction_key: &str, record_idx: usize) -> Vec<u8> {
    let mut key = payload_prefix(pipeline_name, transaction_key);
    key.extend_from_slice(format!("{record_idx:020}").as_bytes());
    key
}

fn pipeline_prefix(pipeline_name: &str) -> Vec<u8> {
    let mut key = CDC_BUFFER_PREFIX.to_vec();
    key.extend_from_slice(b"pipeline/");
    push_component(&mut key, pipeline_name.as_bytes());
    key.extend_from_slice(b"/");
    key
}

fn transaction_key(
    position: &CdcSourcePosition,
    transaction_id: Option<&CdcTransactionId>,
) -> Result<String> {
    let tx = transaction_id.map_or("none".to_string(), |tx| hex(tx.as_str().as_bytes()));
    match position {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => Ok(format!(
            "pg/{:020}/{:020}/{tx}",
            parse_postgres_lsn(commit_lsn)?,
            event_lsn
                .as_deref()
                .map(parse_postgres_lsn)
                .transpose()?
                .unwrap_or(u64::MAX)
        )),
        CdcSourcePosition::Opaque { value } => Ok(format!("opaque/{}/{tx}", hex(value.as_bytes()))),
    }
}

fn parse_postgres_lsn(value: &str) -> Result<u64> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| anyhow!("invalid Postgres LSN '{value}'"))?;
    let high = u64::from_str_radix(high, 16)
        .with_context(|| format!("invalid Postgres LSN high word '{high}'"))?;
    let low = u64::from_str_radix(low, 16)
        .with_context(|| format!("invalid Postgres LSN low word '{low}'"))?;
    Ok((high << 32) | low)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

fn push_component(out: &mut Vec<u8>, component: &[u8]) {
    let len = u32::try_from(component.len()).expect("CDC buffer key component length exceeds u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(component);
}

fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn test_store(name: &str) -> CdcBufferStore {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, object_store).await.expect("open SlateDB"));
        CdcBufferStore::new(db)
    }

    #[tokio::test]
    async fn appends_and_replays_pending_transactions() {
        let store = test_store("cdc-buffer-append").await;
        let append = append("0/10", 1000, vec![record(1), record(2)]);
        let manifest = store.append_transaction(append).await.expect("append");

        let pending = store
            .pending_transactions("pipe", 10)
            .await
            .expect("pending");
        assert_eq!(pending, vec![manifest.clone()]);
        assert_eq!(
            store.records(&manifest).await.unwrap(),
            vec![record(1), record(2)]
        );

        let frontier = store
            .source_frontier("pipe")
            .await
            .expect("frontier")
            .expect("source frontier");
        assert_eq!(
            frontier.source_position(),
            &CdcSourcePosition::postgres("0/10", None).expect("position")
        );
    }

    #[tokio::test]
    async fn delivery_frontier_and_cleanup_only_delete_delivered_transactions() {
        let store = test_store("cdc-buffer-cleanup").await;
        let delivered = store
            .append_transaction(append("0/10", 1000, vec![record(1)]))
            .await
            .expect("append delivered");
        let pending = store
            .append_transaction(append("0/20", 2000, vec![record(2)]))
            .await
            .expect("append pending");

        let delivered = store
            .mark_delivered(&delivered, 3000)
            .await
            .expect("mark delivered");
        assert_eq!(delivered.delivered_at_unix_ms(), Some(3000));

        let delivery = store
            .delivery_frontier("pipe")
            .await
            .expect("frontier")
            .expect("delivery frontier");
        assert_eq!(
            delivery.source_position(),
            &CdcSourcePosition::postgres("0/10", None).expect("position")
        );

        let summary = store
            .cleanup_delivered("pipe", CdcBufferCleanupPolicy::new(0), 3000)
            .await
            .expect("cleanup");
        assert_eq!(summary.deleted_transactions(), 1);
        assert_eq!(summary.deleted_records(), 1);

        let pending_after = store
            .pending_transactions("pipe", 10)
            .await
            .expect("pending after cleanup");
        assert_eq!(pending_after, vec![pending]);
    }

    #[tokio::test]
    async fn stats_report_size_and_oldest_age() {
        let store = test_store("cdc-buffer-stats").await;
        store
            .append_transaction(append("0/10", 1000, vec![record(1), record(2)]))
            .await
            .expect("append one");
        store
            .append_transaction(append("0/20", 1500, vec![record(3)]))
            .await
            .expect("append two");

        let stats = store.stats("pipe", 2500).await.expect("stats");
        assert_eq!(stats.pending_transactions(), 2);
        assert_eq!(stats.pending_records(), 3);
        assert_eq!(stats.oldest_pending_age_ms(), Some(1500));
        assert!(stats.pending_bytes() > 0);
    }

    fn append(
        lsn: &str,
        buffered_at_unix_ms: u64,
        records: Vec<CdcBufferRecord>,
    ) -> CdcBufferAppend {
        CdcBufferAppend::new(
            "pipe",
            "pg_main",
            "orders",
            CdcSourcePosition::postgres(lsn, None).expect("position"),
            Some(CdcTransactionId::new(format!("tx-{lsn}")).expect("tx")),
            records,
            buffered_at_unix_ms,
        )
        .expect("append")
    }

    fn record(id: i64) -> CdcBufferRecord {
        CdcBufferRecord::new(
            Some(format!(r#"{{"id":{id}}}"#).into_bytes()),
            Some(format!(r#"{{"after":{{"id":{id}}}}}"#).into_bytes()),
        )
    }
}
