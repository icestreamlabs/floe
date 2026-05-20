use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use floe_cdc_core::{CdcSchemaVersionMap, CdcSourcePosition, CdcTransactionId, ChangeBatch};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use slatedb::config::{ScanOptions, WriteOptions};
use slatedb::{Db, Error as SlateError, WriteBatch};

mod keys;
mod payload_codec;

#[cfg(test)]
use keys::payload_key;
use keys::{
    delivered_manifest_key, delivered_manifest_prefix, delivery_frontier_key, payload_blob_key,
    payload_object_key, payload_prefix, pending_manifest_key, pending_manifest_prefix,
    source_frontier_key, transaction_key,
};
#[cfg(test)]
use payload_codec::{CDC_BUFFER_PAYLOAD_MAGIC_V1, encode_optional_bytes};
pub use payload_codec::{decode_cdc_buffer_records_payload, encode_cdc_buffer_records_payload};
use payload_codec::{
    decode_payload_change_batches, decode_payload_records, encode_payload_change_batches,
    encode_payload_records,
};

use crate::object_payload::{
    delete_payload_object_if_exists, load_payload_object, put_payload_object,
};

#[derive(Clone)]
pub struct CdcBufferStore {
    db: Arc<Db>,
    object_store: Option<Arc<dyn ObjectStore>>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    change_batches: Vec<ChangeBatch>,
    #[serde(default, skip_serializing_if = "CdcSchemaVersionMap::is_empty")]
    schema_versions: CdcSchemaVersionMap,
    buffered_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcBufferRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    headers: Vec<CdcBufferRecordHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcBufferRecordHeader {
    key: String,
    value: Vec<u8>,
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
    #[serde(default)]
    payload_storage: CdcBufferPayloadStorage,
    #[serde(default)]
    payload_format: CdcBufferPayloadFormat,
    #[serde(default, skip_serializing_if = "CdcSchemaVersionMap::is_empty")]
    schema_versions: CdcSchemaVersionMap,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_object_key: Option<String>,
    buffered_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivered_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CdcBufferPayloadStorage {
    #[default]
    SlateDbBlob,
    ObjectStore,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CdcBufferPayloadFormat {
    #[default]
    KafkaRecords,
    ChangeBatches,
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
        Self {
            db,
            object_store: None,
        }
    }

    pub fn with_object_store(db: Arc<Db>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            db,
            object_store: Some(object_store),
        }
    }

    pub async fn append_transaction(
        &self,
        append: &CdcBufferAppend,
    ) -> Result<CdcBufferedTransactionManifest> {
        self.append_transaction_with_durable_wait(append, true)
            .await
    }

    pub async fn append_transaction_without_durable_wait(
        &self,
        append: &CdcBufferAppend,
    ) -> Result<CdcBufferedTransactionManifest> {
        self.append_transaction_with_durable_wait(append, false)
            .await
    }

    async fn append_transaction_with_durable_wait(
        &self,
        append: &CdcBufferAppend,
        await_durable: bool,
    ) -> Result<CdcBufferedTransactionManifest> {
        append.validate()?;
        let transaction_key =
            transaction_key(&append.source_position, append.transaction_id.as_ref())?;
        let payload = append.encode_payload()?;
        let payload_bytes = payload.len();
        let record_count = append.record_count();
        let payload_format = append.payload_format();
        let payload_object_key = payload_object_key(&append.pipeline_name, &transaction_key);
        let object_store = self.payload_object_store()?;
        put_payload_object(object_store, &payload_object_key, payload, "CDC buffer").await?;
        let manifest = CdcBufferedTransactionManifest {
            pipeline_name: append.pipeline_name.clone(),
            source_name: append.source_name.clone(),
            table_id: append.table_id.clone(),
            transaction_key: transaction_key.clone(),
            source_position: append.source_position.clone(),
            transaction_id: append.transaction_id.clone(),
            record_count,
            payload_bytes,
            payload_storage: CdcBufferPayloadStorage::ObjectStore,
            payload_format,
            schema_versions: append.schema_versions.clone(),
            payload_object_key: Some(payload_object_key),
            buffered_at_unix_ms: append.buffered_at_unix_ms,
            delivered_at_unix_ms: None,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            pending_manifest_key(&append.pipeline_name, &transaction_key),
            serde_json::to_vec(&manifest).context("encode CDC buffer transaction manifest")?,
        );
        let frontier = CdcBufferFrontier {
            pipeline_name: append.pipeline_name.clone(),
            source_position: append.source_position.clone(),
            transaction_id: append.transaction_id.clone(),
            updated_at_unix_ms: append.buffered_at_unix_ms,
        };
        batch.put(
            source_frontier_key(&frontier.pipeline_name),
            serde_json::to_vec(&frontier).context("encode CDC buffer source frontier")?,
        );
        write_batch(self.db.as_ref(), batch, await_durable)
            .await
            .context("append CDC buffer transaction")?;
        Ok(manifest)
    }

    pub async fn flush(&self) -> Result<()> {
        self.db.flush().await.map_err(map_slate_err)
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
        ensure!(
            manifest.payload_format() == CdcBufferPayloadFormat::KafkaRecords,
            "CDC buffer transaction '{}' stores {:?}, not Kafka records",
            manifest.transaction_key(),
            manifest.payload_format()
        );
        let records = match manifest.payload_storage() {
            CdcBufferPayloadStorage::ObjectStore => {
                let payload = self.object_payload(manifest).await?;
                decode_payload_records(&payload).with_context(|| {
                    format!(
                        "decode CDC buffer payload object '{}'",
                        manifest.transaction_key(),
                    )
                })?
            }
            CdcBufferPayloadStorage::SlateDbBlob => self.legacy_slate_records(manifest).await?,
        };
        ensure!(
            records.len() == manifest.record_count(),
            "CDC buffer transaction '{}' expected {} records, found {}",
            manifest.transaction_key(),
            manifest.record_count(),
            records.len()
        );
        Ok(records)
    }

    pub async fn change_batches(
        &self,
        manifest: &CdcBufferedTransactionManifest,
    ) -> Result<Vec<ChangeBatch>> {
        ensure!(
            manifest.payload_format() == CdcBufferPayloadFormat::ChangeBatches,
            "CDC buffer transaction '{}' stores {:?}, not change batches",
            manifest.transaction_key(),
            manifest.payload_format()
        );
        let batches = match manifest.payload_storage() {
            CdcBufferPayloadStorage::ObjectStore => {
                let payload = self.object_payload(manifest).await?;
                decode_payload_change_batches(&payload).with_context(|| {
                    format!(
                        "decode CDC buffer change batch payload '{}'",
                        manifest.transaction_key(),
                    )
                })?
            }
            CdcBufferPayloadStorage::SlateDbBlob => {
                anyhow::bail!(
                    "CDC buffer transaction '{}' stores change batches in unsupported legacy SlateDB payload storage",
                    manifest.transaction_key()
                )
            }
        };
        let change_count = batches.iter().map(ChangeBatch::change_count).sum::<usize>();
        ensure!(
            change_count == manifest.record_count(),
            "CDC buffer transaction '{}' expected {} changes, found {}",
            manifest.transaction_key(),
            manifest.record_count(),
            change_count
        );
        Ok(batches)
    }

    pub async fn mark_delivered(
        &self,
        manifest: &CdcBufferedTransactionManifest,
        delivered_at_unix_ms: u64,
    ) -> Result<CdcBufferedTransactionManifest> {
        self.mark_delivered_with_durable_wait(manifest, delivered_at_unix_ms, true)
            .await
    }

    pub async fn mark_delivered_without_durable_wait(
        &self,
        manifest: &CdcBufferedTransactionManifest,
        delivered_at_unix_ms: u64,
    ) -> Result<CdcBufferedTransactionManifest> {
        self.mark_delivered_with_durable_wait(manifest, delivered_at_unix_ms, false)
            .await
    }

    async fn mark_delivered_with_durable_wait(
        &self,
        manifest: &CdcBufferedTransactionManifest,
        delivered_at_unix_ms: u64,
        await_durable: bool,
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
        write_batch(self.db.as_ref(), batch, await_durable)
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
        if !delivered.is_empty() {
            self.db
                .flush()
                .await
                .map_err(map_slate_err)
                .context("flush CDC buffer delivery markers before payload cleanup")?;
        }
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
            let pending_key =
                pending_manifest_key(manifest.pipeline_name(), manifest.transaction_key());
            if self
                .db
                .get(pending_key)
                .await
                .map_err(map_slate_err)?
                .is_some()
            {
                batch.delete(key);
                summary.deleted_transactions = summary.deleted_transactions.saturating_add(1);
                continue;
            }
            match manifest.payload_storage() {
                CdcBufferPayloadStorage::ObjectStore => {
                    let payload_object_key = manifest.payload_object_key().ok_or_else(|| {
                        anyhow!(
                            "delivered CDC buffer transaction '{}' is missing payload object key",
                            manifest.transaction_key()
                        )
                    })?;
                    self.delete_payload_object(payload_object_key).await?;
                    summary.deleted_records = summary
                        .deleted_records
                        .saturating_add(manifest.record_count());
                    summary.deleted_bytes = summary
                        .deleted_bytes
                        .saturating_add(manifest.payload_bytes());
                }
                CdcBufferPayloadStorage::SlateDbBlob => {
                    let blob_key =
                        payload_blob_key(manifest.pipeline_name(), manifest.transaction_key());
                    let mut deleted_blob = false;
                    if let Some(payload) =
                        self.db.get(blob_key.clone()).await.map_err(map_slate_err)?
                    {
                        batch.delete(blob_key);
                        summary.deleted_records = summary
                            .deleted_records
                            .saturating_add(manifest.record_count());
                        summary.deleted_bytes = summary.deleted_bytes.saturating_add(payload.len());
                        deleted_blob = true;
                    }
                    for (payload_key, payload_value) in scan_prefix(
                        &self.db,
                        &payload_prefix(manifest.pipeline_name(), manifest.transaction_key()),
                    )
                    .await?
                    {
                        batch.delete(payload_key);
                        if !deleted_blob {
                            summary.deleted_records = summary.deleted_records.saturating_add(1);
                        }
                        summary.deleted_bytes =
                            summary.deleted_bytes.saturating_add(payload_value.len());
                    }
                }
            }
            batch.delete(key);
            summary.deleted_transactions = summary.deleted_transactions.saturating_add(1);
        }

        if summary.deleted_transactions > 0 {
            write_batch(self.db.as_ref(), batch, false)
                .await
                .context("cleanup delivered CDC buffer transactions")?;
        }
        Ok(summary)
    }

    pub async fn cleanup_delivered_manifest(
        &self,
        manifest: &CdcBufferedTransactionManifest,
    ) -> Result<CdcBufferCleanupSummary> {
        let Some(delivered_at) = manifest.delivered_at_unix_ms() else {
            return Ok(CdcBufferCleanupSummary {
                deleted_transactions: 0,
                deleted_records: 0,
                deleted_bytes: 0,
            });
        };
        let delivered_key = delivered_manifest_key(
            manifest.pipeline_name(),
            delivered_at,
            manifest.transaction_key(),
        );
        let pending_key =
            pending_manifest_key(manifest.pipeline_name(), manifest.transaction_key());
        let mut batch = WriteBatch::new();
        let mut summary = CdcBufferCleanupSummary {
            deleted_transactions: 0,
            deleted_records: 0,
            deleted_bytes: 0,
        };

        self.db
            .flush()
            .await
            .map_err(map_slate_err)
            .context("flush CDC buffer delivery marker before payload cleanup")?;

        if self
            .db
            .get(pending_key)
            .await
            .map_err(map_slate_err)?
            .is_some()
        {
            batch.delete(delivered_key);
            summary.deleted_transactions = 1;
            write_batch(self.db.as_ref(), batch, false)
                .await
                .context("cleanup stale delivered CDC buffer transaction manifest")?;
            return Ok(summary);
        }

        match manifest.payload_storage() {
            CdcBufferPayloadStorage::ObjectStore => {
                let payload_object_key = manifest.payload_object_key().ok_or_else(|| {
                    anyhow!(
                        "delivered CDC buffer transaction '{}' is missing payload object key",
                        manifest.transaction_key()
                    )
                })?;
                self.delete_payload_object(payload_object_key).await?;
                summary.deleted_records = manifest.record_count();
                summary.deleted_bytes = manifest.payload_bytes();
            }
            CdcBufferPayloadStorage::SlateDbBlob => {
                let blob_key =
                    payload_blob_key(manifest.pipeline_name(), manifest.transaction_key());
                let mut deleted_blob = false;
                if let Some(payload) = self.db.get(blob_key.clone()).await.map_err(map_slate_err)? {
                    batch.delete(blob_key);
                    summary.deleted_records = manifest.record_count();
                    summary.deleted_bytes = payload.len();
                    deleted_blob = true;
                }
                for (payload_key, payload_value) in scan_prefix(
                    &self.db,
                    &payload_prefix(manifest.pipeline_name(), manifest.transaction_key()),
                )
                .await?
                {
                    batch.delete(payload_key);
                    if !deleted_blob {
                        summary.deleted_records = summary.deleted_records.saturating_add(1);
                    }
                    summary.deleted_bytes =
                        summary.deleted_bytes.saturating_add(payload_value.len());
                }
            }
        }
        batch.delete(delivered_key);
        summary.deleted_transactions = 1;
        write_batch(self.db.as_ref(), batch, false)
            .await
            .context("cleanup delivered CDC buffer transaction manifest")?;
        Ok(summary)
    }

    fn payload_object_store(&self) -> Result<&Arc<dyn ObjectStore>> {
        self.object_store.as_ref().ok_or_else(|| {
            anyhow!(
                "CDC buffer payload object store is not configured; use CdcBufferStore::with_object_store"
            )
        })
    }

    async fn object_payload(&self, manifest: &CdcBufferedTransactionManifest) -> Result<Vec<u8>> {
        let payload_object_key = manifest.payload_object_key().ok_or_else(|| {
            anyhow!(
                "CDC buffer transaction '{}' is missing payload object key",
                manifest.transaction_key()
            )
        })?;
        load_payload_object(
            self.payload_object_store()?,
            payload_object_key,
            "CDC buffer",
        )
        .await
    }

    async fn legacy_slate_records(
        &self,
        manifest: &CdcBufferedTransactionManifest,
    ) -> Result<Vec<CdcBufferRecord>> {
        if let Some(value) = self
            .db
            .get(payload_blob_key(
                manifest.pipeline_name(),
                manifest.transaction_key(),
            ))
            .await
            .map_err(map_slate_err)?
        {
            return decode_payload_records(&value).with_context(|| {
                format!(
                    "decode legacy CDC buffer payload blob '{}'",
                    manifest.transaction_key()
                )
            });
        }

        scan_prefix(
            &self.db,
            &payload_prefix(manifest.pipeline_name(), manifest.transaction_key()),
        )
        .await?
        .into_iter()
        .map(|(_, value)| {
            serde_json::from_slice::<CdcBufferRecord>(&value)
                .context("decode legacy CDC buffer payload record")
        })
        .collect::<Result<Vec<_>>>()
    }

    async fn delete_payload_object(&self, payload_object_key: &str) -> Result<()> {
        delete_payload_object_if_exists(
            self.payload_object_store()?,
            payload_object_key,
            "CDC buffer",
        )
        .await
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

    fn encode_payload(&self) -> Result<Vec<u8>> {
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

    pub fn pending_objects(&self) -> usize {
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

#[cfg(test)]
async fn write_durable_batch(db: &Db, batch: WriteBatch) -> Result<()> {
    write_batch(db, batch, true).await
}

async fn write_batch(db: &Db, batch: WriteBatch, await_durable: bool) -> Result<()> {
    db.write_with_options(batch, &WriteOptions { await_durable })
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

fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use floe_cdc_core::{CdcColumnarColumn, CdcColumnarRowBatch, CdcTableId};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;

    async fn test_store(name: &str) -> CdcBufferStore {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open(name, Arc::clone(&object_store))
                .await
                .expect("open SlateDB"),
        );
        CdcBufferStore::with_object_store(db, object_store)
    }

    fn reopened_store(store: &CdcBufferStore) -> CdcBufferStore {
        CdcBufferStore::with_object_store(
            Arc::clone(&store.db),
            Arc::clone(store.object_store.as_ref().expect("object store")),
        )
    }

    #[tokio::test]
    async fn appends_and_replays_pending_transactions() {
        let store = test_store("cdc-buffer-append").await;
        let append = append("0/10", 1000, vec![record(1), record(2)])
            .with_schema_versions(CdcSchemaVersionMap::from([("orders".to_string(), 42)]));
        let manifest = store.append_transaction(&append).await.expect("append");

        let pending = store
            .pending_transactions("pipe", 10)
            .await
            .expect("pending");
        assert_eq!(pending, vec![manifest.clone()]);
        assert_eq!(manifest.schema_versions().get("orders"), Some(&42));
        assert_eq!(
            store.records(&manifest).await.unwrap(),
            vec![record(1), record(2)]
        );
        let payload_object_key = manifest.payload_object_key().expect("payload object key");
        assert!(
            store
                .object_store
                .as_ref()
                .expect("object store")
                .head(&ObjectPath::from(payload_object_key.to_string()))
                .await
                .is_ok()
        );
        assert!(
            store
                .db
                .get(payload_blob_key("pipe", manifest.transaction_key()))
                .await
                .expect("load legacy payload blob")
                .is_none()
        );
        assert!(
            scan_prefix(
                &store.db,
                &payload_prefix("pipe", manifest.transaction_key())
            )
            .await
            .expect("legacy payload records")
            .is_empty()
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
    async fn recovery_replays_after_durable_append_before_target_delivery() {
        let store = test_store("cdc-buffer-recovery-before-delivery").await;
        let first_append = append("0/10", 1000, vec![record(1), record(2)]);
        let manifest = store
            .append_transaction(&first_append)
            .await
            .expect("append");
        let later_append = append("0/20", 1001, vec![record(3)]);
        let later_manifest = store
            .append_transaction(&later_append)
            .await
            .expect("append later transaction");

        let recovered = reopened_store(&store);
        let pending = recovered
            .pending_transactions("pipe", 10)
            .await
            .expect("pending after recovery");

        assert_eq!(pending, vec![manifest.clone(), later_manifest.clone()]);
        assert_transaction_ids(&pending, &["tx-0/10", "tx-0/20"]);
        assert_eq!(
            recovered.records(&manifest).await.expect("records"),
            vec![record(1), record(2)]
        );
        assert_eq!(
            recovered
                .records(&later_manifest)
                .await
                .expect("later records"),
            vec![record(3)]
        );
    }

    #[tokio::test]
    async fn recovery_replays_after_target_delivery_before_delivery_checkpoint() {
        let store = test_store("cdc-buffer-recovery-before-checkpoint").await;
        let first_append = append("0/10", 1000, vec![record(1)]);
        let manifest = store
            .append_transaction(&first_append)
            .await
            .expect("append");
        let later_append = append("0/20", 1001, vec![record(2)]);
        let later_manifest = store
            .append_transaction(&later_append)
            .await
            .expect("append later transaction");

        let recovered = reopened_store(&store);
        let pending = recovered
            .pending_transactions("pipe", 10)
            .await
            .expect("pending after target-only delivery");

        assert_eq!(pending, vec![manifest.clone(), later_manifest]);
        assert_transaction_ids(&pending, &["tx-0/10", "tx-0/20"]);
        assert_eq!(recovered.records(&manifest).await.unwrap(), vec![record(1)]);
    }

    #[tokio::test]
    async fn recovery_skips_after_delivery_checkpoint() {
        let store = test_store("cdc-buffer-recovery-after-checkpoint").await;
        let first_append = append("0/10", 1000, vec![record(1)]);
        let manifest = store
            .append_transaction(&first_append)
            .await
            .expect("append");
        let later_append = append("0/20", 1001, vec![record(2)]);
        let later_manifest = store
            .append_transaction(&later_append)
            .await
            .expect("append later transaction");
        let delivered = store
            .mark_delivered(&manifest, 2000)
            .await
            .expect("mark delivered");

        let recovered = reopened_store(&store);
        let pending = recovered
            .pending_transactions("pipe", 10)
            .await
            .expect("pending after delivery checkpoint");
        let delivery = recovered
            .delivery_frontier("pipe")
            .await
            .expect("delivery frontier")
            .expect("frontier");

        assert_eq!(pending, vec![later_manifest.clone()]);
        assert_transaction_ids(&pending, &["tx-0/20"]);
        assert_eq!(
            delivery.source_position(),
            &CdcSourcePosition::postgres("0/10", None).unwrap()
        );
        assert_eq!(
            recovered.records(&delivered).await.unwrap(),
            vec![record(1)]
        );
        assert_eq!(
            recovered.records(&later_manifest).await.unwrap(),
            vec![record(2)]
        );
    }

    #[tokio::test]
    async fn appends_and_replays_change_batch_payloads() {
        let store = test_store("cdc-buffer-change-batches").await;
        let table_id = CdcTableId::new("orders").unwrap();
        let rows = CdcColumnarRowBatch::new(vec![CdcColumnarColumn::Int64(vec![Some(1), Some(2)])])
            .unwrap();
        let batch =
            ChangeBatch::new_snapshot_insert(table_id.clone(), rows).expect("snapshot batch");
        let append = CdcBufferAppend::new_change_batches(
            "pipe",
            "pg_main",
            table_id.as_str(),
            CdcSourcePosition::postgres("0/10", None).unwrap(),
            None,
            vec![batch.clone()],
            1000,
        )
        .unwrap();
        let manifest = store.append_transaction(&append).await.expect("append");

        assert_eq!(
            manifest.payload_format(),
            CdcBufferPayloadFormat::ChangeBatches
        );
        assert_eq!(manifest.record_count(), 2);
        assert!(store.records(&manifest).await.is_err());
        assert_eq!(store.change_batches(&manifest).await.unwrap(), vec![batch]);
    }

    #[tokio::test]
    async fn delivery_frontier_and_cleanup_only_delete_delivered_transactions() {
        let store = test_store("cdc-buffer-cleanup").await;
        let delivered_append = append("0/10", 1000, vec![record(1)]);
        let delivered = store
            .append_transaction(&delivered_append)
            .await
            .expect("append delivered");
        let pending_append = append("0/20", 2000, vec![record(2)]);
        let pending = store
            .append_transaction(&pending_append)
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
        assert!(summary.deleted_bytes() > 0);
        assert!(
            store
                .object_store
                .as_ref()
                .expect("object store")
                .head(&ObjectPath::from(
                    delivered
                        .payload_object_key()
                        .expect("delivered payload object key")
                        .to_string()
                ))
                .await
                .is_err()
        );

        let pending_after = store
            .pending_transactions("pipe", 10)
            .await
            .expect("pending after cleanup");
        assert_eq!(pending_after, vec![pending]);
    }

    #[tokio::test]
    async fn cleanup_does_not_delete_replayed_pending_payload() {
        let store = test_store("cdc-buffer-cleanup-replayed-pending").await;
        let delivered_append = append("0/10", 1000, vec![record(1)]);
        let delivered = store
            .append_transaction(&delivered_append)
            .await
            .expect("append delivered");
        store
            .mark_delivered(&delivered, 2000)
            .await
            .expect("mark delivered");

        let replayed_append = append("0/10", 3000, vec![record(9)]);
        let pending = store
            .append_transaction(&replayed_append)
            .await
            .expect("append replayed pending");
        let summary = store
            .cleanup_delivered("pipe", CdcBufferCleanupPolicy::new(0), 4000)
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted_transactions(), 1);
        assert_eq!(summary.deleted_records(), 0);
        assert_eq!(store.records(&pending).await.unwrap(), vec![record(9)]);
    }

    #[tokio::test]
    async fn cleanup_delivered_manifest_deletes_one_delivered_payload() {
        let store = test_store("cdc-buffer-cleanup-single-delivered").await;
        let append = append("0/10", 1000, vec![record(1)]);
        let manifest = store
            .append_transaction(&append)
            .await
            .expect("append delivered");
        let delivered = store
            .mark_delivered(&manifest, 2000)
            .await
            .expect("mark delivered");
        let payload_object_key = delivered
            .payload_object_key()
            .expect("payload object key")
            .to_string();

        let summary = store
            .cleanup_delivered_manifest(&delivered)
            .await
            .expect("cleanup single delivered");

        assert_eq!(summary.deleted_transactions(), 1);
        assert_eq!(summary.deleted_records(), 1);
        assert!(summary.deleted_bytes() > 0);
        assert!(
            store
                .object_store
                .as_ref()
                .expect("object store")
                .head(&ObjectPath::from(payload_object_key))
                .await
                .is_err()
        );
        assert!(
            store
                .cleanup_delivered("pipe", CdcBufferCleanupPolicy::new(0), 3000)
                .await
                .expect("cleanup remaining delivered")
                .deleted_transactions()
                == 0
        );
    }

    #[tokio::test]
    async fn stats_report_size_and_oldest_age() {
        let store = test_store("cdc-buffer-stats").await;
        let append_one = append("0/10", 1000, vec![record(1), record(2)]);
        store
            .append_transaction(&append_one)
            .await
            .expect("append one");
        let append_two = append("0/20", 1500, vec![record(3)]);
        store
            .append_transaction(&append_two)
            .await
            .expect("append two");

        let stats = store.stats("pipe", 2500).await.expect("stats");
        assert_eq!(stats.pending_transactions(), 2);
        assert_eq!(stats.pending_records(), 3);
        assert_eq!(stats.oldest_pending_age_ms(), Some(1500));
        assert!(stats.pending_bytes() > 0);
    }

    #[tokio::test]
    async fn appends_and_replays_record_headers() {
        let store = test_store("cdc-buffer-record-headers").await;
        let record = record(1)
            .with_header("floe-idempotency-key", b"pipe/0/10/0".to_vec())
            .with_header("floe-source-position", b"pg/0/10".to_vec());
        let append = append("0/10", 1000, vec![record.clone()]);
        let manifest = store.append_transaction(&append).await.expect("append");

        let records = store.records(&manifest).await.expect("records");

        assert_eq!(records, vec![record]);
        assert_eq!(records[0].headers()[0].key(), "floe-idempotency-key");
        assert_eq!(records[0].headers()[0].value(), b"pipe/0/10/0");
        assert_eq!(records[0].headers()[1].key(), "floe-source-position");
        assert_eq!(records[0].headers()[1].value(), b"pg/0/10");
    }

    #[test]
    fn decodes_v1_payload_blob_without_headers() {
        let records = vec![record(1), record(2)];
        let payload = encode_payload_records_v1(&records).expect("encode v1 payload");

        let decoded = decode_payload_records(&payload).expect("decode v1 payload");

        assert_eq!(decoded, records);
        assert!(decoded.iter().all(|record| record.headers().is_empty()));
    }

    #[tokio::test]
    async fn replays_legacy_json_payload_records() {
        let store = test_store("cdc-buffer-legacy-records").await;
        let append = append("0/10", 1000, vec![record(1), record(2)]);
        let transaction_key =
            transaction_key(append.source_position(), append.transaction_id()).unwrap();
        let manifest = CdcBufferedTransactionManifest {
            pipeline_name: append.pipeline_name().to_string(),
            source_name: append.source_name().to_string(),
            table_id: append.table_id().to_string(),
            transaction_key: transaction_key.clone(),
            source_position: append.source_position().clone(),
            transaction_id: append.transaction_id().cloned(),
            record_count: append.records().len(),
            payload_bytes: append.records().iter().map(CdcBufferRecord::byte_len).sum(),
            payload_storage: CdcBufferPayloadStorage::SlateDbBlob,
            payload_format: CdcBufferPayloadFormat::KafkaRecords,
            schema_versions: CdcSchemaVersionMap::new(),
            payload_object_key: None,
            buffered_at_unix_ms: append.buffered_at_unix_ms(),
            delivered_at_unix_ms: None,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            pending_manifest_key("pipe", &transaction_key),
            serde_json::to_vec(&manifest).unwrap(),
        );
        for (idx, record) in append.records().iter().enumerate() {
            batch.put(
                payload_key("pipe", &transaction_key, idx),
                serde_json::to_vec(record).unwrap(),
            );
        }
        write_durable_batch(store.db.as_ref(), batch)
            .await
            .expect("write legacy payload");

        assert_eq!(
            store.records(&manifest).await.unwrap(),
            vec![record(1), record(2)]
        );
    }

    #[tokio::test]
    async fn replays_legacy_slatedb_payload_blob() {
        let store = test_store("cdc-buffer-legacy-blob").await;
        let append = append("0/10", 1000, vec![record(1), record(2)]);
        let transaction_key =
            transaction_key(append.source_position(), append.transaction_id()).unwrap();
        let payload = encode_payload_records(append.records()).expect("encode payload");
        let manifest = CdcBufferedTransactionManifest {
            pipeline_name: append.pipeline_name().to_string(),
            source_name: append.source_name().to_string(),
            table_id: append.table_id().to_string(),
            transaction_key: transaction_key.clone(),
            source_position: append.source_position().clone(),
            transaction_id: append.transaction_id().cloned(),
            record_count: append.records().len(),
            payload_bytes: payload.len(),
            payload_storage: CdcBufferPayloadStorage::SlateDbBlob,
            payload_format: CdcBufferPayloadFormat::KafkaRecords,
            schema_versions: CdcSchemaVersionMap::new(),
            payload_object_key: None,
            buffered_at_unix_ms: append.buffered_at_unix_ms(),
            delivered_at_unix_ms: None,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            pending_manifest_key("pipe", &transaction_key),
            serde_json::to_vec(&manifest).unwrap(),
        );
        batch.put(payload_blob_key("pipe", &transaction_key), payload);
        write_durable_batch(store.db.as_ref(), batch)
            .await
            .expect("write legacy payload blob");

        assert_eq!(
            store.records(&manifest).await.unwrap(),
            vec![record(1), record(2)]
        );
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

    fn encode_payload_records_v1(records: &[CdcBufferRecord]) -> Result<Vec<u8>> {
        let record_count =
            u64::try_from(records.len()).context("CDC buffer record count exceeds u64")?;
        let mut out = Vec::new();
        out.extend_from_slice(CDC_BUFFER_PAYLOAD_MAGIC_V1);
        out.extend_from_slice(&record_count.to_be_bytes());
        for record in records {
            encode_optional_bytes(&mut out, record.key())?;
            encode_optional_bytes(&mut out, record.value())?;
        }
        Ok(out)
    }

    fn assert_transaction_ids(manifests: &[CdcBufferedTransactionManifest], expected: &[&str]) {
        let actual = manifests
            .iter()
            .map(|manifest| {
                manifest
                    .transaction_id()
                    .map(CdcTransactionId::as_str)
                    .unwrap_or("<none>")
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
