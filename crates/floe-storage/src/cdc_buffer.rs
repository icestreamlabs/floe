use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use floe_cdc_core::{CdcSchemaVersionMap, CdcSourcePosition, CdcTransactionId, ChangeBatch};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde::{Deserialize, Serialize};
use slatedb::config::{ScanOptions, WriteOptions};
use slatedb::{Db, Error as SlateError, WriteBatch};

mod keys;
mod payload_codec;

use keys::{
    delivered_manifest_key, delivered_manifest_prefix, delivery_frontier_key, payload_object_key,
    payload_object_prefix, pending_manifest_key, pending_manifest_prefix, pending_stats_key,
    pending_time_index_key, pending_time_index_prefix, source_frontier_key, transaction_key,
};
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
    pending_objects: usize,
    pending_records: usize,
    pending_bytes: usize,
    oldest_pending_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct CdcBufferPendingStats {
    pending_transactions: usize,
    pending_records: usize,
    pending_bytes: usize,
}

impl CdcBufferPendingStats {
    fn add_counts(&mut self, records: usize, bytes: usize) {
        self.pending_transactions = self.pending_transactions.saturating_add(1);
        self.pending_records = self.pending_records.saturating_add(records);
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
    }

    fn add_manifest(&mut self, manifest: &CdcBufferedTransactionManifest) {
        self.add_counts(manifest.record_count(), manifest.payload_bytes());
    }

    fn subtract_manifest(&mut self, manifest: &CdcBufferedTransactionManifest) {
        self.pending_transactions = self.pending_transactions.saturating_sub(1);
        self.pending_records = self.pending_records.saturating_sub(manifest.record_count());
        self.pending_bytes = self.pending_bytes.saturating_sub(manifest.payload_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcBufferCleanupSummary {
    deleted_transactions: usize,
    deleted_records: usize,
    deleted_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcBufferOrphanCleanupSummary {
    deleted_objects: usize,
    deleted_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcBufferIntegrityReport {
    pending_payload_objects: usize,
    delivered_payload_objects: usize,
    missing_payload_objects: usize,
    orphan_payload_objects: usize,
    orphan_payload_bytes: usize,
}

impl CdcBufferStore {
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
        let pending_key = pending_manifest_key(&append.pipeline_name, &transaction_key);
        let mut pending_stats = self
            .load_or_rebuild_pending_stats(&append.pipeline_name)
            .await?;
        let existing_pending_manifest =
            self.pending_manifest_from_key(pending_key.clone())
                .await
                .context("load existing CDC buffer pending manifest before append")?;
        if let Some(existing) = existing_pending_manifest.as_ref() {
            pending_stats.subtract_manifest(existing);
        }
        pending_stats.add_counts(record_count, payload_bytes);

        let mut batch = WriteBatch::new();
        if let Some(existing) = existing_pending_manifest.as_ref() {
            batch.delete(pending_time_index_key_for_manifest(existing));
        }
        batch.put(
            pending_key,
            serde_json::to_vec(&manifest).context("encode CDC buffer transaction manifest")?,
        );
        stage_pending_time_index(&mut batch, &manifest);
        stage_pending_stats(&mut batch, &append.pipeline_name, &pending_stats)?;
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
        let payload = self.object_payload(manifest).await?;
        let records = decode_payload_records(&payload).with_context(|| {
            format!(
                "decode CDC buffer payload object '{}'",
                manifest.transaction_key(),
            )
        })?;
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
        let payload = self.object_payload(manifest).await?;
        let batches = decode_payload_change_batches(&payload).with_context(|| {
            format!(
                "decode CDC buffer change batch payload '{}'",
                manifest.transaction_key(),
            )
        })?;
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
        let pending_key =
            pending_manifest_key(manifest.pipeline_name(), manifest.transaction_key());
        let mut pending_stats = self
            .load_or_rebuild_pending_stats(manifest.pipeline_name())
            .await?;
        let existing_pending_manifest =
            self.pending_manifest_from_key(pending_key.clone())
                .await
                .context("load existing CDC buffer pending manifest before delivery")?;
        if let Some(existing) = existing_pending_manifest.as_ref() {
            pending_stats.subtract_manifest(existing);
        }

        let mut batch = WriteBatch::new();
        batch.delete(pending_key);
        if let Some(existing) = existing_pending_manifest.as_ref() {
            batch.delete(pending_time_index_key_for_manifest(existing));
        }
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
        stage_pending_stats(&mut batch, manifest.pipeline_name(), &pending_stats)?;
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
        let pending_stats = self.load_or_rebuild_pending_stats(pipeline_name).await?;
        let oldest_pending_age_ms = if pending_stats.pending_transactions == 0 {
            None
        } else {
            self.oldest_pending_buffered_at_unix_ms(pipeline_name)
                .await?
                .map(|buffered_at| now_unix_ms.saturating_sub(buffered_at))
        };
        Ok(CdcBufferStats {
            pending_transactions: pending_stats.pending_transactions,
            pending_objects: pending_stats.pending_transactions,
            pending_records: pending_stats.pending_records,
            pending_bytes: pending_stats.pending_bytes,
            oldest_pending_age_ms,
        })
    }

    async fn load_or_rebuild_pending_stats(
        &self,
        pipeline_name: &str,
    ) -> Result<CdcBufferPendingStats> {
        let loaded_stats = load_json::<CdcBufferPendingStats>(
            &self.db,
            pending_stats_key(pipeline_name),
            "CDC buffer pending stats",
        )
        .await?;
        if let Some(stats) = loaded_stats {
            let has_required_time_index = stats.pending_transactions == 0
                || self
                    .oldest_pending_buffered_at_unix_ms(pipeline_name)
                    .await?
                    .is_some();
            if has_required_time_index {
                return Ok(stats);
            }
        }

        let stats = self.rebuild_pending_stats(pipeline_name).await?;
        Ok(stats)
    }

    async fn rebuild_pending_stats(&self, pipeline_name: &str) -> Result<CdcBufferPendingStats> {
        let manifests = self.pending_transactions(pipeline_name, usize::MAX).await?;
        let mut stats = CdcBufferPendingStats::default();
        let mut batch = WriteBatch::new();
        for manifest in manifests {
            stats.add_manifest(&manifest);
            stage_pending_time_index(&mut batch, &manifest);
        }
        stage_pending_stats(&mut batch, pipeline_name, &stats)?;
        write_batch(self.db.as_ref(), batch, false)
            .await
            .context("persist rebuilt CDC buffer pending indexes")?;
        Ok(stats)
    }

    async fn oldest_pending_buffered_at_unix_ms(&self, pipeline_name: &str) -> Result<Option<u64>> {
        let entries = scan_prefix_limit(&self.db, &pending_time_index_prefix(pipeline_name), 1)
            .await
            .context("scan CDC buffer pending time index")?;
        let Some((_, value)) = entries.into_iter().next() else {
            return Ok(None);
        };
        let bytes: [u8; 8] = value
            .as_slice()
            .try_into()
            .context("decode CDC buffer pending time index timestamp")?;
        Ok(Some(u64::from_be_bytes(bytes)))
    }

    async fn pending_manifest_from_key(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<CdcBufferedTransactionManifest>> {
        let Some(value) = self.db.get(key).await.map_err(map_slate_err)? else {
            return Ok(None);
        };
        serde_json::from_slice::<CdcBufferedTransactionManifest>(&value)
            .context("decode CDC buffer pending manifest")
            .map(Some)
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

        let payload_object_key = manifest.payload_object_key().ok_or_else(|| {
            anyhow!(
                "delivered CDC buffer transaction '{}' is missing payload object key",
                manifest.transaction_key()
            )
        })?;
        self.delete_payload_object(payload_object_key).await?;
        summary.deleted_records = manifest.record_count();
        summary.deleted_bytes = manifest.payload_bytes();
        batch.delete(delivered_key);
        summary.deleted_transactions = 1;
        write_batch(self.db.as_ref(), batch, false)
            .await
            .context("cleanup delivered CDC buffer transaction manifest")?;
        Ok(summary)
    }

    pub async fn integrity_report(&self, pipeline_name: &str) -> Result<CdcBufferIntegrityReport> {
        let references = self.payload_object_references(pipeline_name).await?;
        let object_prefix = payload_object_prefix(pipeline_name);
        let mut objects = self
            .payload_object_store()?
            .list(Some(&ObjectPath::from(object_prefix.clone())));
        let mut orphan_payload_objects = 0usize;
        let mut orphan_payload_bytes = 0usize;
        while let Some(entry) = objects.next().await {
            let entry = entry.with_context(|| {
                format!("list CDC buffer payload objects under {object_prefix}")
            })?;
            let object_key = entry.location.to_string();
            if !references.referenced_object_keys.contains(&object_key) {
                orphan_payload_objects = orphan_payload_objects.saturating_add(1);
                orphan_payload_bytes = orphan_payload_bytes
                    .saturating_add(usize::try_from(entry.size).unwrap_or(usize::MAX));
            }
        }

        Ok(CdcBufferIntegrityReport {
            pending_payload_objects: references.pending_payload_objects,
            delivered_payload_objects: references.delivered_payload_objects,
            missing_payload_objects: references.missing_payload_objects,
            orphan_payload_objects,
            orphan_payload_bytes,
        })
    }

    pub async fn cleanup_orphan_payload_objects(
        &self,
        pipeline_name: &str,
        orphan_retention_ms: u64,
        now_unix_ms: u64,
    ) -> Result<CdcBufferOrphanCleanupSummary> {
        let references = self.payload_object_references(pipeline_name).await?;
        let object_prefix = payload_object_prefix(pipeline_name);
        let mut objects = self
            .payload_object_store()?
            .list(Some(&ObjectPath::from(object_prefix.clone())));
        let mut summary = CdcBufferOrphanCleanupSummary {
            deleted_objects: 0,
            deleted_bytes: 0,
        };
        while let Some(entry) = objects.next().await {
            let entry = entry.with_context(|| {
                format!("list CDC buffer payload objects under {object_prefix}")
            })?;
            let object_key = entry.location.to_string();
            if references.referenced_object_keys.contains(&object_key) {
                continue;
            }
            let last_modified_unix_ms =
                u64::try_from(entry.last_modified.timestamp_millis()).unwrap_or(0);
            if now_unix_ms.saturating_sub(last_modified_unix_ms) < orphan_retention_ms {
                continue;
            }
            self.delete_payload_object(&object_key).await?;
            summary.deleted_objects = summary.deleted_objects.saturating_add(1);
            summary.deleted_bytes = summary
                .deleted_bytes
                .saturating_add(usize::try_from(entry.size).unwrap_or(usize::MAX));
        }
        Ok(summary)
    }

    fn payload_object_store(&self) -> Result<&Arc<dyn ObjectStore>> {
        self.object_store.as_ref().ok_or_else(|| {
            anyhow!(
                "CDC buffer payload object store is not configured; use CdcBufferStore::with_object_store"
            )
        })
    }

    async fn delivered_transactions(
        &self,
        pipeline_name: &str,
    ) -> Result<Vec<CdcBufferedTransactionManifest>> {
        scan_prefix(&self.db, &delivered_manifest_prefix(pipeline_name))
            .await?
            .into_iter()
            .map(|(_, value)| {
                serde_json::from_slice::<CdcBufferedTransactionManifest>(&value)
                    .context("decode CDC buffer delivered manifest")
            })
            .collect()
    }

    async fn payload_object_references(
        &self,
        pipeline_name: &str,
    ) -> Result<CdcBufferPayloadObjectReferences> {
        let pending = self.pending_transactions(pipeline_name, usize::MAX).await?;
        let delivered = self.delivered_transactions(pipeline_name).await?;
        let mut references = CdcBufferPayloadObjectReferences {
            referenced_object_keys: HashSet::new(),
            pending_payload_objects: 0,
            delivered_payload_objects: 0,
            missing_payload_objects: 0,
        };

        for manifest in &pending {
            self.record_manifest_payload_integrity(
                manifest,
                &mut references.referenced_object_keys,
                &mut references.missing_payload_objects,
            )
            .await?;
            references.pending_payload_objects =
                references.pending_payload_objects.saturating_add(1);
        }
        for manifest in &delivered {
            self.record_manifest_payload_integrity(
                manifest,
                &mut references.referenced_object_keys,
                &mut references.missing_payload_objects,
            )
            .await?;
            references.delivered_payload_objects =
                references.delivered_payload_objects.saturating_add(1);
        }

        Ok(references)
    }

    async fn record_manifest_payload_integrity(
        &self,
        manifest: &CdcBufferedTransactionManifest,
        referenced_object_keys: &mut HashSet<String>,
        missing_payload_objects: &mut usize,
    ) -> Result<()> {
        let Some(payload_object_key) = manifest.payload_object_key() else {
            *missing_payload_objects = missing_payload_objects.saturating_add(1);
            return Ok(());
        };
        referenced_object_keys.insert(payload_object_key.to_string());
        if !self.payload_object_exists(payload_object_key).await? {
            *missing_payload_objects = missing_payload_objects.saturating_add(1);
        }
        Ok(())
    }

    async fn payload_object_exists(&self, payload_object_key: &str) -> Result<bool> {
        match self
            .payload_object_store()?
            .head(&ObjectPath::from(payload_object_key.to_string()))
            .await
        {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(err) => Err(err).with_context(|| {
                format!("inspect CDC buffer payload object '{payload_object_key}'")
            }),
        }
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

    async fn delete_payload_object(&self, payload_object_key: &str) -> Result<()> {
        delete_payload_object_if_exists(
            self.payload_object_store()?,
            payload_object_key,
            "CDC buffer",
        )
        .await
    }
}

struct CdcBufferPayloadObjectReferences {
    referenced_object_keys: HashSet<String>,
    pending_payload_objects: usize,
    delivered_payload_objects: usize,
    missing_payload_objects: usize,
}

#[path = "cdc_buffer/record_types.rs"]
mod record_types;

use record_types::*;
#[cfg(test)]
mod tests;
