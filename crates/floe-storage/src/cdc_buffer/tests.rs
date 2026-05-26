use super::keys::{
    payload_blob_key, payload_key, payload_object_key, payload_prefix, pending_manifest_key,
};
use super::payload_codec::{
    CDC_BUFFER_PAYLOAD_MAGIC_V1, decode_payload_records, encode_optional_bytes,
    encode_payload_records,
};
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

async fn write_durable_batch(db: &Db, batch: WriteBatch) -> Result<()> {
    write_batch(db, batch, true).await
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
    let rows =
        CdcColumnarRowBatch::new(vec![CdcColumnarColumn::Int64(vec![Some(1), Some(2)])]).unwrap();
    let batch = ChangeBatch::new_snapshot_insert(table_id.clone(), rows).expect("snapshot batch");
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
async fn integrity_report_detects_missing_and_orphan_payload_objects() {
    let store = test_store("cdc-buffer-integrity").await;
    let append = append("0/10", 1000, vec![record(1)]);
    let manifest = store.append_transaction(&append).await.expect("append");
    let referenced_key = manifest
        .payload_object_key()
        .expect("referenced payload object key")
        .to_string();
    let orphan_key = payload_object_key("pipe", "orphan-transaction");
    let orphan_payload = vec![1, 2, 3, 4];
    let object_store = store.object_store.as_ref().expect("object store");
    object_store
        .put(
            &ObjectPath::from(orphan_key.clone()),
            orphan_payload.clone().into(),
        )
        .await
        .expect("write orphan payload");
    object_store
        .delete(&ObjectPath::from(referenced_key))
        .await
        .expect("delete referenced payload");

    let report = store.integrity_report("pipe").await.expect("integrity");

    assert_eq!(report.pending_payload_objects(), 1);
    assert_eq!(report.delivered_payload_objects(), 0);
    assert_eq!(report.missing_payload_objects(), 1);
    assert_eq!(report.orphan_payload_objects(), 1);
    assert_eq!(report.orphan_payload_bytes(), orphan_payload.len());
}

#[tokio::test]
async fn orphan_payload_cleanup_respects_age_and_keeps_referenced_payloads() {
    let store = test_store("cdc-buffer-orphan-cleanup").await;
    let append = append("0/10", 1000, vec![record(1)]);
    let manifest = store.append_transaction(&append).await.expect("append");
    let referenced_key = manifest
        .payload_object_key()
        .expect("referenced payload object key")
        .to_string();
    let orphan_key = payload_object_key("pipe", "old-orphan-transaction");
    let orphan_payload = vec![5, 6, 7, 8, 9];
    let object_store = store.object_store.as_ref().expect("object store");
    object_store
        .put(
            &ObjectPath::from(orphan_key.clone()),
            orphan_payload.clone().into(),
        )
        .await
        .expect("write orphan payload");

    let early = store
        .cleanup_orphan_payload_objects("pipe", u64::MAX, 0)
        .await
        .expect("early orphan cleanup");
    assert_eq!(early.deleted_objects(), 0);
    assert_eq!(early.deleted_bytes(), 0);

    let cleaned = store
        .cleanup_orphan_payload_objects("pipe", 1, u64::MAX)
        .await
        .expect("orphan cleanup");
    assert_eq!(cleaned.deleted_objects(), 1);
    assert_eq!(cleaned.deleted_bytes(), orphan_payload.len());
    assert!(
        object_store
            .head(&ObjectPath::from(orphan_key))
            .await
            .is_err()
    );
    assert!(
        object_store
            .head(&ObjectPath::from(referenced_key))
            .await
            .is_ok()
    );
    assert_eq!(store.records(&manifest).await.unwrap(), vec![record(1)]);
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

fn append(lsn: &str, buffered_at_unix_ms: u64, records: Vec<CdcBufferRecord>) -> CdcBufferAppend {
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
