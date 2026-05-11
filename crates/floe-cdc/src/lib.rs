use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use dbsp_storage::storage::KeyValueTable;
use floe_cdc_core::{
    CdcChange, CdcCheckpoint, CdcRow, CdcRowKey, CdcSourceDefinition, CdcSourceId,
    CdcTableDefinition, CdcTableId, CdcTableSchema, ChangeBatch, TransactionBatch,
};
use serde::Deserialize;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

const CDC_PREFIX: &[u8] = b"floe_cdc/v1/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcRowDelta {
    row: CdcRow,
    diff: i64,
}

impl CdcRowDelta {
    pub fn insert(row: CdcRow) -> Self {
        Self { row, diff: 1 }
    }

    pub fn delete(row: CdcRow) -> Self {
        Self { row, diff: -1 }
    }

    pub fn row(&self) -> &CdcRow {
        &self.row
    }

    pub fn diff(&self) -> i64 {
        self.diff
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcTableDeltas {
    table_id: CdcTableId,
    deltas: Vec<CdcRowDelta>,
}

impl CdcTableDeltas {
    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn deltas(&self) -> &[CdcRowDelta] {
        &self.deltas
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcApplyResult {
    checkpoint: CdcCheckpoint,
    table_deltas: Vec<CdcTableDeltas>,
    already_committed: bool,
}

impl CdcApplyResult {
    pub fn checkpoint(&self) -> &CdcCheckpoint {
        &self.checkpoint
    }

    pub fn table_deltas(&self) -> &[CdcTableDeltas] {
        &self.table_deltas
    }

    pub fn already_committed(&self) -> bool {
        self.already_committed
    }
}

#[derive(Clone)]
pub struct CdcMetadataStore {
    table: Arc<dyn KeyValueTable>,
}

impl CdcMetadataStore {
    pub fn new(table: Arc<dyn KeyValueTable>) -> Self {
        Self { table }
    }

    pub async fn upsert_source(&self, source: &CdcSourceDefinition) -> Result<()> {
        let encoded = serde_json::to_vec(source).with_context(|| {
            format!(
                "encode CDC source metadata for '{}'",
                source.source_id().as_str()
            )
        })?;
        self.table
            .put(&source_metadata_key(source.source_id()), &encoded)
            .await
            .with_context(|| {
                format!(
                    "persist CDC source metadata for '{}'",
                    source.source_id().as_str()
                )
            })
    }

    pub async fn load_source(
        &self,
        source_id: &CdcSourceId,
    ) -> Result<Option<CdcSourceDefinition>> {
        let Some(bytes) = self
            .table
            .get(&source_metadata_key(source_id))
            .await
            .with_context(|| format!("load CDC source metadata for '{}'", source_id.as_str()))?
        else {
            return Ok(None);
        };
        decode_json(&bytes, "CDC source metadata")
    }

    pub async fn sources(&self) -> Result<Vec<CdcSourceDefinition>> {
        self.table
            .scan_prefix(source_metadata_prefix().as_slice(), &ScanOptions::default())
            .await
            .context("scan CDC source metadata")?
            .into_iter()
            .map(|(_, value)| decode_json_value(&value, "CDC source metadata"))
            .collect()
    }

    pub async fn upsert_table(&self, table_definition: &CdcTableDefinition) -> Result<()> {
        let source = self
            .load_source(table_definition.source_id())
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "CDC source '{}' does not exist",
                    table_definition.source_id().as_str()
                )
            })?;
        source.validate_table_definition(table_definition)?;

        let previous = self.load_table(table_definition.table_id()).await?;
        let encoded = serde_json::to_vec(table_definition).with_context(|| {
            format!(
                "encode CDC table metadata for '{}'",
                table_definition.table_id().as_str()
            )
        })?;

        let mut batch = WriteBatch::new();
        batch.put(
            table_metadata_key(table_definition.table_id()),
            encoded.clone(),
        );
        batch.put(
            source_table_index_key(table_definition.source_id(), table_definition.table_id()),
            encoded,
        );
        if let Some(previous) = previous
            && previous.source_id() != table_definition.source_id()
        {
            batch.delete(source_table_index_key(
                previous.source_id(),
                previous.table_id(),
            ));
        }

        self.table.write_batch(batch).await.with_context(|| {
            format!(
                "persist CDC table metadata for '{}'",
                table_definition.table_id().as_str()
            )
        })
    }

    pub async fn load_table(&self, table_id: &CdcTableId) -> Result<Option<CdcTableDefinition>> {
        let Some(bytes) = self
            .table
            .get(&table_metadata_key(table_id))
            .await
            .with_context(|| format!("load CDC table metadata for '{}'", table_id.as_str()))?
        else {
            return Ok(None);
        };
        decode_json(&bytes, "CDC table metadata")
    }

    pub async fn tables_for_source(
        &self,
        source_id: &CdcSourceId,
    ) -> Result<Vec<CdcTableDefinition>> {
        self.table
            .scan_prefix(
                source_table_index_prefix(source_id).as_slice(),
                &ScanOptions::default(),
            )
            .await
            .with_context(|| format!("scan CDC table metadata for '{}'", source_id.as_str()))?
            .into_iter()
            .map(|(_, value)| decode_json_value(&value, "CDC table metadata"))
            .collect()
    }
}

#[derive(Clone)]
pub struct CdcTableStore {
    table: Arc<dyn KeyValueTable>,
}

impl CdcTableStore {
    pub fn new(table: Arc<dyn KeyValueTable>) -> Self {
        Self { table }
    }

    pub async fn load_checkpoint(&self, source_id: &CdcSourceId) -> Result<Option<CdcCheckpoint>> {
        let Some(bytes) = self
            .table
            .get(&checkpoint_key(source_id))
            .await
            .with_context(|| format!("load CDC checkpoint for source '{}'", source_id.as_str()))?
        else {
            return Ok(None);
        };
        decode_json(&bytes, "CDC checkpoint")
    }

    pub async fn load_row(
        &self,
        table_id: &CdcTableId,
        row_key: &CdcRowKey,
    ) -> Result<Option<CdcRow>> {
        let key = row_key_bytes(table_id, row_key)?;
        self.load_row_by_storage_key(&key).await
    }

    pub async fn apply_transaction(
        &self,
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
        transaction: &TransactionBatch,
    ) -> Result<CdcApplyResult> {
        transaction.validate_against_schemas(schemas)?;
        let next_checkpoint = CdcCheckpoint::from_transaction(transaction);
        if self
            .load_checkpoint(transaction.source_id())
            .await?
            .as_ref()
            == Some(&next_checkpoint)
        {
            return Ok(CdcApplyResult {
                checkpoint: next_checkpoint,
                table_deltas: Vec::new(),
                already_committed: true,
            });
        }

        let mut batch = WriteBatch::new();
        let mut overlay = HashMap::<Vec<u8>, Option<CdcRow>>::new();
        let mut table_deltas = Vec::new();

        for change_batch in transaction.change_batches() {
            let schema = schemas.get(change_batch.table_id()).ok_or_else(|| {
                anyhow!("missing schema for '{}'", change_batch.table_id().as_str())
            })?;
            let deltas = self
                .stage_change_batch(schema, change_batch, &mut overlay, &mut batch)
                .await
                .with_context(|| {
                    format!(
                        "stage CDC change batch for table '{}'",
                        change_batch.table_id().as_str()
                    )
                })?;
            if !deltas.is_empty() {
                table_deltas.push(CdcTableDeltas {
                    table_id: change_batch.table_id().clone(),
                    deltas,
                });
            }
        }

        batch.put(
            checkpoint_key(transaction.source_id()),
            serde_json::to_vec(&next_checkpoint).context("encode CDC checkpoint")?,
        );
        self.table
            .write_batch(batch)
            .await
            .context("commit CDC transaction batch")?;

        Ok(CdcApplyResult {
            checkpoint: next_checkpoint,
            table_deltas,
            already_committed: false,
        })
    }

    async fn stage_change_batch(
        &self,
        schema: &CdcTableSchema,
        change_batch: &ChangeBatch,
        overlay: &mut HashMap<Vec<u8>, Option<CdcRow>>,
        batch: &mut WriteBatch,
    ) -> Result<Vec<CdcRowDelta>> {
        let mut deltas = Vec::new();
        for change in change_batch.changes() {
            match change {
                CdcChange::Insert { row } => {
                    let key = schema.primary_key_from_row(row)?;
                    let storage_key = row_key_bytes(schema.table_id(), &key)?;
                    stage_put_row(batch, overlay, storage_key, row.clone())?;
                    deltas.push(CdcRowDelta::insert(row.clone()));
                }
                CdcChange::Update { key, before, after } => {
                    let before_key = key_for_update_lookup(schema, key.as_ref(), before, after)?;
                    let before_storage_key = row_key_bytes(schema.table_id(), &before_key)?;
                    let old_row = match before {
                        Some(row) => row.clone(),
                        None => self
                            .load_row_with_overlay(&before_storage_key, overlay)
                            .await?
                            .ok_or_else(|| {
                                anyhow!(
                                    "CDC update for table '{}' could not find previous row",
                                    schema.table_id().as_str()
                                )
                            })?,
                    };
                    deltas.push(CdcRowDelta::delete(old_row));

                    let after_key = schema.primary_key_from_row(after)?;
                    let after_storage_key = row_key_bytes(schema.table_id(), &after_key)?;
                    if before_storage_key != after_storage_key {
                        stage_delete_row(batch, overlay, before_storage_key);
                    }
                    stage_put_row(batch, overlay, after_storage_key, after.clone())?;
                    deltas.push(CdcRowDelta::insert(after.clone()));
                }
                CdcChange::Delete { key, before } => {
                    let delete_key = key_for_delete_lookup(schema, key.as_ref(), before)?;
                    let storage_key = row_key_bytes(schema.table_id(), &delete_key)?;
                    let old_row = match before {
                        Some(row) => row.clone(),
                        None => self
                            .load_row_with_overlay(&storage_key, overlay)
                            .await?
                            .ok_or_else(|| {
                                anyhow!(
                                    "CDC delete for table '{}' could not find previous row",
                                    schema.table_id().as_str()
                                )
                            })?,
                    };
                    stage_delete_row(batch, overlay, storage_key);
                    deltas.push(CdcRowDelta::delete(old_row));
                }
                CdcChange::Truncate => {
                    bail!(
                        "CDC truncate for table '{}' is not supported yet",
                        schema.table_id().as_str()
                    );
                }
            }
        }
        Ok(deltas)
    }

    async fn load_row_with_overlay(
        &self,
        storage_key: &[u8],
        overlay: &HashMap<Vec<u8>, Option<CdcRow>>,
    ) -> Result<Option<CdcRow>> {
        if let Some(row) = overlay.get(storage_key) {
            return Ok(row.clone());
        }
        self.load_row_by_storage_key(storage_key).await
    }

    async fn load_row_by_storage_key(&self, storage_key: &[u8]) -> Result<Option<CdcRow>> {
        let Some(bytes) = self
            .table
            .get(storage_key)
            .await
            .context("load CDC row state")?
        else {
            return Ok(None);
        };
        decode_json(&bytes, "CDC row state")
    }
}

fn key_for_update_lookup(
    schema: &CdcTableSchema,
    explicit_key: Option<&CdcRowKey>,
    before: &Option<CdcRow>,
    after: &CdcRow,
) -> Result<CdcRowKey> {
    if let Some(key) = explicit_key {
        return Ok(key.clone());
    }
    if let Some(before) = before {
        return schema.primary_key_from_row(before);
    }
    schema.primary_key_from_row(after)
}

fn key_for_delete_lookup(
    schema: &CdcTableSchema,
    explicit_key: Option<&CdcRowKey>,
    before: &Option<CdcRow>,
) -> Result<CdcRowKey> {
    if let Some(key) = explicit_key {
        return Ok(key.clone());
    }
    let Some(before) = before else {
        bail!("CDC delete requires a key or before row");
    };
    schema.primary_key_from_row(before)
}

fn stage_put_row(
    batch: &mut WriteBatch,
    overlay: &mut HashMap<Vec<u8>, Option<CdcRow>>,
    storage_key: Vec<u8>,
    row: CdcRow,
) -> Result<()> {
    batch.put(
        storage_key.clone(),
        serde_json::to_vec(&row).context("encode CDC row state")?,
    );
    overlay.insert(storage_key, Some(row));
    Ok(())
}

fn stage_delete_row(
    batch: &mut WriteBatch,
    overlay: &mut HashMap<Vec<u8>, Option<CdcRow>>,
    storage_key: Vec<u8>,
) {
    batch.delete(storage_key.clone());
    overlay.insert(storage_key, None);
}

fn source_metadata_prefix() -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"meta/source/");
    key
}

fn source_metadata_key(source_id: &CdcSourceId) -> Vec<u8> {
    let mut key = source_metadata_prefix();
    push_component(&mut key, source_id.as_str().as_bytes());
    key
}

fn table_metadata_key(table_id: &CdcTableId) -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"meta/table/");
    push_component(&mut key, table_id.as_str().as_bytes());
    key
}

fn source_table_index_prefix(source_id: &CdcSourceId) -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"meta/source_table/");
    push_component(&mut key, source_id.as_str().as_bytes());
    key
}

fn source_table_index_key(source_id: &CdcSourceId, table_id: &CdcTableId) -> Vec<u8> {
    let mut key = source_table_index_prefix(source_id);
    push_component(&mut key, table_id.as_str().as_bytes());
    key
}

fn checkpoint_key(source_id: &CdcSourceId) -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"checkpoint/");
    push_component(&mut key, source_id.as_str().as_bytes());
    key
}

fn row_key_bytes(table_id: &CdcTableId, row_key: &CdcRowKey) -> Result<Vec<u8>> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"row/");
    push_component(&mut key, table_id.as_str().as_bytes());
    push_component(
        &mut key,
        &serde_json::to_vec(row_key).context("encode CDC row key")?,
    );
    Ok(key)
}

fn push_component(out: &mut Vec<u8>, component: &[u8]) {
    let len = u32::try_from(component.len()).expect("CDC key component length exceeds u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(component);
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8], label: &str) -> Result<Option<T>> {
    serde_json::from_slice(bytes)
        .with_context(|| format!("decode {label} from JSON"))
        .map(Some)
}

fn decode_json_value<T: for<'de> Deserialize<'de>>(bytes: &[u8], label: &str) -> Result<T> {
    serde_json::from_slice(bytes).with_context(|| format!("decode {label} from JSON"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp_storage::storage::SlateTable;
    use floe_cdc_core::{
        CdcColumn, CdcPrimaryKey, CdcSourceDefinition, CdcSourcePosition, CdcTableDefinition,
        CdcTransactionId, UpstreamTableRef,
    };
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;
    use object_store::memory::InMemory;
    use slatedb::Db;

    async fn test_table(name: &str) -> Arc<dyn KeyValueTable> {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, object_store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    async fn test_store(name: &str) -> CdcTableStore {
        CdcTableStore::new(test_table(name).await)
    }

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
                CdcColumn::new("amount", ColumnType::Int64, true).expect("amount"),
                CdcColumn::new("status", ColumnType::Utf8, true).expect("status"),
            ],
            CdcPrimaryKey::new(["id"]).expect("primary key"),
        )
        .expect("schema")
    }

    fn schemas(schema: CdcTableSchema) -> HashMap<CdcTableId, CdcTableSchema> {
        HashMap::from([(schema.table_id().clone(), schema)])
    }

    fn row(id: i64, amount: Option<i64>, status: Option<&str>) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(id)),
            amount.map(RowValue::Int64),
            status.map(|value| RowValue::Utf8(value.to_string())),
        ])
        .expect("row")
    }

    fn key(id: i64) -> CdcRowKey {
        CdcRowKey::new([RowValue::Int64(id)]).expect("row key")
    }

    fn tx(position: &str, batches: Vec<ChangeBatch>) -> TransactionBatch {
        TransactionBatch::new(
            CdcSourceId::new("pg_main").expect("source id"),
            Some(CdcTransactionId::new(format!("tx-{position}")).expect("txid")),
            None,
            CdcSourcePosition::postgres(position, None).expect("position"),
            batches,
        )
        .expect("transaction")
    }

    #[tokio::test]
    async fn metadata_round_trips_sources_tables_and_checkpoints() {
        let table = test_table("cdc-metadata-round-trip").await;
        let metadata = CdcMetadataStore::new(Arc::clone(&table));
        let apply_store = CdcTableStore::new(Arc::clone(&table));
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let source = CdcSourceDefinition::postgres(source_id.clone())
            .expect("source")
            .with_property("slot.name", "floe_slot")
            .expect("slot property")
            .with_property("publication.name", "floe_publication")
            .expect("publication property");
        metadata
            .upsert_source(&source)
            .await
            .expect("persist source");

        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let table_definition = CdcTableDefinition::new(source_id.clone(), schema.clone());
        metadata
            .upsert_table(&table_definition)
            .await
            .expect("persist table");

        let transaction = tx(
            "0/50",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(50, Some(5000), Some("open")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let checkpoint = apply_store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply transaction")
            .checkpoint()
            .clone();

        let reloaded_metadata = CdcMetadataStore::new(Arc::clone(&table));
        let reloaded_apply_store = CdcTableStore::new(table);
        assert_eq!(
            reloaded_metadata
                .load_source(&source_id)
                .await
                .expect("load source"),
            Some(source.clone())
        );
        assert_eq!(
            reloaded_metadata.sources().await.expect("load sources"),
            vec![source]
        );
        assert_eq!(
            reloaded_metadata
                .load_table(&table_id)
                .await
                .expect("load table"),
            Some(table_definition.clone())
        );
        assert_eq!(
            reloaded_metadata
                .tables_for_source(&source_id)
                .await
                .expect("load source tables"),
            vec![table_definition]
        );
        assert_eq!(
            reloaded_apply_store
                .load_checkpoint(&source_id)
                .await
                .expect("load checkpoint"),
            Some(checkpoint)
        );
    }

    #[tokio::test]
    async fn table_metadata_rejects_missing_source_and_moves_source_index() {
        let table = test_table("cdc-metadata-index").await;
        let metadata = CdcMetadataStore::new(table);
        let pg_main = CdcSourceId::new("pg_main").expect("source id");
        let pg_other = CdcSourceId::new("pg_other").expect("source id");
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let table_definition = CdcTableDefinition::new(pg_main.clone(), schema.clone());

        let err = metadata
            .upsert_table(&table_definition)
            .await
            .expect_err("table should require source metadata first");
        assert!(format!("{err:#}").contains("does not exist"));

        metadata
            .upsert_source(&CdcSourceDefinition::postgres(pg_main.clone()).expect("source"))
            .await
            .expect("persist main source");
        metadata
            .upsert_source(&CdcSourceDefinition::postgres(pg_other.clone()).expect("source"))
            .await
            .expect("persist other source");
        metadata
            .upsert_table(&table_definition)
            .await
            .expect("persist table on main source");
        assert_eq!(
            metadata
                .tables_for_source(&pg_main)
                .await
                .expect("main tables")
                .len(),
            1
        );

        let moved = CdcTableDefinition::new(pg_other.clone(), schema);
        metadata
            .upsert_table(&moved)
            .await
            .expect("move table to other source");
        assert!(
            metadata
                .tables_for_source(&pg_main)
                .await
                .expect("main tables")
                .is_empty()
        );
        assert_eq!(
            metadata
                .tables_for_source(&pg_other)
                .await
                .expect("other tables"),
            vec![moved.clone()]
        );
        assert_eq!(
            metadata.load_table(&table_id).await.expect("load table"),
            Some(moved)
        );
    }

    #[tokio::test]
    async fn applies_insert_update_and_delete_with_atomic_checkpoint() {
        let store = test_store("cdc-apply-insert-update-delete").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let insert_row = row(1, Some(100), Some("open"));
        let update_row = row(1, Some(150), Some("paid"));

        let insert = tx(
            "0/1",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: insert_row.clone(),
                    }],
                )
                .expect("insert batch"),
            ],
        );
        let result = store
            .apply_transaction(&schemas(schema.clone()), &insert)
            .await
            .expect("apply insert");
        assert!(!result.already_committed());
        assert_eq!(result.table_deltas()[0].deltas()[0].diff(), 1);
        assert_eq!(
            store.load_row(&table_id, &key(1)).await.expect("load row"),
            Some(insert_row.clone())
        );
        assert_eq!(
            store
                .load_checkpoint(insert.source_id())
                .await
                .expect("load checkpoint"),
            Some(result.checkpoint().clone())
        );

        let update = tx(
            "0/2",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Update {
                        key: Some(key(1)),
                        before: None,
                        after: update_row.clone(),
                    }],
                )
                .expect("update batch"),
            ],
        );
        let result = store
            .apply_transaction(&schemas(schema.clone()), &update)
            .await
            .expect("apply update");
        assert_eq!(result.table_deltas()[0].deltas().len(), 2);
        assert_eq!(result.table_deltas()[0].deltas()[0].diff(), -1);
        assert_eq!(result.table_deltas()[0].deltas()[1].diff(), 1);
        assert_eq!(
            store
                .load_row(&table_id, &key(1))
                .await
                .expect("load updated row"),
            Some(update_row)
        );

        let delete = tx(
            "0/3",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Delete {
                        key: Some(key(1)),
                        before: None,
                    }],
                )
                .expect("delete batch"),
            ],
        );
        let result = store
            .apply_transaction(&schemas(schema), &delete)
            .await
            .expect("apply delete");
        assert_eq!(result.table_deltas()[0].deltas()[0].diff(), -1);
        assert_eq!(
            store
                .load_row(&table_id, &key(1))
                .await
                .expect("load deleted row"),
            None
        );
    }

    #[tokio::test]
    async fn overlay_handles_multiple_changes_for_same_key_in_one_transaction() {
        let store = test_store("cdc-apply-overlay").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/10",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![
                        CdcChange::Insert {
                            row: row(5, Some(10), Some("open")),
                        },
                        CdcChange::Update {
                            key: Some(key(5)),
                            before: None,
                            after: row(5, Some(20), Some("paid")),
                        },
                        CdcChange::Delete {
                            key: Some(key(5)),
                            before: None,
                        },
                    ],
                )
                .expect("batch"),
            ],
        );

        let result = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply transaction");
        let diffs: Vec<i64> = result.table_deltas()[0]
            .deltas()
            .iter()
            .map(CdcRowDelta::diff)
            .collect();
        assert_eq!(diffs, vec![1, -1, 1, -1]);
        assert_eq!(
            store.load_row(&table_id, &key(5)).await.expect("load row"),
            None
        );
    }

    #[tokio::test]
    async fn exact_checkpoint_reapply_is_idempotent() {
        let store = test_store("cdc-apply-idempotent").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/20",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(9, Some(90), Some("open")),
                    }],
                )
                .expect("batch"),
            ],
        );

        store
            .apply_transaction(&schemas(schema.clone()), &transaction)
            .await
            .expect("first apply");
        let replay = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("reapply");
        assert!(replay.already_committed());
        assert!(replay.table_deltas().is_empty());
        assert_eq!(
            store.load_row(&table_id, &key(9)).await.expect("load row"),
            Some(row(9, Some(90), Some("open")))
        );
    }

    #[tokio::test]
    async fn fresh_store_reloads_checkpoint_and_rows_from_slate_table() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("cdc-apply-reload", object_store)
                .await
                .expect("open SlateDB"),
        );
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        let store = CdcTableStore::new(Arc::clone(&table));
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/25",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(10, Some(1000), Some("open")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let checkpoint = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply")
            .checkpoint()
            .clone();

        let reloaded = CdcTableStore::new(table);
        assert_eq!(
            reloaded
                .load_checkpoint(transaction.source_id())
                .await
                .expect("load checkpoint"),
            Some(checkpoint)
        );
        assert_eq!(
            reloaded
                .load_row(&table_id, &key(10))
                .await
                .expect("load row"),
            Some(row(10, Some(1000), Some("open")))
        );
    }

    #[tokio::test]
    async fn missing_previous_row_for_key_only_delete_is_rejected() {
        let store = test_store("cdc-apply-missing-delete").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/30",
            vec![
                ChangeBatch::new(
                    table_id,
                    vec![CdcChange::Delete {
                        key: Some(key(404)),
                        before: None,
                    }],
                )
                .expect("batch"),
            ],
        );

        let err = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect_err("delete should fail");
        assert!(format!("{err:#}").contains("could not find previous row"));
    }

    #[tokio::test]
    async fn truncate_is_rejected_without_mutating_checkpoint() {
        let store = test_store("cdc-apply-truncate").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/40",
            vec![ChangeBatch::new(table_id, vec![CdcChange::Truncate]).expect("batch")],
        );

        let err = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect_err("truncate should fail");
        assert!(format!("{err:#}").contains("truncate"));
        assert_eq!(
            store
                .load_checkpoint(transaction.source_id())
                .await
                .expect("load checkpoint"),
            None
        );
    }
}
