use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use dbsp_storage::storage::KeyValueTable;
use floe_cdc_core::{
    CdcChange, CdcCheckpoint, CdcColumnarColumn, CdcColumnarRowBatch, CdcRow, CdcRowKey,
    CdcSourceDefinition, CdcSourceId, CdcTableDefinition, CdcTableId, CdcTableSchema, ChangeBatch,
    TransactionBatch,
};
use floe_core::RowValue;
use serde::Deserialize;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

const CDC_PREFIX: &[u8] = b"floe_cdc/v1/";
const CDC_ROW_STATE_MAGIC: &[u8; 8] = b"FCDCRW1\0";
const CDC_ROW_VALUE_NULL: u8 = 0;
const CDC_ROW_VALUE_INT64: u8 = 1;
const CDC_ROW_VALUE_BOOL: u8 = 2;
const CDC_ROW_VALUE_UTF8: u8 = 3;
const CDC_ROW_VALUE_TIMESTAMP_MILLIS: u8 = 4;

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
    payload: CdcTableDeltaPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CdcTableDeltaPayload {
    RowDeltas(Vec<CdcRowDelta>),
    SnapshotInserts(CdcColumnarRowBatch),
}

impl CdcTableDeltas {
    pub fn new(table_id: CdcTableId, deltas: Vec<CdcRowDelta>) -> Self {
        Self {
            table_id,
            payload: CdcTableDeltaPayload::RowDeltas(deltas),
        }
    }

    pub fn snapshot_insert(table_id: CdcTableId, rows: CdcColumnarRowBatch) -> Self {
        Self {
            table_id,
            payload: CdcTableDeltaPayload::SnapshotInserts(rows),
        }
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn deltas(&self) -> &[CdcRowDelta] {
        match &self.payload {
            CdcTableDeltaPayload::RowDeltas(deltas) => deltas,
            CdcTableDeltaPayload::SnapshotInserts(_) => &[],
        }
    }

    pub fn snapshot_insert_rows(&self) -> Option<&CdcColumnarRowBatch> {
        match &self.payload {
            CdcTableDeltaPayload::RowDeltas(_) => None,
            CdcTableDeltaPayload::SnapshotInserts(rows) => Some(rows),
        }
    }

    pub fn row_count(&self) -> usize {
        match &self.payload {
            CdcTableDeltaPayload::RowDeltas(deltas) => deltas.len(),
            CdcTableDeltaPayload::SnapshotInserts(rows) => rows.row_count(),
        }
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

    pub async fn commit_checkpoint(&self, checkpoint: &CdcCheckpoint) -> Result<()> {
        let mut batch = WriteBatch::new();
        stage_checkpoint(checkpoint, &mut batch)?;
        self.table.write_batch(batch).await.with_context(|| {
            format!(
                "commit CDC checkpoint for source '{}'",
                checkpoint.source_id().as_str()
            )
        })
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
        let mut batch = WriteBatch::new();
        let result = self
            .stage_transaction(schemas, transaction, &mut batch)
            .await?;
        if !result.already_committed {
            self.table
                .write_batch(batch)
                .await
                .context("commit CDC transaction batch")?;
        }
        Ok(result)
    }

    pub async fn stage_transaction(
        &self,
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
        transaction: &TransactionBatch,
        batch: &mut WriteBatch,
    ) -> Result<CdcApplyResult> {
        transaction.validate_against_schemas(schemas)?;
        let next_checkpoint = CdcCheckpoint::from_transaction(transaction);
        if let Some(current_checkpoint) = self.load_checkpoint(transaction.source_id()).await?
            && current_checkpoint.covers(&next_checkpoint)?
        {
            return Ok(CdcApplyResult {
                checkpoint: current_checkpoint,
                table_deltas: Vec::new(),
                already_committed: true,
            });
        }

        let mut overlay = HashMap::<Vec<u8>, Option<CdcRow>>::new();
        let mut table_deltas = Vec::new();

        for change_batch in transaction.change_batches() {
            let schema = schemas.get(change_batch.table_id()).ok_or_else(|| {
                anyhow!("missing schema for '{}'", change_batch.table_id().as_str())
            })?;
            let table_delta = self
                .stage_change_batch(schema, change_batch, &mut overlay, batch)
                .await
                .with_context(|| {
                    format!(
                        "stage CDC change batch for table '{}'",
                        change_batch.table_id().as_str()
                    )
                })?;
            if let Some(table_delta) = table_delta {
                table_deltas.push(table_delta);
            }
        }

        stage_checkpoint(&next_checkpoint, batch)?;

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
    ) -> Result<Option<CdcTableDeltas>> {
        if let Some(rows) = change_batch.snapshot_insert_rows() {
            self.stage_snapshot_insert_batch(schema, rows, batch)?;
            return Ok(Some(CdcTableDeltas::snapshot_insert(
                schema.table_id().clone(),
                rows.clone(),
            )));
        }

        let mut deltas = Vec::new();
        let prefetched_rows = self
            .prefetch_old_rows(schema, change_batch, overlay)
            .await?;
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
                        None => row_with_overlay(&before_storage_key, overlay, &prefetched_rows)
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
                        None => row_with_overlay(&storage_key, overlay, &prefetched_rows)
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
        if deltas.is_empty() {
            Ok(None)
        } else {
            Ok(Some(CdcTableDeltas::new(
                change_batch.table_id().clone(),
                deltas,
            )))
        }
    }

    fn stage_snapshot_insert_batch(
        &self,
        schema: &CdcTableSchema,
        rows: &CdcColumnarRowBatch,
        batch: &mut WriteBatch,
    ) -> Result<()> {
        schema.validate_columnar_rows(rows)?;
        for row_idx in 0..rows.row_count() {
            let key = schema.primary_key_from_columnar_row(rows, row_idx)?;
            let storage_key = row_key_bytes(schema.table_id(), &key)?;
            batch.put(storage_key, encode_cdc_columnar_row_state(rows, row_idx)?);
        }
        Ok(())
    }

    async fn prefetch_old_rows(
        &self,
        schema: &CdcTableSchema,
        change_batch: &ChangeBatch,
        overlay: &HashMap<Vec<u8>, Option<CdcRow>>,
    ) -> Result<HashMap<Vec<u8>, Option<CdcRow>>> {
        let mut storage_keys = Vec::new();
        let mut seen = HashSet::new();
        for change in change_batch.changes() {
            let storage_key = match change {
                CdcChange::Update { key, before, after } if before.is_none() => {
                    let before_key = key_for_update_lookup(schema, key.as_ref(), before, after)?;
                    row_key_bytes(schema.table_id(), &before_key)?
                }
                CdcChange::Delete { key, before } if before.is_none() => {
                    let delete_key = key_for_delete_lookup(schema, key.as_ref(), before)?;
                    row_key_bytes(schema.table_id(), &delete_key)?
                }
                _ => continue,
            };
            if overlay.contains_key(&storage_key) {
                continue;
            }
            if seen.insert(storage_key.clone()) {
                storage_keys.push(storage_key);
            }
        }
        self.load_rows_by_storage_key(&storage_keys).await
    }

    async fn load_rows_by_storage_key(
        &self,
        storage_keys: &[Vec<u8>],
    ) -> Result<HashMap<Vec<u8>, Option<CdcRow>>> {
        let mut rows = HashMap::with_capacity(storage_keys.len());
        for storage_key in storage_keys {
            rows.insert(
                storage_key.clone(),
                self.load_row_by_storage_key(storage_key).await?,
            );
        }
        Ok(rows)
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
        decode_cdc_row_state(&bytes).map(Some)
    }
}

fn row_with_overlay(
    storage_key: &[u8],
    overlay: &HashMap<Vec<u8>, Option<CdcRow>>,
    prefetched_rows: &HashMap<Vec<u8>, Option<CdcRow>>,
) -> Option<CdcRow> {
    if let Some(row) = overlay.get(storage_key) {
        return row.clone();
    }
    prefetched_rows.get(storage_key).cloned().flatten()
}

fn stage_checkpoint(checkpoint: &CdcCheckpoint, batch: &mut WriteBatch) -> Result<()> {
    batch.put(
        checkpoint_key(checkpoint.source_id()),
        serde_json::to_vec(checkpoint).context("encode CDC checkpoint")?,
    );
    Ok(())
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
    batch.put(storage_key.clone(), encode_cdc_row_state(&row)?);
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

fn encode_cdc_row_state(row: &CdcRow) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(CDC_ROW_STATE_MAGIC.len() + 4 + row.values().len() * 9);
    out.extend_from_slice(CDC_ROW_STATE_MAGIC);
    push_u32(&mut out, row.values().len(), "CDC row value count")?;
    for value in row.values() {
        encode_cdc_row_value(&mut out, value.as_ref())?;
    }
    Ok(out)
}

fn encode_cdc_columnar_row_state(rows: &CdcColumnarRowBatch, row_idx: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(CDC_ROW_STATE_MAGIC.len() + 4 + rows.columns().len() * 9);
    out.extend_from_slice(CDC_ROW_STATE_MAGIC);
    push_u32(&mut out, rows.columns().len(), "CDC row value count")?;
    for column in rows.columns() {
        encode_cdc_columnar_row_value(&mut out, column, row_idx)?;
    }
    Ok(out)
}

fn encode_cdc_row_value(out: &mut Vec<u8>, value: Option<&RowValue>) -> Result<()> {
    match value {
        None => out.push(CDC_ROW_VALUE_NULL),
        Some(RowValue::Int64(value)) => {
            out.push(CDC_ROW_VALUE_INT64);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Some(RowValue::Bool(value)) => {
            out.push(CDC_ROW_VALUE_BOOL);
            out.push(u8::from(*value));
        }
        Some(RowValue::Utf8(value)) => {
            out.push(CDC_ROW_VALUE_UTF8);
            push_u32(out, value.len(), "CDC UTF-8 value length")?;
            out.extend_from_slice(value.as_bytes());
        }
        Some(RowValue::TimestampMillis(value)) => {
            out.push(CDC_ROW_VALUE_TIMESTAMP_MILLIS);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    Ok(())
}

fn encode_cdc_columnar_row_value(
    out: &mut Vec<u8>,
    column: &CdcColumnarColumn,
    row_idx: usize,
) -> Result<()> {
    match column {
        CdcColumnarColumn::Int64(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_INT64);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::Bool(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_BOOL);
                out.push(u8::from(*value));
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::Utf8(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_UTF8);
                push_u32(out, value.len(), "CDC UTF-8 value length")?;
                out.extend_from_slice(value.as_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::TimestampMillis(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_TIMESTAMP_MILLIS);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
    }
    Ok(())
}

fn decode_cdc_row_state(bytes: &[u8]) -> Result<CdcRow> {
    if !bytes.starts_with(CDC_ROW_STATE_MAGIC) {
        return decode_json_value(bytes, "legacy CDC row state");
    }
    let mut cursor = CdcRowStateCursor::new(&bytes[CDC_ROW_STATE_MAGIC.len()..]);
    let value_count = cursor.read_u32()? as usize;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        values.push(cursor.read_value()?);
    }
    if !cursor.is_empty() {
        bail!(
            "CDC row state has {} trailing bytes",
            cursor.remaining_len()
        );
    }
    CdcRow::new(values)
}

fn push_u32(out: &mut Vec<u8>, value: usize, label: &str) -> Result<()> {
    let value = u32::try_from(value).with_context(|| format!("{label} exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

struct CdcRowStateCursor<'a> {
    bytes: &'a [u8],
}

impl<'a> CdcRowStateCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len()
    }

    fn read_value(&mut self) -> Result<Option<RowValue>> {
        let tag = self.read_u8()?;
        match tag {
            CDC_ROW_VALUE_NULL => Ok(None),
            CDC_ROW_VALUE_INT64 => Ok(Some(RowValue::Int64(self.read_i64()?))),
            CDC_ROW_VALUE_BOOL => match self.read_u8()? {
                0 => Ok(Some(RowValue::Bool(false))),
                1 => Ok(Some(RowValue::Bool(true))),
                other => bail!("invalid CDC bool value byte {other}"),
            },
            CDC_ROW_VALUE_UTF8 => {
                let len = self.read_u32()? as usize;
                let bytes = self.take(len)?;
                let value = std::str::from_utf8(bytes)
                    .context("decode CDC UTF-8 row value")?
                    .to_string();
                Ok(Some(RowValue::Utf8(value)))
            }
            CDC_ROW_VALUE_TIMESTAMP_MILLIS => Ok(Some(RowValue::TimestampMillis(self.read_i64()?))),
            other => bail!("unknown CDC row value tag {other}"),
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.bytes.len() < len {
            bail!(
                "CDC row state ended early: needed {len} bytes, had {}",
                self.bytes.len()
            );
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }
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

    #[test]
    fn binary_row_state_codec_round_trips_all_value_types() {
        let source = CdcRow::new([
            Some(RowValue::Int64(7)),
            Some(RowValue::Bool(true)),
            Some(RowValue::Utf8("paid".to_string())),
            Some(RowValue::TimestampMillis(1_700_000_000_000)),
            None,
        ])
        .expect("row");

        let encoded = encode_cdc_row_state(&source).expect("encode row state");
        assert!(encoded.starts_with(CDC_ROW_STATE_MAGIC));
        assert_eq!(
            decode_cdc_row_state(&encoded).expect("decode row state"),
            source
        );
    }

    #[tokio::test]
    async fn applies_columnar_snapshot_insert_batch() {
        let store = test_store("cdc-table-columnar-snapshot").await;
        let schema = orders_schema();
        let rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(1), Some(2)]),
            CdcColumnarColumn::Int64(vec![Some(10), None]),
            CdcColumnarColumn::Utf8(vec![Some("open".to_string()), Some("closed".to_string())]),
        ])
        .expect("columnar rows");
        let transaction = tx(
            "0/10",
            vec![
                ChangeBatch::new_snapshot_insert(schema.table_id().clone(), rows)
                    .expect("snapshot batch"),
            ],
        );

        let apply_result = store
            .apply_transaction(&schemas(schema.clone()), &transaction)
            .await
            .expect("apply columnar snapshot");
        assert_eq!(apply_result.table_deltas().len(), 1);
        assert_eq!(apply_result.table_deltas()[0].row_count(), 2);
        assert_eq!(
            apply_result.table_deltas()[0]
                .snapshot_insert_rows()
                .expect("snapshot rows")
                .row_count(),
            2
        );
        assert!(apply_result.table_deltas()[0].deltas().is_empty());

        assert_eq!(
            store
                .load_row(schema.table_id(), &key(1))
                .await
                .expect("load first row")
                .expect("first row exists")
                .values(),
            &[
                Some(RowValue::Int64(1)),
                Some(RowValue::Int64(10)),
                Some(RowValue::Utf8("open".to_string()))
            ]
        );
        assert_eq!(
            store
                .load_row(schema.table_id(), &key(2))
                .await
                .expect("load second row")
                .expect("second row exists")
                .values(),
            &[
                Some(RowValue::Int64(2)),
                None,
                Some(RowValue::Utf8("closed".to_string()))
            ]
        );
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
    async fn explicit_checkpoint_commit_round_trips_without_rows() {
        let store = test_store("cdc-explicit-checkpoint").await;
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let checkpoint = CdcCheckpoint::new(
            source_id.clone(),
            CdcSourcePosition::postgres("0/70", None).expect("position"),
            Some(CdcTransactionId::new("snapshot-0-70").expect("transaction id")),
        );

        store
            .commit_checkpoint(&checkpoint)
            .await
            .expect("commit checkpoint");

        assert_eq!(
            store
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
    async fn row_state_uses_binary_codec_and_reads_legacy_json() {
        let table = test_table("cdc-row-state-binary").await;
        let store = CdcTableStore::new(Arc::clone(&table));
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let binary_row = row(1, Some(100), Some("open"));
        let transaction = tx(
            "0/5",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: binary_row.clone(),
                    }],
                )
                .expect("batch"),
            ],
        );
        store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply binary row");

        let binary_key = row_key_bytes(&table_id, &key(1)).expect("row key");
        let binary_bytes = table
            .get(&binary_key)
            .await
            .expect("load raw binary row")
            .expect("binary row should exist");
        assert!(binary_bytes.starts_with(CDC_ROW_STATE_MAGIC));
        assert_ne!(binary_bytes.first(), Some(&b'{'));
        assert_eq!(
            store.load_row(&table_id, &key(1)).await.expect("load row"),
            Some(binary_row)
        );

        let legacy_row = row(88, None, Some("legacy"));
        let legacy_key = row_key_bytes(&table_id, &key(88)).expect("legacy row key");
        let mut batch = WriteBatch::new();
        batch.put(
            legacy_key,
            serde_json::to_vec(&legacy_row).expect("legacy JSON row"),
        );
        table.write_batch(batch).await.expect("write legacy row");
        assert_eq!(
            store
                .load_row(&table_id, &key(88))
                .await
                .expect("load legacy row"),
            Some(legacy_row)
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
    async fn stale_checkpoint_replay_is_ignored_without_rewinding_state() {
        let store = test_store("cdc-apply-stale-replay").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let newer = tx(
            "0/20",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(20, Some(200), Some("newer")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let newer_checkpoint = store
            .apply_transaction(&schemas(schema.clone()), &newer)
            .await
            .expect("apply newer")
            .checkpoint()
            .clone();

        let stale = tx(
            "0/10",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(10, Some(100), Some("stale")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let replay = store
            .apply_transaction(&schemas(schema), &stale)
            .await
            .expect("ignore stale replay");
        assert!(replay.already_committed());
        assert_eq!(replay.checkpoint(), &newer_checkpoint);
        assert!(replay.table_deltas().is_empty());
        assert_eq!(
            store.load_row(&table_id, &key(10)).await.expect("load row"),
            None
        );
        assert_eq!(
            store
                .load_checkpoint(stale.source_id())
                .await
                .expect("load checkpoint"),
            Some(newer_checkpoint)
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
