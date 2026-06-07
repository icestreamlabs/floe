use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use dbsp_storage::storage::KeyValueTable;
use floe_cdc_core::{
    CdcChange, CdcCheckpoint, CdcColumnarRowBatch, CdcRow, CdcRowKey, CdcSourceId, CdcTableId,
    CdcTableSchema, ChangeBatch, TransactionBatch,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use slatedb::WriteBatch;

use crate::codec::{decode_cdc_row_state, encode_cdc_columnar_row_state, encode_cdc_row_state};
use crate::deltas::{CdcApplyResult, CdcRowDelta, CdcTableDeltas};
use crate::json::decode_json;
use crate::keys::{checkpoint_key, row_key_bytes};

const CDC_OLD_ROW_PREFETCH_CONCURRENCY: usize = 64;

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

    pub async fn complete_unchanged_toast(
        &self,
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
        transaction: &TransactionBatch,
    ) -> Result<TransactionBatch> {
        transaction.validate_against_schemas(schemas)?;
        if !transaction_has_unchanged_toast(transaction) {
            return Ok(transaction.clone());
        }

        let mut overlay = HashMap::<Vec<u8>, Option<CdcRow>>::new();
        let mut change_batches = Vec::with_capacity(transaction.change_batches().len());
        for change_batch in transaction.change_batches() {
            let schema = schemas.get(change_batch.table_id()).ok_or_else(|| {
                anyhow!("missing schema for '{}'", change_batch.table_id().as_str())
            })?;
            change_batches.push(
                self.complete_unchanged_toast_in_change_batch(schema, change_batch, &mut overlay)
                    .await
                    .with_context(|| {
                        format!(
                            "complete unchanged TOAST values for table '{}'",
                            change_batch.table_id().as_str()
                        )
                    })?,
            );
        }

        Ok(TransactionBatch::new(
            transaction.source_id().clone(),
            transaction.transaction_id().cloned(),
            transaction.start_position().cloned(),
            transaction.commit_position().clone(),
            change_batches,
        )?
        .with_schema_versions(transaction.schema_versions().clone()))
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
                    let stored_old_row =
                        row_with_overlay(&before_storage_key, overlay, &prefetched_rows);
                    let old_row = match before {
                        Some(row) if row.has_unchanged_toast() => {
                            let previous = stored_old_row.as_ref().ok_or_else(|| {
                                anyhow!(
                                    "CDC update for table '{}' could not resolve unchanged TOAST because previous row was not found",
                                    schema.table_id().as_str()
                                )
                            })?;
                            row.resolve_unchanged_toast(previous)?
                        }
                        Some(row) => row.clone(),
                        None => stored_old_row.ok_or_else(|| {
                            anyhow!(
                                "CDC update for table '{}' could not find previous row",
                                schema.table_id().as_str()
                            )
                        })?,
                    };
                    let after = if after.has_unchanged_toast() {
                        after.resolve_unchanged_toast(&old_row)?
                    } else {
                        after.clone()
                    };
                    schema.validate_row(&old_row)?;
                    schema.validate_row(&after)?;
                    deltas.push(CdcRowDelta::delete(old_row));

                    let after_key = schema.primary_key_from_row(&after)?;
                    let after_storage_key = row_key_bytes(schema.table_id(), &after_key)?;
                    if before_storage_key != after_storage_key {
                        stage_delete_row(batch, overlay, before_storage_key);
                    }
                    stage_put_row(batch, overlay, after_storage_key, after.clone())?;
                    deltas.push(CdcRowDelta::insert(after));
                }
                CdcChange::Delete { key, before } => {
                    let delete_key = key_for_delete_lookup(schema, key.as_ref(), before)?;
                    let storage_key = row_key_bytes(schema.table_id(), &delete_key)?;
                    let stored_old_row = row_with_overlay(&storage_key, overlay, &prefetched_rows);
                    let old_row = match before {
                        Some(row) if row.has_unchanged_toast() => {
                            let previous = stored_old_row.as_ref().ok_or_else(|| {
                                anyhow!(
                                    "CDC delete for table '{}' could not resolve unchanged TOAST because previous row was not found",
                                    schema.table_id().as_str()
                                )
                            })?;
                            row.resolve_unchanged_toast(previous)?
                        }
                        Some(row) => row.clone(),
                        None => stored_old_row.ok_or_else(|| {
                            anyhow!(
                                "CDC delete for table '{}' could not find previous row",
                                schema.table_id().as_str()
                            )
                        })?,
                    };
                    schema.validate_row(&old_row)?;
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

    async fn complete_unchanged_toast_in_change_batch(
        &self,
        schema: &CdcTableSchema,
        change_batch: &ChangeBatch,
        overlay: &mut HashMap<Vec<u8>, Option<CdcRow>>,
    ) -> Result<ChangeBatch> {
        if let Some(rows) = change_batch.snapshot_insert_rows() {
            for row_idx in 0..rows.row_count() {
                let row = rows.row(row_idx)?;
                let key = schema.primary_key_from_row(&row)?;
                let storage_key = row_key_bytes(schema.table_id(), &key)?;
                overlay.insert(storage_key, Some(row));
            }
            return Ok(change_batch.clone());
        }

        let prefetched_rows = self
            .prefetch_old_rows(schema, change_batch, overlay)
            .await?;
        let mut changes = Vec::with_capacity(change_batch.changes().len());
        for change in change_batch.changes() {
            match change {
                CdcChange::Insert { row } => {
                    ensure_no_unresolved_toast("insert", schema.table_id(), row)?;
                    let key = schema.primary_key_from_row(row)?;
                    let storage_key = row_key_bytes(schema.table_id(), &key)?;
                    overlay.insert(storage_key, Some(row.clone()));
                    changes.push(change.clone());
                }
                CdcChange::Update { key, before, after } => {
                    let before_key = key_for_update_lookup(schema, key.as_ref(), before, after)?;
                    let before_storage_key = row_key_bytes(schema.table_id(), &before_key)?;
                    let stored_old_row =
                        row_with_overlay(&before_storage_key, overlay, &prefetched_rows);
                    let before = match before {
                        Some(row) if row.has_unchanged_toast() => {
                            let previous = stored_old_row.as_ref().ok_or_else(|| {
                                anyhow!(
                                    "CDC update for table '{}' could not resolve unchanged TOAST because previous row was not found",
                                    schema.table_id().as_str()
                                )
                            })?;
                            Some(row.resolve_unchanged_toast(previous)?)
                        }
                        Some(row) => Some(row.clone()),
                        None => None,
                    };
                    let base_row =
                        before.as_ref().or(stored_old_row.as_ref()).ok_or_else(|| {
                            anyhow!(
                                "CDC update for table '{}' could not find previous row",
                                schema.table_id().as_str()
                            )
                        })?;
                    let after = if after.has_unchanged_toast() {
                        after.resolve_unchanged_toast(base_row)?
                    } else {
                        after.clone()
                    };
                    schema.validate_row(&after)?;
                    let after_key = schema.primary_key_from_row(&after)?;
                    if before_storage_key != row_key_bytes(schema.table_id(), &after_key)? {
                        overlay.insert(before_storage_key, None);
                    }
                    let after_storage_key = row_key_bytes(schema.table_id(), &after_key)?;
                    overlay.insert(after_storage_key, Some(after.clone()));
                    changes.push(CdcChange::Update {
                        key: key.clone(),
                        before,
                        after,
                    });
                }
                CdcChange::Delete { key, before } => {
                    let delete_key = key_for_delete_lookup(schema, key.as_ref(), before)?;
                    let storage_key = row_key_bytes(schema.table_id(), &delete_key)?;
                    let stored_old_row = row_with_overlay(&storage_key, overlay, &prefetched_rows);
                    let before = match before {
                        Some(row) if row.has_unchanged_toast() => {
                            let previous = stored_old_row.as_ref().ok_or_else(|| {
                                anyhow!(
                                    "CDC delete for table '{}' could not resolve unchanged TOAST because previous row was not found",
                                    schema.table_id().as_str()
                                )
                            })?;
                            Some(row.resolve_unchanged_toast(previous)?)
                        }
                        Some(row) => Some(row.clone()),
                        None => None,
                    };
                    overlay.insert(storage_key, None);
                    changes.push(CdcChange::Delete {
                        key: key.clone(),
                        before,
                    });
                }
                CdcChange::Truncate => changes.push(CdcChange::Truncate),
            }
        }

        ChangeBatch::new(change_batch.table_id().clone(), changes)
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
                CdcChange::Update { key, before, after }
                    if before.is_none()
                        || before.as_ref().is_some_and(CdcRow::has_unchanged_toast)
                        || after.has_unchanged_toast() =>
                {
                    let before_key = key_for_update_lookup(schema, key.as_ref(), before, after)?;
                    row_key_bytes(schema.table_id(), &before_key)?
                }
                CdcChange::Delete { key, before }
                    if before.is_none()
                        || before.as_ref().is_some_and(CdcRow::has_unchanged_toast) =>
                {
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
        if storage_keys.is_empty() {
            return Ok(HashMap::new());
        }

        let table = Arc::clone(&self.table);
        stream::iter(storage_keys.iter().cloned())
            .map(|storage_key| {
                let table = Arc::clone(&table);
                async move {
                    let row = load_row_by_storage_key_from_table(table.as_ref(), &storage_key)
                        .await
                        .with_context(|| {
                            format!("load prefetched CDC row state for key {:?}", storage_key)
                        })?;
                    Ok::<_, anyhow::Error>((storage_key, row))
                }
            })
            .buffer_unordered(CDC_OLD_ROW_PREFETCH_CONCURRENCY)
            .try_collect::<HashMap<_, _>>()
            .await
    }

    async fn load_row_by_storage_key(&self, storage_key: &[u8]) -> Result<Option<CdcRow>> {
        load_row_by_storage_key_from_table(self.table.as_ref(), storage_key).await
    }
}

async fn load_row_by_storage_key_from_table(
    table: &dyn KeyValueTable,
    storage_key: &[u8],
) -> Result<Option<CdcRow>> {
    let Some(bytes) = table.get(storage_key).await.context("load CDC row state")? else {
        return Ok(None);
    };
    decode_cdc_row_state(&bytes).map(Some)
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

fn transaction_has_unchanged_toast(transaction: &TransactionBatch) -> bool {
    transaction
        .change_batches()
        .iter()
        .flat_map(ChangeBatch::changes)
        .any(change_has_unchanged_toast)
}

fn change_has_unchanged_toast(change: &CdcChange) -> bool {
    match change {
        CdcChange::Insert { row } => row.has_unchanged_toast(),
        CdcChange::Update { before, after, .. } => {
            before.as_ref().is_some_and(CdcRow::has_unchanged_toast) || after.has_unchanged_toast()
        }
        CdcChange::Delete { before, .. } => {
            before.as_ref().is_some_and(CdcRow::has_unchanged_toast)
        }
        CdcChange::Truncate => false,
    }
}

fn ensure_no_unresolved_toast(operation: &str, table_id: &CdcTableId, row: &CdcRow) -> Result<()> {
    ensure!(
        !row.has_unchanged_toast(),
        "CDC {operation} for table '{}' contains unresolved unchanged TOAST",
        table_id.as_str()
    );
    Ok(())
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
        return schema.primary_key_from_row_allowing_unchanged_toast(before);
    }
    schema.primary_key_from_row_allowing_unchanged_toast(after)
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
    schema.primary_key_from_row_allowing_unchanged_toast(before)
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
