use std::sync::Arc;

use ahash::AHashMap;
use anyhow::{Context, Result};
use dbsp::storage::KeyValueTable;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

pub(super) struct PersistentTransientInputState {
    table: Option<Arc<dyn KeyValueTable>>,
    prefix: Vec<u8>,
    rows: AHashMap<Vec<u8>, i64>,
}

impl PersistentTransientInputState {
    pub(super) async fn load(
        table: Option<Arc<dyn KeyValueTable>>,
        graph_id: &str,
        label: impl AsRef<str>,
    ) -> Result<Self> {
        let prefix = transient_helper_state_prefix(graph_id, label.as_ref());
        let entries = match table.as_ref() {
            Some(table) => table
                .scan_prefix(&prefix, &ScanOptions::default())
                .await
                .with_context(|| {
                    format!(
                        "load transient helper input state for graph '{graph_id}' label '{}'",
                        label.as_ref()
                    )
                })?,
            None => Vec::new(),
        };
        let mut rows = AHashMap::with_capacity(entries.len());
        for (key, value) in entries {
            if value.len() != std::mem::size_of::<i64>() {
                tracing::warn!(
                    graph_id,
                    label = label.as_ref(),
                    key_len = key.len(),
                    value_len = value.len(),
                    "skipping malformed transient helper state row"
                );
                continue;
            }
            let row = key[prefix.len()..].to_vec();
            let mut weight = [0_u8; 8];
            weight.copy_from_slice(&value);
            let weight = i64::from_le_bytes(weight);
            if weight != 0 {
                rows.insert(row, weight);
            }
        }
        Ok(Self {
            table,
            prefix,
            rows,
        })
    }

    pub(super) fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.rows
            .iter()
            .map(|(row, weight)| (row.clone(), *weight))
            .collect()
    }

    pub(super) async fn apply_deltas(&mut self, deltas: &[(Vec<u8>, i64)]) -> Result<()> {
        if self.table.is_none() {
            return Ok(());
        }
        if deltas.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        let mut dirty = false;
        for (row, diff) in deltas {
            if *diff == 0 {
                continue;
            }
            let previous = self.rows.get(row).copied().unwrap_or(0);
            let next = previous.saturating_add(*diff);
            let key = transient_helper_state_key(&self.prefix, row);
            if next == 0 {
                self.rows.remove(row);
                batch.delete(key);
            } else {
                self.rows.insert(row.clone(), next);
                batch.put(key, next.to_le_bytes());
            }
            dirty = true;
        }
        if dirty && let Some(table) = self.table.as_ref() {
            table.write_batch(batch).await?;
        }
        Ok(())
    }

    pub(super) async fn replace_with_snapshot(&mut self, rows: Vec<(Vec<u8>, i64)>) -> Result<()> {
        if self.table.is_none() {
            return Ok(());
        }
        let next_rows = rows
            .into_iter()
            .filter(|(_, weight)| *weight != 0)
            .collect::<AHashMap<_, _>>();
        if self.rows == next_rows {
            return Ok(());
        }

        let mut batch = WriteBatch::new();
        for row in self.rows.keys() {
            if !next_rows.contains_key(row) {
                let key = transient_helper_state_key(&self.prefix, row);
                batch.delete(key);
            }
        }
        for (row, weight) in &next_rows {
            if self.rows.get(row).copied() != Some(*weight) {
                let key = transient_helper_state_key(&self.prefix, row);
                batch.put(key, weight.to_le_bytes());
            }
        }
        if let Some(table) = self.table.as_ref() {
            table.write_batch(batch).await?;
        }
        self.rows = next_rows;
        Ok(())
    }
}

fn transient_helper_state_prefix(graph_id: &str, label: &str) -> Vec<u8> {
    let mut prefix = b"floe/transient_helper_state/".to_vec();
    prefix.extend_from_slice(graph_id.as_bytes());
    prefix.push(b'/');
    prefix.extend_from_slice(label.as_bytes());
    prefix.push(b'/');
    prefix
}

fn transient_helper_state_key(prefix: &[u8], row: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + row.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(row);
    key
}
