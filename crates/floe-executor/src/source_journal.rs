use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use dbsp::storage::KeyValueTable;
use slatedb::config::ScanOptions;
use slatedb::WriteBatch;

use crate::outer_stream::OuterStreamRegistry;

const SOURCE_BATCH_JOURNAL_PREFIX: &str = "source_journal";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceBatchJournalEntry {
    pub source: String,
    pub tick_id: u64,
    pub max_event_time_ms: Option<i64>,
    pub deltas: Vec<(Vec<u8>, i64)>,
}

#[derive(Clone)]
pub struct SourceBatchJournal {
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
        self.table
            .write_batch(batch)
            .await
            .with_context(|| {
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
        Ok(replayed)
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

fn encode_entry(max_event_time_ms: Option<i64>, deltas: &[(Vec<u8>, i64)]) -> Result<Vec<u8>> {
    let count = u32::try_from(deltas.len()).context("too many rows in source batch journal")?;
    let mut encoded = Vec::with_capacity(
        8 + 4
            + deltas
                .iter()
                .map(|(key, _)| 4 + key.len() + std::mem::size_of::<i64>())
                .sum::<usize>(),
    );
    encoded.extend_from_slice(&max_event_time_ms.unwrap_or(-1).to_le_bytes());
    encoded.extend_from_slice(&count.to_le_bytes());
    for (key, diff) in deltas {
        let len = u32::try_from(key.len()).context("source batch journal row key too large")?;
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(key);
        encoded.extend_from_slice(&diff.to_le_bytes());
    }
    Ok(encoded)
}

fn decode_entry(value: &[u8]) -> Result<(Option<i64>, Vec<(Vec<u8>, i64)>)> {
    if value.len() < 12 {
        bail!("source batch journal entry missing header");
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

    let mut deltas = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor + 4 > value.len() {
            bail!("source batch journal entry truncated before row length");
        }
        let key_len = u32::from_le_bytes(
            value[cursor..cursor + 4]
                .try_into()
                .expect("slice width already checked"),
        ) as usize;
        cursor += 4;
        if cursor + key_len + 8 > value.len() {
            bail!("source batch journal entry truncated while decoding row");
        }
        let key = value[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let diff = i64::from_le_bytes(
            value[cursor..cursor + 8]
                .try_into()
                .expect("slice width already checked"),
        );
        cursor += 8;
        deltas.push((key, diff));
    }
    if cursor != value.len() {
        bail!("source batch journal entry had trailing bytes");
    }
    Ok((
        (max_event_time_ms >= 0).then_some(max_event_time_ms),
        deltas,
    ))
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
}
