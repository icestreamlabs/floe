use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::memory::InMemory;
use slatedb::Db;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;
use std::ops::Range;

use crate::storage::{KeyValueTable, SlateTable};

use super::IndexedBatchZSet;

async fn build_table(namespace: &str) -> Arc<dyn crate::storage::KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(namespace, store).await.expect("open SlateDB"));
    Arc::new(SlateTable::new(db))
}

struct CountingTable {
    inner: Arc<dyn KeyValueTable>,
    scan_range_entries: AtomicUsize,
    scan_until_entries: AtomicUsize,
}

impl CountingTable {
    fn new(inner: Arc<dyn KeyValueTable>) -> Self {
        Self {
            inner,
            scan_range_entries: AtomicUsize::new(0),
            scan_until_entries: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.scan_range_entries.store(0, Ordering::Relaxed);
        self.scan_until_entries.store(0, Ordering::Relaxed);
    }

    fn scan_range_entries(&self) -> usize {
        self.scan_range_entries.load(Ordering::Relaxed)
    }

    fn scan_until_entries(&self) -> usize {
        self.scan_until_entries.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl KeyValueTable for CountingTable {
    async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_bytes(key).await
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.inner.write_batch(batch).await
    }

    async fn scan_range_bytes(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let entries = self.inner.scan_range_bytes(range, options).await?;
        self.scan_range_entries
            .fetch_add(entries.len(), Ordering::Relaxed);
        Ok(entries)
    }

    async fn scan_range_bytes_until(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
        should_continue_after_entry: &mut (
                 dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<bool> + Send
             ),
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let entries = self
            .inner
            .scan_range_bytes_until(range, options, should_continue_after_entry)
            .await?;
        self.scan_until_entries
            .fetch_add(entries.len(), Ordering::Relaxed);
        Ok(entries)
    }
}

#[tokio::test]
async fn arrow_indexed_lookup_aggregates_weights() {
    let table = build_table("arrow-indexed-lookup").await;
    let index = IndexedBatchZSet::<i64, i64>::new(table, "arrow_indexed_lookup");
    index
        .apply_deltas(vec![(1, 10, 1), (1, 11, 2), (1, 10, -1), (2, 20, 3)])
        .await
        .expect("apply deltas");

    let mut values = index.values_for_key(&1).await.expect("lookup key");
    values.sort_unstable();
    assert_eq!(values, vec![(11, 2)]);
}

#[tokio::test]
async fn arrow_indexed_lookup_reports_index_work() {
    let table = build_table("arrow-indexed-lookup-metrics").await;
    let index = IndexedBatchZSet::<i64, i64>::new(table, "arrow_indexed_lookup_metrics");
    index
        .apply_deltas(vec![(1, 10, 1), (2, 20, 1)])
        .await
        .expect("apply first segment");
    index
        .apply_deltas(vec![(1, 11, 1)])
        .await
        .expect("apply second segment");

    let (mut values, first_metrics) = index
        .values_for_key_with_metrics(&1)
        .await
        .expect("first lookup");
    values.sort_unstable();
    assert_eq!(values, vec![(10, 1), (11, 1)]);
    assert_eq!(first_metrics.lookup_keys, 1);
    assert_eq!(first_metrics.returned_rows, 2);
    assert_eq!(first_metrics.index_segments_examined, 2);
    assert_eq!(first_metrics.index_postings_examined, 2);
    assert_eq!(first_metrics.cache_hits, 0);
    assert_eq!(first_metrics.cache_misses, 1);

    let (_values, second_metrics) = index
        .values_for_key_with_metrics(&1)
        .await
        .expect("cached lookup");
    assert_eq!(second_metrics.lookup_keys, 1);
    assert_eq!(second_metrics.index_segments_examined, 0);
    assert_eq!(second_metrics.index_postings_examined, 0);
    assert_eq!(second_metrics.cache_hits, 1);
    assert_eq!(second_metrics.cache_misses, 0);
}

#[tokio::test]
async fn arrow_indexed_hot_key_compaction_bounds_read_amplification() {
    let table = build_table("arrow-indexed-hot-key-compaction").await;
    let index = IndexedBatchZSet::<i64, i64>::with_hot_key_compaction_threshold(
        table,
        "arrow_indexed_hot_key_compaction",
        2,
    );
    index
        .apply_deltas(vec![(1, 10, 1)])
        .await
        .expect("apply first hot-key segment");
    index
        .apply_deltas(vec![(1, 11, 1)])
        .await
        .expect("apply second hot-key segment");
    index
        .apply_deltas(vec![(1, 12, 1)])
        .await
        .expect("apply third hot-key segment");

    assert_eq!(
        index
            .estimated_read_amplification_for_key(&1)
            .await
            .expect("estimate read amplification"),
        1
    );
    let mut values = index
        .values_for_key(&1)
        .await
        .expect("lookup compacted key");
    values.sort_unstable();
    assert_eq!(values, vec![(10, 1), (11, 1), (12, 1)]);
}

#[tokio::test]
async fn arrow_indexed_cache_stays_consistent_across_updates() {
    let table = build_table("arrow-indexed-cache").await;
    let index = IndexedBatchZSet::<i64, i64>::new(table, "arrow_indexed_cache");
    index
        .apply_deltas(vec![(1, 10, 1), (1, 11, 1)])
        .await
        .expect("seed deltas");
    let mut first = index.values_for_key(&1).await.expect("seed cache");
    first.sort_unstable();
    assert_eq!(first, vec![(10, 1), (11, 1)]);

    index
        .apply_deltas(vec![(1, 10, -1), (1, 12, 3)])
        .await
        .expect("apply cache updates");
    let mut second = index.values_for_key(&1).await.expect("read updated cache");
    second.sort_unstable();
    assert_eq!(second, vec![(11, 1), (12, 3)]);
}

#[tokio::test]
async fn arrow_indexed_replayable_reads_overlay_without_persisting() {
    let table = build_table("arrow-indexed-replayable").await;
    let namespace = "arrow_indexed_replayable";
    let index = IndexedBatchZSet::<i64, i64>::new_replayable(table.clone(), namespace);
    index
        .apply_deltas(vec![(1, 10, 1), (1, 11, 1), (2, 20, 2)])
        .await
        .expect("apply replayable deltas");

    let mut values = index.values_for_key(&1).await.expect("overlay lookup");
    values.sort_unstable();
    assert_eq!(values, vec![(10, 1), (11, 1)]);

    let reopened = IndexedBatchZSet::<i64, i64>::new(table, namespace);
    let persisted = reopened
        .values_for_key(&1)
        .await
        .expect("persisted lookup should ignore replayable overlay");
    assert!(persisted.is_empty());
}

#[tokio::test]
async fn arrow_indexed_replayable_reverse_lookup_reads_overlay() {
    let table = build_table("arrow-indexed-replayable-reverse").await;
    let index = IndexedBatchZSet::<i64, i64>::with_reverse_index_replayable(
        table,
        "arrow_indexed_replayable_reverse",
    );
    index
        .apply_deltas(vec![(1, 10, 1), (2, 10, 2), (1, 10, -1)])
        .await
        .expect("apply replayable reverse deltas");

    let mut keys = index
        .keys_for_value(&10)
        .await
        .expect("overlay reverse lookup");
    keys.sort_unstable();
    assert_eq!(keys, vec![(2, 2)]);
}

#[tokio::test]
async fn arrow_indexed_replayable_range_lookup_reads_overlay() {
    let table = build_table("arrow-indexed-replayable-range").await;
    let index = IndexedBatchZSet::<i64, i64>::with_range_index_replayable(
        table,
        "arrow_indexed_replayable_range",
    );
    index
        .apply_deltas_with_range(vec![(1, 10, 1), (2, 20, 2), (3, 30, 3)])
        .await
        .expect("apply replayable range deltas");

    let mut rows = index
        .values_for_key_range(&2, &4)
        .await
        .expect("overlay range lookup");
    rows.sort_unstable();
    assert_eq!(rows, vec![(2, 20, 2), (3, 30, 3)]);
}

#[tokio::test]
async fn arrow_indexed_reopen_preserves_persisted_state() {
    let table = build_table("arrow-indexed-reopen").await;
    let namespace = "arrow_indexed_reopen";
    let writer = IndexedBatchZSet::<i64, i64>::new(table.clone(), namespace);
    writer
        .apply_deltas(vec![(1, 10, 1), (1, 11, 2), (1, 10, -1), (2, 20, 3)])
        .await
        .expect("seed and update Arrow-index state");

    let reader = IndexedBatchZSet::<i64, i64>::new(table, namespace);
    let mut key_one = reader.values_for_key(&1).await.expect("reopen key lookup");
    key_one.sort_unstable();
    assert_eq!(key_one, vec![(11, 2)]);

    let mut key_two = reader.values_for_key(&2).await.expect("reopen key lookup");
    key_two.sort_unstable();
    assert_eq!(key_two, vec![(20, 3)]);
}

#[tokio::test]
async fn arrow_indexed_restore_truncates_uncommitted_segments() {
    crate::operator_state_registry::clear_operator_state_registry();
    let table = build_table("arrow-indexed-checkpoint-restore").await;
    let namespace = "op/arrow_indexed_checkpoint_restore/0/index";
    let writer = IndexedBatchZSet::<i64, i64>::new(table.clone(), namespace);
    writer
        .restore_committed_checkpoint()
        .await
        .expect("initialize checkpoint handle");
    writer
        .apply_deltas(vec![(1, 10, 1)])
        .await
        .expect("apply committed segment");
    let committed_handle = crate::operator_state_registry::snapshot_operator_states()
        .into_iter()
        .find(|handle| handle.namespace == namespace)
        .expect("checkpointed index handle");
    assert_eq!(committed_handle.version, 2);

    writer
        .apply_deltas(vec![(1, 99, 1)])
        .await
        .expect("apply uncommitted segment");
    crate::operator_state_registry::install_operator_state_restore(vec![committed_handle]);

    let restored = IndexedBatchZSet::<i64, i64>::new(table.clone(), namespace);
    restored
        .restore_committed_checkpoint()
        .await
        .expect("restore committed checkpoint");
    let mut values = restored
        .values_for_key(&1)
        .await
        .expect("lookup restored key");
    values.sort_unstable();
    assert_eq!(values, vec![(10, 1)]);

    restored
        .apply_deltas(vec![(1, 20, 1)])
        .await
        .expect("apply after restore");
    let mut values = restored
        .values_for_key(&1)
        .await
        .expect("lookup restored key after write");
    values.sort_unstable();
    assert_eq!(values, vec![(10, 1), (20, 1)]);
    crate::operator_state_registry::clear_operator_state_registry();
}

#[tokio::test]
async fn arrow_indexed_reverse_lookup_aggregates_keys() {
    let table = build_table("arrow-indexed-reverse").await;
    let index = IndexedBatchZSet::<i64, i64>::with_reverse_index(table, "arrow_indexed_reverse");
    index
        .apply_deltas(vec![(1, 10, 1), (2, 10, 3), (1, 10, -1), (3, 11, 2)])
        .await
        .expect("apply deltas");

    let mut keys = index.keys_for_value(&10).await.expect("reverse lookup");
    keys.sort_unstable();
    assert_eq!(keys, vec![(2, 3)]);
}

#[tokio::test]
async fn arrow_indexed_range_scan_filters_keys() {
    let table = build_table("arrow-indexed-range").await;
    let index = IndexedBatchZSet::<i64, i64>::with_range_index(table, "arrow_indexed_range");
    index
        .apply_deltas_with_range(vec![(1, 10, 1), (2, 20, 2), (3, 30, 3), (4, 40, 4)])
        .await
        .expect("apply deltas");

    let mut rows = index
        .values_for_key_range(&2, &4)
        .await
        .expect("range lookup");
    rows.sort_unstable();
    assert_eq!(rows, vec![(2, 20, 2), (3, 30, 3)]);
}

#[tokio::test]
async fn arrow_indexed_range_only_writes_no_lookup_postings() {
    let table = build_table("arrow-indexed-range-only").await;
    let index =
        IndexedBatchZSet::<i64, i64>::with_range_index(table.clone(), "arrow_indexed_range_only");
    index
        .apply_deltas_with_range_only(vec![(1, 10, 1), (2, 20, 1), (3, 30, 1)])
        .await
        .expect("apply range-only deltas");

    let key_bytes = crate::storage::encoding::encode(&1_i64).expect("encode key");
    let lookup_prefix = index
        .index_prefix_for_key(&key_bytes)
        .expect("build lookup postings prefix");
    let lookup_entries = table
        .scan_prefix(&lookup_prefix, &ScanOptions::default())
        .await
        .expect("scan lookup postings");
    assert!(
        lookup_entries.is_empty(),
        "range-only index should not write point-lookup postings",
    );

    let mut rows = index
        .values_for_key_range(&1, &3)
        .await
        .expect("range lookup");
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 10, 1), (2, 20, 1)]);
}

#[tokio::test]
async fn arrow_indexed_first_range_lookup_stops_after_first_posting_group() {
    let inner = build_table("arrow-indexed-first-range").await;
    let counting = Arc::new(CountingTable::new(inner));
    let table = counting.clone() as Arc<dyn KeyValueTable>;
    let index = IndexedBatchZSet::<i64, i64>::with_range_index(table, "arrow_indexed_first_range");
    index
        .apply_deltas_with_range(vec![(1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 40, 1)])
        .await
        .expect("apply deltas");

    counting.reset();
    let rows = index
        .first_values_for_key_range(&1, &5)
        .await
        .expect("first range lookup");
    assert_eq!(rows, vec![(1, 10, 1)]);
    assert_eq!(
        counting.scan_until_entries(),
        2,
        "first-key lookup should read the first posting group plus one boundary entry",
    );

    counting.reset();
    let rows = index
        .values_for_key_range(&1, &5)
        .await
        .expect("full range lookup");
    assert_eq!(rows.len(), 4);
    assert_eq!(
        counting.scan_range_entries(),
        4,
        "full range lookup still materializes all postings in the requested range",
    );
}

#[tokio::test]
async fn arrow_indexed_range_scan_rejects_legacy_layout() {
    let table = build_table("arrow-indexed-range-legacy").await;
    let index =
        IndexedBatchZSet::<i64, i64>::with_range_index(table.clone(), "arrow_indexed_range_legacy");
    index
        .apply_deltas(vec![(1, 10, 1), (2, 20, 1)])
        .await
        .expect("apply legacy deltas");

    let err = index
        .values_for_key_range(&1, &3)
        .await
        .expect_err("legacy range layout should require rebuild");
    assert!(
        err.to_string().contains("legacy layout"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn arrow_indexed_writes_one_posting_per_key_per_segment() {
    let table = build_table("arrow-indexed-postings").await;
    let index = IndexedBatchZSet::<i64, i64>::new(table.clone(), "arrow_indexed_postings");
    index
        .apply_deltas(vec![(1, 10, 1), (1, 11, 1), (1, 12, 1), (2, 20, 1)])
        .await
        .expect("apply deltas");

    let key_bytes = crate::storage::encoding::encode(&1_i64).expect("encode key");
    let prefix = index
        .index_prefix_for_key(&key_bytes)
        .expect("build postings prefix");
    let entries = table
        .scan_prefix(&prefix, &ScanOptions::default())
        .await
        .expect("scan postings entries");
    assert_eq!(entries.len(), 1, "expected one key+segment posting record");
}

#[tokio::test]
async fn arrow_indexed_segments_store_only_values() {
    let table = build_table("arrow-indexed-value-only-segment").await;
    let index =
        IndexedBatchZSet::<i64, i64>::new(table.clone(), "arrow_indexed_value_only_segment");
    index
        .apply_deltas(vec![(1, 10, 1), (1, 11, 1), (2, 20, 1)])
        .await
        .expect("apply deltas");

    let segment = index
        .segment_store
        .read_segment(1)
        .await
        .expect("read segment")
        .expect("segment exists");
    assert_eq!(segment.batches.len(), 1);
    assert_eq!(
        segment.batches[0].num_columns(),
        1,
        "indexed segments should not duplicate key or delta columns"
    );

    let mut values = index.values_for_key(&1).await.expect("lookup values");
    values.sort_unstable();
    assert_eq!(values, vec![(10, 1), (11, 1)]);
}
