use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::IndexedBatchZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

type BatchKeyPartsFn<K, P, O> =
    Arc<dyn Fn(&[(K, i64)]) -> Vec<(K, i64, Option<P>, Option<O>)> + Send + Sync>;

/// Partition-local top-1 operator used for ROW_NUMBER() <= 1 style queries.
///
/// It keeps a persisted index of rows per partition and recomputes the winner
/// only for partitions touched in the current batch.
pub struct PartitionedTop1Op<K, P, O>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    P::Archived: RkyvDeserialize<P, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Clone + Ord + Send + Sync + 'static,
{
    pub input_index: IndexedBatchZSet<P, K>,
    pub table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    partition_output_cache: BTreeMap<P, K>,
    partition_order_index: BTreeMap<P, BTreeMap<(O, K), i64>>,
    row_key_cache: HashMap<K, (Option<P>, Option<O>)>,
    key_parts: BatchKeyPartsFn<K, P, O>,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K, P, O> PartitionedTop1Op<K, P, O>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    P::Archived: RkyvDeserialize<P, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Clone + Ord + Send + Sync + 'static,
{
    pub fn new_with_batch_key_extractor(
        input_index: IndexedBatchZSet<P, K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
        key_parts: BatchKeyPartsFn<K, P, O>,
    ) -> Self {
        Self {
            input_index,
            table,
            output,
            dict_cache: HashMap::new(),
            partition_output_cache: BTreeMap::new(),
            partition_order_index: BTreeMap::new(),
            row_key_cache: HashMap::new(),
            key_parts,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    pub fn enable_live_output_replayable(&mut self) {
        self.output.enable_replayable_persistence();
    }

    fn keys_for(&mut self, key: &K) -> (Option<P>, Option<O>) {
        if let Some(cached) = self.row_key_cache.get(key) {
            return cached.clone();
        }
        let computed = (self.key_parts)(&[(key.clone(), 1)])
            .into_iter()
            .next()
            .map(|(_, _, partition, order)| (partition, order))
            .unwrap_or((None, None));
        self.row_key_cache.insert(key.clone(), computed.clone());
        computed
    }

    fn keys_for_delta_map(
        &mut self,
        rows: &HashMap<K, i64>,
    ) -> Vec<(K, i64, Option<P>, Option<O>)> {
        let mut missing = Vec::new();
        let mut keyed = Vec::with_capacity(rows.len());
        for (key, weight) in rows {
            if let Some((partition, order)) = self.row_key_cache.get(key) {
                keyed.push((key.clone(), *weight, partition.clone(), order.clone()));
            } else {
                missing.push((key.clone(), *weight));
            }
        }
        for (key, weight, partition, order) in (self.key_parts)(&missing) {
            self.row_key_cache
                .insert(key.clone(), (partition.clone(), order.clone()));
            keyed.push((key, weight, partition, order));
        }
        keyed
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn ensure_partition_cache(
        &mut self,
        partition_key: &P,
        logical_work: Option<&mut metrics::LogicalWorkSnapshot>,
    ) -> Result<()> {
        if self.partition_order_index.contains_key(partition_key) {
            return Ok(());
        }
        let (values, lookup_metrics) = self
            .input_index
            .values_for_key_with_metrics(partition_key)
            .await
            .context("load top1 partition values")?;
        if let Some(work) = logical_work {
            work.add_lookup_metrics(lookup_metrics);
            work.partition_rows_examined = work
                .partition_rows_examined
                .saturating_add(values.len() as u64);
        }
        let mut index: BTreeMap<(O, K), i64> = BTreeMap::new();
        for (row, weight) in values {
            if weight <= 0 {
                continue;
            }
            let (_, Some(order_key)) = self.keys_for(&row) else {
                continue;
            };
            let index_key = (order_key, row);
            let next_weight = index
                .get(&index_key)
                .copied()
                .unwrap_or(0_i64)
                .saturating_add(weight);
            if next_weight <= 0 {
                index.remove(&index_key);
            } else {
                index.insert(index_key, next_weight);
            }
        }
        if let Some((_, row)) = index.keys().next() {
            self.partition_output_cache
                .insert(partition_key.clone(), row.clone());
        } else {
            self.partition_output_cache.remove(partition_key);
        }
        self.partition_order_index
            .insert(partition_key.clone(), index);
        Ok(())
    }

    fn apply_partition_delta(&mut self, partition_key: &P, row: &K, diff_weight: i64) {
        if diff_weight == 0 {
            return;
        }
        let (_, Some(order_key)) = self.keys_for(row) else {
            return;
        };
        let partition_index = self
            .partition_order_index
            .entry(partition_key.clone())
            .or_default();
        let index_key = (order_key, row.clone());
        let next_weight = partition_index
            .get(&index_key)
            .copied()
            .unwrap_or(0_i64)
            .saturating_add(diff_weight);
        if next_weight <= 0 {
            partition_index.remove(&index_key);
        } else {
            partition_index.insert(index_key, next_weight);
        }
    }

    fn cached_partition_top1(&self, partition_key: &P) -> Option<K> {
        self.partition_order_index
            .get(partition_key)
            .and_then(|index| index.keys().next().map(|(_, row)| row.clone()))
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<K>,
        deltas: &HashMap<K, i64>,
        base: Option<u64>,
        state_label: &'static str,
    ) -> Result<ZSetHandle> {
        let staged = deltas
            .iter()
            .filter_map(|(key, delta)| (*delta != 0).then_some((key.clone(), *delta)))
            .collect::<Vec<_>>();
        if staged.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        if versioned.uses_replayable_persistence() {
            anyhow::ensure!(
                base.is_none(),
                "replayable versioned ZSet does not support persisted base chaining"
            );
            return Ok(versioned.publish_replayable_batch(Arc::new(staged)));
        }

        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let mut dict_batch = dict.batch();
        for (key, delta) in staged {
            let id = dict_batch
                .intern(&key)
                .await
                .context("intern key while staging top1 delta")?;
            buckets.entry(bucket_for(id)).or_default().push((id, delta));
        }
        drop(dict_batch);

        let mut segments = Vec::new();
        for (bucket, mut bucket_deltas) in buckets {
            bucket_deltas.sort_by_key(|(id, _)| *id);
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas: bucket_deltas,
            });
        }

        let persist_start = std::time::Instant::now();
        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule top1 version update")?;
        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write top1 version update")?;
        versioned.apply_version_plan(&plan);
        metrics::observe_operator_persistence_latency_ms(
            "top1",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K, P, O> DeltaOperator for PartitionedTop1Op<K, P, O>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    P::Archived: RkyvDeserialize<P, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Clone + Ord + Send + Sync + 'static,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("top1 operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle_batch::<K>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for top1")?;
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(delta_values.len());
        if delta_values.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let mut delta_map = HashMap::new();
        let mut affected_partitions = BTreeSet::new();
        let mut index_updates = Vec::new();
        for (key, diff_weight) in delta_values.iter() {
            let entry = delta_map.entry(key.clone()).or_insert(0);
            *entry += *diff_weight;
            if *entry == 0 {
                delta_map.remove(key);
            }
        }
        if delta_map.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        for (key, diff_weight, partition_key, _) in self.keys_for_delta_map(&delta_map) {
            let Some(partition_key) = partition_key else {
                continue;
            };
            affected_partitions.insert(partition_key.clone());
            index_updates.push((partition_key, key, diff_weight));
        }
        if affected_partitions.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.changed_partitions = affected_partitions.len() as u64;

        for partition_key in &affected_partitions {
            self.ensure_partition_cache(partition_key, Some(&mut work))
                .await
                .context("seed top1 partition cache")?;
        }

        let input_index_persist_start = std::time::Instant::now();
        work.record_persisted_rows(index_updates.len());
        self.input_index
            .apply_deltas(index_updates.iter().cloned())
            .await
            .context("update top1 input index")?;
        metrics::observe_operator_persistence_latency_ms(
            "top1",
            "input_index",
            input_index_persist_start.elapsed().as_millis() as u64,
        );

        for (partition_key, key, diff_weight) in &index_updates {
            self.apply_partition_delta(partition_key, key, *diff_weight);
        }

        let mut output_delta = HashMap::new();
        for partition_key in affected_partitions {
            let old_top = self.partition_output_cache.get(&partition_key).cloned();
            if let Some(partition_index) = self.partition_order_index.get(&partition_key) {
                work.partition_rows_examined = work
                    .partition_rows_examined
                    .saturating_add(partition_index.len() as u64);
            }
            let new_top = self.cached_partition_top1(&partition_key);
            if old_top == new_top {
                continue;
            }
            if let Some(old_row) = old_top {
                *output_delta.entry(old_row).or_insert(0) -= 1;
            }
            if let Some(new_row) = new_top.clone() {
                *output_delta.entry(new_row).or_insert(0) += 1;
            }
            if let Some(new_row) = new_top {
                self.partition_output_cache.insert(partition_key, new_row);
            } else {
                self.partition_output_cache.remove(&partition_key);
            }
        }
        output_delta.retain(|_, delta| *delta != 0);
        if output_delta.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.replacement_rows = output_delta.len() as u64;
        work.record_output_delta_rows(output_delta.len());

        let output_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &output_delta, None, "output")
                .await
                .context("persist top1 output delta")?;
        work.record_persisted_rows(output_delta.len());
        publish_transient_zset_batch(
            &output_handle,
            Arc::new(output_delta.into_iter().collect::<Vec<_>>()),
        );
        self.logical_work.finish_tick(work);
        Ok(Some(output_handle))
    }

    fn logical_work(&self) -> Option<metrics::LogicalWorkSnapshot> {
        Some(self.logical_work.last_tick())
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::VersionedZSet;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open db"));
        Arc::new(crate::storage::SlateTable::new(db))
    }

    async fn stage_version(
        dict: Arc<Dictionary<i64>>,
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        deltas: &[(i64, i64)],
    ) -> ZSetHandle {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            let id = dict_batch.intern(key).await.expect("intern key");
            buckets
                .entry(bucket_for(id))
                .or_default()
                .push((id, *delta));
        }
        drop(dict_batch);

        let mut segments = Vec::new();
        for (bucket, mut bucket_deltas) in buckets {
            bucket_deltas.retain(|(_, delta)| *delta != 0);
            if bucket_deltas.is_empty() {
                continue;
            }
            bucket_deltas.sort_by_key(|(id, _)| *id);
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas: bucket_deltas,
            });
        }

        let dict = Arc::clone(&dict);
        let mut zset = VersionedZSet::new(dict, table, namespace.to_string())
            .await
            .expect("build zset");
        let version = zset.create_version(segments).await.expect("create version");
        zset.handle_for_version(version)
    }

    #[tokio::test]
    async fn partitioned_top1_tracks_latest_per_partition() {
        let table = build_table("partitioned-top1-latest").await;
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "input_top1_latest".to_string(), None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "output_top1_latest".to_string(), None)
                .await
                .expect("output dict"),
        );
        let output =
            VersionedZSet::new(output_dict, table.clone(), "output_top1_latest".to_string())
                .await
                .expect("output zset");
        let input_index = IndexedBatchZSet::new(table.clone(), "top1_input_latest");
        let key_parts = Arc::new(|deltas: &[(i64, i64)]| {
            deltas
                .iter()
                .map(|(key, weight)| (*key, *weight, Some(key / 10), Some(key % 10)))
                .collect()
        });
        let mut op = PartitionedTop1Op::new_with_batch_key_extractor(
            input_index,
            table.clone(),
            output,
            key_parts,
        );

        let handle = stage_version(
            input_dict,
            table.clone(),
            "input_top1_latest",
            &[(11, 1), (12, 1), (21, 1), (23, 1)],
        )
        .await;
        let out = op
            .on_step(1, &[handle])
            .await
            .expect("step")
            .expect("output handle");
        let rows = materialize_zset_handle::<i64>(table, &mut HashMap::new(), &out)
            .await
            .expect("materialize output");
        assert_eq!(rows.get(&11), Some(&1));
        assert_eq!(rows.get(&21), Some(&1));
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn partitioned_top1_recomputes_on_delete() {
        let table = build_table("partitioned-top1-delete").await;
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "input_top1_delete".to_string(), None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "output_top1_delete".to_string(), None)
                .await
                .expect("output dict"),
        );
        let output =
            VersionedZSet::new(output_dict, table.clone(), "output_top1_delete".to_string())
                .await
                .expect("output zset");
        let input_index = IndexedBatchZSet::new(table.clone(), "top1_input_delete");
        let key_parts = Arc::new(|deltas: &[(i64, i64)]| {
            deltas
                .iter()
                .map(|(key, weight)| (*key, *weight, Some(key / 10), Some(key % 10)))
                .collect()
        });
        let mut op = PartitionedTop1Op::new_with_batch_key_extractor(
            input_index,
            table.clone(),
            output,
            key_parts,
        );

        let first = stage_version(
            Arc::clone(&input_dict),
            table.clone(),
            "input_top1_delete",
            &[(11, 1), (12, 1)],
        )
        .await;
        op.on_step(1, &[first]).await.expect("first step");

        let second =
            stage_version(input_dict, table.clone(), "input_top1_delete", &[(11, -1)]).await;
        let out = op
            .on_step(2, &[second])
            .await
            .expect("second step")
            .expect("output handle");
        let rows = materialize_zset_handle::<i64>(table, &mut HashMap::new(), &out)
            .await
            .expect("materialize output");
        assert_eq!(rows.get(&11), Some(&-1));
        assert_eq!(rows.get(&12), Some(&1));
        assert_eq!(rows.len(), 2);
    }

    async fn run_top1_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
        let table = build_table(&format!("partitioned-top1-history-{history_rows}")).await;
        let input_ns = format!("input_top1_history_{history_rows}");
        let output_ns = format!("output_top1_history_{history_rows}");
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
                .await
                .expect("input dict"),
        );
        let output = VersionedZSet::new(
            Arc::new(
                Dictionary::<i64>::with_table(table.clone(), output_ns.clone(), None)
                    .await
                    .expect("output dict"),
            ),
            table.clone(),
            output_ns,
        )
        .await
        .expect("output zset");
        let key_parts = Arc::new(|deltas: &[(i64, i64)]| {
            deltas
                .iter()
                .map(|(key, weight)| (*key, *weight, Some(key / 10), Some(key % 10)))
                .collect()
        });
        let mut op = PartitionedTop1Op::new_with_batch_key_extractor(
            IndexedBatchZSet::new(table.clone(), format!("top1_history_index_{history_rows}")),
            table.clone(),
            output,
            key_parts,
        );

        let mut history = (0..history_rows)
            .map(|idx| ((10_000_000 + idx) * 10, 1))
            .collect::<Vec<_>>();
        history.push((72, 1));
        let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
        op.on_step(1, &[seed]).await.expect("seed top1 history");

        let fixed = stage_version(input_dict, table.clone(), &input_ns, &[(71, 1)]).await;
        let output = op
            .on_step(2, &[fixed])
            .await
            .expect("fixed top1 history")
            .expect("top1 output");
        let materialized = materialize_zset_handle::<i64>(table, &mut HashMap::new(), &output)
            .await
            .expect("materialize fixed top1");
        assert_eq!(materialized, HashMap::from([(72, -1), (71, 1)]));

        op.last_logical_work()
    }

    #[tokio::test]
    async fn partitioned_top1_logical_work_uses_changed_partitions() {
        let baseline = run_top1_history_probe(8).await;
        for history_rows in [128, 1024] {
            let actual = run_top1_history_probe(history_rows).await;
            assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
            assert_eq!(actual.changed_partitions, baseline.changed_partitions);
            assert_eq!(
                actual.partition_rows_examined,
                baseline.partition_rows_examined
            );
            assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
            assert_eq!(actual.state_full_scan_count, 0);
            assert_eq!(actual.cache_rebuild_rows, 0);
        }

        assert_eq!(baseline.input_delta_rows, 1);
        assert_eq!(baseline.changed_partitions, 1);
        assert_eq!(baseline.partition_rows_examined, 2);
        assert_eq!(baseline.output_delta_rows, 2);
        assert_eq!(baseline.replacement_rows, 2);
    }
}
