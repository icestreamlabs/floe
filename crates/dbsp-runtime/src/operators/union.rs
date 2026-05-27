use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

/// Unions multiple delta inputs into a single delta with coalesced weights.
pub struct UnionOp<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K> UnionOp<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(table: Arc<dyn KeyValueTable>, output: VersionedZSet<K>) -> Self {
        Self {
            table,
            output,
            dict_cache: HashMap::new(),
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<K>,
        deltas: &HashMap<K, i64>,
    ) -> Result<ZSetHandle> {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            let id = dict_batch
                .intern(key)
                .await
                .context("intern key while staging union delta")?;
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

        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, None, 0, &mut batch)
            .await
            .context("schedule union version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write union version update")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K> DeltaOperator for UnionOp<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        if inputs.is_empty() {
            return Err(anyhow!("union operator requires at least one input"));
        }

        let mut work = metrics::LogicalWorkSnapshot::default();
        let mut merged: HashMap<K, i64> = HashMap::new();
        for handle in inputs {
            let deltas =
                delta_zset_handle_batch::<K>(self.table.clone(), &mut self.dict_cache, handle)
                    .await
                    .context("load delta for union")?;
            work.input_delta_rows = work.input_delta_rows.saturating_add(deltas.len() as u64);
            work.input_delta_batches = work
                .input_delta_batches
                .saturating_add((!deltas.is_empty()) as u64);
            for (key, delta) in deltas.iter() {
                let entry = merged.entry(key.clone()).or_insert(0);
                *entry += *delta;
                if *entry == 0 {
                    merged.remove(key);
                }
            }
        }

        if merged.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.record_output_delta_rows(merged.len());
        work.record_persisted_rows(merged.len());

        let handle = Self::apply_deltas_to_versioned(&mut self.output, &merged)
            .await
            .context("persist union deltas")?;
        publish_transient_zset_batch(&handle, Arc::new(merged.into_iter().collect::<Vec<_>>()));
        self.logical_work.finish_tick(work);
        Ok(Some(handle))
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

        let mut zset = VersionedZSet::new(dict, table, namespace.to_string())
            .await
            .expect("build zset");
        let version = zset.create_version(segments).await.expect("create version");
        zset.handle_for_version(version)
    }

    async fn run_union_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
        let table = build_table(&format!("union-history-{history_rows}")).await;
        let input_ns = format!("union_history_input_{history_rows}");
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(
                table.clone(),
                format!("union_history_output_{history_rows}"),
                None,
            )
            .await
            .expect("output dict"),
        );
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            format!("union_history_output_{history_rows}"),
        )
        .await
        .expect("output");
        let mut op = UnionOp::new(table.clone(), output);

        let history = (0..history_rows)
            .map(|idx| (1_000_000 + idx, 1))
            .collect::<Vec<_>>();
        let history_handle =
            stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
        op.on_step(1, &[history_handle]).await.expect("seed union");

        let fixed_handle = stage_version(input_dict, table.clone(), &input_ns, &[(7, 1)]).await;
        let output_handle = op
            .on_step(2, &[fixed_handle])
            .await
            .expect("fixed union")
            .expect("union output handle");
        let mut cache = HashMap::new();
        cache.insert(format!("union_history_output_{history_rows}"), output_dict);
        let materialized = materialize_zset_handle::<i64>(table, &mut cache, &output_handle)
            .await
            .expect("materialize fixed union");
        assert_eq!(materialized, HashMap::from([(7, 1)]));
        op.last_logical_work()
    }

    #[tokio::test]
    async fn union_logical_work_is_delta_local() {
        let baseline = run_union_history_probe(8).await;
        for history_rows in [128, 1024] {
            let actual = run_union_history_probe(history_rows).await;
            assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
            assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
            assert_eq!(actual.persisted_rows, baseline.persisted_rows);
            assert_eq!(actual.state_scan_rows, 0);
            assert_eq!(actual.state_full_scan_count, 0);
            assert_eq!(actual.cache_rebuild_rows, 0);
        }

        assert_eq!(baseline.input_delta_rows, 1);
        assert_eq!(baseline.output_delta_rows, 1);
        assert_eq!(baseline.persisted_rows, 1);
    }
}
