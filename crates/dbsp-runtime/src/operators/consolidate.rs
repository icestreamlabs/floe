use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::delta_zset_handle;

/// Consolidates delta updates per tick into a single delta with coalesced weights.
pub struct ConsolidateOp<K>
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
    pub table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
}

impl<K> ConsolidateOp<K>
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
        }
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
                .context("intern key while staging consolidate delta")?;
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
            .context("schedule consolidate version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write consolidated version update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear consolidate intent")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K> DeltaOperator for ConsolidateOp<K>
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
        let delta_handle = inputs
            .first()
            .cloned()
            .context("consolidate operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<K>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for consolidate")?;

        if delta_values.is_empty() {
            return Ok(None);
        }

        let mut consolidated = HashMap::new();
        for (key, delta) in delta_values {
            let entry = consolidated.entry(key.clone()).or_insert(0);
            *entry += delta;
            if *entry == 0 {
                consolidated.remove(&key);
            }
        }

        if consolidated.is_empty() {
            return Ok(None);
        }

        let handle = Self::apply_deltas_to_versioned(&mut self.output, &consolidated)
            .await
            .context("persist consolidated deltas")?;
        Ok(Some(handle))
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

    async fn stage_version(
        dict: Arc<Dictionary<String>>,
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        deltas: &[(String, i64)],
    ) -> ZSetHandle {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern test key for consolidate");
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

        let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
            .await
            .expect("build versioned");
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create version");
        versioned.handle_for_version(version)
    }

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("consolidate", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn consolidate_operator_coalesces_deltas() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "consolidate_input", None)
                .await
                .expect("build input dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "consolidate_output", None)
                .await
                .expect("build output dictionary"),
        );
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "consolidate_output".to_string(),
        )
        .await
        .expect("output state");

        let mut op = ConsolidateOp::new(table.clone(), output);

        let delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "consolidate_input",
            &[
                ("a".to_string(), 1),
                ("a".to_string(), 2),
                ("b".to_string(), 1),
                ("b".to_string(), -1),
                ("c".to_string(), -2),
                ("c".to_string(), 1),
            ],
        )
        .await;

        let out = op
            .on_step(1, &[delta])
            .await
            .expect("consolidate step")
            .expect("non-empty consolidate output");

        let mut cache = HashMap::new();
        cache.insert("consolidate_output".to_string(), output_dict);
        let materialized =
            materialize_zset_handle::<String>(table.clone(), &mut cache, &out)
                .await
                .expect("materialize consolidate output");
        assert_eq!(materialized.get("a"), Some(&3));
        assert_eq!(materialized.get("c"), Some(&-1));
        assert!(!materialized.contains_key("b"));
    }

    #[tokio::test]
    async fn consolidate_operator_skips_empty_output() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "consolidate_empty_input", None)
                .await
                .expect("build input dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "consolidate_empty_output", None)
                .await
                .expect("build output dictionary"),
        );
        let output = VersionedZSet::new(
            output_dict,
            table.clone(),
            "consolidate_empty_output".to_string(),
        )
        .await
        .expect("output state");

        let mut op = ConsolidateOp::new(table.clone(), output);

        let delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "consolidate_empty_input",
            &[("a".to_string(), 1), ("a".to_string(), -1)],
        )
        .await;

        let out = op
            .on_step(1, &[delta])
            .await
            .expect("consolidate step");
        assert!(out.is_none());
    }
}
