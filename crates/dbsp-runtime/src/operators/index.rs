use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::IndexedZSet;
use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::delta_zset_handle;

type KeyExtractor<V, K> = Arc<dyn Fn(&V) -> Option<K> + Send + Sync>;

pub struct ArrangeByKeyOp<K, V>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub index: IndexedZSet<K, V>,
    pub table: Arc<dyn KeyValueTable>,
    pub key_extractor: KeyExtractor<V, K>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
}

impl<K, V> ArrangeByKeyOp<K, V>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(
        index: IndexedZSet<K, V>,
        table: Arc<dyn KeyValueTable>,
        key_extractor: KeyExtractor<V, K>,
    ) -> Self {
        Self {
            index,
            table,
            key_extractor,
            dict_cache: HashMap::new(),
        }
    }

    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64>
    where
        V: Clone + Eq + Hash,
    {
        let mut merged = HashMap::new();
        for (row, weight) in deltas {
            let entry = merged.entry(row.clone()).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                merged.remove(&row);
            }
        }
        merged
    }
}

#[async_trait]
impl<K, V> DeltaOperator for ArrangeByKeyOp<K, V>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("arrange operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for arrangement")?;

        if delta_values.is_empty() {
            return Ok(None);
        }

        let coalesced = self.coalesce_deltas(delta_values);
        if coalesced.is_empty() {
            return Ok(None);
        }

        let mut updates = Vec::new();
        for (value, weight) in coalesced {
            if weight == 0 {
                continue;
            }
            if let Some(key) = (self.key_extractor)(&value) {
                updates.push((key, value, weight));
            }
        }

        if updates.is_empty() {
            return Ok(None);
        }

        self.index
            .apply_deltas(updates)
            .await
            .context("apply arranged index deltas")?;

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn bucket_for(id: u64) -> u16 {
        (id >> 48) as u16
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
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern test key for arrangement");
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
        Arc::new(Db::open("arrange_by_key", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn arrange_by_key_updates_index() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "arrange_input", None)
                .await
                .expect("build input dictionary"),
        );

        let index = IndexedZSet::new(table.clone(), "arrange_index");
        let key_extractor: KeyExtractor<i64, i64> = Arc::new(|value: &i64| {
            if *value >= 0 {
                Some(value % 2)
            } else {
                None
            }
        });
        let mut op = ArrangeByKeyOp::new(index, table.clone(), key_extractor);

        let first_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "arrange_input",
            &[(1, 1), (2, 1), (3, 2), (-1, 1)],
        )
        .await;
        let out = op
            .on_step(1, &[first_delta])
            .await
            .expect("arrange step 1");
        assert!(out.is_none());

        let mut odd = op
            .index
            .values_for_key(&1)
            .await
            .expect("read odd index");
        odd.sort_by_key(|(value, _)| *value);
        assert_eq!(odd, vec![(1, 1), (3, 2)]);

        let even = op
            .index
            .values_for_key(&0)
            .await
            .expect("read even index");
        assert_eq!(even, vec![(2, 1)]);

        let second_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "arrange_input",
            &[(1, -1), (4, 3)],
        )
        .await;
        op.on_step(2, &[second_delta])
            .await
            .expect("arrange step 2");

        let mut odd_after = op
            .index
            .values_for_key(&1)
            .await
            .expect("read odd index after");
        odd_after.sort_by_key(|(value, _)| *value);
        assert_eq!(odd_after, vec![(3, 2)]);

        let mut even_after = op
            .index
            .values_for_key(&0)
            .await
            .expect("read even index after");
        even_after.sort_by_key(|(value, _)| *value);
        assert_eq!(even_after, vec![(2, 1), (4, 3)]);
    }
}
