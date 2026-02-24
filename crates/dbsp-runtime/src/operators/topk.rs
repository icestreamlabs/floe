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
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{compute_delta, delta_zset_handle};

type OrderKeyFn<K, O> = Arc<dyn Fn(&K) -> Option<O> + Send + Sync>;

/// Top-K operator that keeps the first `k` distinct rows by order key,
/// preserving their weights (including negative weights).
pub struct TopKOp<K, O>
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
    O: Ord + Clone + Send + Sync + 'static,
{
    pub state: RelationState<K>,
    pub table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    input_cache: Option<HashMap<K, i64>>,
    output_cache: HashMap<K, i64>,
    // In-memory ordering index for top-k distinct semantics; rebuilt on restart.
    order_index: Option<BTreeMap<(O, K), i64>>,
    row_order_cache: HashMap<K, Option<O>>,
    order_key: OrderKeyFn<K, O>,
    limit: usize,
}

impl<K, O> TopKOp<K, O>
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
    O: Ord + Clone + Send + Sync + 'static,
{
    pub fn new(
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
        order_key: OrderKeyFn<K, O>,
        limit: usize,
    ) -> Self {
        Self {
            state,
            table,
            output,
            dict_cache: HashMap::new(),
            input_cache: None,
            output_cache: HashMap::new(),
            order_index: None,
            row_order_cache: HashMap::new(),
            order_key,
            limit,
        }
    }

    async fn ensure_input_cache(&mut self) -> Result<()> {
        if self.input_cache.is_some() {
            return Ok(());
        }

        let mut materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize topk input state")?;
        materialized.retain(|_, weight| *weight != 0);
        self.input_cache = Some(materialized);
        Ok(())
    }

    async fn ensure_order_index(&mut self) -> Result<()> {
        if self.order_index.is_some() {
            return Ok(());
        }

        let input_cache = self
            .input_cache
            .as_ref()
            .context("topk input cache missing while building order index")?;
        let entries: Vec<(K, i64)> = input_cache
            .iter()
            .map(|(key, weight)| (key.clone(), *weight))
            .collect();
        let mut index = BTreeMap::new();
        for (key, weight) in entries {
            if weight == 0 {
                continue;
            }
            let order_key = self.order_key_for(&key);
            if let Some(order_key) = order_key {
                index.insert((order_key, key), weight);
            }
        }
        self.order_index = Some(index);
        Ok(())
    }

    fn order_key_for(&mut self, key: &K) -> Option<O> {
        if let Some(cached) = self.row_order_cache.get(key) {
            return cached.clone();
        }
        let computed = (self.order_key)(key);
        self.row_order_cache.insert(key.clone(), computed.clone());
        computed
    }

    fn compute_topk(&self, order_index: &BTreeMap<(O, K), i64>) -> HashMap<K, i64> {
        if self.limit == 0 {
            return HashMap::new();
        }

        let mut output = HashMap::new();
        for (count, ((_order_key, row), weight)) in order_index.iter().enumerate() {
            if count >= self.limit {
                break;
            }
            output.insert(row.clone(), *weight);
        }
        output
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<K>,
        deltas: &HashMap<K, i64>,
        base: Option<u64>,
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
                .context("intern key while staging topk delta")?;
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

        if segments.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule topk version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write topk version update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear topk intent")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K, O> DeltaOperator for TopKOp<K, O>
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
    O: Ord + Clone + Send + Sync + 'static,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("topk operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<K>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for topk")?;

        if delta_values.is_empty() {
            return Ok(None);
        }

        self.ensure_input_cache()
            .await
            .context("load topk input cache")?;

        let mut delta_map = HashMap::new();
        for (key, diff_weight) in delta_values {
            let entry = delta_map.entry(key.clone()).or_insert(0);
            *entry += diff_weight;
            if *entry == 0 {
                delta_map.remove(&key);
            }
        }

        if delta_map.is_empty() {
            return Ok(None);
        }

        self.ensure_order_index()
            .await
            .context("build topk order index")?;

        let mut cache_updates = Vec::new();
        for (key, diff_weight) in &delta_map {
            let existing = self
                .input_cache
                .as_ref()
                .and_then(|cache| cache.get(key).copied())
                .unwrap_or(0);
            let new_weight = existing + diff_weight;
            let order_key = if existing != 0 || new_weight != 0 {
                self.order_key_for(key)
            } else {
                None
            };
            cache_updates.push((key.clone(), existing, new_weight, order_key));
        }

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_integrated_handle =
            Self::apply_deltas_to_versioned(&mut self.state.integrated, &delta_map, base_version)
                .await
                .context("update topk input state")?;
        self.state.update_handle(new_integrated_handle);

        if let Some(input_cache) = self.input_cache.as_mut() {
            for (key, _old_weight, new_weight, _order_key) in &cache_updates {
                if *new_weight == 0 {
                    input_cache.remove(key);
                } else {
                    input_cache.insert(key.clone(), *new_weight);
                }
            }
        }

        let mut cache_prune = Vec::new();
        if let Some(mut order_index) = self.order_index.take() {
            for (key, old_weight, new_weight, order_key) in &cache_updates {
                let old_nonzero = *old_weight != 0;
                let new_nonzero = *new_weight != 0;
                if !old_nonzero && !new_nonzero {
                    if *new_weight == 0 {
                        cache_prune.push(key.clone());
                    }
                    continue;
                }
                let Some(order_key) = order_key.clone() else {
                    if *new_weight == 0 {
                        cache_prune.push(key.clone());
                    }
                    continue;
                };

                let index_key = (order_key, key.clone());
                if old_nonzero && new_nonzero {
                    order_index.insert(index_key, *new_weight);
                } else if old_nonzero {
                    order_index.remove(&index_key);
                } else if new_nonzero {
                    order_index.insert(index_key, *new_weight);
                }

                if *new_weight == 0 {
                    cache_prune.push(key.clone());
                }
            }
            self.order_index = Some(order_index);
        }
        for key in cache_prune {
            self.row_order_cache.remove(&key);
        }

        let order_index = self
            .order_index
            .as_ref()
            .context("topk order index missing after update")?;
        let new_output = self.compute_topk(order_index);
        let output_delta_vec = compute_delta(&self.output_cache, &new_output);
        self.output_cache = new_output;

        if output_delta_vec.is_empty() {
            return Ok(None);
        }

        let output_delta: HashMap<K, i64> = output_delta_vec.into_iter().collect();
        let output_handle = Self::apply_deltas_to_versioned(&mut self.output, &output_delta, None)
            .await
            .context("persist topk output delta")?;
        Ok(Some(output_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::SegmentRecord;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;

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
                .expect("intern test key for topk");
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
        Arc::new(Db::open("topk", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn topk_operator_emits_distinct_entries() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topk_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topk_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topk_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict,
                table.clone(),
                "topk_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "topk_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "topk_output".to_string(),
        )
        .await
        .expect("output");

        let order_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value));
        let mut op = TopKOp::new(state, table.clone(), output, order_key, 2);

        let first_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topk_input",
            &[(1, 3), (2, 1), (3, 1)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("topk t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("topk_output".to_string(), output_dict.clone());
        let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize output t1");
        assert_eq!(out1_materialized, HashMap::from([(1, 3), (2, 1)]));

        let second_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topk_input",
            &[(1, -3), (4, 1)],
        )
        .await;
        let out2 = op
            .on_step(2, &[second_delta])
            .await
            .expect("topk t2")
            .expect("non-empty t2");
        let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize output t2");
        assert_eq!(out2_materialized, HashMap::from([(1, -3), (3, 1)]));
    }

    #[tokio::test]
    async fn topk_preserves_negative_weights() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topk_neg_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topk_neg_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topk_neg_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict,
                table.clone(),
                "topk_neg_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "topk_neg_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "topk_neg_output".to_string(),
        )
        .await
        .expect("output");

        let order_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value));
        let mut op = TopKOp::new(state, table.clone(), output, order_key, 1);

        let first_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topk_neg_input",
            &[(1, -1), (2, 1)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("topk neg t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("topk_neg_output".to_string(), output_dict.clone());
        let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize output t1");
        assert_eq!(out1_materialized, HashMap::from([(1, -1)]));
    }
}
