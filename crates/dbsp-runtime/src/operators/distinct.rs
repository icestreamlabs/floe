use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

pub struct DistinctOp<K>
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
    pub(crate) state: RelationState<K>,
    pub(crate) table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    integrated_cache: Option<HashMap<K, i64>>,
    append_only_input: bool,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K> DistinctOp<K>
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
    pub fn new(
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
    ) -> Self {
        Self {
            state,
            table,
            output,
            dict_cache: HashMap::new(),
            integrated_cache: None,
            append_only_input: false,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn enable_append_only_input(&mut self) {
        self.append_only_input = true;
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn ensure_integrated_cache(&mut self) -> Result<usize> {
        if self.integrated_cache.is_some() {
            return Ok(0);
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize integrated state for distinct cache")?;
        let rebuild_rows = materialized.len();
        self.integrated_cache = Some(materialized);
        Ok(rebuild_rows)
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<K>,
        deltas: &HashMap<K, i64>,
        base: Option<u64>,
        state_label: &'static str,
    ) -> Result<ZSetHandle> {
        let mut keyed_deltas: Vec<(&K, i64)> = Vec::new();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            keyed_deltas.push((key, *delta));
        }

        if keyed_deltas.is_empty() {
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
            let batch = Arc::new(
                keyed_deltas
                    .iter()
                    .map(|(key, delta)| ((*key).clone(), *delta))
                    .collect(),
            );
            return Ok(versioned.publish_replayable_batch(batch));
        }

        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let mut dict_batch = dict.batch();
        for (key, delta) in keyed_deltas {
            let id = dict_batch
                .intern(key)
                .await
                .context("intern key while staging distinct delta")?;
            buckets.entry(bucket_for(id)).or_default().push((id, delta));
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

        let persist_start = std::time::Instant::now();
        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule distinct version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write distinct version update")?;

        versioned.apply_version_plan(&plan);
        metrics::observe_operator_persistence_latency_ms(
            "distinct",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K> DeltaOperator for DistinctOp<K>
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
            .context("distinct operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle_batch::<K>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for distinct")?;
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(delta_values.len());

        if delta_values.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let rebuild_rows = self
            .ensure_integrated_cache()
            .await
            .context("load distinct cache")?;
        if rebuild_rows != 0 {
            work.cache_rebuild_rows = work.cache_rebuild_rows.saturating_add(rebuild_rows as u64);
            work.state_scan_rows = work.state_scan_rows.saturating_add(rebuild_rows as u64);
            work.state_full_scan_count = work.state_full_scan_count.saturating_add(1);
        }

        let mut delta_map = HashMap::new();
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
        work.state_lookup_keys = work
            .state_lookup_keys
            .saturating_add(delta_map.len() as u64);
        work.distinct_aux_rows_examined = work
            .distinct_aux_rows_examined
            .saturating_add(delta_map.len() as u64);

        let mut h_deltas = HashMap::new();
        let mut cache_updates = Vec::new();
        {
            let integrated_map = self
                .integrated_cache
                .as_ref()
                .context("integrated cache missing for distinct")?;
            for (key, diff_weight) in &delta_map {
                if self.append_only_input && *diff_weight < 0 {
                    anyhow::bail!("append-only distinct received negative input weight");
                }
                let state_weight = integrated_map.get(key).copied().unwrap_or(0);
                let coalesced = diff_weight + state_weight;
                if state_weight > 0 && coalesced <= 0 {
                    h_deltas.insert(key.clone(), -1);
                } else if state_weight <= 0 && coalesced > 0 {
                    h_deltas.insert(key.clone(), 1);
                }
                let cached_weight =
                    if self.append_only_input && (state_weight > 0 || *diff_weight > 0) {
                        1
                    } else {
                        coalesced
                    };
                cache_updates.push((key.clone(), cached_weight));
            }
        }

        let state_deltas = if self.append_only_input {
            &h_deltas
        } else {
            &delta_map
        };
        work.record_persisted_rows(state_deltas.len());
        let base_version = self.state.base_version_for_update();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            state_deltas,
            base_version,
            "integrated_input",
        )
        .await
        .context("update integrated state for distinct")?;
        self.state.update_handle(new_integrated_handle);

        if let Some(integrated_map) = self.integrated_cache.as_mut() {
            for (key, weight) in cache_updates {
                if weight == 0 {
                    integrated_map.remove(&key);
                } else {
                    integrated_map.insert(key, weight);
                }
            }
        }

        if h_deltas.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.record_output_delta_rows(h_deltas.len());
        work.record_persisted_rows(h_deltas.len());

        let h_handle = Self::apply_deltas_to_versioned(&mut self.output, &h_deltas, None, "output")
            .await
            .context("persist distinct H output")?;
        publish_transient_zset_batch(
            &h_handle,
            Arc::new(h_deltas.into_iter().collect::<Vec<_>>()),
        );
        self.logical_work.finish_tick(work);
        Ok(Some(h_handle))
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
    use crate::collections::zset::SegmentRecord;
    use crate::stream::util::{compute_delta, materialize_zset_handle};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;

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
                .expect("intern test key for distinct");
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
    use std::collections::HashMap;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("distinct", store).await.expect("open SlateDB"))
    }

    fn apply_deltas(state: &mut HashMap<String, i64>, deltas: &[(String, i64)]) {
        for (key, delta) in deltas {
            let entry = state.entry(key.clone()).or_insert(0);
            *entry += *delta;
            if *entry == 0 {
                state.remove(key);
            }
        }
    }

    #[tokio::test]
    async fn distinct_operator_tracks_membership_changes() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let delta_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "distinct_delta", None)
                .await
                .expect("build delta dictionary"),
        );
        let state_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "distinct_state", None)
                .await
                .expect("build state dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "distinct_output", None)
                .await
                .expect("build output dictionary"),
        );

        let integrated = VersionedZSet::new(
            state_dict.clone(),
            table.clone(),
            "distinct_state".to_string(),
        )
        .await
        .expect("integrated state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "distinct_output".to_string(),
        )
        .await
        .expect("output state");
        let initial_handle = ZSetHandle {
            ns: integrated.namespace().to_string(),
            version: 0,
        };
        let state = RelationState {
            integrated,
            latest_handle: initial_handle,
        };

        let mut op = DistinctOp::new(state, table.clone(), output);

        let first_delta = stage_version(
            delta_dict.clone(),
            table.clone(),
            "distinct_delta",
            &[("a".to_string(), 1)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("run distinct t1")
            .expect("non-empty output t1");

        let mut cache = HashMap::new();
        cache.insert("distinct_output".to_string(), output_dict.clone());
        let out1_materialized = materialize_zset_handle::<String>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize output t1");
        let integrated_after_t1 = op
            .state
            .integrated
            .materialize()
            .await
            .expect("integrated t1");
        assert_eq!(out1_materialized.get("a"), Some(&1));
        assert_eq!(integrated_after_t1.get("a"), Some(&1));

        let second_delta = stage_version(
            delta_dict.clone(),
            table.clone(),
            "distinct_delta",
            &[("a".to_string(), -1), ("b".to_string(), 1)],
        )
        .await;
        let out2 = op
            .on_step(2, &[second_delta])
            .await
            .expect("run distinct t2")
            .expect("non-empty output t2");

        let out2_materialized = materialize_zset_handle::<String>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize output t2");
        let integrated_after_t2 = op
            .state
            .integrated
            .materialize()
            .await
            .expect("integrated t2");

        let mut expected_out2 = HashMap::new();
        expected_out2.insert("a".to_string(), -1);
        expected_out2.insert("b".to_string(), 1);
        assert_eq!(out2_materialized, expected_out2);
        assert_eq!(integrated_after_t2.get("a"), None);
        assert_eq!(integrated_after_t2.get("b"), Some(&1));
    }

    #[tokio::test]
    async fn append_only_distinct_persists_membership_not_multiplicity() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let delta_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "append_distinct_delta", None)
                .await
                .expect("build delta dictionary"),
        );
        let state_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "append_distinct_state", None)
                .await
                .expect("build state dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "append_distinct_output", None)
                .await
                .expect("build output dictionary"),
        );
        let integrated = VersionedZSet::new(
            state_dict.clone(),
            table.clone(),
            "append_distinct_state".to_string(),
        )
        .await
        .expect("integrated state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "append_distinct_output".to_string(),
        )
        .await
        .expect("output state");
        let state = RelationState {
            latest_handle: ZSetHandle {
                ns: integrated.namespace().to_string(),
                version: 0,
            },
            integrated,
        };
        let mut op = DistinctOp::new(state, table.clone(), output);
        op.enable_append_only_input();

        let first_delta = stage_version(
            delta_dict.clone(),
            table.clone(),
            "append_distinct_delta",
            &[("a".to_string(), 3)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("run append-only distinct t1")
            .expect("output t1");
        let mut cache = HashMap::new();
        cache.insert("append_distinct_output".to_string(), output_dict.clone());
        let out1_materialized = materialize_zset_handle::<String>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize output t1");
        assert_eq!(out1_materialized, HashMap::from([("a".to_string(), 1)]));
        assert_eq!(
            op.state
                .integrated
                .materialize()
                .await
                .expect("integrated t1"),
            HashMap::from([("a".to_string(), 1)])
        );

        let duplicate_delta = stage_version(
            delta_dict,
            table.clone(),
            "append_distinct_delta",
            &[("a".to_string(), 2)],
        )
        .await;
        let out2 = op
            .on_step(2, &[duplicate_delta])
            .await
            .expect("run append-only distinct t2")
            .expect("empty output t2");
        assert_eq!(out2.version, 0);
        assert_eq!(
            op.state
                .integrated
                .materialize()
                .await
                .expect("integrated t2"),
            HashMap::from([("a".to_string(), 1)])
        );
    }

    #[tokio::test]
    async fn distinct_operator_matches_full_recompute() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let delta_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "distinct_recompute_delta", None)
                .await
                .expect("build delta dictionary"),
        );
        let state_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "distinct_recompute_state", None)
                .await
                .expect("build state dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "distinct_recompute_output", None)
                .await
                .expect("build output dictionary"),
        );

        let integrated = VersionedZSet::new(
            state_dict.clone(),
            table.clone(),
            "distinct_recompute_state".to_string(),
        )
        .await
        .expect("integrated state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "distinct_recompute_output".to_string(),
        )
        .await
        .expect("output state");
        let initial_handle = ZSetHandle {
            ns: "distinct_recompute_state".to_string(),
            version: 0,
        };
        let state = RelationState {
            integrated,
            latest_handle: initial_handle,
        };

        let mut op = DistinctOp::new(state, table.clone(), output);

        let steps = vec![
            vec![("a".to_string(), 1), ("b".to_string(), 2)],
            vec![
                ("a".to_string(), -1),
                ("b".to_string(), -2),
                ("c".to_string(), 1),
            ],
        ];

        let mut full_input: HashMap<String, i64> = HashMap::new();
        let mut full_distinct: HashMap<String, i64> = HashMap::new();

        for (idx, deltas) in steps.into_iter().enumerate() {
            let delta_handle = stage_version(
                delta_dict.clone(),
                table.clone(),
                "distinct_recompute_delta",
                &deltas,
            )
            .await;
            let output_handle = op
                .on_step(idx as i64 + 1, &[delta_handle])
                .await
                .expect("run distinct step");

            apply_deltas(&mut full_input, &deltas);
            let mut recompute_distinct = HashMap::new();
            for (key, weight) in &full_input {
                if *weight > 0 {
                    recompute_distinct.insert(key.clone(), 1);
                }
            }

            let expected_delta_vec = compute_delta(&full_distinct, &recompute_distinct);
            let expected_delta: HashMap<String, i64> = expected_delta_vec.into_iter().collect();

            if let Some(handle) = output_handle {
                let mut cache = HashMap::new();
                cache.insert("distinct_recompute_output".to_string(), output_dict.clone());
                let actual_delta =
                    materialize_zset_handle::<String>(table.clone(), &mut cache, &handle)
                        .await
                        .expect("materialize distinct output");
                assert_eq!(actual_delta, expected_delta);
            } else {
                assert!(expected_delta.is_empty());
            }

            let integrated_after = op
                .state
                .integrated
                .materialize()
                .await
                .expect("materialize integrated");
            assert_eq!(integrated_after, full_input);

            full_distinct = recompute_distinct;
        }
    }

    async fn run_distinct_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let delta_ns = format!("distinct_history_delta_{history_rows}");
        let delta_dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), delta_ns.clone(), None)
                .await
                .expect("build delta dictionary"),
        );
        let state_dict = Arc::new(
            Dictionary::<String>::with_table(
                table.clone(),
                format!("distinct_history_state_{history_rows}"),
                None,
            )
            .await
            .expect("build state dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<String>::with_table(
                table.clone(),
                format!("distinct_history_output_{history_rows}"),
                None,
            )
            .await
            .expect("build output dictionary"),
        );

        let integrated = VersionedZSet::new(
            state_dict,
            table.clone(),
            format!("distinct_history_state_{history_rows}"),
        )
        .await
        .expect("integrated state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            format!("distinct_history_output_{history_rows}"),
        )
        .await
        .expect("output state");
        let state = RelationState {
            latest_handle: ZSetHandle {
                ns: format!("distinct_history_state_{history_rows}"),
                version: 0,
            },
            integrated,
        };
        let mut op = DistinctOp::new(state, table.clone(), output);

        let history = (0..history_rows)
            .map(|idx| (format!("history-{idx}"), 1))
            .collect::<Vec<_>>();
        let seed = stage_version(delta_dict.clone(), table.clone(), &delta_ns, &history).await;
        op.on_step(1, &[seed]).await.expect("seed distinct");

        let fixed = stage_version(
            delta_dict,
            table.clone(),
            &delta_ns,
            &[("needle".to_string(), 1)],
        )
        .await;
        let output_handle = op
            .on_step(2, &[fixed])
            .await
            .expect("fixed distinct")
            .expect("distinct output");
        let mut cache = HashMap::new();
        cache.insert(
            format!("distinct_history_output_{history_rows}"),
            output_dict,
        );
        let materialized = materialize_zset_handle::<String>(table, &mut cache, &output_handle)
            .await
            .expect("materialize fixed distinct");
        assert_eq!(materialized, HashMap::from([("needle".to_string(), 1)]));
        op.last_logical_work()
    }

    #[tokio::test]
    async fn distinct_logical_work_uses_changed_values_not_unrelated_history() {
        let baseline = run_distinct_history_probe(8).await;
        for history_rows in [128, 1024] {
            let actual = run_distinct_history_probe(history_rows).await;
            assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
            assert_eq!(actual.state_lookup_keys, baseline.state_lookup_keys);
            assert_eq!(
                actual.distinct_aux_rows_examined,
                baseline.distinct_aux_rows_examined
            );
            assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
            assert_eq!(actual.state_scan_rows, 0);
            assert_eq!(actual.state_full_scan_count, 0);
            assert_eq!(actual.cache_rebuild_rows, 0);
        }

        assert_eq!(baseline.input_delta_rows, 1);
        assert_eq!(baseline.state_lookup_keys, 1);
        assert_eq!(baseline.distinct_aux_rows_examined, 1);
        assert_eq!(baseline.output_delta_rows, 1);
    }
}
