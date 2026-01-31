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
use crate::stream::util::delta_zset_handle;

type TimestampExtractor<V, TS> = Arc<dyn Fn(&V) -> TS + Send + Sync>;

pub struct WaterlineOp<V, TS>
where
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    TS: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    TS::Archived: RkyvDeserialize<TS, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub state: RelationState<TS>,
    pub table: Arc<dyn KeyValueTable>,
    pub extractor: TimestampExtractor<V, TS>,
    output: VersionedZSet<TS>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    current: TS,
    emitted: bool,
}

impl<V, TS> WaterlineOp<V, TS>
where
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    TS: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    TS::Archived: RkyvDeserialize<TS, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(
        state: RelationState<TS>,
        table: Arc<dyn KeyValueTable>,
        extractor: TimestampExtractor<V, TS>,
        output: VersionedZSet<TS>,
        initial: TS,
    ) -> Self {
        Self {
            state,
            table,
            extractor,
            output,
            dict_cache: HashMap::new(),
            current: initial,
            emitted: false,
        }
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<TS>,
        deltas: &HashMap<TS, i64>,
        base: Option<u64>,
    ) -> Result<ZSetHandle>
    where
        TS: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        TS::Archived: RkyvDeserialize<TS, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
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
                .context("intern key while staging waterline delta")?;
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
            .context("schedule waterline update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write waterline update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear waterline intent")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<V, TS> DeltaOperator for WaterlineOp<V, TS>
where
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    TS: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    TS::Archived: RkyvDeserialize<TS, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("waterline requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for waterline")?;

        let mut max_ts: Option<TS> = None;
        for (row, _weight) in delta_values {
            let ts = (self.extractor)(&row);
            max_ts = Some(match max_ts {
                Some(current) => current.max(ts),
                None => ts,
            });
        }

        let new_current = match max_ts {
            Some(ts) if ts > self.current => ts,
            _ => self.current.clone(),
        };

        let mut updates = HashMap::new();
        if !self.emitted {
            updates.insert(new_current.clone(), 1);
            self.emitted = true;
        } else if new_current != self.current {
            updates.insert(self.current.clone(), -1);
            updates.insert(new_current.clone(), 1);
        }

        if updates.is_empty() {
            return Ok(None);
        }

        self.current = new_current;

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_integrated_handle =
            Self::apply_deltas_to_versioned(&mut self.state.integrated, &updates, base_version)
                .await
                .context("update waterline state")?;
        self.state.update_handle(new_integrated_handle);

        let delta_handle = Self::apply_deltas_to_versioned(&mut self.output, &updates, None)
            .await
            .context("persist waterline output")?;
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use crate::storage::dictionary::Dictionary;
    use crate::stream::runtime::DeltaOperator;
    use crate::stream::util::{compute_delta, materialize_zset_handle};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;

    type Row = i64;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("waterline", store).await.expect("open SlateDB"))
    }

    async fn stage_version<K>(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        deltas: &[(K, i64)],
    ) -> ZSetHandle
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern key for waterline test");
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

    #[tokio::test]
    async fn waterline_tracks_max_timestamp() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let input_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "waterline_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "waterline_output", None)
                .await
                .expect("output dict"),
        );

        let state = RelationState::empty(table.clone(), "waterline_state".to_string())
            .await
            .expect("waterline state");
        let output = VersionedZSet::new(output_dict.clone(), table.clone(), "waterline_output")
            .await
            .expect("output zset");

        let extractor = Arc::new(|row: &Row| *row);
        let mut op = WaterlineOp::new(state, table.clone(), extractor, output, 0i64);

        let deltas: Vec<Vec<(Row, i64)>> =
            vec![vec![(5, 1)], vec![(3, 1), (7, 1)], vec![(-1, 1)], vec![]];

        let mut prev_output: HashMap<i64, i64> = HashMap::new();
        let mut cache_out = HashMap::new();
        cache_out.insert("waterline_output".to_string(), output_dict.clone());

        for (step, delta) in deltas.iter().enumerate() {
            let max_ts = delta
                .iter()
                .filter(|(_, w)| *w > 0)
                .map(|(row, _)| *row)
                .max();
            let expected_current = match max_ts {
                Some(ts) => prev_output.keys().copied().max().unwrap_or(0).max(ts),
                None => prev_output.keys().copied().max().unwrap_or(0),
            };
            let mut expected_state = HashMap::new();
            expected_state.insert(expected_current, 1);
            let expected_delta: HashMap<i64, i64> = compute_delta(&prev_output, &expected_state)
                .into_iter()
                .collect();

            let handle = if delta.is_empty() {
                ZSetHandle {
                    ns: "waterline_input".to_string(),
                    version: 0,
                }
            } else {
                stage_version(input_dict.clone(), table.clone(), "waterline_input", delta).await
            };

            let out_handle = op
                .on_step(step as i64, &[handle])
                .await
                .expect("waterline step");

            if expected_delta.is_empty() {
                assert!(out_handle.is_none(), "expected empty output at step {step}");
            } else {
                let out_handle = out_handle.expect("output handle");
                let materialized =
                    materialize_zset_handle::<i64>(table.clone(), &mut cache_out, &out_handle)
                        .await
                        .expect("materialize output");
                assert_eq!(materialized, expected_delta, "step {step}");
            }

            prev_output = expected_state;
        }
    }
}
