use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use dbsp::collections::zset::{SegmentRecord, VersionedZSet};
use dbsp::handles::ZSetHandle;
use dbsp::relation_state::RelationState;
use dbsp::storage::KeyValueTable;
use dbsp::stream::runtime::DeltaOperator;
use dbsp::stream::util::materialize_zset_handle;
use dbsp::storage::dictionary::Dictionary;

use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};

pub struct MvSinkOp {
    pub state: RelationState<Vec<u8>>,
    pub view_name: String,
    pub registry: Arc<MaterializedViewRegistry>,
    pub table: Arc<dyn KeyValueTable>,
    dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
}

impl MvSinkOp {
    pub fn new(
        state: RelationState<Vec<u8>>,
        view_name: impl Into<String>,
        registry: Arc<MaterializedViewRegistry>,
        table: Arc<dyn KeyValueTable>,
    ) -> Self {
        Self {
            state,
            view_name: view_name.into(),
            registry,
            table,
            dict_cache: HashMap::new(),
        }
    }

    async fn apply_deltas_to_versioned(
        dictionary: Arc<Dictionary<Vec<u8>>>,
        versioned: &mut VersionedZSet<Vec<u8>>,
        deltas: &HashMap<Vec<u8>, i64>,
    ) -> Result<ZSetHandle> {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dictionary.batch();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            let id = dict_batch
                .intern(key)
                .await
                .context("intern key while staging mv sink delta")?;
            buckets.entry(bucket_for(id)).or_default().push((id, *delta));
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
            if let Some(handle) = versioned.current_handle() {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let base = versioned.current_handle().map(|h| h.version);
        let new_version = versioned
            .create_version_with_base(segments, base)
            .await
            .context("create mv sink version")?;
        Ok(versioned.handle_for_version(new_version))
    }
}

#[async_trait]
impl DeltaOperator for MvSinkOp {
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("mv sink operator requires one input delta handle")?;

        let delta_map = materialize_zset_handle::<Vec<u8>>(
            self.table.clone(),
            &mut self.dict_cache,
            &delta_handle,
        )
        .await
        .context("materialize mv sink delta")?;

        if delta_map.is_empty() {
            return Ok(None);
        }

        let dict = self.state.dictionary();
        let new_handle = Self::apply_deltas_to_versioned(dict, &mut self.state.integrated, &delta_map)
            .await
            .context("update materialized view state")?;
        self.state.update_handle(new_handle.clone());

        let view = self
            .registry
            .get(&self.view_name)
            .context("materialized view not registered")?;

        let persisted = DbspPersistedState::new(
            self.state.dictionary(),
            self.table.clone(),
            new_handle.ns.clone(),
            new_handle.version,
        );
        view.set_dbsp_state(persisted);

        Ok(None)
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::{StreamRetention, ZSetStream};
    use object_store::memory::InMemory;
    use slatedb::Db;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("mv_sink", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn mv_sink_updates_registry_state() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(dbsp::storage::SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::<Vec<u8>>::with_table(table.clone(), "mv_sink_input", None)
                .await
                .expect("build dictionary"),
        );

        let mut delta_stream = ZSetStream::new(
            dict.clone(),
            table.clone(),
            "mv_sink_input".to_string(),
            StreamRetention::KeepLast { keep_last: 2 },
        )
        .await
        .expect("delta stream");

        let integrated = VersionedZSet::new(
            dict.clone(),
            table.clone(),
            "mv_sink_state".to_string(),
        )
        .await
        .expect("integrated state");
        let state = RelationState {
            integrated,
            latest_handle: ZSetHandle {
                ns: "mv_sink_state".to_string(),
                version: 0,
            },
        };
        let registry = Arc::new(MaterializedViewRegistry::new());
        let view_handle = registry.register("mv_sink_view");

        let mut op = MvSinkOp::new(state, "mv_sink_view", registry.clone(), table.clone());

        delta_stream.add_delta(b"a".to_vec(), 1);
        let delta_handle = delta_stream.flush().await.expect("flush t1");

        op.on_step(1, &[delta_handle])
            .await
            .expect("run mv sink");

        let persisted = view_handle.dbsp_state().expect("persisted state");
        assert_eq!(persisted.namespace(), "mv_sink_state");
        assert_eq!(persisted.version(), 1);
    }
}
