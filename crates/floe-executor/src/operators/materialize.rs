use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use dbsp::handles::{ZSetHandle, ZSetHandleView};
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::Stream;

use crate::checkpoint::MaterializedViewCheckpointEntry;
use crate::dbsp_bridge::DbspView;
use crate::encoding::decode_projected_row_key;
use crate::materialized_view::{
    DbspPersistedState, MaterializedViewHandle, MaterializedViewRegistry,
};
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct MaterializeOperator {
    input: InputPort,
    sink: Box<dyn RowSink>,
    view: Arc<MaterializedViewHandle>,
    dbsp: Option<DbspView>,
    upstream: Stream<ZSetHandle>,
    table: Arc<dyn KeyValueTable>,
    dictionary_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
    prev_snapshot: HashMap<Vec<u8>, i64>,
    last_published_ts: Option<i64>,
}

impl MaterializeOperator {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        input: InputPort,
        view_name: impl Into<String>,
        registry: Arc<MaterializedViewRegistry>,
        sink: impl RowSink,
        upstream: Stream<ZSetHandle>,
        table: Arc<dyn KeyValueTable>,
        dbsp: Option<DbspView>,
        checkpoint: Option<DbspPersistedState>,
    ) -> Result<Self> {
        if dbsp.is_none() {
            return Err(anyhow!(
                "materialize operator requires a DBSP view to publish handles"
            ));
        }
        let view = registry.register(view_name.into());
        let mut prev_snapshot = HashMap::new();
        if let Some(state) = checkpoint {
            view.set_dbsp_state(state.clone());
            let snapshot = materialize_checkpoint(&state).await?;
            apply_snapshot_to_view(&view, &snapshot)?;
            prev_snapshot = snapshot;
        } else if let Some(ref dbsp_view) = dbsp {
            let latest = dbsp_view.latest_handle_view();
            let (dict, table, namespace, version) = latest.into_parts();
            view.set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));
        }
        Ok(Self {
            input,
            sink: Box::new(sink),
            view,
            dbsp,
            upstream,
            table,
            dictionary_cache: HashMap::new(),
            prev_snapshot,
            last_published_ts: None,
        })
    }

    pub fn view(&self) -> Arc<MaterializedViewHandle> {
        Arc::clone(&self.view)
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }

    pub async fn checkpoint_state(&mut self) -> Result<Option<MaterializedViewCheckpointEntry>> {
        self.publish_pending().await?;
        if self.dbsp.is_none() {
            return Ok(None);
        }
        if let Some(state) = self.view.dbsp_state() {
            Ok(Some(MaterializedViewCheckpointEntry {
                view: self.view.name().to_string(),
                namespace: state.namespace().to_string(),
                version: state.version(),
            }))
        } else {
            Ok(None)
        }
    }

    async fn publish_pending(&mut self) -> Result<()> {
        let mut next_ts = self.last_published_ts.unwrap_or(0) + 1;
        let frontier = self.upstream.current_time();
        while next_ts <= frontier {
            let handle = self
                .upstream
                .get(next_ts)
                .await
                .with_context(|| format!("load upstream handle at ts {next_ts}"))?;
            self.replicate_handle(next_ts, handle).await?;
            self.last_published_ts = Some(next_ts);
            next_ts += 1;
        }
        Ok(())
    }

    async fn replicate_handle(&mut self, ts: i64, handle: ZSetHandle) -> Result<()> {
        let snapshot = self
            .materialize_upstream_handle(&handle)
            .await
            .with_context(|| {
                format!(
                    "materialize upstream handle {}@{}",
                    handle.ns, handle.version
                )
            })?;
        let deltas = compute_delta(&self.prev_snapshot, &snapshot);
        self.apply_view_deltas(&deltas)?;
        let dbsp_view = self
            .dbsp
            .as_mut()
            .ok_or_else(|| anyhow!("materialize operator requires DBSP view for publishing"))?;
        for (key, diff) in &deltas {
            if *diff == 0 {
                continue;
            }
            dbsp_view.add_delta(key.clone(), *diff);
        }
        dbsp_view.flush().await?;
        let view_handle = dbsp_view.latest_handle_view();
        let (dict, table, namespace, version) = view_handle.into_parts();
        self.view
            .set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));
        self.prev_snapshot = snapshot;
        self.last_published_ts = Some(ts);
        Ok(())
    }

    fn apply_view_deltas(&self, deltas: &[(Vec<u8>, i64)]) -> Result<()> {
        for (key, diff) in deltas {
            if *diff == 0 {
                continue;
            }
            let row =
                decode_projected_row_key(key).context("decode row while applying MV delta")?;
            self.view.apply(row, *diff);
        }
        Ok(())
    }

    async fn materialize_upstream_handle(
        &mut self,
        handle: &ZSetHandle,
    ) -> Result<HashMap<Vec<u8>, i64>> {
        let dict = if let Some(existing) = self.dictionary_cache.get(&handle.ns) {
            existing.clone()
        } else {
            let dictionary = Arc::new(
                Dictionary::with_table(self.table.clone(), handle.ns.clone(), None).await?,
            );
            self.dictionary_cache
                .insert(handle.ns.clone(), dictionary.clone());
            dictionary
        };
        let view = ZSetHandleView::new(dict, self.table.clone(), handle.ns.clone(), handle.version);
        let mut snapshot = view
            .materialize()
            .await
            .context("materialize upstream handle contents")?;
        snapshot.retain(|_, diff| *diff != 0);
        Ok(snapshot)
    }
}

impl StreamOperator for MaterializeOperator {
    fn on_input(
        &mut self,
        input: InputPort,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()> {
        if input != self.input {
            bail!(
                "materialize operator for view {} received unexpected input",
                self.view.name()
            );
        }

        if diff == 0 {
            return Ok(());
        }

        // Row-level data is unused in the handle-driven path but we still
        // route it through the sink so tests can observe propagated events.
        self.sink.push(row, diff, timestamp)
    }

    fn on_watermark(&mut self, watermark: Timestamp) -> Result<()> {
        self.view.update_watermark(watermark);
        self.sink.watermark(watermark)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

async fn materialize_checkpoint(state: &DbspPersistedState) -> Result<HashMap<Vec<u8>, i64>> {
    let handle_view = ZSetHandleView::new(
        state.dictionary(),
        state.table(),
        state.namespace().to_string(),
        state.version(),
    );
    handle_view
        .materialize()
        .await
        .context("materialize persisted MV snapshot")
}

fn apply_snapshot_to_view(
    view: &Arc<MaterializedViewHandle>,
    snapshot: &HashMap<Vec<u8>, i64>,
) -> Result<()> {
    for (key, diff) in snapshot {
        if *diff == 0 {
            continue;
        }
        let row = decode_projected_row_key(key)
            .context("decode checkpoint row when hydrating MV view")?;
        view.apply(row, *diff);
    }
    Ok(())
}

fn compute_delta(
    previous: &HashMap<Vec<u8>, i64>,
    next: &HashMap<Vec<u8>, i64>,
) -> Vec<(Vec<u8>, i64)> {
    let mut deltas = Vec::new();
    for (key, next_weight) in next {
        let prev_weight = previous.get(key).copied().unwrap_or(0);
        if *next_weight != prev_weight {
            deltas.push((key.clone(), next_weight - prev_weight));
        }
    }
    for (key, prev_weight) in previous {
        if !next.contains_key(key) && *prev_weight != 0 {
            deltas.push((key.clone(), -*prev_weight));
        }
    }
    deltas.retain(|(_, delta)| *delta != 0);
    deltas
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use dbsp::StreamRetention;
    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::Db;

    use super::*;
    use crate::dbsp_bridge::DbspBridge;
    use crate::encoding::encode_projected_row_key;
    use crate::materialized_view::MaterializedViewRegistry;
    use crate::namespaces;
    use crate::operators::NullSink;
    use crate::stream_types::{InputPort, OperatorId, OutputPort};

    fn row(values: &[i64]) -> Row {
        values
            .iter()
            .map(|v| ScalarValue::Int64(Some(*v)))
            .collect()
    }

    #[tokio::test]
    async fn publishes_materialized_view_handles() {
        let port = OutputPort::new(OperatorId(0), 0);
        let sink = NullSink::default();
        let registry = Arc::new(MaterializedViewRegistry::new());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("materialize-test", store).await.expect("open db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let upstream_ns =
            namespaces::operator_state("materialize_test", 0, "upstream").expect("ns");
        let mut upstream = bridge
            .new_stream(upstream_ns, StreamRetention::KeepLast { keep_last: 1 })
            .await
            .expect("upstream stream");
        let upstream_stream = upstream.handle_stream();
        let table = bridge.table();
        let dbsp_view = bridge.new_view("mv_q0").await.expect("mv view");
        let mut operator = MaterializeOperator::new(
            InputPort::new(port.operator, 0),
            "mv_q0",
            registry.clone(),
            sink,
            upstream_stream,
            table,
            Some(dbsp_view),
            None,
        )
        .await
        .expect("materialize operator");

        let first = row(&[1]);
        upstream.add_delta(encode_projected_row_key(&first).expect("encode first"), 1);
        upstream.flush().await.expect("flush first");
        operator.checkpoint_state().await.expect("checkpoint state");

        let view = registry.get("mv_q0").expect("view registered");
        assert_eq!(view.snapshot().get(&first), Some(&1));

        upstream.add_delta(encode_projected_row_key(&first).expect("encode first"), -1);
        let second = row(&[2]);
        upstream.add_delta(encode_projected_row_key(&second).expect("encode second"), 1);
        upstream.flush().await.expect("flush second");
        operator.checkpoint_state().await.expect("checkpoint state");

        assert!(view.snapshot().get(&first).is_none());
        assert_eq!(view.snapshot().get(&second), Some(&1));
    }
}
