use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use dbsp::handles::{ZSetHandle, ZSetHandleView};
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::util::{compute_delta, materialize_zset_handle};
use dbsp::stream::{Stream, StreamCursor};

use crate::checkpoint::{DbspHandleRecord, handle_kinds, record_if_nonzero};
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
    dbsp_view: Option<DbspView>,
    upstream: Stream<ZSetHandle>,
    cursor: StreamCursor<ZSetHandle>,
    table: Arc<dyn KeyValueTable>,
    dictionary_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
    prev_snapshot: HashMap<Vec<u8>, i64>,
    latest_persisted: Option<(String, u64)>,
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
        dbsp_view: Option<DbspView>,
        checkpoint: Option<DbspPersistedState>,
    ) -> Result<Self> {
        if dbsp_view.is_none() {
            return Err(anyhow!(
                "materialize operator requires a DBSP view to publish handles"
            ));
        }
        let view = registry.register(view_name.into());
        let mut prev_snapshot = HashMap::new();
        let mut latest_persisted = None;
        if let Some(state) = checkpoint {
            view.set_dbsp_state(state.clone());
            let snapshot = materialize_checkpoint(&state).await?;
            apply_snapshot_to_view(&view, &snapshot)?;
            prev_snapshot = snapshot;
            latest_persisted = Some((state.namespace().to_string(), state.version()));
        } else if let Some(ref view_state) = dbsp_view {
            let latest = view_state.latest_handle_view();
            let (dict, table, namespace, version) = latest.into_parts();
            if version > 0 {
                let state = DbspPersistedState::new(dict, table, namespace.clone(), version);
                view.set_dbsp_state(state.clone());
                let snapshot = materialize_checkpoint(&state).await?;
                apply_snapshot_to_view(&view, &snapshot)?;
                prev_snapshot = snapshot;
                latest_persisted = Some((namespace, version));
            }
        }
        let cursor = StreamCursor::new(upstream.clone());
        Ok(Self {
            input,
            sink: Box::new(sink),
            view,
            dbsp_view,
            upstream,
            cursor,
            table,
            dictionary_cache: HashMap::new(),
            prev_snapshot,
            latest_persisted,
        })
    }

    pub fn view(&self) -> Arc<MaterializedViewHandle> {
        Arc::clone(&self.view)
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }

    async fn publish_pending(&mut self) -> Result<()> {
        while self.upstream.current_time() > self.cursor.observed() {
            let (_ts, handle) = self.cursor.next().await?;
            self.replicate_handle(&handle).await?;
        }
        Ok(())
    }

    async fn replicate_handle(&mut self, handle: &ZSetHandle) -> Result<()> {
        let snapshot = materialize_zset_handle::<Vec<u8>>(
            self.table.clone(),
            &mut self.dictionary_cache,
            handle,
        )
        .await
        .with_context(|| {
            format!(
                "materialize upstream handle {}@{}",
                handle.ns, handle.version
            )
        })?;
        let deltas = compute_delta(&self.prev_snapshot, &snapshot);
        if deltas.is_empty() {
            self.prev_snapshot = snapshot;
            return Ok(());
        }
        self.apply_view_deltas(&deltas)?;
        self.persist_dbsp_state(deltas).await?;
        self.prev_snapshot = snapshot;
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

    async fn persist_dbsp_state(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let dbsp_view = self
            .dbsp_view
            .as_mut()
            .ok_or_else(|| anyhow!("materialize operator requires DBSP view for publishing"))?;
        dbsp_view.add_deltas(deltas);
        let mv_handle = dbsp_view.flush().await?;
        if mv_handle.version > 0 {
            let handle_view = dbsp_view.latest_handle_view();
            let (dict, table, namespace, version) = handle_view.into_parts();
            self.view.set_dbsp_state(DbspPersistedState::new(
                dict,
                table,
                namespace.clone(),
                version,
            ));
            self.latest_persisted = Some((namespace, version));
        }
        Ok(())
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

    fn checkpoint<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<DbspHandleRecord>>>> + Send + 'a>> {
        Box::pin(async move {
            self.publish_pending().await?;
            if let Some((namespace, version)) = &self.latest_persisted {
                if let Some(record) = record_if_nonzero(
                    handle_kinds::MATERIALIZED_VIEW,
                    self.view.name(),
                    namespace,
                    *version,
                ) {
                    return Ok(Some(vec![record]));
                }
            }
            Ok(None)
        })
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
        operator.checkpoint().await.expect("checkpoint state");

        let view = registry.get("mv_q0").expect("view registered");
        assert_eq!(view.snapshot().get(&first), Some(&1));

        upstream.add_delta(encode_projected_row_key(&first).expect("encode first"), -1);
        let second = row(&[2]);
        upstream.add_delta(encode_projected_row_key(&second).expect("encode second"), 1);
        upstream.flush().await.expect("flush second");
        operator.checkpoint().await.expect("checkpoint state");

        assert!(view.snapshot().get(&first).is_none());
        assert_eq!(view.snapshot().get(&second), Some(&1));
    }

    #[tokio::test]
    async fn skips_zero_version_state_updates() {
        let port = OutputPort::new(OperatorId(0), 0);
        let sink = NullSink::default();
        let registry = Arc::new(MaterializedViewRegistry::new());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("materialize-zero", store).await.expect("open db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let upstream_ns =
            namespaces::operator_state("materialize_zero", 0, "upstream").expect("ns");
        let upstream = bridge
            .new_stream(upstream_ns, StreamRetention::KeepLast { keep_last: 1 })
            .await
            .expect("upstream stream");
        let upstream_stream = upstream.handle_stream();
        let table = bridge.table();
        let dbsp_view = bridge.new_view("mv_zero").await.expect("mv view");
        let mut operator = MaterializeOperator::new(
            InputPort::new(port.operator, 0),
            "mv_zero",
            registry.clone(),
            sink,
            upstream_stream,
            table,
            Some(dbsp_view),
            None,
        )
        .await
        .expect("materialize operator");

        let checkpoint = operator.checkpoint().await.expect("checkpoint state");
        assert!(checkpoint.is_none(), "v0 handles must be skipped");
        let view = registry.get("mv_zero").expect("view registered");
        assert!(view.dbsp_state().is_none(), "registry should remain empty");
    }
}
