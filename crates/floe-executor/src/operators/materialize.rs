use std::any::Any;
use std::sync::Arc;

use anyhow::{Result, bail};

use crate::checkpoint::MaterializedViewCheckpointEntry;
use crate::dbsp_bridge::DbspView;
use crate::encoding::encode_projected_row_key;
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
    pending_flush: bool,
}

impl MaterializeOperator {
    pub fn new(
        input: InputPort,
        view_name: impl Into<String>,
        registry: Arc<MaterializedViewRegistry>,
        sink: impl RowSink,
        dbsp: Option<DbspView>,
        checkpoint: Option<DbspPersistedState>,
    ) -> Self {
        let view = registry.register(view_name.into());
        if let Some(state) = checkpoint {
            view.set_dbsp_state(state);
        } else if let Some(ref dbsp_view) = dbsp {
            let latest = dbsp_view.latest_handle_view();
            let (dict, table, namespace, version) = latest.into_parts();
            view.set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));
        }
        Self {
            input,
            sink: Box::new(sink),
            view,
            dbsp,
            pending_flush: false,
        }
    }

    pub fn view(&self) -> Arc<MaterializedViewHandle> {
        Arc::clone(&self.view)
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }

    pub async fn flush_dbsp_if_needed(&mut self) -> Result<()> {
        if self.pending_flush {
            if let Some(dbsp) = &mut self.dbsp {
                dbsp.flush().await?;
                let view = dbsp.latest_handle_view();
                let (dict, table, namespace, version) = view.into_parts();
                self.view
                    .set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));
            }
            self.pending_flush = false;
        }
        Ok(())
    }

    pub async fn checkpoint_state(&mut self) -> Result<Option<MaterializedViewCheckpointEntry>> {
        self.flush_dbsp_if_needed().await?;
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

        let encoded_key = if self.dbsp.is_some() {
            Some(encode_projected_row_key(&row)?)
        } else {
            None
        };

        self.view.apply(row.clone(), diff);

        if let (Some(dbsp), Some(key)) = (self.dbsp.as_mut(), encoded_key) {
            dbsp.add_delta(key, diff);
        }

        self.sink.push(row, diff, timestamp)
    }

    fn on_watermark(&mut self, watermark: Timestamp) -> Result<()> {
        self.view.update_watermark(watermark);
        if self.dbsp.is_some() {
            self.pending_flush = true;
        }
        self.sink.watermark(watermark)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::*;
    use crate::materialized_view::MaterializedViewRegistry;
    use crate::operators::test_support::TestSink;
    use crate::stream_types::{InputPort, OperatorId, OutputPort};

    #[test]
    fn materializes_view_state() {
        let port = OutputPort::new(OperatorId(0), 0);
        let sink = TestSink::default();
        let registry = Arc::new(MaterializedViewRegistry::new());
        let mut op = MaterializeOperator::new(
            InputPort::new(port.operator, 0),
            "mv_q0",
            registry.clone(),
            sink,
            None,
            None,
        );

        let row = vec![ScalarValue::Int64(Some(1))];
        op.on_input(InputPort::new(port.operator, 0), row.clone(), 1, 1)
            .expect("insert");
        let view = registry.get("mv_q0").expect("view registered");
        assert_eq!(view.snapshot().get(&row), Some(&1));

        op.on_input(InputPort::new(port.operator, 0), row.clone(), -1, 2)
            .expect("delete");
        assert!(view.snapshot().is_empty());
        assert_eq!(view.watermark(), None);

        op.on_watermark(5).expect("watermark");
        assert_eq!(view.watermark(), Some(5));
        let sink = op.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(sink.watermarks, vec![5]);
    }
}
