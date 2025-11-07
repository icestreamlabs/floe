use std::any::Any;
use std::sync::Arc;

use anyhow::{Result, bail};

use crate::materialized_view::{MaterializedViewHandle, MaterializedViewRegistry};
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct MaterializeOperator {
    input: InputPort,
    sink: Box<dyn RowSink>,
    view: Arc<MaterializedViewHandle>,
}

impl MaterializeOperator {
    pub fn new(
        input: InputPort,
        view_name: impl Into<String>,
        registry: Arc<MaterializedViewRegistry>,
        sink: impl RowSink,
    ) -> Self {
        let view = registry.register(view_name.into());
        Self {
            input,
            sink: Box::new(sink),
            view,
        }
    }

    pub fn view(&self) -> Arc<MaterializedViewHandle> {
        Arc::clone(&self.view)
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
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

        self.view.apply(row.clone(), diff);
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
