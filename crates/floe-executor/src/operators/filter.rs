use std::any::Any;

use anyhow::{Result, bail};

use crate::dataflow_plan::Expr;
use crate::expr_eval::evaluate_bool;
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct FilterOperator {
    input: InputPort,
    predicate: Expr,
    sink: Box<dyn RowSink>,
}

impl FilterOperator {
    pub fn new(input: InputPort, predicate: Expr, sink: impl RowSink) -> Self {
        Self {
            input,
            predicate,
            sink: Box::new(sink),
        }
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }
}

impl StreamOperator for FilterOperator {
    fn on_input(
        &mut self,
        input: InputPort,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()> {
        if input != self.input {
            bail!("filter received input from unexpected port: {:?}", input);
        }

        if evaluate_bool(&self.predicate, &row)? {
            self.sink.push(row, diff, timestamp)
        } else {
            Ok(())
        }
    }

    fn on_watermark(&mut self, watermark: Timestamp) -> Result<()> {
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
    use crate::dataflow_plan::Expr;
    use crate::operators::test_support::TestSink;
    use crate::stream_types::{InputPort, OperatorId, OutputPort};

    #[test]
    fn filters_rows() {
        let port = OutputPort::new(OperatorId(0), 0);
        let sink = TestSink::default();
        let predicate = Expr::Eq(
            Box::new(Expr::column(0)),
            Box::new(Expr::literal(ScalarValue::Int64(Some(42)))),
        );
        let mut operator = FilterOperator::new(InputPort::new(port.operator, 0), predicate, sink);

        let accepted = vec![ScalarValue::Int64(Some(42))];
        operator
            .on_input(InputPort::new(port.operator, 0), accepted.clone(), 1, 1)
            .expect("filter pass");
        let test_sink = operator.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(test_sink.rows.len(), 1);

        let rejected = vec![ScalarValue::Int64(Some(0))];
        operator
            .on_input(InputPort::new(port.operator, 0), rejected, 1, 1)
            .expect("filter drop");
        let test_sink = operator.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(test_sink.rows.len(), 1, "second row filtered out");
        assert_eq!(test_sink.rows[0].0, accepted);
    }
}
