use anyhow::{Result, bail};

use crate::dataflow_plan::Expr;
use crate::expr_eval::evaluate;
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct MapOperator<S: RowSink> {
    input: InputPort,
    expressions: Vec<Expr>,
    sink: S,
}

impl<S: RowSink> MapOperator<S> {
    pub fn new(input: InputPort, expressions: Vec<Expr>, sink: S) -> Self {
        Self {
            input,
            expressions,
            sink,
        }
    }

    #[cfg(test)]
    pub fn sink(&self) -> &S {
        &self.sink
    }
}

impl<S: RowSink> StreamOperator for MapOperator<S> {
    fn on_input(
        &mut self,
        input: InputPort,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()> {
        if input != self.input {
            bail!(
                "map operator received input from unexpected port: {:?}",
                input
            );
        }

        let mut projected = Vec::with_capacity(self.expressions.len());
        for expr in &self.expressions {
            projected.push(evaluate(expr, &row)?);
        }
        self.sink.push(projected, diff, timestamp)
    }

    fn on_watermark(&mut self, watermark: Timestamp) -> Result<()> {
        self.sink.watermark(watermark)
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
    fn projects_rows() {
        let input = OutputPort::new(OperatorId(0), 0);
        let sink = TestSink::default();
        let mut operator = MapOperator::new(
            InputPort::new(input.operator, input.port_index),
            vec![
                Expr::column(0),
                Expr::Add(
                    Box::new(Expr::column(1)),
                    Box::new(Expr::literal(ScalarValue::Int64(Some(1)))),
                ),
            ],
            sink,
        );

        let row = vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(5))];
        operator
            .on_input(InputPort::new(input.operator, 0), row.clone(), 1, 1)
            .expect("map input");
        assert_eq!(operator.sink().rows.len(), 1);
        assert_eq!(operator.sink().rows[0].0[0], row[0]);
        assert_eq!(operator.sink().rows[0].0[1], ScalarValue::Int64(Some(6)));
    }
}
