use std::any::Any;
use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use datafusion::scalar::ScalarValue;

use crate::dataflow_plan::Expr;
use crate::expr_eval::evaluate;
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

#[derive(Clone)]
struct StoredRow {
    row: Row,
    multiplicity: Diff,
}

pub struct JoinOperator {
    left_input: InputPort,
    right_input: InputPort,
    join_keys: Vec<(usize, usize)>,
    projection: Vec<Expr>,
    sink: Box<dyn RowSink>,
    left_state: HashMap<Vec<ScalarValue>, Vec<StoredRow>>,
    right_state: HashMap<Vec<ScalarValue>, Vec<StoredRow>>,
}

impl JoinOperator {
    pub fn new(
        left_input: InputPort,
        right_input: InputPort,
        join_keys: Vec<(usize, usize)>,
        projection: Vec<Expr>,
        sink: impl RowSink,
    ) -> Self {
        Self {
            left_input,
            right_input,
            join_keys,
            projection,
            sink: Box::new(sink),
            left_state: HashMap::new(),
            right_state: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }

    fn handle_row(
        &mut self,
        side: JoinSide,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()> {
        if diff == 0 {
            return Ok(());
        }

        let key = match side {
            JoinSide::Left => self.build_left_key(&row)?,
            JoinSide::Right => self.build_right_key(&row)?,
        };
        let (state, other_state) = match side {
            JoinSide::Left => (&mut self.left_state, &self.right_state),
            JoinSide::Right => (&mut self.right_state, &self.left_state),
        };

        let entries = state.entry(key.clone()).or_default();
        apply_state_change(entries, &row, diff);

        if let Some(matches) = other_state.get(&key) {
            for matched in matches {
                let (left_row, right_row) = match side {
                    JoinSide::Left => (&row, &matched.row),
                    JoinSide::Right => (&matched.row, &row),
                };
                let joined = build_join_row(left_row, right_row);
                let projected = project_row(&self.projection, &joined)?;
                let output_diff = diff * matched.multiplicity;
                if output_diff != 0 {
                    self.sink.push(projected, output_diff, timestamp)?;
                }
            }
        }

        Ok(())
    }

    fn build_left_key(&self, row: &Row) -> Result<Vec<ScalarValue>> {
        build_key(row, self.join_keys.iter().map(|(l, _)| *l))
    }

    fn build_right_key(&self, row: &Row) -> Result<Vec<ScalarValue>> {
        build_key(row, self.join_keys.iter().map(|(_, r)| *r))
    }
}

impl StreamOperator for JoinOperator {
    fn on_input(
        &mut self,
        input: InputPort,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()> {
        if input == self.left_input {
            self.handle_row(JoinSide::Left, row, diff, timestamp)
        } else if input == self.right_input {
            self.handle_row(JoinSide::Right, row, diff, timestamp)
        } else {
            bail!("join received input from unexpected port: {:?}", input)
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

#[derive(Copy, Clone)]
enum JoinSide {
    Left,
    Right,
}

fn build_key(row: &Row, indexes: impl IntoIterator<Item = usize>) -> Result<Vec<ScalarValue>> {
    indexes
        .into_iter()
        .map(|idx| {
            row.get(idx)
                .cloned()
                .ok_or_else(|| anyhow!("join key index {idx} out of bounds"))
        })
        .collect()
}

fn apply_state_change(entries: &mut Vec<StoredRow>, row: &Row, diff: Diff) {
    if diff > 0 {
        entries.push(StoredRow {
            row: row.clone(),
            multiplicity: diff,
        });
    } else {
        let removal = -diff;
        if entries.len() == 1 && entries[0].row == *row && entries[0].multiplicity == removal {
            entries.clear();
            return;
        }
        let mut remaining = removal;
        entries.retain_mut(|stored| {
            if stored.row == *row && remaining > 0 {
                let to_remove = remaining.min(stored.multiplicity);
                stored.multiplicity -= to_remove;
                remaining -= to_remove;
            }
            stored.multiplicity != 0
        });
    }
}

fn build_join_row(left: &Row, right: &Row) -> Row {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    combined
}

fn project_row(exprs: &[Expr], row: &Row) -> Result<Row> {
    exprs.iter().map(|expr| evaluate(expr, row)).collect()
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::*;
    use crate::dataflow_plan::Expr;
    use crate::operators::test_support::TestSink;
    use crate::stream_types::{InputPort, OperatorId, OutputPort};

    #[test]
    fn apply_state_change_removes_exact_match_fast() {
        let row = vec![ScalarValue::Int64(Some(1))];
        let mut entries = vec![StoredRow {
            row: row.clone(),
            multiplicity: 2,
        }];
        apply_state_change(&mut entries, &row, -2);
        assert!(entries.is_empty());
    }

    #[test]
    fn joins_on_single_key() {
        let left_port = OutputPort::new(OperatorId(0), 0);
        let right_port = OutputPort::new(OperatorId(1), 0);
        let sink = TestSink::default();
        let projection = vec![Expr::column(0), Expr::column(3)];
        let mut op = JoinOperator::new(
            InputPort::new(left_port.operator, 0),
            InputPort::new(right_port.operator, 0),
            vec![(0, 0)],
            projection,
            sink,
        );

        let left_row = vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(100))];
        let right_row = vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(200))];

        op.on_input(
            InputPort::new(left_port.operator, 0),
            left_row.clone(),
            1,
            1,
        )
        .expect("left insert");
        op.on_input(
            InputPort::new(right_port.operator, 0),
            right_row.clone(),
            1,
            1,
        )
        .expect("right insert");

        let sink = op.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(sink.rows.len(), 1);
        assert_eq!(sink.rows[0].0[0], left_row[0]);
        assert_eq!(sink.rows[0].0[1], right_row[1]);
    }
}
