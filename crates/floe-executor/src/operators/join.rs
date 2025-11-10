use std::any::Any;
use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use datafusion::scalar::ScalarValue;
use dbsp::ZSetStream;
use dbsp::handles::ZSetHandle;

use crate::dataflow_plan::Expr;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::expr_eval::evaluate;
use crate::operator_state::{OperatorStateHandle, StateTable};
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
    left_store: StateTable,
    right_store: StateTable,
    left_state: HashMap<Vec<ScalarValue>, Vec<StoredRow>>,
    right_state: HashMap<Vec<ScalarValue>, Vec<StoredRow>>,
    output_stream: Option<ZSetStream<Vec<u8>>>,
    output_table: Option<String>,
    output_namespace: Option<String>,
    latest_output_handle: Option<ZSetHandle>,
}

impl JoinOperator {
    pub async fn new(
        left_input: InputPort,
        right_input: InputPort,
        join_keys: Vec<(usize, usize)>,
        projection: Vec<Expr>,
        sink: impl RowSink,
        left_store: StateTable,
        right_store: StateTable,
        output_stream: Option<ZSetStream<Vec<u8>>>,
        output_table: Option<String>,
        output_namespace: Option<String>,
        left_snapshot: Option<HashMap<Vec<u8>, Diff>>,
        right_snapshot: Option<HashMap<Vec<u8>, Diff>>,
    ) -> Result<Self> {
        let latest_output_handle = output_stream
            .as_ref()
            .map(|stream| stream.current_handle().clone());
        let mut operator = Self {
            left_input,
            right_input,
            join_keys,
            projection,
            sink: Box::new(sink),
            left_store,
            right_store,
            left_state: HashMap::new(),
            right_state: HashMap::new(),
            output_stream,
            output_table,
            output_namespace,
            latest_output_handle,
        };
        operator
            .restore_persisted_state(left_snapshot, right_snapshot)
            .await?;
        Ok(operator)
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }

    #[cfg(test)]
    pub fn latest_output_handle(&self) -> Option<&ZSetHandle> {
        self.latest_output_handle.as_ref()
    }

    async fn restore_persisted_state(
        &mut self,
        left_snapshot: Option<HashMap<Vec<u8>, Diff>>,
        right_snapshot: Option<HashMap<Vec<u8>, Diff>>,
    ) -> Result<()> {
        if let Some(snapshot) = left_snapshot {
            self.load_snapshot(snapshot, JoinSide::Left)?;
        } else {
            let snapshot = self.left_store.snapshot().await?;
            self.load_snapshot(snapshot, JoinSide::Left)?;
        }
        if let Some(snapshot) = right_snapshot {
            self.load_snapshot(snapshot, JoinSide::Right)?;
        } else {
            let snapshot = self.right_store.snapshot().await?;
            self.load_snapshot(snapshot, JoinSide::Right)?;
        }
        Ok(())
    }

    fn load_snapshot(&mut self, snapshot: HashMap<Vec<u8>, Diff>, side: JoinSide) -> Result<()> {
        for (encoded, weight) in snapshot {
            if weight == 0 {
                continue;
            }
            let row = decode_projected_row_key(&encoded)?;
            let key = match side {
                JoinSide::Left => self.build_left_key(&row)?,
                JoinSide::Right => self.build_right_key(&row)?,
            };
            let state = match side {
                JoinSide::Left => &mut self.left_state,
                JoinSide::Right => &mut self.right_state,
            };
            state.entry(key).or_default().push(StoredRow {
                row,
                multiplicity: weight,
            });
        }
        Ok(())
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
        {
            let state_map = match side {
                JoinSide::Left => &mut self.left_state,
                JoinSide::Right => &mut self.right_state,
            };
            let entries = state_map.entry(key.clone()).or_default();
            apply_state_change(entries, &row, diff);
        }
        self.record_persistent_change(side, &row, diff)?;

        let other_state = match side {
            JoinSide::Left => &self.right_state,
            JoinSide::Right => &self.left_state,
        };
        let mut pending_outputs = Vec::new();
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
                    pending_outputs.push((projected, output_diff));
                }
            }
        }
        for (projected, output_diff) in pending_outputs {
            self.record_output_change(&projected, output_diff)?;
            self.sink.push(projected, output_diff, timestamp)?;
        }

        Ok(())
    }

    fn build_left_key(&self, row: &Row) -> Result<Vec<ScalarValue>> {
        build_key(row, self.join_keys.iter().map(|(l, _)| *l))
    }

    fn build_right_key(&self, row: &Row) -> Result<Vec<ScalarValue>> {
        build_key(row, self.join_keys.iter().map(|(_, r)| *r))
    }

    fn record_persistent_change(&mut self, side: JoinSide, row: &Row, diff: Diff) -> Result<()> {
        if diff == 0 {
            return Ok(());
        }
        let encoded = encode_projected_row_key(row)?;
        match side {
            JoinSide::Left => {
                self.left_store.add_delta(encoded, diff);
            }
            JoinSide::Right => {
                self.right_store.add_delta(encoded, diff);
            }
        }
        Ok(())
    }

    fn record_output_change(&mut self, row: &Row, diff: Diff) -> Result<()> {
        if diff == 0 {
            return Ok(());
        }
        if let Some(stream) = self.output_stream.as_mut() {
            let encoded = encode_projected_row_key(row)?;
            stream.add_delta(encoded, diff);
        }
        Ok(())
    }

    async fn flush_output_stream(&mut self) -> Result<Option<OperatorStateHandle>> {
        if let Some(stream) = self.output_stream.as_mut() {
            let handle = stream.flush().await?;
            self.latest_output_handle = Some(handle.clone());
            if let (Some(table), Some(namespace)) =
                (self.output_table.as_ref(), self.output_namespace.as_ref())
            {
                return Ok(Some(OperatorStateHandle::new(
                    table.clone(),
                    namespace.clone(),
                    handle.version,
                )));
            }
        }
        Ok(None)
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

    fn checkpoint<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Vec<OperatorStateHandle>>>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut handles = Vec::new();
            handles.push(self.left_store.flush().await?);
            handles.push(self.right_store.flush().await?);
            if let Some(handle) = self.flush_output_stream().await? {
                handles.push(handle);
            }
            Ok(Some(handles))
        })
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
    use dbsp::StreamRetention;
    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::Db;
    use std::sync::Arc;

    use super::*;
    use crate::dataflow_plan::Expr;
    use crate::dbsp_bridge::DbspBridge;
    use crate::encoding::encode_projected_row_key;
    use crate::namespaces;
    use crate::operators::test_support::TestSink;
    use crate::stream_types::{Diff, InputPort, OperatorId, OutputPort, Timestamp};
    use std::collections::HashMap;

    async fn build_join_operator_with_sink(
        db_label: &str,
        sink: impl RowSink,
    ) -> (JoinOperator, InputPort, InputPort, Arc<Db>, String) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(db_label, store).await.expect("open SlateDB"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let left_namespace =
            namespaces::operator_state("join_test", 0, "left").expect("left namespace");
        let right_namespace =
            namespaces::operator_state("join_test", 0, "right").expect("right namespace");
        let output_namespace =
            namespaces::operator_state("join_test", 0, "output").expect("output namespace");

        let left_stream = bridge
            .new_stream(
                left_namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("left stream");
        let right_stream = bridge
            .new_stream(
                right_namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("right stream");
        let output_stream = bridge
            .new_stream(
                output_namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("output stream");

        let left_state = StateTable::new("join_left".to_string(), left_namespace, left_stream);
        let right_state = StateTable::new("join_right".to_string(), right_namespace, right_stream);

        let left_port = OutputPort::new(OperatorId(0), 0);
        let right_port = OutputPort::new(OperatorId(1), 0);
        let projection = vec![Expr::column(0), Expr::column(3)];
        let left_input = InputPort::new(left_port.operator, 0);
        let right_input = InputPort::new(right_port.operator, 0);
        let operator = JoinOperator::new(
            left_input,
            right_input,
            vec![(0, 0)],
            projection,
            sink,
            left_state,
            right_state,
            Some(output_stream),
            Some("join_output".to_string()),
            Some(output_namespace.clone()),
            None,
            None,
        )
        .await
        .expect("join operator");

        (operator, left_input, right_input, db, output_namespace)
    }

    async fn build_join_operator_fixture(
        db_label: &str,
    ) -> (JoinOperator, InputPort, InputPort, Arc<Db>, String) {
        build_join_operator_with_sink(db_label, TestSink::default()).await
    }

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

    #[tokio::test]
    async fn joins_on_single_key() {
        let (mut op, left_input, right_input, _, _) =
            build_join_operator_fixture("join-operator-single").await;

        let left_row = vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(100))];
        let right_row = vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(200))];

        op.on_input(left_input, left_row.clone(), 1, 1)
            .expect("left insert");
        op.on_input(right_input, right_row.clone(), 1, 1)
            .expect("right insert");

        let sink = op.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(sink.rows.len(), 1);
        assert_eq!(sink.rows[0].0[0], left_row[0]);
        assert_eq!(sink.rows[0].0[1], right_row[1]);
    }

    #[tokio::test]
    async fn join_emits_output_stream_handles() {
        let (mut op, left_input, right_input, _, _) =
            build_join_operator_fixture("join-output-handles").await;

        let left_row = vec![ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(10))];
        let right_row = vec![ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(11))];

        op.on_input(left_input, left_row.clone(), 1, 1)
            .expect("left insert");
        op.on_input(right_input, right_row, 1, 1)
            .expect("right insert");

        let handles = op.checkpoint().await.expect("checkpoint").expect("handles");
        assert_eq!(handles.len(), 3);
        let output = handles
            .iter()
            .find(|handle| handle.table == "join_output")
            .expect("output handle present");
        assert!(output.version >= 1);
        assert!(op.latest_output_handle().is_some());
    }

    #[tokio::test]
    async fn dbsp_join_output_matches_sink_state() {
        let sink = AccumulatingSink::default();
        let (mut op, left_input, right_input, db, output_ns) =
            build_join_operator_with_sink("join-output-equivalence", sink).await;

        let steps = vec![
            vec![InputEvent::new(left_input, row(&[1, 10, 100]), 1, 1)],
            vec![InputEvent::new(right_input, row(&[1, 20, 200]), 1, 2)],
            vec![
                InputEvent::new(left_input, row(&[2, 30, 300]), 1, 3),
                InputEvent::new(right_input, row(&[2, 40, 400]), 1, 4),
                InputEvent::new(right_input, row(&[2, 50, 500]), 1, 5),
            ],
            vec![InputEvent::new(left_input, row(&[1, 10, 100]), -1, 6)],
        ];

        for (step_idx, events) in steps.into_iter().enumerate() {
            for event in events {
                op.on_input(event.port, event.row, event.diff, event.ts)
                    .expect("process join input");
            }
            let handles = op.checkpoint().await.expect("checkpoint").expect("handles");
            let output_handle = handles
                .iter()
                .find(|handle| handle.table == "join_output")
                .expect("output handle present");
            let db_state =
                materialize_output_state(Arc::clone(&db), &output_ns, output_handle.version).await;
            let sink_state = {
                let sink = op
                    .sink()
                    .as_any()
                    .downcast_ref::<AccumulatingSink>()
                    .expect("accumulating sink");
                sink.snapshot()
            };
            assert_eq!(db_state, sink_state, "mismatch after step {step_idx}");
        }
    }

    fn row(values: &[i64]) -> Row {
        values
            .iter()
            .map(|v| ScalarValue::Int64(Some(*v)))
            .collect()
    }

    async fn materialize_output_state(
        db: Arc<Db>,
        namespace: &str,
        version: u64,
    ) -> HashMap<Vec<u8>, Diff> {
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let snapshot = bridge
            .handle_view_for(namespace, version)
            .await
            .expect("handle view")
            .materialize()
            .await
            .expect("materialize");
        snapshot
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect()
    }

    #[derive(Clone)]
    struct InputEvent {
        port: InputPort,
        row: Row,
        diff: Diff,
        ts: Timestamp,
    }

    impl InputEvent {
        fn new(port: InputPort, row: Row, diff: Diff, ts: Timestamp) -> Self {
            Self {
                port,
                row,
                diff,
                ts,
            }
        }
    }

    #[derive(Default)]
    struct AccumulatingSink {
        state: HashMap<Vec<u8>, Diff>,
    }

    impl AccumulatingSink {
        fn snapshot(&self) -> HashMap<Vec<u8>, Diff> {
            self.state.clone()
        }
    }

    impl RowSink for AccumulatingSink {
        fn push(&mut self, row: Row, diff: Diff, _timestamp: Timestamp) -> Result<()> {
            let key = encode_projected_row_key(&row)?;
            let updated = self.state.get(&key).copied().unwrap_or(0) + diff;
            if updated == 0 {
                self.state.remove(&key);
            } else {
                self.state.insert(key, updated);
            }
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }
}
