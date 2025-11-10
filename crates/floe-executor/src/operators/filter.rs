use std::any::Any;

use anyhow::{Result, bail};
use dbsp::handles::ZSetHandle;
use dbsp::{StreamRetention, ZSetStream};

use crate::dataflow_plan::Expr;
use crate::encoding::encode_projected_row_key;
use crate::expr_eval::evaluate_bool;
use crate::operator_state::OperatorStateHandle;
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct FilterOperator {
    input: InputPort,
    predicate: Expr,
    sink: Box<dyn RowSink>,
    dbsp: Option<FilterDbspState>,
}

impl FilterOperator {
    pub fn new(
        input: InputPort,
        predicate: Expr,
        sink: impl RowSink,
        dbsp: Option<FilterDbspState>,
    ) -> Self {
        Self {
            input,
            predicate,
            sink: Box::new(sink),
            dbsp,
        }
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }

    #[cfg(test)]
    pub fn latest_handle(&self) -> Option<&ZSetHandle> {
        self.dbsp.as_ref().and_then(|state| state.latest_handle())
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
            if let Some(dbsp) = self.dbsp.as_mut() {
                dbsp.record_row(&row, diff)?;
            }
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

    fn checkpoint<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Vec<OperatorStateHandle>>>> + Send + 'a>,
    > {
        Box::pin(async move {
            if let Some(state) = self.dbsp.as_mut() {
                let handle = state.flush().await?;
                return Ok(Some(vec![handle]));
            }
            Ok(None)
        })
    }
}

pub struct FilterDbspState {
    stream: ZSetStream<Vec<u8>>,
    table: String,
    namespace: String,
    latest_handle: Option<ZSetHandle>,
}

impl FilterDbspState {
    pub fn new(
        stream: ZSetStream<Vec<u8>>,
        table: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        let latest_handle = Some(stream.current_handle().clone());
        Self {
            stream,
            table: table.into(),
            namespace: namespace.into(),
            latest_handle,
        }
    }

    fn record_row(&mut self, row: &Row, diff: Diff) -> Result<()> {
        if diff == 0 {
            return Ok(());
        }
        let key = encode_projected_row_key(row)?;
        self.stream.add_delta(key, diff);
        Ok(())
    }

    async fn flush(&mut self) -> Result<OperatorStateHandle> {
        let handle = self.stream.flush().await?;
        self.latest_handle = Some(handle.clone());
        Ok(OperatorStateHandle::new(
            self.table.clone(),
            self.namespace.clone(),
            handle.version,
        ))
    }

    #[cfg(test)]
    fn latest_handle(&self) -> Option<&ZSetHandle> {
        self.latest_handle.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::Db;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::dataflow_plan::Expr;
    use crate::dbsp_bridge::DbspBridge;
    use crate::namespaces;
    use crate::operators::test_support::TestSink;
    use crate::stream_types::{Diff, InputPort, OperatorId, OutputPort, Timestamp};

    #[test]
    fn filters_rows() {
        let port = OutputPort::new(OperatorId(0), 0);
        let sink = TestSink::default();
        let predicate = Expr::Eq(
            Box::new(Expr::column(0)),
            Box::new(Expr::literal(ScalarValue::Int64(Some(42)))),
        );
        let mut operator =
            FilterOperator::new(InputPort::new(port.operator, 0), predicate, sink, None);

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

    #[tokio::test]
    async fn filter_dbsp_output_matches_sink_state() {
        let predicate = Expr::Eq(
            Box::new(Expr::column(0)),
            Box::new(Expr::literal(ScalarValue::Int64(Some(42)))),
        );
        let sink = AccumulatingSink::default();
        let (mut operator, input_port, db, namespace) =
            build_filter_with_dbsp("filter-dbsp-equivalence", predicate, sink).await;

        let to_process = vec![
            (row(&[42]), 1, 1),
            (row(&[7]), 1, 2),
            (row(&[42]), 1, 3),
            (row(&[42]), -1, 4),
        ];
        for (row, diff, ts) in to_process {
            operator
                .on_input(input_port, row, diff, ts)
                .expect("apply filter");
        }

        let handles = operator
            .checkpoint()
            .await
            .expect("checkpoint")
            .expect("handles");
        assert_eq!(handles.len(), 1);
        let db_state =
            materialize_output_state(Arc::clone(&db), &namespace, handles[0].version).await;
        let sink_state = operator
            .sink()
            .as_any()
            .downcast_ref::<AccumulatingSink>()
            .expect("accumulating sink")
            .snapshot();
        assert_eq!(db_state, sink_state);
        assert!(operator.latest_handle().is_some());
    }

    async fn build_filter_with_dbsp(
        db_label: &str,
        predicate: Expr,
        sink: impl RowSink,
    ) -> (FilterOperator, InputPort, Arc<Db>, String) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(db_label, store).await.expect("open SlateDB"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let namespace =
            namespaces::operator_state("filter_test", 0, "output").expect("filter namespace");
        let stream = bridge
            .new_stream(
                namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("filter stream");
        let dbsp_state = FilterDbspState::new(stream, "filter_output", namespace.clone());

        let port = OutputPort::new(OperatorId(0), 0);
        let input = InputPort::new(port.operator, 0);
        let operator = FilterOperator::new(input, predicate, sink, Some(dbsp_state));
        (operator, input, db, namespace)
    }

    async fn materialize_output_state(
        db: Arc<Db>,
        namespace: &str,
        version: u64,
    ) -> HashMap<Vec<u8>, Diff> {
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        bridge
            .handle_view_for(namespace, version)
            .await
            .expect("handle view")
            .materialize()
            .await
            .expect("materialize")
    }

    fn row(values: &[i64]) -> Row {
        values
            .iter()
            .map(|v| ScalarValue::Int64(Some(*v)))
            .collect()
    }

    #[derive(Default)]
    struct AccumulatingSink {
        state: std::sync::Mutex<HashMap<Vec<u8>, Diff>>,
    }

    impl AccumulatingSink {
        fn snapshot(&self) -> HashMap<Vec<u8>, Diff> {
            self.state.lock().expect("acc state lock").clone()
        }
    }

    impl RowSink for AccumulatingSink {
        fn push(&mut self, row: Row, diff: Diff, _timestamp: Timestamp) -> Result<()> {
            let key = encode_projected_row_key(&row)?;
            let mut state = self.state.lock().expect("acc state lock");
            let updated = state.get(&key).copied().unwrap_or(0) + diff;
            if updated == 0 {
                state.remove(&key);
            } else {
                state.insert(key, updated);
            }
            Ok(())
        }

        fn watermark(&mut self, _watermark: Timestamp) -> Result<()> {
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
