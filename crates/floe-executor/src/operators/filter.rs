use std::any::Any;

use anyhow::{Result, bail};
use dbsp::handles::ZSetHandle;
use dbsp::stream::Stream as DbspHandleStream;

use crate::dataflow_plan::Expr;
use crate::expr_eval::evaluate_bool;
use crate::operator_state::OperatorStateHandle;
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct FilterOperator {
    input: InputPort,
    predicate: Expr,
    sink: Box<dyn RowSink>,
    derived_state: Option<FilterDerivedState>,
}

impl FilterOperator {
    pub fn new(
        input: InputPort,
        predicate: Expr,
        sink: impl RowSink,
        derived_state: Option<FilterDerivedState>,
    ) -> Self {
        Self {
            input,
            predicate,
            sink: Box::new(sink),
            derived_state,
        }
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }

    #[cfg(test)]
    pub fn latest_handle(&self) -> Option<&ZSetHandle> {
        self.derived_state
            .as_ref()
            .and_then(|derived| derived.current_handle())
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

    fn checkpoint<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Vec<OperatorStateHandle>>>> + Send + 'a>,
    > {
        Box::pin(async move {
            if let Some(derived) = self.derived_state.as_mut() {
                let handle = derived.latest_handle().await?;
                return Ok(Some(vec![handle]));
            }
            Ok(None)
        })
    }
}

pub struct FilterDerivedState {
    stream: DbspHandleStream<ZSetHandle>,
    table: String,
    latest: Option<ZSetHandle>,
}

impl FilterDerivedState {
    pub fn new(stream: DbspHandleStream<ZSetHandle>, table: impl Into<String>) -> Self {
        Self {
            stream,
            table: table.into(),
            latest: None,
        }
    }

    pub fn handle_stream(&self) -> DbspHandleStream<ZSetHandle> {
        self.stream.clone()
    }

    pub async fn latest_handle(&mut self) -> Result<OperatorStateHandle> {
        let handle = self.stream.latest().await?;
        self.latest = Some(handle.clone());
        Ok(OperatorStateHandle::new(
            self.table.clone(),
            handle.ns.clone(),
            handle.version,
        ))
    }

    #[cfg(test)]
    pub fn current_handle(&self) -> Option<&ZSetHandle> {
        self.latest.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use dbsp::{DbspFilter, StreamRetention};
    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::Db;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::dataflow_plan::Expr;
    use crate::dbsp_bridge::DbspBridge;
    use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
    use crate::expr_eval::evaluate_bool;
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
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("filter-dbsp-equivalence", store)
                .await
                .expect("open db"),
        );
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let namespace =
            namespaces::operator_state("filter_test", 0, "input").expect("filter namespace");
        let mut upstream = bridge
            .new_stream(
                namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("upstream stream");
        let upstream_stream = upstream.handle_stream();
        let predicate_expr = Arc::new(predicate.clone());
        let derived_predicate = {
            let expr = Arc::clone(&predicate_expr);
            move |key: &Vec<u8>| -> bool {
                let row = decode_projected_row_key(key).expect("decode row for dbsp filter");
                evaluate_bool(&expr, &row).expect("evaluate predicate for dbsp filter")
            }
        };
        let dbsp_filter = DbspFilter::new::<Vec<u8>, _>(&upstream_stream, derived_predicate)
            .await
            .expect("build dbsp filter");
        let derived_stream = dbsp_filter.stream();
        let derived_state = FilterDerivedState::new(derived_stream, "filter_output_test");

        let port = OutputPort::new(OperatorId(0), 0);
        let input_port = InputPort::new(port.operator, 0);
        let mut operator = FilterOperator::new(input_port, predicate, sink, Some(derived_state));

        let events = vec![
            (row(&[42]), 1, 1),
            (row(&[7]), 1, 2),
            (row(&[42]), 1, 3),
            (row(&[42]), -1, 4),
        ];
        for (row, diff, _) in &events {
            upstream.add_delta(
                encode_projected_row_key(row).expect("encode row for dbsp filter"),
                *diff,
            );
            upstream.flush().await.expect("flush upstream state");
        }
        for (row, diff, ts) in events.clone() {
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
        let db_state = materialize_output_state(Arc::clone(&db), &handles[0]).await;
        let sink_state = operator
            .sink()
            .as_any()
            .downcast_ref::<AccumulatingSink>()
            .expect("accumulating sink")
            .snapshot();
        assert_eq!(db_state, sink_state);
        assert!(operator.latest_handle().is_some());
    }

    async fn materialize_output_state(
        db: Arc<Db>,
        handle: &OperatorStateHandle,
    ) -> HashMap<Vec<u8>, Diff> {
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        bridge
            .handle_view_for(&handle.namespace, handle.version)
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
        state: Mutex<HashMap<Vec<u8>, Diff>>,
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
