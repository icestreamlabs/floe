use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use dbsp::ZSetStream;
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::Stream as DbspHandleStream;
use dbsp::stream::StreamCursor;
use dbsp::stream::util::{compute_delta, materialize_zset_handle};

use crate::checkpoint::DbspHandleRecord;
use crate::dataflow_plan::Expr;
use crate::encoding::decode_projected_row_key;
use crate::expr_eval::evaluate_bool;
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct FilterOperator {
    input: InputPort,
    predicate: Expr,
    sink: Box<dyn RowSink>,
    live_state: Option<FilterLiveState>,
}

impl FilterOperator {
    pub fn new(input: InputPort, predicate: Expr, sink: impl RowSink) -> Self {
        Self {
            input,
            predicate,
            sink: Box::new(sink),
            live_state: None,
        }
    }

    pub fn new_live(
        input: InputPort,
        predicate: Expr,
        sink: impl RowSink,
        upstream: DbspHandleStream<ZSetHandle>,
        table: Arc<dyn KeyValueTable>,
        out: ZSetStream<Vec<u8>>,
    ) -> Self {
        let cursor = StreamCursor::new(upstream.clone());
        Self {
            input,
            predicate,
            sink: Box::new(sink),
            live_state: Some(FilterLiveState::new(upstream, cursor, table, out)),
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

        if !evaluate_bool(&self.predicate, &row)? {
            return Ok(());
        }
        self.sink.push(row, diff, timestamp)
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
        Box<dyn std::future::Future<Output = Result<Option<Vec<DbspHandleRecord>>>> + Send + 'a>,
    > {
        Box::pin(async move {
            if let Some(state) = self.live_state.as_mut() {
                state.publish_pending(&self.predicate).await?;
            }
            Ok(None)
        })
    }
}

struct FilterLiveState {
    upstream: DbspHandleStream<ZSetHandle>,
    cursor: StreamCursor<ZSetHandle>,
    table: Arc<dyn KeyValueTable>,
    out: ZSetStream<Vec<u8>>,
    prev_snapshot: HashMap<Vec<u8>, Diff>,
    dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
}

impl FilterLiveState {
    fn new(
        upstream: DbspHandleStream<ZSetHandle>,
        cursor: StreamCursor<ZSetHandle>,
        table: Arc<dyn KeyValueTable>,
        out: ZSetStream<Vec<u8>>,
    ) -> Self {
        Self {
            upstream,
            cursor,
            table,
            out,
            prev_snapshot: HashMap::new(),
            dict_cache: HashMap::new(),
        }
    }

    async fn publish_pending(&mut self, predicate: &Expr) -> Result<()> {
        while self.upstream.current_time() > self.cursor.observed() {
            let (_ts, handle) = self.cursor.next().await?;
            let upstream_state = materialize_zset_handle::<Vec<u8>>(
                self.table.clone(),
                &mut self.dict_cache,
                &handle,
            )
            .await?;

            let mut filtered: HashMap<Vec<u8>, Diff> = HashMap::new();
            for (key, diff) in upstream_state {
                if diff == 0 {
                    continue;
                }
                let row = decode_projected_row_key(&key)?;
                if evaluate_bool(predicate, &row)? {
                    filtered.insert(key, diff);
                }
            }
            let deltas = compute_delta(&self.prev_snapshot, &filtered);
            if deltas.is_empty() {
                self.prev_snapshot = filtered;
                continue;
            }
            self.out.add_deltas(deltas);
            self.out.flush().await?;
            self.prev_snapshot = filtered;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use dbsp::StreamRetention;
    use dbsp::stream::util::materialize_zset_handle;
    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::Db;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::dataflow_plan::Expr;
    use crate::dbsp_bridge::DbspBridge;
    use crate::encoding::encode_projected_row_key;
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

    #[tokio::test]
    async fn filter_output_matches_sink_output() {
        let predicate = Expr::Eq(
            Box::new(Expr::column(0)),
            Box::new(Expr::literal(ScalarValue::Int64(Some(42)))),
        );
        let sink = AccumulatingSink::default();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("filter-live-equivalence", store)
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
        let output_namespace =
            namespaces::operator_state("filter_test", 0, "output").expect("filter output ns");
        let output_stream = bridge
            .new_stream(
                output_namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("filter state stream");
        let mut output_handle_stream = output_stream.handle_stream();
        let table = bridge.table();

        let port = OutputPort::new(OperatorId(0), 0);
        let input_port = InputPort::new(port.operator, 0);
        let mut operator = FilterOperator::new_live(
            input_port,
            predicate.clone(),
            sink,
            upstream_stream,
            Arc::clone(&table),
            output_stream,
        );

        let events = vec![
            (row(&[42]), 1, 1),
            (row(&[7]), 1, 2),
            (row(&[42]), 1, 3),
            (row(&[42]), -1, 4),
        ];
        for (row, diff, _) in &events {
            upstream.add_delta(
                encode_projected_row_key(row).expect("encode row for filter"),
                *diff,
            );
            upstream.flush().await.expect("flush upstream");
        }
        for (row, diff, ts) in events.clone() {
            operator
                .on_input(input_port, row, diff, ts)
                .expect("apply filter");
        }

        operator.checkpoint().await.expect("checkpoint");

        let latest = output_handle_stream
            .latest()
            .await
            .expect("latest filter handle");
        let mut dict_cache = HashMap::new();
        let db_state = materialize_zset_handle::<Vec<u8>>(table, &mut dict_cache, &latest)
            .await
            .expect("materialize filter output handle");
        let sink_state = operator
            .sink()
            .as_any()
            .downcast_ref::<AccumulatingSink>()
            .expect("accumulating sink")
            .snapshot();
        assert_eq!(db_state, sink_state);
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
