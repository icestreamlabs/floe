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

use crate::checkpoint::{DbspHandleRecord, handle_kinds, record_if_nonzero};
use crate::dataflow_plan::Expr;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::expr_eval::evaluate;
use crate::operator_state::OperatorStateHandle;
use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

pub struct MapOperator {
    input: InputPort,
    expressions: Vec<Expr>,
    sink: Box<dyn RowSink>,
    derived_state: Option<MapDerivedState>,
    live_state: Option<MapLiveState>,
}

impl MapOperator {
    pub fn new(
        input: InputPort,
        expressions: Vec<Expr>,
        sink: impl RowSink + 'static,
        derived_state: Option<MapDerivedState>,
    ) -> Self {
        Self {
            input,
            expressions,
            sink: Box::new(sink),
            derived_state,
            live_state: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_live(
        input: InputPort,
        expressions: Vec<Expr>,
        sink: impl RowSink + 'static,
        upstream: DbspHandleStream<ZSetHandle>,
        table: Arc<dyn KeyValueTable>,
        out: ZSetStream<Vec<u8>>,
        derived_state: Option<MapDerivedState>,
    ) -> Self {
        let cursor = StreamCursor::new(upstream.clone());
        Self {
            input,
            expressions,
            sink: Box::new(sink),
            derived_state,
            live_state: Some(MapLiveState::new(upstream, cursor, table, out)),
        }
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }
}

impl StreamOperator for MapOperator {
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
                state.publish_pending(&self.expressions).await?;
            }
            if let Some(state) = self.derived_state.as_mut() {
                let handle = state.latest_handle().await?;
                let OperatorStateHandle {
                    table,
                    namespace,
                    version,
                } = handle;
                if let Some(record) =
                    record_if_nonzero(handle_kinds::OPERATOR_STATE, &table, &namespace, version)
                {
                    Ok(Some(vec![record]))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        })
    }
}

pub struct MapDerivedState {
    stream: DbspHandleStream<ZSetHandle>,
    table: String,
    latest: Option<ZSetHandle>,
}

impl MapDerivedState {
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

struct MapLiveState {
    upstream: DbspHandleStream<ZSetHandle>,
    cursor: StreamCursor<ZSetHandle>,
    table: Arc<dyn KeyValueTable>,
    out: ZSetStream<Vec<u8>>,
    prev_snapshot: HashMap<Vec<u8>, Diff>,
    dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
}

impl MapLiveState {
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

    async fn publish_pending(&mut self, expressions: &[Expr]) -> Result<()> {
        while self.upstream.current_time() > self.cursor.observed() {
            let (_ts, handle) = self.cursor.next().await?;
            let upstream_state = materialize_zset_handle::<Vec<u8>>(
                self.table.clone(),
                &mut self.dict_cache,
                &handle,
            )
            .await?;

            let mut projected: HashMap<Vec<u8>, Diff> = HashMap::new();
            for (key, diff) in upstream_state {
                if diff == 0 {
                    continue;
                }
                let row = decode_projected_row_key(&key)?;
                let mut mapped = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    mapped.push(evaluate(expr, &row)?);
                }
                let encoded = encode_projected_row_key(&mapped)?;
                *projected.entry(encoded).or_insert(0) += diff;
            }
            projected.retain(|_, diff| *diff != 0);
            let deltas = compute_delta(&self.prev_snapshot, &projected);
            if deltas.is_empty() {
                self.prev_snapshot = projected;
                continue;
            }
            self.out.add_deltas(deltas);
            self.out.flush().await?;
            self.prev_snapshot = projected;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use dbsp::StreamRetention;
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
            None,
        );

        let row = vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(5))];
        operator
            .on_input(InputPort::new(input.operator, 0), row.clone(), 1, 1)
            .expect("map input");
        let test_sink = operator.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(test_sink.rows.len(), 1);
        assert_eq!(test_sink.rows[0].0[0], row[0]);
        assert_eq!(test_sink.rows[0].0[1], ScalarValue::Int64(Some(6)));
    }

    #[tokio::test]
    async fn map_dbsp_output_matches_sink_state() {
        let expressions = vec![
            Expr::column(0),
            Expr::Add(
                Box::new(Expr::column(1)),
                Box::new(Expr::literal(ScalarValue::Int64(Some(1)))),
            ),
        ];
        let sink = AccumulatingSink::default();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("map-derived", store).await.expect("open db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let namespace =
            namespaces::operator_state("map_test", 0, "input").expect("map input namespace");
        let mut upstream = bridge
            .new_stream(
                namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("upstream stream");

        let events = vec![
            (row(&[10, 1]), 1, 1),
            (row(&[20, 2]), 1, 2),
            (row(&[10, 1]), -1, 3),
        ];

        let upstream_stream = upstream.handle_stream();
        let output_namespace =
            namespaces::operator_state("map_test", 0, "output").expect("map output namespace");
        let output_stream = bridge
            .new_stream(
                output_namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("output stream");
        let output_handle_stream = output_stream.handle_stream();
        let derived_state = MapDerivedState::new(output_handle_stream.clone(), "map_output_test");
        let table = bridge.table();

        let input = OutputPort::new(OperatorId(0), 0);
        let input_port = InputPort::new(input.operator, input.port_index);
        let mut operator = MapOperator::new_live(
            input_port,
            expressions,
            sink,
            upstream_stream,
            Arc::clone(&table),
            output_stream,
            Some(derived_state),
        );

        for (row, diff, ts) in events.clone() {
            upstream.add_delta(encode_projected_row_key(&row).expect("encode"), diff);
            upstream.flush().await.expect("flush upstream");
            operator
                .on_input(input_port, row, diff, ts)
                .expect("map row");
        }

        let handles = operator
            .checkpoint()
            .await
            .expect("checkpoint")
            .expect("handles");
        assert_eq!(handles.len(), 1);
        let handle = &handles[0];

        let db_state: HashMap<Vec<u8>, Diff> = bridge
            .handle_view_for(&handle.namespace, handle.version)
            .await
            .expect("handle view")
            .materialize()
            .await
            .expect("materialize");
        let sink_state: HashMap<Vec<u8>, Diff> = operator
            .sink()
            .as_any()
            .downcast_ref::<AccumulatingSink>()
            .expect("acc sink")
            .snapshot()
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect();
        let db_state: HashMap<Vec<u8>, Diff> = db_state
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect();
        assert_eq!(db_state, sink_state);
        assert!(
            operator
                .derived_state
                .as_ref()
                .and_then(|state| state.current_handle())
                .is_some()
        );
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
