use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use floe_core::source::SourceEvent;
use slatedb::Db;

use crate::circuit_builder::{Circuit, CircuitContext, RowStreamHandle, SourceRegistry};
use crate::dataflow_plan::{DataflowPlan, OperatorNode};
use crate::dbsp_bridge::DbspBridge;
use crate::materialized_view::MaterializedViewRegistry;
use crate::operators::{
    DispatchEvent, DispatchSink, EventQueue, FilterOperator, JoinOperator, MapOperator,
    MaterializeOperator, NullSink, ScanOperator,
};
use crate::stream_types::{Diff, InputPort, OperatorId, Row, StreamOperator, Timestamp};

/// Represents a decoded row ready to be inserted into a scan operator stream.
#[derive(Debug, Clone)]
pub struct IngestedRow {
    pub source: String,
    pub handle: RowStreamHandle,
    pub row: Row,
    pub diff: Diff,
    pub timestamp: Timestamp,
}

/// Tracks registered scan operators and routes incoming source events to them.
pub struct ScanRuntime {
    registry: Arc<SourceRegistry>,
    bindings: HashMap<String, RowStreamHandle>,
    default_diff: Diff,
}

impl ScanRuntime {
    pub fn new(registry: Arc<SourceRegistry>) -> Self {
        Self {
            registry,
            bindings: HashMap::new(),
            default_diff: 1,
        }
    }

    pub fn register_scan(
        &mut self,
        source_name: impl Into<String>,
        handle: RowStreamHandle,
    ) -> Result<()> {
        let source_name = source_name.into();
        if !self.registry.contains(&source_name) {
            bail!("source '{source_name}' is not registered in SourceRegistry");
        }
        if self.bindings.insert(source_name.clone(), handle).is_some() {
            bail!("scan for source '{source_name}' already registered");
        }
        Ok(())
    }

    pub fn ingest_event(
        &self,
        event: SourceEvent,
        fallback_timestamp: Timestamp,
    ) -> Result<IngestedRow> {
        let source_name = event.source().to_string();
        let handle = self
            .bindings
            .get(&source_name)
            .copied()
            .ok_or_else(|| anyhow!("no scan registered for source '{source_name}'"))?;
        let (row, event_timestamp) = self.registry.decode_event(&event)?;
        let timestamp = event_timestamp.unwrap_or(fallback_timestamp);
        Ok(IngestedRow {
            source: source_name,
            handle,
            row,
            diff: self.default_diff,
            timestamp,
        })
    }
}

pub struct ExecutionRuntime {
    pub scan_runtime: ScanRuntime,
}

impl ExecutionRuntime {
    pub fn new(scan_runtime: ScanRuntime) -> Self {
        Self { scan_runtime }
    }

    pub fn register_bindings(&mut self, bindings: &[(String, RowStreamHandle)]) -> Result<()> {
        for (source, handle) in bindings {
            self.scan_runtime
                .register_scan(source.clone(), *handle)
                .context("register scan binding")?;
        }
        Ok(())
    }

    pub fn process_event(
        &mut self,
        event: SourceEvent,
        fallback_timestamp: Timestamp,
    ) -> Result<IngestedRow> {
        self.scan_runtime.ingest_event(event, fallback_timestamp)
    }
}

pub struct TickLoop {
    runtime: ExecutionRuntime,
    current_watermark: Timestamp,
    ops: Vec<Box<dyn StreamOperator>>,
    queue: EventQueue,
    scan_operators: HashMap<RowStreamHandle, usize>,
    source_watermarks: HashMap<String, Timestamp>,
}

impl TickLoop {
    pub fn with_graph(
        runtime: ExecutionRuntime,
        ops: Vec<Box<dyn StreamOperator>>,
        queue: EventQueue,
        scan_operators: HashMap<RowStreamHandle, usize>,
    ) -> Self {
        Self {
            runtime,
            current_watermark: 0,
            ops,
            queue,
            scan_operators,
            source_watermarks: HashMap::new(),
        }
    }

    pub fn register_bindings(&mut self, bindings: &[(String, RowStreamHandle)]) -> Result<()> {
        self.runtime.register_bindings(bindings)?;
        for (source, _) in bindings {
            self.source_watermarks.entry(source.clone()).or_insert(0);
        }
        Ok(())
    }

    pub async fn process_events<I>(&mut self, events: I) -> Result<()>
    where
        I: IntoIterator<Item = (SourceEvent, Timestamp)>,
    {
        for (event, ts) in events {
            let ingested = self.runtime.process_event(event, ts)?;
            let operator_index = *self
                .scan_operators
                .get(&ingested.handle)
                .ok_or_else(|| anyhow!("no scan operator for handle {:?}", ingested.handle))?;
            self.ingest_into_scan(
                operator_index,
                ingested.row,
                ingested.diff,
                ingested.timestamp,
            )?;
            self.advance_source_watermark(&ingested.source, ingested.timestamp)
                .await?;
            self.drain_queue()?;
        }
        Ok(())
    }

    fn ingest_into_scan(
        &mut self,
        operator_index: usize,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()> {
        let scan = self.ops[operator_index]
            .as_any_mut()
            .downcast_mut::<ScanOperator>()
            .ok_or_else(|| anyhow!("operator at index {operator_index} is not a ScanOperator"))?;
        scan.ingest(row, diff, timestamp)
    }

    fn drain_queue(&mut self) -> Result<()> {
        loop {
            let event = {
                let mut queue = self.queue.lock().expect("dispatch queue lock");
                queue.pop_front()
            };
            match event {
                Some(DispatchEvent {
                    target_op_index,
                    input_port,
                    row,
                    diff,
                    ts,
                }) => {
                    self.ops[target_op_index].on_input(input_port, row, diff, ts)?;
                }
                None => break,
            }
        }
        Ok(())
    }

    pub async fn advance_watermark(&mut self, watermark: Timestamp) -> Result<()> {
        if watermark <= self.current_watermark {
            return Ok(());
        }
        self.current_watermark = watermark;
        self.drain_queue()?;
        for operator in self.ops.iter_mut() {
            operator.on_watermark(watermark)?;
        }
        self.flush_dbsp_views().await
    }

    pub fn current_watermark(&self) -> Timestamp {
        self.current_watermark
    }

    pub async fn advance_source_watermark(
        &mut self,
        source: &str,
        watermark: Timestamp,
    ) -> Result<()> {
        let entry = self
            .source_watermarks
            .get_mut(source)
            .ok_or_else(|| anyhow!("no watermark tracking for source '{source}'"))?;
        if watermark <= *entry {
            return Ok(());
        }
        *entry = watermark;
        let frontier = self.current_frontier();
        self.advance_watermark(frontier).await
    }

    fn current_frontier(&self) -> Timestamp {
        self.source_watermarks
            .values()
            .copied()
            .min()
            .unwrap_or(self.current_watermark)
    }

    async fn flush_dbsp_views(&mut self) -> Result<()> {
        for operator in self.ops.iter_mut() {
            if let Some(materialize) = operator.as_any_mut().downcast_mut::<MaterializeOperator>() {
                materialize.flush_dbsp_if_needed().await?;
            }
        }
        Ok(())
    }
}

pub struct BuiltGraph {
    pub ops: Vec<Box<dyn StreamOperator>>,
    pub scan_bindings: Vec<(String, RowStreamHandle)>,
    pub scan_operator_map: HashMap<RowStreamHandle, usize>,
}

pub async fn build_graph(
    ctx: &CircuitContext<'_>,
    plan: &DataflowPlan,
    mv_registry: Arc<MaterializedViewRegistry>,
    queue: &EventQueue,
    mut bridge: Option<&mut DbspBridge>,
) -> Result<BuiltGraph> {
    let mut downstreams: HashMap<OperatorId, Vec<(usize, InputPort)>> = HashMap::new();
    for (idx, _) in plan.operators.iter().enumerate() {
        downstreams.insert(OperatorId(idx), Vec::new());
    }

    for (idx, node) in plan.operators.iter().enumerate() {
        match node {
            OperatorNode::Map(map) => {
                let src = map.input.operator;
                downstreams
                    .get_mut(&src)
                    .ok_or_else(|| anyhow!("operator {:?} missing downstream entry", src))?
                    .push((
                        idx,
                        InputPort::new(map.input.operator, map.input.port_index),
                    ));
            }
            OperatorNode::Filter(filter) => {
                let src = filter.input.operator;
                downstreams
                    .get_mut(&src)
                    .ok_or_else(|| anyhow!("operator {:?} missing downstream entry", src))?
                    .push((
                        idx,
                        InputPort::new(filter.input.operator, filter.input.port_index),
                    ));
            }
            OperatorNode::Join(join) => {
                let left_src = join.left.operator;
                downstreams
                    .get_mut(&left_src)
                    .ok_or_else(|| anyhow!("operator {:?} missing downstream entry", left_src))?
                    .push((
                        idx,
                        InputPort::new(join.left.operator, join.left.port_index),
                    ));
                let right_src = join.right.operator;
                downstreams
                    .get_mut(&right_src)
                    .ok_or_else(|| anyhow!("operator {:?} missing downstream entry", right_src))?
                    .push((
                        idx,
                        InputPort::new(join.right.operator, join.right.port_index),
                    ));
            }
            OperatorNode::Materialize(materialize) => {
                let src = materialize.input.operator;
                downstreams
                    .get_mut(&src)
                    .ok_or_else(|| anyhow!("operator {:?} missing downstream entry", src))?
                    .push((
                        idx,
                        InputPort::new(materialize.input.operator, materialize.input.port_index),
                    ));
            }
            OperatorNode::Scan(_) => {}
        }
    }

    let mut operator_outputs: HashMap<OperatorId, RowStreamHandle> = HashMap::new();
    for connected in ctx.connected() {
        operator_outputs.insert(connected.operator_id(), connected.output());
    }

    let mut scan_operator_map = HashMap::new();
    for (idx, node) in plan.operators.iter().enumerate() {
        if matches!(node, OperatorNode::Scan(_)) {
            let op_id = OperatorId(idx);
            let handle = operator_outputs
                .get(&op_id)
                .copied()
                .ok_or_else(|| anyhow!("missing output handle for scan {:?}", op_id))?;
            scan_operator_map.insert(handle, idx);
        }
    }

    let mut built_ops: Vec<Box<dyn StreamOperator>> = Vec::with_capacity(plan.operators.len());
    for (idx, node) in plan.operators.iter().enumerate() {
        let op_id = OperatorId(idx);
        let targets = downstreams.get(&op_id).cloned().unwrap_or_else(Vec::new);

        let operator: Box<dyn StreamOperator> = match node {
            OperatorNode::Scan(scan) => {
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                Box::new(ScanOperator::new(scan.source_name.clone(), sink))
            }
            OperatorNode::Map(map) => {
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                Box::new(MapOperator::new(
                    InputPort::new(map.input.operator, map.input.port_index),
                    map.expressions.clone(),
                    sink,
                ))
            }
            OperatorNode::Filter(filter) => {
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                Box::new(FilterOperator::new(
                    InputPort::new(filter.input.operator, filter.input.port_index),
                    filter.predicate.clone(),
                    sink,
                ))
            }
            OperatorNode::Join(join) => {
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                Box::new(JoinOperator::new(
                    InputPort::new(join.left.operator, join.left.port_index),
                    InputPort::new(join.right.operator, join.right.port_index),
                    join.on.clone(),
                    join.projection.clone(),
                    sink,
                ))
            }
            OperatorNode::Materialize(materialize) => {
                let dbsp_view = if let Some(bridge_ref) = bridge.as_mut() {
                    Some(
                        (*bridge_ref)
                            .new_view(&materialize.view_name)
                            .await
                            .context("create DBSP view")?,
                    )
                } else {
                    None
                };
                Box::new(MaterializeOperator::new(
                    InputPort::new(materialize.input.operator, materialize.input.port_index),
                    materialize.view_name.clone(),
                    mv_registry.clone(),
                    NullSink::default(),
                    dbsp_view,
                ))
            }
        };
        built_ops.push(operator);
    }

    Ok(BuiltGraph {
        ops: built_ops,
        scan_bindings: ctx.scan_bindings(),
        scan_operator_map,
    })
}

pub async fn instantiate_tick_loop(
    plan: &DataflowPlan,
    sources: Arc<SourceRegistry>,
    mv_registry: Arc<MaterializedViewRegistry>,
    db: Option<Arc<Db>>,
) -> Result<TickLoop> {
    let mut circuit = Circuit::new();
    let mut ctx = CircuitContext::new(&mut circuit, Arc::clone(&sources));
    ctx.build_plan(plan)
        .context("build circuit plan from dataflow")?;

    let queue: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
    let mut bridge = match db {
        Some(db) => Some(
            DbspBridge::new(db)
                .await
                .context("initialize DBSP bridge")?,
        ),
        None => None,
    };
    let built = build_graph(
        &ctx,
        plan,
        Arc::clone(&mv_registry),
        &queue,
        bridge.as_mut(),
    )
    .await?;

    let runtime = ExecutionRuntime::new(ScanRuntime::new(sources));
    let mut tick = TickLoop::with_graph(runtime, built.ops, queue, built.scan_operator_map);
    tick.register_bindings(&built.scan_bindings)?;
    Ok(tick)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::scalar::ScalarValue;
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
    use object_store::{ObjectStore, memory::InMemory};
    use serde_json::json;
    use slatedb::Db;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::circuit_builder::{Circuit, CircuitContext};
    use crate::dataflow_plan::{
        DataflowPlan, Expr, MapNode, MaterializeNode, OperatorNode, ScanNode,
    };
    use crate::materialized_view::MaterializedViewRegistry;
    use crate::operators::test_support::TestSink;
    use crate::table_provider::MaterializedViewTableProvider;
    use crate::{OperatorId, OutputPort};

    fn bid_definition() -> SourceDefinition {
        SourceDefinition::new(
            "bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
            ],
        )
        .expect("definition")
    }

    #[test]
    fn ingests_decoded_rows_for_registered_scan() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let handle = RowStreamHandle::new(0);

        let mut runtime = ScanRuntime::new(registry.clone());
        runtime.register_scan("bid", handle).expect("register scan");

        let event = SourceEvent::new("bid", json!({"auction": 7, "bidder": 9}));
        let ingested = runtime.ingest_event(event, 42).expect("ingest");
        assert_eq!(ingested.source, "bid");
        assert_eq!(ingested.handle, handle);
        assert_eq!(ingested.row[0], ScalarValue::Int64(Some(7)));
        assert_eq!(ingested.row[1], ScalarValue::Int64(Some(9)));
        assert_eq!(ingested.timestamp, 42);
    }

    #[tokio::test]
    async fn runtime_processes_events_via_scan_operator() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let scan_handle = RowStreamHandle::new(0);
        let scan_runtime = ScanRuntime::new(registry.clone());
        let runtime = ExecutionRuntime::new(scan_runtime);
        let bindings = vec![("bid".to_string(), scan_handle)];

        let queue: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
        let sink = TestSink::default();
        let scan = ScanOperator::new("bid", sink);
        let ops: Vec<Box<dyn StreamOperator>> = vec![Box::new(scan)];
        let mut scan_map = HashMap::new();
        scan_map.insert(scan_handle, 0);

        let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map);
        tick.register_bindings(&bindings).expect("register binding");

        let events = vec![(
            SourceEvent::new("bid", json!({"auction": 11, "bidder": 22})),
            100,
        )];
        tick.process_events(events).await.expect("process event");

        let scan = tick.ops[0]
            .as_any()
            .downcast_ref::<ScanOperator>()
            .expect("scan operator");
        let operator_sink = scan.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(operator_sink.rows.len(), 1);
        assert_eq!(operator_sink.rows[0].0[0], ScalarValue::Int64(Some(11)));
        assert_eq!(operator_sink.rows[0].0[1], ScalarValue::Int64(Some(22)));
        assert_eq!(operator_sink.rows[0].2, 100);
    }

    #[test]
    fn register_bindings_from_context() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let mut circuit = Circuit::new();
        let mut ctx = CircuitContext::new(&mut circuit, registry.clone());
        let mut plan = DataflowPlan::new();
        let scan_id = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        plan.set_root(scan_id);
        let _ = ctx
            .connect_scan(
                scan_id,
                match &plan.operators[0] {
                    OperatorNode::Scan(node) => node,
                    _ => unreachable!(),
                },
            )
            .expect("connect scan");

        let bindings = ctx.scan_bindings();
        assert_eq!(bindings.len(), 1);

        let scan_runtime = ScanRuntime::new(registry.clone());
        let mut runtime = ExecutionRuntime::new(scan_runtime);
        runtime
            .register_bindings(&bindings)
            .expect("register from bindings");

        let event = SourceEvent::new("bid", json!({"auction": 11, "bidder": 12}));
        let ingested = runtime.process_event(event, 7).expect("process event");
        assert_eq!(ingested.source, "bid");
        assert_eq!(ingested.handle, bindings[0].1);
        assert_eq!(ingested.row[0], ScalarValue::Int64(Some(11)));
        assert_eq!(ingested.row[1], ScalarValue::Int64(Some(12)));
        assert_eq!(ingested.timestamp, 7);
    }

    #[tokio::test]
    async fn tick_loop_advances_watermark() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let mut circuit = Circuit::new();
        let mut ctx = CircuitContext::new(&mut circuit, registry.clone());
        let mut plan = DataflowPlan::new();
        let scan_id = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        plan.set_root(scan_id);
        ctx.connect_scan(
            scan_id,
            match &plan.operators[0] {
                OperatorNode::Scan(node) => node,
                _ => unreachable!(),
            },
        )
        .expect("connect scan");
        let bindings = ctx.scan_bindings();

        let scan_runtime = ScanRuntime::new(registry.clone());
        let queue: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
        let sink = TestSink::default();
        let scan = ScanOperator::new("bid", sink);
        let ops: Vec<Box<dyn StreamOperator>> = vec![Box::new(scan)];
        let mut scan_map = HashMap::new();
        scan_map.insert(bindings[0].1, 0);

        let mut tick_loop = TickLoop::with_graph(
            ExecutionRuntime::new(scan_runtime),
            ops,
            queue.clone(),
            scan_map,
        );
        tick_loop.register_bindings(&bindings).expect("register");

        let events = vec![
            (
                SourceEvent::new("bid", json!({"auction": 1, "bidder": 2})),
                1,
            ),
            (
                SourceEvent::new("bid", json!({"auction": 2, "bidder": 3})),
                2,
            ),
        ];
        tick_loop.process_events(events).await.expect("process");
        tick_loop.advance_watermark(5).await.expect("watermark");
        assert_eq!(tick_loop.current_watermark(), 5);
        let scan = tick_loop.ops[0]
            .as_any()
            .downcast_ref::<ScanOperator>()
            .expect("scan operator");
        let sink = scan.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(sink.rows.len(), 2);
        assert_eq!(sink.watermarks, vec![1, 2, 5]);
    }

    #[tokio::test]
    async fn build_graph_routes_rows_through_runtime() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let mut plan = DataflowPlan::new();
        let scan_id = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let map_id = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(scan_id, 0),
            output: OutputPort::new(OperatorId(1), 0),
            expressions: vec![Expr::column(0)],
        }));
        let mat_id = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(map_id, 0),
            view_name: "mv_test".to_string(),
        }));
        plan.set_root(mat_id);

        let mut circuit = Circuit::new();
        let mut ctx = CircuitContext::new(&mut circuit, registry.clone());
        ctx.build_plan(&plan).expect("build plan");

        let queue: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let built = build_graph(&ctx, &plan, mv_registry.clone(), &queue, None)
            .await
            .expect("build graph");

        let scan_runtime = ScanRuntime::new(registry.clone());
        let runtime = ExecutionRuntime::new(scan_runtime);
        let mut tick =
            TickLoop::with_graph(runtime, built.ops, queue.clone(), built.scan_operator_map);
        tick.register_bindings(&built.scan_bindings)
            .expect("register bindings");

        let events = vec![(
            SourceEvent::new("bid", json!({"auction": 10, "bidder": 20})),
            1,
        )];
        tick.process_events(events).await.expect("process events");
        tick.advance_watermark(5).await.expect("watermark");

        let view = mv_registry.get("mv_test").expect("view registered");
        let snapshot = view.snapshot();
        let expected_row = vec![ScalarValue::Int64(Some(10))];
        assert_eq!(snapshot.get(&expected_row), Some(&1));
        assert_eq!(view.watermark(), Some(5));
    }

    #[tokio::test]
    async fn instantiate_tick_loop_executes_plan() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let mut plan = DataflowPlan::new();
        let scan_id = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let map_id = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(scan_id, 0),
            output: OutputPort::new(OperatorId(1), 0),
            expressions: vec![Expr::column(0), Expr::column(1)],
        }));
        let materialize_id = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(map_id, 0),
            view_name: "mv_exec".to_string(),
        }));
        plan.set_root(materialize_id);

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick =
            instantiate_tick_loop(&plan, Arc::clone(&registry), Arc::clone(&mv_registry), None)
                .await
                .expect("instantiate tick loop");

        let events = vec![
            (
                SourceEvent::new("bid", json!({"auction": 7, "bidder": 9})),
                1,
            ),
            (
                SourceEvent::new("bid", json!({"auction": 8, "bidder": 10})),
                2,
            ),
        ];
        tick.process_events(events).await.expect("process events");
        tick.advance_watermark(11).await.expect("watermark");

        let view = mv_registry.get("mv_exec").expect("view registered");
        let snapshot = view.snapshot();
        let row_a = vec![ScalarValue::Int64(Some(7)), ScalarValue::Int64(Some(9))];
        let row_b = vec![ScalarValue::Int64(Some(8)), ScalarValue::Int64(Some(10))];
        assert_eq!(snapshot.get(&row_a), Some(&1));
        assert_eq!(snapshot.get(&row_b), Some(&1));
        assert_eq!(view.watermark(), Some(11));
    }

    #[tokio::test]
    async fn tracks_source_watermarks_and_frontier() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        registry.register(
            SourceDefinition::new(
                "person",
                vec![SourceColumn::new("id", SourceDataType::Int64)],
            )
            .expect("person source"),
        );
        let registry = Arc::new(registry);

        let queue: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
        let scan_a = ScanOperator::new("bid", DispatchSink::new(Vec::new(), queue.clone()));
        let scan_b = ScanOperator::new("person", DispatchSink::new(Vec::new(), queue.clone()));
        let ops: Vec<Box<dyn StreamOperator>> = vec![Box::new(scan_a), Box::new(scan_b)];

        let handle_a = RowStreamHandle::new(0);
        let handle_b = RowStreamHandle::new(1);
        let mut scan_map = HashMap::new();
        scan_map.insert(handle_a, 0);
        scan_map.insert(handle_b, 1);

        let runtime = ExecutionRuntime::new(ScanRuntime::new(registry.clone()));
        let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map);
        tick.register_bindings(&vec![
            ("bid".to_string(), handle_a),
            ("person".to_string(), handle_b),
        ])
        .expect("register bindings");

        tick.advance_source_watermark("bid", 5)
            .await
            .expect("advance bid watermark");
        assert_eq!(tick.current_watermark(), 0);

        tick.advance_source_watermark("person", 3)
            .await
            .expect("advance person watermark");
        assert_eq!(tick.current_watermark(), 3);

        tick.advance_source_watermark("bid", 10)
            .await
            .expect("advance bid again");
        assert_eq!(tick.current_watermark(), 3);

        tick.advance_source_watermark("person", 6)
            .await
            .expect("advance person again");
        assert_eq!(tick.current_watermark(), 6);
    }

    #[tokio::test]
    async fn derives_timestamp_from_source_payload() {
        let definition = SourceDefinition::new(
            "with_ts",
            vec![
                SourceColumn::new("id", SourceDataType::Int64),
                SourceColumn::new("ts", SourceDataType::TimestampMillis),
            ],
        )
        .expect("definition");
        let mut registry = SourceRegistry::new();
        registry.register(definition);
        let registry = Arc::new(registry);

        let scan_handle = RowStreamHandle::new(0);
        let scan_runtime = ScanRuntime::new(registry.clone());
        let runtime = ExecutionRuntime::new(scan_runtime);
        let bindings = vec![("with_ts".to_string(), scan_handle)];

        let queue: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
        let sink = TestSink::default();
        let scan = ScanOperator::new("with_ts", sink);
        let ops: Vec<Box<dyn StreamOperator>> = vec![Box::new(scan)];
        let mut scan_map = HashMap::new();
        scan_map.insert(scan_handle, 0);

        let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map);
        tick.register_bindings(&bindings).expect("register binding");

        let events = vec![(
            SourceEvent::new(
                "with_ts",
                json!({
                    "id": 1,
                    "ts": 5_000_i64,
                }),
            ),
            1,
        )];
        tick.process_events(events).await.expect("process event");

        let scan = tick.ops[0]
            .as_any()
            .downcast_ref::<ScanOperator>()
            .expect("scan operator");
        let operator_sink = scan.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(operator_sink.rows.len(), 1);
        assert_eq!(operator_sink.rows[0].2, 5_000);
        assert_eq!(operator_sink.watermarks, vec![5_000]);
    }

    #[tokio::test]
    async fn persisted_materialized_view_survives_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("mv_persist", store).await.expect("open SlateDB"));

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        let source_registry = Arc::new(source_registry);

        let plan = {
            let mut plan = DataflowPlan::new();
            let scan_id = plan.add_operator(OperatorNode::Scan(ScanNode {
                source_name: "bid".to_string(),
                output: OutputPort::new(OperatorId(0), 0),
            }));
            let map_id = plan.add_operator(OperatorNode::Map(MapNode {
                input: OutputPort::new(scan_id, 0),
                output: OutputPort::new(OperatorId(1), 0),
                expressions: vec![Expr::column(0), Expr::column(1)],
            }));
            let mat_id = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
                input: OutputPort::new(map_id, 0),
                view_name: "mv_exec".to_string(),
            }));
            plan.set_root(mat_id);
            plan
        };

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate tick loop");

        let events = vec![(
            SourceEvent::new("bid", json!({"auction": 7, "bidder": 9})),
            1,
        )];
        tick.process_events(events).await.expect("process events");
        tick.advance_watermark(5).await.expect("watermark");

        drop(tick);

        let mv_registry_restart = Arc::new(MaterializedViewRegistry::new());
        let _tick2 = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry_restart),
            Some(db.clone()),
        )
        .await
        .expect("instantiate tick loop restart");

        let schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
        ]));
        let provider =
            MaterializedViewTableProvider::new(Arc::clone(&mv_registry_restart), "mv_exec", schema);
        let batches = provider
            .build_batches_for_test()
            .await
            .expect("build batches");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}
