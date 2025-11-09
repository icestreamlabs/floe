use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use dbsp::StreamRetention;
use floe_core::source::SourceEvent;
use slatedb::Db;

use crate::barrier_clock::{BarrierClock, StepId};
use crate::checkpoint::{
    CheckpointManager, CheckpointManifest, CheckpointStore, MaterializedViewCheckpointEntry,
    OperatorCheckpointEntry,
};
use crate::circuit_builder::{Circuit, CircuitContext, RowStreamHandle, SourceRegistry};
use crate::dataflow_plan::{DataflowPlan, OperatorNode};
use crate::dbsp_bridge::DbspBridge;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::namespaces;
use crate::operator_state::StateTable;
use crate::operators::{
    DispatchEvent, DispatchSink, EventQueue, FilterOperator, JoinOperator, MapOperator,
    MaterializeOperator, NullSink, ScanOperator,
};
use crate::stream_types::{Diff, InputPort, OperatorId, Row, StreamOperator, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BarrierStage {
    AfterOperatorFlush,
    AfterMaterializedViewFlush,
    BeforeManifestWrite,
    AfterManifestWrite,
}

fn run_barrier_hook(stage: BarrierStage) -> Result<()> {
    #[cfg(test)]
    {
        barrier_failpoints::maybe_trigger(stage)
    }
    #[cfg(not(test))]
    {
        let _ = stage;
        Ok(())
    }
}

#[cfg(test)]
mod barrier_failpoints {
    use super::BarrierStage;
    use anyhow::{Result, bail};
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::thread::ThreadId;

    static FAILPOINTS: OnceLock<Mutex<HashMap<ThreadId, BarrierStage>>> = OnceLock::new();

    fn registry() -> &'static Mutex<HashMap<ThreadId, BarrierStage>> {
        FAILPOINTS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub struct FailpointGuard {
        thread: ThreadId,
    }

    impl FailpointGuard {
        pub fn new(stage: BarrierStage) -> Self {
            let thread = std::thread::current().id();
            let mut guard = registry().lock().expect("failpoint registry lock");
            guard.insert(thread, stage);
            Self { thread }
        }
    }

    impl Drop for FailpointGuard {
        fn drop(&mut self) {
            if let Ok(mut guard) = registry().lock() {
                guard.remove(&self.thread);
            }
        }
    }

    pub fn install_failpoint(stage: BarrierStage) -> FailpointGuard {
        FailpointGuard::new(stage)
    }

    pub fn maybe_trigger(stage: BarrierStage) -> Result<()> {
        let thread = std::thread::current().id();
        if let Ok(mut guard) = registry().lock() {
            if let Some(current) = guard.get(&thread).copied() {
                if current == stage {
                    guard.remove(&thread);
                    bail!("barrier failpoint triggered at {:?}", stage);
                }
            }
        }
        Ok(())
    }
}

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
    barrier_clock: BarrierClock,
    ops: Vec<Box<dyn StreamOperator>>,
    queue: EventQueue,
    scan_operators: HashMap<RowStreamHandle, usize>,
    source_watermarks: HashMap<String, Timestamp>,
    checkpoint: Option<CheckpointManager>,
}

impl TickLoop {
    pub fn with_graph(
        runtime: ExecutionRuntime,
        ops: Vec<Box<dyn StreamOperator>>,
        queue: EventQueue,
        scan_operators: HashMap<RowStreamHandle, usize>,
        checkpoint: Option<CheckpointManager>,
    ) -> Self {
        Self {
            runtime,
            barrier_clock: BarrierClock::new(),
            ops,
            queue,
            scan_operators,
            source_watermarks: HashMap::new(),
            checkpoint,
        }
    }

    pub fn register_bindings(&mut self, bindings: &[(String, RowStreamHandle)]) -> Result<()> {
        self.runtime.register_bindings(bindings)?;
        for (source, _) in bindings {
            let initial = self
                .checkpoint
                .as_ref()
                .and_then(|manager| manager.latest_offsets().get(source))
                .copied()
                .unwrap_or(0);
            self.source_watermarks.insert(source.clone(), initial);
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
            self.record_source_offset(&ingested.source, ingested.timestamp);
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
        if self.barrier_clock.advance(watermark).is_none() {
            return Ok(());
        }
        let step_id = self.barrier_clock.step();
        self.drain_queue()?;
        for operator in self.ops.iter_mut() {
            operator.on_watermark(watermark)?;
        }
        self.seal_step(step_id, watermark).await
    }

    async fn seal_step(&mut self, _step_id: StepId, watermark: Timestamp) -> Result<()> {
        let operator_states = self.collect_operator_checkpoints().await?;
        run_barrier_hook(BarrierStage::AfterOperatorFlush)?;
        let materialized_views = self.collect_materialized_view_checkpoints().await?;
        run_barrier_hook(BarrierStage::AfterMaterializedViewFlush)?;
        run_barrier_hook(BarrierStage::BeforeManifestWrite)?;
        self.persist_checkpoint(watermark, operator_states, materialized_views)
            .await?;
        run_barrier_hook(BarrierStage::AfterManifestWrite)?;
        Ok(())
    }

    pub fn current_watermark(&self) -> Timestamp {
        self.barrier_clock.watermark()
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
            .unwrap_or(self.barrier_clock.watermark())
    }

    async fn collect_materialized_view_checkpoints(
        &mut self,
    ) -> Result<Vec<MaterializedViewCheckpointEntry>> {
        let mut entries = Vec::new();
        for operator in self.ops.iter_mut() {
            if let Some(materialize) = operator.as_any_mut().downcast_mut::<MaterializeOperator>() {
                if let Some(entry) = materialize.checkpoint_state().await? {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    async fn collect_operator_checkpoints(&mut self) -> Result<Vec<OperatorCheckpointEntry>> {
        let mut entries = Vec::new();
        for (idx, operator) in self.ops.iter_mut().enumerate() {
            if let Some(handles) = operator.checkpoint().await? {
                if !handles.is_empty() {
                    entries.push(OperatorCheckpointEntry {
                        operator_index: idx,
                        handles,
                    });
                }
            }
        }
        Ok(entries)
    }

    async fn persist_checkpoint(
        &mut self,
        watermark: Timestamp,
        operator_states: Vec<OperatorCheckpointEntry>,
        materialized_views: Vec<MaterializedViewCheckpointEntry>,
    ) -> Result<()> {
        if let Some(manager) = self.checkpoint.as_mut() {
            manager
                .persist(watermark, operator_states, materialized_views)
                .await?;
        }
        Ok(())
    }

    fn record_source_offset(&mut self, source: &str, offset: Timestamp) {
        if let Some(manager) = self.checkpoint.as_mut() {
            manager.update_offset(source, offset);
        }
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
    checkpoint_manifest: Option<&CheckpointManifest>,
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

    let view_checkpoint_map: HashMap<String, MaterializedViewCheckpointEntry> = checkpoint_manifest
        .map(|manifest| {
            manifest
                .materialized_views
                .iter()
                .cloned()
                .map(|entry| (entry.view.clone(), entry))
                .collect()
        })
        .unwrap_or_default();

    let mut built_ops: Vec<Box<dyn StreamOperator>> = Vec::with_capacity(plan.operators.len());
    for (idx, node) in plan.operators.iter().enumerate() {
        let op_id = OperatorId(idx);
        let targets = downstreams.get(&op_id).cloned().unwrap_or_else(Vec::new);
        let checkpoint_handles = checkpoint_manifest
            .and_then(|manifest| {
                manifest
                    .operator_states
                    .iter()
                    .find(|entry| entry.operator_index == idx)
            })
            .map(|entry| entry.handles.clone());

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
                let mut left_snapshot_data = None;
                let mut right_snapshot_data = None;
                let (left_state, right_state) = if let Some(bridge_ref) = bridge.as_mut() {
                    let left_table_name = format!("join_left_{idx}");
                    let right_table_name = format!("join_right_{idx}");
                    let left_namespace =
                        namespaces::operator_state(&plan.graph_id, idx, "left")?;
                    let right_namespace =
                        namespaces::operator_state(&plan.graph_id, idx, "right")?;
                    let left_stream = bridge_ref
                        .new_stream(
                            left_namespace.clone(),
                            StreamRetention::KeepLast { keep_last: 1 },
                        )
                        .await
                        .context("initialize left join state stream")?;
                    let right_stream = bridge_ref
                        .new_stream(
                            right_namespace.clone(),
                            StreamRetention::KeepLast { keep_last: 1 },
                        )
                        .await
                        .context("initialize right join state stream")?;
                    let left = StateTable::new(left_table_name.clone(), left_namespace.clone(), left_stream);
                    let right =
                        StateTable::new(right_table_name.clone(), right_namespace.clone(), right_stream);
                    if let Some(handles) = &checkpoint_handles {
                        for handle in handles {
                            let snapshot = bridge_ref
                                .handle_view_for(&handle.namespace, handle.version)
                                .await
                                .context("open join checkpoint handle")?
                                .materialize()
                                .await
                                .context("materialize join checkpoint")?;
                            if handle.table == left_table_name {
                                left_snapshot_data = Some(snapshot);
                            } else if handle.table == right_table_name {
                                right_snapshot_data = Some(snapshot);
                            }
                        }
                    }
                    (Some(left), Some(right))
                } else {
                    (None, None)
                };
                Box::new(
                    JoinOperator::new(
                        InputPort::new(join.left.operator, join.left.port_index),
                        InputPort::new(join.right.operator, join.right.port_index),
                        join.on.clone(),
                        join.projection.clone(),
                        sink,
                        left_state,
                        right_state,
                        left_snapshot_data,
                        right_snapshot_data,
                    )
                    .await
                    .context("create join operator")?,
                )
            }
            OperatorNode::Materialize(materialize) => {
                let checkpoint_entry = view_checkpoint_map.get(&materialize.view_name).cloned();
                let checkpoint_state = if let Some(entry) = checkpoint_entry {
                    let bridge_ref = bridge
                        .as_mut()
                        .ok_or_else(|| anyhow!("checkpoint manifest present without DB bridge"))?;
                    let handle = bridge_ref
                        .handle_view_for(&entry.namespace, entry.version)
                        .await
                        .context("open materialized view checkpoint handle")?;
                    let (dict, table, namespace, version) = handle.into_parts();
                    Some(DbspPersistedState::new(dict, table, namespace, version))
                } else {
                    None
                };
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
                    checkpoint_state,
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
    let (checkpoint_table, checkpoint_manifest) = if let Some(bridge_ref) = bridge.as_ref() {
        let table = bridge_ref.table();
        let store = CheckpointStore::new(table.clone(), plan.graph_id.clone());
        let manifest = store.load_latest().await?;
        (Some(table), manifest)
    } else {
        (None, None)
    };
    let built = build_graph(
        &ctx,
        plan,
        Arc::clone(&mv_registry),
        &queue,
        checkpoint_manifest.as_ref(),
        bridge.as_mut(),
    )
    .await?;

    let checkpoint = if let Some(table) = checkpoint_table {
        Some(
            CheckpointManager::new_with_manifest(
                plan.graph_id.clone(),
                table,
                checkpoint_manifest.clone(),
            )
            .await
            .context("initialize checkpoint manager")?,
        )
    } else {
        None
    };

    let runtime = ExecutionRuntime::new(ScanRuntime::new(sources));
    let mut tick = TickLoop::with_graph(
        runtime,
        built.ops,
        queue,
        built.scan_operator_map,
        checkpoint,
    );
    if let Some(manager) = tick.checkpoint.as_ref() {
        if let Some(manifest) = manager.latest_manifest() {
            tick.barrier_clock.bootstrap(manifest.watermark);
        }
    }
    tick.register_bindings(&built.scan_bindings)?;
    Ok(tick)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::scalar::ScalarValue;
    use dbsp::handles::ZSetHandleView;
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
    use object_store::{ObjectStore, memory::InMemory};
    use serde_json::json;
    use slatedb::Db;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::{BarrierStage, barrier_failpoints, *};
    use crate::checkpoint::CheckpointStore;
    use crate::circuit_builder::{Circuit, CircuitContext};
    use crate::dataflow_plan::{
        DataflowPlan, Expr, FilterNode, JoinNode, MapNode, MaterializeNode, OperatorNode, ScanNode,
    };
    use crate::encoding::encode_projected_row_key;
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

        let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map, None);
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
        let mut plan = DataflowPlan::new("bindings_test_plan");
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
        let mut plan = DataflowPlan::new("watermark_plan");
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
            None,
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

        let mut plan = DataflowPlan::new("build_graph_plan");
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
        let built = build_graph(&ctx, &plan, mv_registry.clone(), &queue, None, None)
            .await
            .expect("build graph");

        let scan_runtime = ScanRuntime::new(registry.clone());
        let runtime = ExecutionRuntime::new(scan_runtime);
        let mut tick = TickLoop::with_graph(
            runtime,
            built.ops,
            queue.clone(),
            built.scan_operator_map,
            None,
        );
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

        let mut plan = DataflowPlan::new("instantiate_plan");
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
        let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map, None);
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

        let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map, None);
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
    async fn nexmark_q0_plan_runs() {
        let snapshot = run_nexmark_plan(
            build_q0_plan(),
            vec![bid_full_definition()],
            vec![
                (
                    SourceEvent::new(
                        "bid",
                        json!({"auction": 1, "bidder": 2, "price": 100, "date_time": 10, "extra": 1}),
                    ),
                    1,
                ),
                (
                    SourceEvent::new(
                        "bid",
                        json!({"auction": 2, "bidder": 3, "price": 200, "date_time": 11, "extra": 2}),
                    ),
                    2,
                ),
            ],
            "mv_q0",
        )
        .await;
        assert_eq!(snapshot.len(), 2);
    }

    #[tokio::test]
    async fn nexmark_q1_plan_runs() {
        let snapshot = run_nexmark_plan(
            build_q1_plan(),
            vec![bid_full_definition()],
            vec![(
                SourceEvent::new(
                    "bid",
                    json!({"auction": 1, "bidder": 2, "price": 50, "date_time": 10, "extra": 1}),
                ),
                1,
            )],
            "mv_q1",
        )
        .await;
        let row = snapshot.keys().next().unwrap();
        assert_eq!(row[0], ScalarValue::Int64(Some(1)));
        assert_eq!(row[2], ScalarValue::Int64(Some(100)));
    }

    #[tokio::test]
    async fn nexmark_q2_plan_runs() {
        let snapshot = run_nexmark_plan(
            build_q2_plan(),
            vec![bid_full_definition()],
            vec![
                (
                    SourceEvent::new(
                        "bid",
                        json!({"auction": 123, "bidder": 3, "price": 75, "date_time": 10, "extra": 1}),
                    ),
                    1,
                ),
                (
                    SourceEvent::new(
                        "bid",
                        json!({"auction": 50, "bidder": 4, "price": 10, "date_time": 11, "extra": 2}),
                    ),
                    2,
                ),
            ],
            "mv_q2",
        )
        .await;
        assert_eq!(snapshot.len(), 1);
        assert!(
            snapshot
                .keys()
                .any(|row| row[0] == ScalarValue::Int64(Some(123)))
        );
    }

    #[tokio::test]
    async fn nexmark_q3_plan_runs() {
        let snapshot = run_nexmark_plan(
            build_q3_plan(),
            vec![auction_definition(), person_definition()],
            vec![
                (
                    SourceEvent::new("auction", json!({"id": 10, "seller": 1, "category": 10})),
                    1,
                ),
                (
                    SourceEvent::new(
                        "person",
                        json!({"id": 1, "name": "Alice", "city": "Portland", "state": "or"}),
                    ),
                    2,
                ),
            ],
            "mv_q3",
        )
        .await;
        assert!(
            snapshot
                .keys()
                .any(|row| row[1] == ScalarValue::Utf8(Some("Alice".to_string())))
        );
    }

    #[tokio::test]
    async fn persisted_materialized_view_survives_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("mv_persist", store).await.expect("open SlateDB"));

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        let source_registry = Arc::new(source_registry);

        let plan = {
            let mut plan = DataflowPlan::new("mv_persist_plan");
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

    #[tokio::test]
    async fn checkpoint_manifest_restores_materialized_view_version() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("mv_manifest_restore", store)
                .await
                .expect("open SlateDB"),
        );

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        let source_registry = Arc::new(source_registry);

        let plan = {
            let mut plan = DataflowPlan::new("mv_manifest_plan");
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

        let row = vec![ScalarValue::Int64(Some(7)), ScalarValue::Int64(Some(9))];
        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 7, "bidder": 9})),
            0,
        )])
        .await
        .expect("process events");
        tick.advance_watermark(5).await.expect("watermark");
        let original_key = encode_projected_row_key(&row).expect("encode original row");

        let manifest_bridge = DbspBridge::new(db.clone()).await.expect("manifest bridge");
        let checkpoint_store = CheckpointStore::new(manifest_bridge.table(), plan.graph_id.clone());
        let manifest = checkpoint_store
            .load_latest()
            .await
            .expect("load checkpoint")
            .expect("manifest exists");
        assert_eq!(manifest.materialized_views.len(), 1);
        let view_entry = manifest.materialized_views[0].clone();

        drop(tick);

        let extra_row = vec![ScalarValue::Int64(Some(999)), ScalarValue::Int64(Some(111))];
        let extra_key = encode_projected_row_key(&extra_row).expect("encode extra row");
        let mut stray_bridge = DbspBridge::new(db.clone()).await.expect("stray bridge");
        let mut stray_view = stray_bridge.new_view("mv_exec").await.expect("new view");
        stray_view.add_delta(extra_key.clone(), 1);
        stray_view.flush().await.expect("flush stray version");

        let mv_registry_restart = Arc::new(MaterializedViewRegistry::new());
        let tick_restart = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry_restart),
            Some(db.clone()),
        )
        .await
        .expect("instantiate restart");

        let view_restart = mv_registry_restart.get("mv_exec").expect("view registered");
        let dbsp_state = view_restart.dbsp_state().expect("dbsp state");
        assert_eq!(dbsp_state.version(), view_entry.version);

        let handle_view = ZSetHandleView::new(
            dbsp_state.dictionary(),
            dbsp_state.table(),
            dbsp_state.namespace().to_string(),
            dbsp_state.version(),
        );
        let snapshot = handle_view
            .materialize()
            .await
            .expect("materialize snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.get(&original_key), Some(&1));
        assert!(!snapshot.contains_key(&extra_key));

        drop(tick_restart);
    }

    #[tokio::test]
    async fn mid_tick_crash_discards_uncheckpointed_data() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("mid_tick_crash", store)
                .await
                .expect("open SlateDB"),
        );

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        let source_registry = Arc::new(source_registry);
        let plan = build_simple_materialize_plan("mid_tick_graph", "mv_mid_tick");

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate tick loop");

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 1, "bidder": 2})),
            0,
        )])
        .await
        .expect("process events");

        drop(tick);

        let mv_restart = Arc::new(MaterializedViewRegistry::new());
        let _ = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_restart),
            Some(db.clone()),
        )
        .await
        .expect("restart tick loop");

        let schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
        ]));
        let provider =
            MaterializedViewTableProvider::new(Arc::clone(&mv_restart), "mv_mid_tick", schema);
        let batches = provider
            .build_batches_for_test()
            .await
            .expect("materialize batches");
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 0);

        let table = DbspBridge::new(db.clone()).await.expect("bridge").table();
        let checkpoint_store = CheckpointStore::new(table, plan.graph_id.clone());
        assert!(
            checkpoint_store
                .load_latest()
                .await
                .expect("load checkpoint")
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operator_checkpoint_failpoint_discards_pending_state() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("operator_checkpoint_failpoint", store)
                .await
                .expect("open SlateDB"),
        );

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        source_registry.register(person_definition());
        let source_registry = Arc::new(source_registry);
        let plan = build_join_plan("join_failpoint_graph", "mv_join_failpoint");

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate tick loop");

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 10, "bidder": 1})),
            0,
        )])
        .await
        .expect("baseline left");
        tick.advance_watermark(5)
            .await
            .expect("baseline checkpoint");

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 20, "bidder": 2})),
            0,
        )])
        .await
        .expect("pending left");

        {
            let _guard = barrier_failpoints::install_failpoint(BarrierStage::AfterOperatorFlush);
            let err = tick
                .advance_watermark(10)
                .await
                .expect_err("failpoint error");
            assert!(
                err.to_string()
                    .contains("barrier failpoint triggered at AfterOperatorFlush")
            );
        }

        drop(tick);

        let mv_restart = Arc::new(MaterializedViewRegistry::new());
        let mut restarted = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_restart),
            Some(db.clone()),
        )
        .await
        .expect("restart tick");

        let right_events = vec![
            (
                SourceEvent::new(
                    "person",
                    json!({"id": 10, "name": "Alice", "city": "Portland", "state": "or"}),
                ),
                0,
            ),
            (
                SourceEvent::new(
                    "person",
                    json!({"id": 20, "name": "Bob", "city": "Seattle", "state": "wa"}),
                ),
                0,
            ),
        ];
        restarted
            .process_events(right_events)
            .await
            .expect("ingest right side");
        restarted
            .advance_watermark(30)
            .await
            .expect("post-restart watermark");

        let view = mv_restart
            .get("mv_join_failpoint")
            .expect("view registered");
        let snapshot = view.snapshot();
        assert_eq!(snapshot.len(), 1);
        let row = snapshot.keys().next().expect("row present");
        assert_eq!(row[0], ScalarValue::Int64(Some(10)));
        assert!(
            snapshot
                .keys()
                .all(|r| r[0] != ScalarValue::Int64(Some(20))),
            "uncommitted state should not be restored"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn materialized_view_failpoint_discards_pending_flush() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("mv_failpoint_flush", store)
                .await
                .expect("open SlateDB"),
        );

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        let source_registry = Arc::new(source_registry);
        let plan = build_simple_materialize_plan("mv_flush_graph", "mv_flush_failpoint");

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate tick loop");

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 7, "bidder": 9})),
            1,
        )])
        .await
        .expect("baseline event");
        tick.advance_watermark(5)
            .await
            .expect("baseline checkpoint");

        let baseline_version = {
            let table = DbspBridge::new(db.clone()).await.expect("bridge").table();
            let checkpoint_store = CheckpointStore::new(table, plan.graph_id.clone());
            checkpoint_store
                .load_latest()
                .await
                .expect("load baseline manifest")
                .expect("manifest present")
                .materialized_views
                .get(0)
                .map(|entry| entry.version)
                .expect("view entry")
        };

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 8, "bidder": 10})),
            0,
        )])
        .await
        .expect("pending row");

        {
            let _guard =
                barrier_failpoints::install_failpoint(BarrierStage::AfterMaterializedViewFlush);
            let err = tick
                .advance_watermark(10)
                .await
                .expect_err("failpoint error");
            assert!(
                err.to_string()
                    .contains("barrier failpoint triggered at AfterMaterializedViewFlush")
            );
        }

        drop(tick);

        let latest_version = {
            let table = DbspBridge::new(db.clone()).await.expect("bridge").table();
            let checkpoint_store = CheckpointStore::new(table, plan.graph_id.clone());
            checkpoint_store
                .load_latest()
                .await
                .expect("load manifest")
                .expect("manifest present")
                .materialized_views
                .get(0)
                .map(|entry| entry.version)
                .expect("view entry")
        };
        assert_eq!(latest_version, baseline_version);

        let mv_restart = Arc::new(MaterializedViewRegistry::new());
        let _ = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_restart),
            Some(db.clone()),
        )
        .await
        .expect("restart tick");

        let schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
        ]));
        let provider = MaterializedViewTableProvider::new(
            Arc::clone(&mv_restart),
            "mv_flush_failpoint",
            schema,
        );
        let batches = provider
            .build_batches_for_test()
            .await
            .expect("materialize batches");
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn offsets_failpoint_preserves_previous_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("offset_failpoint", store)
                .await
                .expect("open SlateDB"),
        );

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        let source_registry = Arc::new(source_registry);
        let plan = build_simple_materialize_plan("offset_graph", "mv_offset");

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate tick loop");

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 5, "bidder": 6})),
            0,
        )])
        .await
        .expect("baseline event");
        tick.advance_watermark(5)
            .await
            .expect("baseline checkpoint");

        let baseline_offset = {
            let table = DbspBridge::new(db.clone()).await.expect("bridge").table();
            let checkpoint_store = CheckpointStore::new(table, plan.graph_id.clone());
            let manifest = checkpoint_store
                .load_latest()
                .await
                .expect("load baseline manifest")
                .expect("manifest present");
            manifest
                .source_offsets
                .iter()
                .find(|offset| offset.source == "bid")
                .map(|offset| offset.offset)
                .expect("offset recorded")
        };

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 9, "bidder": 10})),
            0,
        )])
        .await
        .expect("pending event");

        {
            let _guard = barrier_failpoints::install_failpoint(BarrierStage::BeforeManifestWrite);
            let err = tick
                .advance_watermark(10)
                .await
                .expect_err("failpoint error");
            assert!(
                err.to_string()
                    .contains("barrier failpoint triggered at BeforeManifestWrite")
            );
        }

        drop(tick);

        let table = DbspBridge::new(db.clone()).await.expect("bridge").table();
        let checkpoint_store = CheckpointStore::new(table, plan.graph_id.clone());
        let manifest = checkpoint_store
            .load_latest()
            .await
            .expect("load manifest")
            .expect("manifest present");
        assert_eq!(manifest.source_offsets.len(), 1);
        let offset = &manifest.source_offsets[0];
        assert_eq!(offset.offset, baseline_offset);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn post_manifest_failpoint_preserves_commit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("post_manifest_failpoint", store)
                .await
                .expect("open SlateDB"),
        );

        let mut source_registry = SourceRegistry::new();
        source_registry.register(bid_definition());
        let source_registry = Arc::new(source_registry);
        let plan = build_simple_materialize_plan("post_manifest_graph", "mv_post_manifest");

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate tick loop");

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 3, "bidder": 4})),
            0,
        )])
        .await
        .expect("baseline event");
        tick.advance_watermark(5)
            .await
            .expect("baseline checkpoint");

        let baseline_manifest_id = {
            let table = DbspBridge::new(db.clone()).await.expect("bridge").table();
            let checkpoint_store = CheckpointStore::new(table, plan.graph_id.clone());
            checkpoint_store
                .load_latest()
                .await
                .expect("load baseline manifest")
                .expect("manifest present")
                .id
        };

        tick.process_events(vec![(
            SourceEvent::new("bid", json!({"auction": 4, "bidder": 5})),
            0,
        )])
        .await
        .expect("second event");

        {
            let _guard = barrier_failpoints::install_failpoint(BarrierStage::AfterManifestWrite);
            let err = tick
                .advance_watermark(10)
                .await
                .expect_err("failpoint error");
            assert!(
                err.to_string()
                    .contains("barrier failpoint triggered at AfterManifestWrite")
            );
        }

        drop(tick);

        let latest_manifest = {
            let table = DbspBridge::new(db.clone()).await.expect("bridge").table();
            let checkpoint_store = CheckpointStore::new(table, plan.graph_id.clone());
            checkpoint_store
                .load_latest()
                .await
                .expect("load manifest")
                .expect("manifest present")
        };
        assert!(latest_manifest.id > baseline_manifest_id);

        let mv_restart = Arc::new(MaterializedViewRegistry::new());
        let _ = instantiate_tick_loop(
            &plan,
            Arc::clone(&source_registry),
            Arc::clone(&mv_restart),
            Some(db.clone()),
        )
        .await
        .expect("restart tick");

        let schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
        ]));
        let provider =
            MaterializedViewTableProvider::new(Arc::clone(&mv_restart), "mv_post_manifest", schema);
        let batches = provider
            .build_batches_for_test()
            .await
            .expect("build batches");
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn join_state_restores_from_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("join_state_checkpoint", store)
                .await
                .expect("open SlateDB"),
        );

        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        registry.register(person_definition());
        let registry = Arc::new(registry);

        let mut plan = DataflowPlan::new("join_persist");
        let bid_scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let person_scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "person".to_string(),
            output: OutputPort::new(OperatorId(1), 0),
        }));
        let projection = (0..6).map(Expr::column).collect();
        let join = plan.add_operator(OperatorNode::Join(JoinNode {
            left: OutputPort::new(bid_scan, 0),
            right: OutputPort::new(person_scan, 0),
            output: OutputPort::new(OperatorId(2), 0),
            on: vec![(0, 0)],
            projection,
        }));
        let materialize = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(join, 0),
            view_name: "mv_join".to_string(),
        }));
        plan.set_root(materialize);

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        // First execution: ingest left side only and checkpoint.
        let mut tick = instantiate_tick_loop(
            &plan,
            Arc::clone(&registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate first tick loop");
        let left_event = vec![(
            SourceEvent::new("bid", json!({"auction": 10, "bidder": 77})),
            1,
        )];
        tick.process_events(left_event)
            .await
            .expect("process left side");
        tick.advance_watermark(5).await.expect("watermark left");
        drop(tick);

        // Restart and ingest right side to complete the join using restored state.
        let mut restarted = instantiate_tick_loop(
            &plan,
            Arc::clone(&registry),
            Arc::clone(&mv_registry),
            Some(db.clone()),
        )
        .await
        .expect("instantiate restart");
        let right_event = vec![(
            SourceEvent::new(
                "person",
                json!({"id": 10, "name": "Alice", "city": "Portland", "state": "or"}),
            ),
            6,
        )];
        restarted
            .process_events(right_event)
            .await
            .expect("process right side");
        restarted
            .advance_watermark(10)
            .await
            .expect("watermark right");

        let view = mv_registry.get("mv_join").expect("view registered");
        let snapshot = view.snapshot();
        assert_eq!(snapshot.len(), 1);
    }

    async fn run_nexmark_plan(
        plan: DataflowPlan,
        sources: Vec<SourceDefinition>,
        events: Vec<(SourceEvent, Timestamp)>,
        view_name: &str,
    ) -> HashMap<Row, Diff> {
        let mut registry = SourceRegistry::new();
        for definition in sources {
            registry.register(definition);
        }
        let registry = Arc::new(registry);
        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut tick =
            instantiate_tick_loop(&plan, Arc::clone(&registry), Arc::clone(&mv_registry), None)
                .await
                .expect("instantiate plan");
        tick.process_events(events).await.expect("process events");
        tick.advance_watermark(20).await.expect("watermark");
        mv_registry.get(view_name).expect("view").snapshot()
    }

    fn bid_full_definition() -> SourceDefinition {
        SourceDefinition::new(
            "bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
                SourceColumn::new("price", SourceDataType::Int64),
                SourceColumn::new("date_time", SourceDataType::Int64),
                SourceColumn::new("extra", SourceDataType::Int64),
            ],
        )
        .expect("definition")
    }

    fn auction_definition() -> SourceDefinition {
        SourceDefinition::new(
            "auction",
            vec![
                SourceColumn::new("id", SourceDataType::Int64),
                SourceColumn::new("seller", SourceDataType::Int64),
                SourceColumn::new("category", SourceDataType::Int64),
            ],
        )
        .expect("auction definition")
    }

    fn person_definition() -> SourceDefinition {
        SourceDefinition::new(
            "person",
            vec![
                SourceColumn::new("id", SourceDataType::Int64),
                SourceColumn::new("name", SourceDataType::Utf8),
                SourceColumn::new("city", SourceDataType::Utf8),
                SourceColumn::new("state", SourceDataType::Utf8),
            ],
        )
        .expect("person definition")
    }

    fn build_q0_plan() -> DataflowPlan {
        let mut plan = DataflowPlan::new("mv_q0");
        let scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let map = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(scan, 0),
            output: OutputPort::new(OperatorId(1), 0),
            expressions: vec![Expr::column(0), Expr::column(1), Expr::column(2)],
        }));
        let materialize = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(map, 0),
            view_name: "mv_q0".to_string(),
        }));
        plan.set_root(materialize);
        plan
    }

    fn build_q1_plan() -> DataflowPlan {
        let mut plan = DataflowPlan::new("mv_q1");
        let scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let map = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(scan, 0),
            output: OutputPort::new(OperatorId(1), 0),
            expressions: vec![
                Expr::column(0),
                Expr::column(1),
                Expr::Add(Box::new(Expr::column(2)), Box::new(Expr::column(2))),
            ],
        }));
        let materialize = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(map, 0),
            view_name: "mv_q1".to_string(),
        }));
        plan.set_root(materialize);
        plan
    }

    fn build_q2_plan() -> DataflowPlan {
        let mut plan = DataflowPlan::new("mv_q2");
        let scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let filter = plan.add_operator(OperatorNode::Filter(FilterNode {
            input: OutputPort::new(scan, 0),
            output: OutputPort::new(OperatorId(1), 0),
            predicate: Expr::Eq(
                Box::new(Expr::column(0)),
                Box::new(Expr::Literal(ScalarValue::Int64(Some(123)))),
            ),
        }));
        let map = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(filter, 0),
            output: OutputPort::new(OperatorId(2), 0),
            expressions: vec![Expr::column(0), Expr::column(2)],
        }));
        let materialize = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(map, 0),
            view_name: "mv_q2".to_string(),
        }));
        plan.set_root(materialize);
        plan
    }

    fn build_q3_plan() -> DataflowPlan {
        let mut plan = DataflowPlan::new("mv_q3");
        let auction_scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "auction".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let auction_filter = plan.add_operator(OperatorNode::Filter(FilterNode {
            input: OutputPort::new(auction_scan, 0),
            output: OutputPort::new(OperatorId(1), 0),
            predicate: Expr::Eq(
                Box::new(Expr::column(2)),
                Box::new(Expr::Literal(ScalarValue::Int64(Some(10)))),
            ),
        }));
        let auction_map = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(auction_filter, 0),
            output: OutputPort::new(OperatorId(2), 0),
            expressions: vec![Expr::column(0), Expr::column(1), Expr::column(2)],
        }));

        let person_scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "person".to_string(),
            output: OutputPort::new(OperatorId(3), 0),
        }));
        let person_filter = plan.add_operator(OperatorNode::Filter(FilterNode {
            input: OutputPort::new(person_scan, 0),
            output: OutputPort::new(OperatorId(4), 0),
            predicate: Expr::Or(
                Box::new(Expr::Eq(
                    Box::new(Expr::column(3)),
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some("or".into())))),
                )),
                Box::new(Expr::Or(
                    Box::new(Expr::Eq(
                        Box::new(Expr::column(3)),
                        Box::new(Expr::Literal(ScalarValue::Utf8(Some("id".into())))),
                    )),
                    Box::new(Expr::Eq(
                        Box::new(Expr::column(3)),
                        Box::new(Expr::Literal(ScalarValue::Utf8(Some("ca".into())))),
                    )),
                )),
            ),
        }));
        let person_map = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(person_filter, 0),
            output: OutputPort::new(OperatorId(5), 0),
            expressions: vec![
                Expr::column(0),
                Expr::column(1),
                Expr::column(2),
                Expr::column(3),
            ],
        }));

        let join = plan.add_operator(OperatorNode::Join(JoinNode {
            left: OutputPort::new(auction_map, 0),
            right: OutputPort::new(person_map, 0),
            output: OutputPort::new(OperatorId(6), 0),
            on: vec![(1, 0)],
            projection: vec![Expr::column(0), Expr::column(4)],
        }));

        let materialize = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(join, 0),
            view_name: "mv_q3".to_string(),
        }));
        plan.set_root(materialize);
        plan
    }

    fn build_simple_materialize_plan(graph_id: &str, view_name: &str) -> DataflowPlan {
        let mut plan = DataflowPlan::new(graph_id);
        let scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let map = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(scan, 0),
            output: OutputPort::new(OperatorId(1), 0),
            expressions: vec![Expr::column(0), Expr::column(1)],
        }));
        let materialize = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(map, 0),
            view_name: view_name.to_string(),
        }));
        plan.set_root(materialize);
        plan
    }

    fn build_join_plan(graph_id: &str, view_name: &str) -> DataflowPlan {
        let mut plan = DataflowPlan::new(graph_id);
        let bid_scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        let person_scan = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "person".to_string(),
            output: OutputPort::new(OperatorId(1), 0),
        }));
        let projection = (0..6).map(Expr::column).collect();
        let join = plan.add_operator(OperatorNode::Join(JoinNode {
            left: OutputPort::new(bid_scan, 0),
            right: OutputPort::new(person_scan, 0),
            output: OutputPort::new(OperatorId(2), 0),
            on: vec![(0, 0)],
            projection,
        }));
        let materialize = plan.add_operator(OperatorNode::Materialize(MaterializeNode {
            input: OutputPort::new(join, 0),
            view_name: view_name.to_string(),
        }));
        plan.set_root(materialize);
        plan
    }
}
