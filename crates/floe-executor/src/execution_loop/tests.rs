use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::scalar::ScalarValue;
use dbsp::handles::ZSetHandleView;
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition, SourceEvent};
use object_store::{ObjectStore, memory::InMemory};
use serde_json::json;
use slatedb::Db;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use super::barrier::{BarrierStage, barrier_failpoints};
use super::*;
use crate::checkpoint::CheckpointStore;
use crate::circuit_builder::{Circuit, CircuitContext, RowStreamHandle, SourceRegistry};
use crate::dataflow_plan::{
    DataflowPlan, Expr, FilterNode, JoinNode, MapNode, MaterializeNode, OperatorNode, ScanNode,
};
use crate::dbsp_bridge::DbspBridge;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::materialized_view::MaterializedViewRegistry;
use crate::operators::test_support::TestSink;
use crate::operators::{DispatchSink, EventQueue, ScanOperator};
use crate::stream_types::{Diff, Row, StreamOperator, Timestamp};
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

    let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map, None, None);
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
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("build-graph-runtime", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let built = build_graph(&ctx, &plan, mv_registry.clone(), &queue, None, &mut bridge)
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
    let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map, None, None);
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

    let mut tick = TickLoop::with_graph(runtime, ops, queue.clone(), scan_map, None, None);
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
    let provider =
        MaterializedViewTableProvider::new(Arc::clone(&mv_restart), "mv_flush_failpoint", schema);
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("materialize batches");
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn checkpoint_records_outer_stream_handles_with_offsets() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("outer_stream_handles", store)
            .await
            .expect("open SlateDB"),
    );

    let mut registry = SourceRegistry::new();
    registry.register(bid_full_definition());
    let registry = Arc::new(registry);
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let plan = build_simple_materialize_plan("outer_stream_graph", "mv_outer_stream");
    let mut tick = instantiate_tick_loop(
        &plan,
        Arc::clone(&registry),
        Arc::clone(&mv_registry),
        Some(db.clone()),
    )
    .await
    .expect("instantiate tick loop");

    tick.process_events(vec![(
        SourceEvent::new(
            "bid",
            json!({"auction": 1, "bidder": 2, "price": 3, "date_time": 4, "extra": 5}),
        ),
        1,
    )])
    .await
    .expect("process event");
    tick.advance_watermark(10).await.expect("seal checkpoint");

    let manifest = tick
        .checkpoint
        .as_ref()
        .expect("checkpoint manager")
        .store()
        .load_latest()
        .await
        .expect("load manifest")
        .expect("manifest present");
    assert_eq!(manifest.outer_streams.len(), 1);
    let entry = &manifest.outer_streams[0];
    assert_eq!(entry.source, "bid");
    assert_eq!(entry.namespace, "src/bid");
    assert_eq!(entry.partition, 0);
    assert_eq!(entry.offset, 1);
    assert!(entry.version >= 1);
}

#[tokio::test]
async fn source_replay_materializes_outer_stream_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("outer_stream_replay", store)
            .await
            .expect("open SlateDB"),
    );

    let mut registry = SourceRegistry::new();
    registry.register(bid_full_definition());
    let registry = Arc::new(registry);
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let plan = build_simple_materialize_plan("outer_stream_replay_graph", "mv_outer_replay");
    let mut tick = instantiate_tick_loop(
        &plan,
        Arc::clone(&registry),
        Arc::clone(&mv_registry),
        Some(db.clone()),
    )
    .await
    .expect("instantiate tick loop");

    let row = SourceEvent::new(
        "bid",
        json!({"auction": 9, "bidder": 10, "price": 11, "date_time": 12, "extra": 13}),
    );
    tick.process_events(vec![(row, 4)])
        .await
        .expect("process bid");
    tick.advance_watermark(20).await.expect("checkpoint");

    let manifest = tick
        .checkpoint
        .as_ref()
        .expect("checkpoint manager")
        .store()
        .load_latest()
        .await
        .expect("load manifest")
        .expect("manifest present");
    assert_eq!(manifest.outer_streams.len(), 1);
    let entry = &manifest.outer_streams[0];

    let mut bridge = DbspBridge::new(db.clone()).await.expect("bridge");
    let view = bridge
        .handle_view_for(&entry.namespace, entry.version)
        .await
        .expect("open outer stream handle");
    let materialized = view.materialize().await.expect("materialize outer stream");
    assert_eq!(materialized.len(), 1);
    let (key, diff) = materialized.iter().next().expect("entry");
    assert_eq!(*diff, 1);
    let decoded = decode_projected_row_key(key).expect("decode row");
    assert_eq!(decoded[0], ScalarValue::Int64(Some(9)));
    assert_eq!(decoded[1], ScalarValue::Int64(Some(10)));
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
