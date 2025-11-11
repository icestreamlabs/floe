use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use dbsp::handles::ZSetHandle;
use dbsp::{Stream, StreamRetention};

use crate::checkpoint::{CheckpointManifest, DbspHandleRecord, handle_kinds};
use crate::circuit_builder::{CircuitContext, RowStreamHandle};
use crate::dataflow_plan::{DataflowPlan, OperatorNode};
use crate::dbsp_bridge::DbspBridge;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::namespaces;
use crate::operator_state::StateTable;
use crate::operators::{
    DispatchSink, EventQueue, FilterOperator, JoinOperator, MapDerivedState, MapOperator,
    MaterializeOperator, NullSink, ScanOperator,
};
use crate::stream_types::{InputPort, OperatorId, StreamOperator};

pub struct BuiltGraph {
    pub ops: Vec<Box<dyn StreamOperator>>,
    pub scan_bindings: Vec<(String, RowStreamHandle)>,
    pub scan_operator_map: HashMap<RowStreamHandle, usize>,
}

#[allow(clippy::too_many_arguments)]
pub async fn build_graph(
    ctx: &CircuitContext<'_>,
    plan: &DataflowPlan,
    mv_registry: Arc<MaterializedViewRegistry>,
    queue: &EventQueue,
    checkpoint_manifest: Option<&CheckpointManifest>,
    bridge: &mut DbspBridge,
    scan_handle_streams: &HashMap<String, Stream<ZSetHandle>>,
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
    let mut operator_handle_streams: Vec<Option<Stream<ZSetHandle>>> =
        vec![None; plan.operators.len()];
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

    let handle_lookup: HashMap<String, DbspHandleRecord> = checkpoint_manifest
        .map(|manifest| {
            manifest
                .dbsp_handles
                .iter()
                .cloned()
                .map(|handle| (handle_lookup_key(&handle.kind, &handle.name), handle))
                .collect()
        })
        .unwrap_or_default();

    let mut built_ops: Vec<Box<dyn StreamOperator>> = Vec::with_capacity(plan.operators.len());
    for (idx, node) in plan.operators.iter().enumerate() {
        let op_id = OperatorId(idx);
        let targets = downstreams.get(&op_id).cloned().unwrap_or_else(Vec::new);

        let operator: Box<dyn StreamOperator> = match node {
            OperatorNode::Scan(scan) => {
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                let handle_stream = scan_handle_streams
                    .get(&scan.source_name)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!(
                            "source '{}' does not expose a handle stream; handles are required",
                            scan.source_name
                        )
                    })?;
                operator_handle_streams[idx] = Some(handle_stream.clone());
                Box::new(ScanOperator::new(scan.source_name.clone(), sink))
            }
            OperatorNode::Map(map) => {
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                let upstream_stream =
                    require_handle_stream(&operator_handle_streams, map.input.operator)?;
                let output_table_name = format!("map_output_{idx}");
                let output_namespace = namespaces::operator_state(&plan.graph_id, idx, "output")?;
                let output_stream = bridge
                    .new_stream(
                        output_namespace.clone(),
                        StreamRetention::KeepLast { keep_last: 1 },
                    )
                    .await
                    .context("initialize map output stream")?;
                let output_handle_stream = output_stream.handle_stream();
                operator_handle_streams[idx] = Some(output_handle_stream.clone());
                let derived_state = Some(MapDerivedState::new(
                    output_handle_stream,
                    output_table_name.clone(),
                ));
                let table = bridge.table();
                Box::new(MapOperator::new_live(
                    InputPort::new(map.input.operator, map.input.port_index),
                    map.expressions.clone(),
                    sink,
                    upstream_stream,
                    table,
                    output_stream,
                    derived_state,
                ))
            }
            OperatorNode::Filter(filter) => {
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                let upstream_stream =
                    require_handle_stream(&operator_handle_streams, filter.input.operator)?;
                let filter_namespace = namespaces::operator_state(&plan.graph_id, idx, "filter")?;
                let output_stream = bridge
                    .new_stream(
                        filter_namespace.clone(),
                        StreamRetention::KeepLast { keep_last: 1 },
                    )
                    .await
                    .context("initialize filter output stream")?;
                let output_handle_stream = output_stream.handle_stream();
                operator_handle_streams[idx] = Some(output_handle_stream);
                let table = bridge.table();
                Box::new(FilterOperator::new_live(
                    InputPort::new(filter.input.operator, filter.input.port_index),
                    filter.predicate.clone(),
                    sink,
                    upstream_stream,
                    table,
                    output_stream,
                ))
            }
            OperatorNode::Join(join) => {
                let _ = require_handle_stream(&operator_handle_streams, join.left.operator)?;
                let _ = require_handle_stream(&operator_handle_streams, join.right.operator)?;
                let sink = DispatchSink::new(targets, Arc::clone(queue));
                let mut left_snapshot_data = None;
                let mut right_snapshot_data = None;
                let left_table_name = format!("join_left_{idx}");
                let right_table_name = format!("join_right_{idx}");
                let output_table_name = format!("join_output_{idx}");
                let left_namespace = namespaces::operator_state(&plan.graph_id, idx, "left")?;
                let right_namespace = namespaces::operator_state(&plan.graph_id, idx, "right")?;
                let output_namespace = namespaces::operator_state(&plan.graph_id, idx, "output")?;
                let left_stream = bridge
                    .new_stream(
                        left_namespace.clone(),
                        StreamRetention::KeepLast { keep_last: 1 },
                    )
                    .await
                    .context("initialize left join state stream")?;
                let right_stream = bridge
                    .new_stream(
                        right_namespace.clone(),
                        StreamRetention::KeepLast { keep_last: 1 },
                    )
                    .await
                    .context("initialize right join state stream")?;
                let output_stream = bridge
                    .new_stream(
                        output_namespace.clone(),
                        StreamRetention::KeepLast { keep_last: 1 },
                    )
                    .await
                    .context("initialize join output stream")?;
                let output_handle_stream = output_stream.handle_stream();
                operator_handle_streams[idx] = Some(output_handle_stream);
                let output_state = StateTable::new(
                    output_table_name.clone(),
                    output_namespace.clone(),
                    output_stream,
                );
                let left_state =
                    StateTable::new(left_table_name.clone(), left_namespace.clone(), left_stream);
                let right_state = StateTable::new(
                    right_table_name.clone(),
                    right_namespace.clone(),
                    right_stream,
                );
                if let Some(handle) = lookup_handle(
                    &handle_lookup,
                    handle_kinds::OPERATOR_STATE,
                    &left_table_name,
                ) {
                    left_snapshot_data = Some(
                        bridge
                            .handle_view_for(&handle.namespace, handle.version)
                            .await
                            .context("open join checkpoint handle")?
                            .materialize()
                            .await
                            .context("materialize join checkpoint")?,
                    );
                }
                if let Some(handle) = lookup_handle(
                    &handle_lookup,
                    handle_kinds::OPERATOR_STATE,
                    &right_table_name,
                ) {
                    right_snapshot_data = Some(
                        bridge
                            .handle_view_for(&handle.namespace, handle.version)
                            .await
                            .context("open join checkpoint handle")?
                            .materialize()
                            .await
                            .context("materialize join checkpoint")?,
                    );
                }
                Box::new(
                    JoinOperator::new(
                        InputPort::new(join.left.operator, join.left.port_index),
                        InputPort::new(join.right.operator, join.right.port_index),
                        join.on.clone(),
                        join.projection.clone(),
                        sink,
                        left_state,
                        right_state,
                        Some(output_state),
                        left_snapshot_data,
                        right_snapshot_data,
                    )
                    .await
                    .context("create join operator")?,
                )
            }
            OperatorNode::Materialize(materialize) => {
                let upstream_stream =
                    require_handle_stream(&operator_handle_streams, materialize.input.operator)?;
                let checkpoint_state = if let Some(handle) = lookup_handle(
                    &handle_lookup,
                    handle_kinds::MATERIALIZED_VIEW,
                    &materialize.view_name,
                ) {
                    let view_handle = bridge
                        .handle_view_for(&handle.namespace, handle.version)
                        .await
                        .context("open materialized view checkpoint handle")?;
                    let (dict, table, namespace, version) = view_handle.into_parts();
                    Some(DbspPersistedState::new(dict, table, namespace, version))
                } else {
                    None
                };
                let dbsp_view = Some(
                    bridge
                        .new_view(&materialize.view_name)
                        .await
                        .context("create DBSP view")?,
                );
                if let Some(view) = dbsp_view.as_ref() {
                    operator_handle_streams[idx] = Some(view.handle_stream());
                }
                let table = bridge.table();
                Box::new(
                    MaterializeOperator::new(
                        InputPort::new(materialize.input.operator, materialize.input.port_index),
                        materialize.view_name.clone(),
                        mv_registry.clone(),
                        NullSink::default(),
                        upstream_stream,
                        table,
                        dbsp_view,
                        checkpoint_state,
                    )
                    .await
                    .context("construct materialize operator")?,
                )
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

fn require_handle_stream(
    streams: &[Option<Stream<ZSetHandle>>],
    operator: OperatorId,
) -> Result<Stream<ZSetHandle>> {
    streams
        .get(operator.0)
        .and_then(|entry| entry.clone())
        .ok_or_else(|| {
            anyhow!(
                "operator {:?} requires upstream handle stream; delta fallback removed for MVP",
                operator
            )
        })
}

fn lookup_handle(
    handles: &HashMap<String, DbspHandleRecord>,
    kind: &str,
    name: &str,
) -> Option<DbspHandleRecord> {
    handles.get(&handle_lookup_key(kind, name)).cloned()
}

fn handle_lookup_key(kind: &str, name: &str) -> String {
    format!("{kind}:{name}")
}
