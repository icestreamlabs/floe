use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use dbsp::handles::ZSetHandle;
use dbsp::{DbspFilter, Stream, StreamRetention};

use crate::checkpoint::{CheckpointManifest, MaterializedViewCheckpointEntry};
use crate::circuit_builder::{CircuitContext, RowStreamHandle};
use crate::dataflow_plan::{DataflowPlan, OperatorNode};
use crate::dbsp_bridge::DbspBridge;
use crate::encoding::decode_projected_row_key;
use crate::expr_eval::evaluate_bool;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::namespaces;
use crate::operator_state::StateTable;
use crate::operators::{
    DispatchSink, EventQueue, FilterDbspState, FilterDerivedState, FilterOperator, JoinOperator,
    MapOperator, MaterializeOperator, NullSink, ScanOperator,
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
                let upstream_stream = operator_handle_streams
                    .get(filter.input.operator.0)
                    .and_then(|stream| stream.clone());
                let mut manual_state = None;
                let mut derived_state = None;
                if let Some(upstream) = upstream_stream {
                    let predicate_expr = std::sync::Arc::new(filter.predicate.clone());
                    let derived_predicate = {
                        let expr = std::sync::Arc::clone(&predicate_expr);
                        move |key: &Vec<u8>| -> bool {
                            let row =
                                decode_projected_row_key(key).expect("decode row for dbsp filter");
                            evaluate_bool(&expr, &row).expect("evaluate predicate for dbsp filter")
                        }
                    };
                    let dbsp_filter = DbspFilter::new::<Vec<u8>, _>(&upstream, derived_predicate)
                        .await
                        .context("build dbsp-derived filter stream")?;
                    let derived_stream = dbsp_filter.stream();
                    operator_handle_streams[idx] = Some(derived_stream.clone());
                    derived_state = Some(FilterDerivedState::new(
                        derived_stream,
                        format!("filter_output_{idx}"),
                    ));
                } else {
                    let output_table_name = format!("filter_output_{idx}");
                    let output_namespace =
                        namespaces::operator_state(&plan.graph_id, idx, "output")?;
                    let output_stream = bridge
                        .new_stream(
                            output_namespace.clone(),
                            StreamRetention::KeepLast { keep_last: 1 },
                        )
                        .await
                        .context("initialize filter output stream")?;
                    operator_handle_streams[idx] = Some(output_stream.handle_stream());
                    manual_state = Some(FilterDbspState::new(
                        output_stream,
                        output_table_name,
                        output_namespace,
                    ));
                }
                Box::new(FilterOperator::new(
                    InputPort::new(filter.input.operator, filter.input.port_index),
                    filter.predicate.clone(),
                    sink,
                    manual_state,
                    derived_state,
                ))
            }
            OperatorNode::Join(join) => {
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
                operator_handle_streams[idx] = Some(output_stream.handle_stream());
                let left_state =
                    StateTable::new(left_table_name.clone(), left_namespace.clone(), left_stream);
                let right_state = StateTable::new(
                    right_table_name.clone(),
                    right_namespace.clone(),
                    right_stream,
                );
                if let Some(handles) = &checkpoint_handles {
                    for handle in handles {
                        let snapshot = bridge
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
                Box::new(
                    JoinOperator::new(
                        InputPort::new(join.left.operator, join.left.port_index),
                        InputPort::new(join.right.operator, join.right.port_index),
                        join.on.clone(),
                        join.projection.clone(),
                        sink,
                        left_state,
                        right_state,
                        Some(output_stream),
                        Some(output_table_name.clone()),
                        Some(output_namespace.clone()),
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
                    let handle = bridge
                        .handle_view_for(&entry.namespace, entry.version)
                        .await
                        .context("open materialized view checkpoint handle")?;
                    let (dict, table, namespace, version) = handle.into_parts();
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
