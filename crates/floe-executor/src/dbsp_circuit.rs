use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use dbsp::circuit::{
    CircuitNode, CircuitPlan, DbspJoinNode, DbspNodeKind, DbspProjectNode, DbspSelectNode,
};
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::StreamCursor;
use dbsp::stream::util::materialize_zset_handle;
use dbsp::{DbspFilter, DbspJoin, DbspMap, DeltaHandleStream};
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, DbspView};
use crate::dbsp_table_environment::DbspTableEnvironment;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::expression::ExpressionEvaluator;
use crate::join::JoinEvaluator;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::projection::ProjectionEvaluator;
use crate::stream_types::Row;
use crate::task_events::{GraphTaskSender, report_graph_task_error};

/// Runtime instance of a compiled DBSP circuit.
pub struct DbspCircuitInstance {
    /// DBSP plan we're executing.
    pub plan: CircuitPlan,

    /// For each node id: the stream of ZSetHandles this node produces.
    pub node_streams: Vec<DeltaHandleStream>,

    /// Helper map from node id: index in `node_streams`.
    pub id_to_ix: HashMap<usize, usize>,

    /// Root node id (taken from `plan.root`).
    pub root_id: usize,
}

impl DbspCircuitInstance {
    pub async fn build(plan: CircuitPlan, table_env: &DbspTableEnvironment) -> Result<Self> {
        let nodes = plan.nodes.clone();
        let mut instance = Self {
            root_id: plan.root,
            node_streams: Vec::with_capacity(nodes.len()),
            id_to_ix: HashMap::with_capacity(nodes.len()),
            plan,
        };

        for node in nodes {
            let stream = match &node.kind {
                DbspNodeKind::Source(source) => base_stream_for(table_env, source.table.name)?,
                DbspNodeKind::Select(select) => {
                    let upstream = instance.stream_for_input(&node, 0)?;
                    instance.compile_filter(select, upstream.clone()).await?
                }
                DbspNodeKind::Project(project) => {
                    let upstream = instance.stream_for_input(&node, 0)?;
                    instance.compile_map(project, upstream.clone()).await?
                }
                DbspNodeKind::Join(join) => {
                    let left = instance.stream_for_input(&node, 0)?;
                    let right = instance.stream_for_input(&node, 1)?;
                    instance
                        .compile_join(join, left.clone(), right.clone())
                        .await?
                }
                DbspNodeKind::Sink(_) | DbspNodeKind::Passthrough => {
                    let upstream = instance.stream_for_input(&node, 0)?;
                    upstream.clone()
                }
                DbspNodeKind::Aggregate(_)
                | DbspNodeKind::WindowAggregate(_)
                | DbspNodeKind::TopN(_)
                | DbspNodeKind::Union(_) => {
                    bail!("unsupported node kind in circuit executor: {:?}", node.kind)
                }
            };

            instance.set_stream(node.id, stream);
        }

        Ok(instance)
    }

    fn stream_for_input(&self, node: &CircuitNode, pos: usize) -> Result<&DeltaHandleStream> {
        let input_id = node.inputs.get(pos).copied().with_context(|| {
            anyhow!(
                "node {} missing required input at position {}",
                node.id,
                pos
            )
        })?;
        Ok(self.get_stream(input_id))
    }

    fn get_stream(&self, node_id: usize) -> &DeltaHandleStream {
        let ix = self.id_to_ix[&node_id];
        &self.node_streams[ix]
    }

    fn set_stream(&mut self, node_id: usize, stream: DeltaHandleStream) {
        let ix = self.node_streams.len();
        self.node_streams.push(stream);
        self.id_to_ix.insert(node_id, ix);
    }

    pub async fn attach_view(
        &self,
        bridge: &mut DbspBridge,
        registry: &MaterializedViewRegistry,
        view_name: &str,
        cancel: CancellationToken,
        task_events: GraphTaskSender,
    ) -> Result<()> {
        let root_stream = self.get_stream(self.root_id).clone();
        let mut cursor = StreamCursor::new(root_stream.stream());
        let table = bridge.table();
        let mut view = bridge.new_view(view_name).await?;

        let mut cache: HashMap<String, Arc<Dictionary<Vec<u8>>>> = HashMap::new();
        let view_handle = registry.register(view_name.to_string());

        if let Ok((ts, delta_handle)) = cursor.snapshot().await {
            match apply_delta_handle_to_view(
                &mut view,
                table.clone(),
                &mut cache,
                &delta_handle,
            )
            .await
            {
                Ok(snapshot_handle) => {
                    let latest = view.latest_handle_view();
                    let (dict, table, namespace, version) = latest.into_parts();
                    view_handle.set_dbsp_state(DbspPersistedState::new(
                        dict, table, namespace, version,
                    ));
                    view_handle.publish_version(ts, snapshot_handle);
                }
                Err(err) => {
                    tracing::error!(
                        view = %view_name,
                        error = %err,
                        "failed to seed materialized view"
                    );
                }
            }
        }

        let view_label = view_name.to_string();
        let task_label = format!("attach-view:{view_label}");
        let graph_id = view_label.clone();
        let task_events = task_events.clone();
        let table_for_task = table.clone();
        let view_handle = view_handle.clone();

        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut cursor = cursor;
            let mut view = view;
            let mut cache = cache;
            let table = table_for_task;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = cursor.next() => {
                        match result {
                            Ok((ts, delta_handle)) => match apply_delta_handle_to_view(
                                &mut view,
                                table.clone(),
                                &mut cache,
                                &delta_handle,
                            )
                            .await
                            {
                                Ok(snapshot_handle) => {
                                    let latest = view.latest_handle_view();
                                    let (dict, table, namespace, version) = latest.into_parts();
                                    view_handle.set_dbsp_state(DbspPersistedState::new(
                                        dict, table, namespace, version,
                                    ));
                                    view_handle.publish_version(ts, snapshot_handle);
                                }
                                Err(err) => {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        anyhow!(
                                            "failed to update materialized view '{view_label}': {err}"
                                        ),
                                    );
                                    break;
                                }
                            },
                            Err(err) => {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow!(
                                        "root stream cursor for view '{}' closed unexpectedly: {err}",
                                        view_label
                                    ),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn compile_filter(
        &self,
        node: &DbspSelectNode,
        upstream: DeltaHandleStream,
    ) -> Result<DeltaHandleStream> {
        let evaluator = Arc::new(ExpressionEvaluator::new(
            Arc::clone(node.output_schema()),
            node.predicate().expression(),
        ));
        let filter_pred = move |bytes: &Vec<u8>| -> bool {
            let row = match decode_row(bytes) {
                Ok(row) => row,
                Err(_) => return false,
            };
            evaluator.eval_bool(&row).unwrap_or(false)
        };
        let filter = DbspFilter::new::<Vec<u8>, _>(&upstream, filter_pred, None).await?;
        Ok(filter.stream())
    }

    async fn compile_map(
        &self,
        node: &DbspProjectNode,
        upstream: DeltaHandleStream,
    ) -> Result<DeltaHandleStream> {
        let projector_eval = Arc::new(ProjectionEvaluator::new(
            Arc::clone(node.input_schema()),
            node.expressions(),
        ));
        let projector = move |bytes: &Vec<u8>| -> Vec<u8> {
            let row = match decode_row(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to decode projection row");
                    return Vec::new();
                }
            };
            let projected = match projector_eval.project(&row) {
                Ok(projected) => projected,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to evaluate projection");
                    return Vec::new();
                }
            };
            match encode_projected_row_key(&projected) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to encode projected row");
                    Vec::new()
                }
            }
        };
        let map = DbspMap::new::<Vec<u8>, Vec<u8>, _>(&upstream, projector, None).await?;
        Ok(map.stream())
    }

    async fn compile_join(
        &self,
        node: &DbspJoinNode,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
    ) -> Result<DeltaHandleStream> {
        let evaluator = Arc::new(JoinEvaluator::new(node));
        let has_residual = evaluator.has_residual();

        let left_key_eval = Arc::clone(&evaluator);
        let left_key = move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let left_row = match decode_row(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to decode join left key");
                    return None;
                }
            };
            let key_columns = match left_key_eval.left_key(&left_row) {
                Ok(Some(columns)) => columns,
                Ok(None) => return None,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to evaluate join left key");
                    return None;
                }
            };
            match encode_projected_row_key(&key_columns) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(error = %err, "failed to encode join left key");
                    None
                }
            }
        };

        let right_key_eval = Arc::clone(&evaluator);
        let right_key = move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let right_row = match decode_row(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to decode join right key");
                    return None;
                }
            };
            let key_columns = match right_key_eval.right_key(&right_row) {
                Ok(Some(columns)) => columns,
                Ok(None) => return None,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to evaluate join right key");
                    return None;
                }
            };
            match encode_projected_row_key(&key_columns) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(error = %err, "failed to encode join right key");
                    None
                }
            }
        };

        let predicate_eval = Arc::clone(&evaluator);
        let predicate = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
            if !has_residual {
                return true;
            }
            let left_row = match decode_row(left_bytes) {
                Ok(row) => row,
                Err(_) => return false,
            };
            let right_row = match decode_row(right_bytes) {
                Ok(row) => row,
                Err(_) => return false,
            };
            predicate_eval
                .residual_matches(&left_row, &right_row)
                .unwrap_or(false)
        };

        let projector_eval = Arc::clone(&evaluator);
        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            let left_row = match decode_row(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to decode join left row");
                    return Vec::new();
                }
            };
            let right_row = match decode_row(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to decode join right row");
                    return Vec::new();
                }
            };
            let combined = projector_eval.project(&left_row, &right_row);
            match encode_projected_row_key(&combined) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to encode join projection row");
                    Vec::new()
                }
            }
        };

        let join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left,
            &right,
            left_key,
            right_key,
            predicate,
            projector,
            None,
        )
        .await?;
        Ok(join.stream())
    }
}

fn base_stream_for(
    env: &DbspTableEnvironment,
    table_name: &'static str,
) -> Result<DeltaHandleStream> {
    match table_name {
        "nexmark_person" | "person" => Ok(env.person.delta_handle_stream()),
        "nexmark_auction" | "auction" => Ok(env.auction.delta_handle_stream()),
        "nexmark_bid" | "bid" => Ok(env.bid.delta_handle_stream()),
        other => bail!("unknown source table '{other}'"),
    }
}

fn decode_row(bytes: &[u8]) -> Result<Row> {
    decode_projected_row_key(bytes).map_err(|err| anyhow!("failed to decode row: {err}"))
}

async fn apply_delta_handle_to_view(
    view: &mut DbspView,
    table: Arc<dyn KeyValueTable>,
    cache: &mut HashMap<String, Arc<Dictionary<Vec<u8>>>>,
    delta_handle: &ZSetHandle,
) -> Result<ZSetHandle> {
    let deltas = materialize_zset_handle::<Vec<u8>>(table, cache, delta_handle)
        .await
        .context("materialize delta handle for view")?;
    if !deltas.is_empty() {
        view.add_deltas(deltas);
    }
    view.flush().await.context("flush materialized view")
}
