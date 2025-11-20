use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use dbsp::circuit::{
    CircuitNode, CircuitPlan, DbspJoinNode, DbspNodeKind, DbspProjectNode, DbspSelectNode,
};
use dbsp::handles::ZSetHandle;
use dbsp::stream::StreamCursor;
use dbsp::stream::util::{compute_delta, materialize_zset_handle};
use dbsp::{DbspFilter, DbspJoin, DbspMap, Stream};

use crate::dbsp_bridge::DbspBridge;
use crate::dbsp_table_environment::DbspTableEnvironment;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::expression::ExpressionEvaluator;
use crate::join::JoinEvaluator;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::projection::ProjectionEvaluator;
use crate::stream_types::Row;

/// Runtime instance of a compiled DBSP circuit.
pub struct DbspCircuitInstance {
    /// DBSP plan we're executing.
    pub plan: CircuitPlan,

    /// For each node id: the stream of ZSetHandles this node produces.
    pub node_streams: Vec<Stream<ZSetHandle>>,

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

    fn stream_for_input(&self, node: &CircuitNode, pos: usize) -> Result<&Stream<ZSetHandle>> {
        let input_id = node.inputs.get(pos).copied().with_context(|| {
            anyhow!(
                "node {} missing required input at position {}",
                node.id,
                pos
            )
        })?;
        Ok(self.get_stream(input_id))
    }

    fn get_stream(&self, node_id: usize) -> &Stream<ZSetHandle> {
        let ix = self.id_to_ix[&node_id];
        &self.node_streams[ix]
    }

    fn set_stream(&mut self, node_id: usize, stream: Stream<ZSetHandle>) {
        let ix = self.node_streams.len();
        self.node_streams.push(stream);
        self.id_to_ix.insert(node_id, ix);
    }

    pub async fn attach_view(
        &self,
        bridge: &mut DbspBridge,
        registry: &MaterializedViewRegistry,
        view_name: &str,
    ) -> Result<()> {
        let root_stream = self.get_stream(self.root_id).clone();
        let mut cursor = StreamCursor::new(root_stream.clone());
        let table = bridge.table();
        let mut view = bridge.new_view(view_name).await?;

        let mut cache = HashMap::new();
        let mut previous: HashMap<Vec<u8>, i64> = HashMap::new();

        if let Ok((_ts, handle)) = cursor.snapshot().await {
            let current =
                materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &handle).await?;
            let deltas = compute_delta(&previous, &current);
            for (key, diff) in deltas {
                view.add_delta(key, diff);
            }
            view.flush().await?;
            previous = current;
        }

        let view_label = view_name.to_string();
        let table_for_task = table.clone();

        let registry_handle = registry.register(view_name.to_string());
        let view_handle = registry_handle.clone();

        tokio::spawn(async move {
            let mut cursor = cursor;
            let mut view = view;
            let mut cache = cache;
            let mut previous = previous;
            let table = table_for_task;
            loop {
                match cursor.next().await {
                    Ok((_ts, handle)) => {
                        match materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &handle)
                            .await
                        {
                            Ok(current) => {
                                let deltas = compute_delta(&previous, &current);
                                for (key, diff) in deltas {
                                    view.add_delta(key, diff);
                                }
                                if let Err(err) = view.flush().await {
                                    eprintln!(
                                        "failed to flush materialized view '{}': {err}",
                                        view_label
                                    );
                                } else {
                                    let latest = view.latest_handle_view();
                                    let (dict, table, namespace, version) = latest.into_parts();
                                    view_handle.set_dbsp_state(DbspPersistedState::new(
                                        dict, table, namespace, version,
                                    ));
                                }
                                previous = current;
                            }
                            Err(err) => {
                                eprintln!(
                                    "failed to materialize handle for view '{}': {err}",
                                    view_label
                                );
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "root stream cursor for view '{}' closed unexpectedly: {err}",
                            view_label
                        );
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn compile_filter(
        &self,
        node: &DbspSelectNode,
        upstream: Stream<ZSetHandle>,
    ) -> Result<Stream<ZSetHandle>> {
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
        let filter = DbspFilter::new::<Vec<u8>, _>(&upstream, filter_pred).await?;
        Ok(filter.stream())
    }

    async fn compile_map(
        &self,
        node: &DbspProjectNode,
        upstream: Stream<ZSetHandle>,
    ) -> Result<Stream<ZSetHandle>> {
        let projector_eval = Arc::new(ProjectionEvaluator::new(
            Arc::clone(node.input_schema()),
            node.expressions(),
        ));
        let projector = move |bytes: &Vec<u8>| -> Vec<u8> {
            let row = match decode_row(bytes) {
                Ok(row) => row,
                Err(_) => return Vec::new(),
            };
            match projector_eval.project(&row) {
                Ok(projected) => encode_projected_row_key(&projected)
                    .expect("projected row encoding must succeed"),
                Err(_) => Vec::new(),
            }
        };
        let map = DbspMap::new::<Vec<u8>, Vec<u8>, _>(&upstream, projector).await?;
        Ok(map.stream())
    }

    async fn compile_join(
        &self,
        node: &DbspJoinNode,
        left: Stream<ZSetHandle>,
        right: Stream<ZSetHandle>,
    ) -> Result<Stream<ZSetHandle>> {
        let evaluator = Arc::new(JoinEvaluator::new(node));

        let predicate_eval = Arc::clone(&evaluator);
        let predicate = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
            let left_row = match decode_row(left_bytes) {
                Ok(row) => row,
                Err(_) => return false,
            };
            let right_row = match decode_row(right_bytes) {
                Ok(row) => row,
                Err(_) => return false,
            };
            predicate_eval
                .matches(&left_row, &right_row)
                .unwrap_or(false)
        };

        let projector_eval = Arc::clone(&evaluator);
        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            let left_row = match decode_row(left_bytes) {
                Ok(row) => row,
                Err(_) => return Vec::new(),
            };
            let right_row = match decode_row(right_bytes) {
                Ok(row) => row,
                Err(_) => return Vec::new(),
            };
            let combined = projector_eval.project(&left_row, &right_row);
            encode_projected_row_key(&combined).expect("combined join row encoding must succeed")
        };

        let join =
            DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(&left, &right, predicate, projector)
                .await?;
        Ok(join.stream())
    }
}

fn base_stream_for(
    env: &DbspTableEnvironment,
    table_name: &'static str,
) -> Result<Stream<ZSetHandle>> {
    match table_name {
        "nexmark_person" | "person" => Ok(env.person.handle_stream()),
        "nexmark_auction" | "auction" => Ok(env.auction.handle_stream()),
        "nexmark_bid" | "bid" => Ok(env.bid.handle_stream()),
        other => bail!("unknown source table '{other}'"),
    }
}

fn decode_row(bytes: &Vec<u8>) -> Result<Row> {
    decode_projected_row_key(bytes).map_err(|err| anyhow!("failed to decode row: {err}"))
}
