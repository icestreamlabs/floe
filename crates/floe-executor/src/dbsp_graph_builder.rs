use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::dbsp_bridge::{DbspBridge, DbspView};
use crate::dbsp_plan::{ValidatedPlan, validate_dbsp_plan};
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use anyhow::{Context, Result, anyhow, bail};
use async_recursion::async_recursion;
use datafusion::common::Column;
use datafusion::logical_expr::{Case, Expr as DfExpr, Operator};
use datafusion::scalar::ScalarValue;
use dbsp::circuit::plan::{DbspJoinKey, DbspProjectExpr};
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use dbsp::stream::util::materialize_zset_handle;
use dbsp::{
    CircuitNode, CircuitPlan, DbspExpression, DbspFilter, DbspJoin, DbspJoinNode, DbspMap,
    DbspNodeKind, DbspPredicate, DbspProjectNode, DbspSelectNode, DbspSourceNode, RowSchema,
};
use tokio::sync::Mutex;

/// Orchestrates compilation of a [`CircuitPlan`] into DBSP streams backed by SlateDB.
pub struct DbspGraphBuilder {
    bridge: Arc<Mutex<DbspBridge>>,
    ns: GraphNamespace,
}

impl DbspGraphBuilder {
    pub async fn new(db: Arc<slatedb::Db>) -> Result<Self> {
        let bridge = DbspBridge::new(db).await?;
        Ok(Self {
            bridge: Arc::new(Mutex::new(bridge)),
            ns: GraphNamespace::default(),
        })
    }
    pub async fn build(&mut self, inputs: BuildInputs<'_>) -> Result<BuildOutputs> {
        self.ns.set_graph_id(inputs.graph_id);
        let available_sources: BTreeSet<String> =
            inputs.outer_handle_streams.keys().cloned().collect();
        let ValidatedPlan {
            required_sources, ..
        } = validate_dbsp_plan(inputs.plan, &available_sources, inputs.view_name)
            .context("validating query plan before DBSP graph build")?;
        let mut built = HashMap::new();
        let mut mv_latest = HashMap::new();
        let root_stream = self
            .compile_node(
                inputs.plan,
                inputs.plan.root,
                inputs.outer_handle_streams,
                &mut built,
                &inputs.mv_registry,
                &mut mv_latest,
            )
            .await?;

        if !mv_latest.contains_key(inputs.view_name) {
            let root_node = inputs.plan.node(inputs.plan.root).with_context(|| {
                anyhow!("root node {} missing from circuit plan", inputs.plan.root)
            })?;
            let root_schema = Arc::clone(&root_node.output_schema);
            self.materialize_view(
                inputs.view_name,
                root_schema,
                root_stream,
                &inputs.mv_registry,
                &mut mv_latest,
            )
            .await?;
        }

        Ok(BuildOutputs {
            node_streams: built,
            mv_latest,
            required_sources,
        })
    }

    #[async_recursion]
    async fn compile_node(
        &mut self,
        plan: &CircuitPlan,
        node_idx: usize,
        outer_streams: &HashMap<String, DeltaHandleStream>,
        built: &mut HashMap<usize, DeltaHandleStream>,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
    ) -> Result<DeltaHandleStream> {
        if let Some(stream) = built.get(&node_idx) {
            return Ok(stream.clone());
        }
        let node = plan
            .node(node_idx)
            .with_context(|| anyhow!("node {node_idx} missing from circuit plan"))?;

        let stream = match &node.kind {
            DbspNodeKind::Source(source) => self
                .compile_source(source, outer_streams)
                .await
                .with_context(|| anyhow!("source {}", source.table.name))?,
            DbspNodeKind::Select(select) => {
                let input_idx = first_input(node, "select")?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        built,
                        mv_registry,
                        mv_latest,
                    )
                    .await?;
                self.compile_filter(select, upstream).await?
            }
            DbspNodeKind::Project(project) => {
                let input_idx = first_input(node, "project")?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        built,
                        mv_registry,
                        mv_latest,
                    )
                    .await?;
                self.compile_map(project, upstream).await?
            }
            DbspNodeKind::Join(join) => {
                let (left_idx, right_idx) = join_inputs(node)?;
                let left = self
                    .compile_node(plan, left_idx, outer_streams, built, mv_registry, mv_latest)
                    .await?;
                let right = self
                    .compile_node(
                        plan,
                        right_idx,
                        outer_streams,
                        built,
                        mv_registry,
                        mv_latest,
                    )
                    .await?;
                self.compile_join(join, left, right).await?
            }
            DbspNodeKind::Sink(sink) => {
                let input_idx = first_input(node, "sink")?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        built,
                        mv_registry,
                        mv_latest,
                    )
                    .await?;
                self.materialize_view(
                    &sink.name,
                    Arc::clone(sink.input_schema()),
                    upstream,
                    mv_registry,
                    mv_latest,
                )
                .await?
            }
            DbspNodeKind::Aggregate(_)
            | DbspNodeKind::WindowAggregate(_)
            | DbspNodeKind::TopN(_)
            | DbspNodeKind::Union(_)
            | DbspNodeKind::Passthrough => {
                bail!("Unsupported in MVP: {:?}", node.kind)
            }
        };

        built.insert(node_idx, stream.clone());
        Ok(stream)
    }

    async fn compile_source(
        &self,
        source: &DbspSourceNode,
        outer_streams: &HashMap<String, DeltaHandleStream>,
    ) -> Result<DeltaHandleStream> {
        eprintln!(
            "Attaching DBSP source node '{}' to outer stream",
            source.table.name
        );
        let snapshot_stream = outer_streams
            .get(source.table.name)
            .cloned()
            .with_context(|| anyhow!("source '{}' has no handle stream", source.table.name))?;
        Ok(snapshot_stream)
    }

    async fn compile_filter(
        &mut self,
        node: &DbspSelectNode,
        upstream: DeltaHandleStream,
    ) -> Result<DeltaHandleStream> {
        let predicate = node.predicate().clone();
        let schema = Arc::clone(node.output_schema());
        let filter_pred = move |bytes: &Vec<u8>| -> bool {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode filter row: {err}");
                    return false;
                }
            };
            match eval_predicate(&predicate, &row, schema.as_ref()) {
                Ok(result) => result,
                Err(err) => {
                    eprintln!("failed to evaluate filter predicate: {err}");
                    false
                }
            }
        };
        let filter = DbspFilter::new::<Vec<u8>, _>(&upstream, filter_pred)
            .await
            .context("initialize DBSP filter")?;
        Ok(filter.stream())
    }

    async fn compile_map(
        &mut self,
        node: &DbspProjectNode,
        upstream: DeltaHandleStream,
    ) -> Result<DeltaHandleStream> {
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
        let schema = Arc::clone(node.input_schema());
        let projector = move |bytes: &Vec<u8>| -> Vec<u8> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode projection row: {err}");
                    return Vec::new();
                }
            };
            let projected = match eval_projection(expressions.as_ref(), &row, schema.as_ref()) {
                Ok(projected) => projected,
                Err(err) => {
                    eprintln!("failed to evaluate projection: {err}");
                    return Vec::new();
                }
            };
            match encode_projected_row_key(&projected) {
                Ok(encoded) => encoded,
                Err(err) => {
                    eprintln!("failed to encode projected row: {err}");
                    Vec::new()
                }
            }
        };
        let map = DbspMap::new::<Vec<u8>, Vec<u8>, _>(&upstream, projector)
            .await
            .context("initialize DBSP map")?;
        Ok(map.stream())
    }

    async fn compile_join(
        &mut self,
        node: &DbspJoinNode,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
    ) -> Result<DeltaHandleStream> {
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let residual = node.residual.clone();
        let output_schema = Arc::clone(&node.output_schema);

        let left_log_limit = Arc::new(AtomicUsize::new(3));
        let right_log_limit = Arc::new(AtomicUsize::new(3));

        let mut left_cursor = StreamCursor::new(left.stream());
        let mut right_cursor = StreamCursor::new(right.stream());
        if let Ok((ts, handle)) = left_cursor.snapshot().await
            && left_log_limit.fetch_sub(1, Ordering::Relaxed) > 0
        {
            eprintln!(
                "join left snapshot version {ts}, handle {}, schema width {}",
                handle.version,
                left_schema.len()
            );
            log_handle_rows("left snapshot", &handle, &self.bridge).await?;
        }
        if let Ok((ts, handle)) = right_cursor.snapshot().await
            && right_log_limit.fetch_sub(1, Ordering::Relaxed) > 0
        {
            eprintln!(
                "join right snapshot version {ts}, handle {}, schema width {}",
                handle.version,
                right_schema.len()
            );
            log_handle_rows("right snapshot", &handle, &self.bridge).await?;
        }
        let left_log_limit_clone = Arc::clone(&left_log_limit);
        let left_schema_clone = Arc::clone(&left_schema);
        let bridge_clone = Arc::clone(&self.bridge);
        tokio::spawn(async move {
            let mut cursor = left_cursor;
            while let Ok((ts, handle)) = cursor.next().await {
                if left_log_limit_clone.fetch_sub(1, Ordering::Relaxed) > 0 {
                    eprintln!(
                        "join left handle version {} at ts {} (schema width {})",
                        handle.version,
                        ts,
                        left_schema_clone.len()
                    );
                    if let Err(err) = log_handle_rows("left handle", &handle, &bridge_clone).await {
                        eprintln!("failed to log left handle rows: {err}");
                    }
                }
            }
        });
        let right_log_limit_clone = Arc::clone(&right_log_limit);
        let right_schema_clone = Arc::clone(&right_schema);
        let bridge_clone = Arc::clone(&self.bridge);
        tokio::spawn(async move {
            let mut cursor = right_cursor;
            while let Ok((ts, handle)) = cursor.next().await {
                if right_log_limit_clone.fetch_sub(1, Ordering::Relaxed) > 0 {
                    eprintln!(
                        "join right handle version {} at ts {} (schema width {})",
                        handle.version,
                        ts,
                        right_schema_clone.len()
                    );
                    if let Err(err) = log_handle_rows("right handle", &handle, &bridge_clone).await
                    {
                        eprintln!("failed to log right handle rows: {err}");
                    }
                }
            }
        });

        let key_indices =
            resolve_join_key_indices(&node.keys, left_schema.as_ref(), right_schema.as_ref())
                .context("resolve join key indices")?;
        let key_indices = Arc::new(key_indices);
        let left_key_indices = Arc::clone(&key_indices);
        let right_key_indices = Arc::clone(&key_indices);

        let left_key = move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let left_row = match decode_projected_row_key(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode join left key: {err}");
                    return None;
                }
            };
            let mut key_columns = Vec::with_capacity(left_key_indices.len());
            for (li, _) in left_key_indices.iter() {
                let value = match left_row.get(*li) {
                    Some(value) => value.clone(),
                    None => {
                        eprintln!("join left key index {li} out of bounds");
                        return None;
                    }
                };
                if value.is_null() {
                    return None;
                }
                key_columns.push(value);
            }
            match encode_projected_row_key(&key_columns) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    eprintln!("failed to encode join left key: {err}");
                    None
                }
            }
        };

        let right_key = move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let right_row = match decode_projected_row_key(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode join right key: {err}");
                    return None;
                }
            };
            let mut key_columns = Vec::with_capacity(right_key_indices.len());
            for (_, ri) in right_key_indices.iter() {
                let value = match right_row.get(*ri) {
                    Some(value) => value.clone(),
                    None => {
                        eprintln!("join right key index {ri} out of bounds");
                        return None;
                    }
                };
                if value.is_null() {
                    return None;
                }
                key_columns.push(value);
            }
            match encode_projected_row_key(&key_columns) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    eprintln!("failed to encode join right key: {err}");
                    None
                }
            }
        };

        let predicate = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
            let Some(expr) = residual.as_ref() else {
                return true;
            };
            let left_row = match decode_projected_row_key(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode join left row: {err}");
                    return false;
                }
            };
            let right_row = match decode_projected_row_key(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode join right row: {err}");
                    return false;
                }
            };
            let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
            combined.extend(left_row.into_iter());
            combined.extend(right_row.into_iter());
            match eval_expression(expr, &combined, output_schema.as_ref()) {
                Ok(result) => result,
                Err(err) => {
                    eprintln!("failed to evaluate join residual: {err}");
                    false
                }
            }
        };

        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            let mut combined = match decode_projected_row_key(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode join projection left row: {err}");
                    return Vec::new();
                }
            };
            let right_row = match decode_projected_row_key(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    eprintln!("failed to decode join projection right row: {err}");
                    return Vec::new();
                }
            };
            combined.extend(right_row);
            match encode_projected_row_key(&combined) {
                Ok(encoded) => encoded,
                Err(err) => {
                    eprintln!("failed to encode join projection row: {err}");
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
        )
        .await
        .context("initialize DBSP join")?;
        // Log the first output handle, if any, to verify join activity.
        let mut join_cursor = StreamCursor::new(join.stream().stream());
        if let Ok((ts, handle)) = join_cursor.snapshot().await {
            eprintln!(
                "join output snapshot version {} at ts {}",
                handle.version, ts
            );
            log_handle_rows("join output snapshot", &handle, &self.bridge).await?;
        }
        Ok(join.stream())
    }

    async fn materialize_view(
        &mut self,
        view_name: &str,
        schema: Arc<RowSchema>,
        upstream: DeltaHandleStream,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
    ) -> Result<DeltaHandleStream> {
        let handle_stream = upstream.clone();
        let registry_handle = mv_registry.register(view_name.to_string());
        let arrow_schema = schema.to_arrow_schema();
        mv_registry.set_schema(view_name.to_string(), Arc::clone(&arrow_schema));
        {
            let bridge = self.bridge.lock().await;
            bridge
                .save_mv_schema(view_name, Arc::clone(&arrow_schema))
                .await
                .with_context(|| format!("persist schema metadata for '{view_name}'"))?;
        }

        let mut view = {
            let mut bridge = self.bridge.lock().await;
            bridge
                .new_view(view_name)
                .await
                .with_context(|| format!("provision materialized view '{view_name}'"))?
        };
        let mut view_handle_stream = view.handle_stream();
        let view_frontier = view_handle_stream.committed_frontier();
        if view_frontier >= 0 {
            let handle = view_handle_stream.get(view_frontier).await?;
            let state = self.state_from_handle(&handle).await?;
            registry_handle.set_dbsp_state(state);
            registry_handle.publish_version(view_frontier, handle.clone());
            mv_latest.insert(view_name.to_string(), (view_frontier, handle));
        }

        let registry_clone = registry_handle.clone();
        let table = {
            let bridge = self.bridge.lock().await;
            bridge.table()
        };
        let cursor = StreamCursor::new(upstream.stream());
        let upstream_frontier = cursor.observed();
        let mut upstream_stream = handle_stream.stream();
        let mut dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>> = HashMap::new();
        if view_frontier < upstream_frontier {
            for ts in (view_frontier + 1)..=upstream_frontier {
                let delta_handle = upstream_stream
                    .get(ts)
                    .await
                    .with_context(|| {
                        format!("load delta handle for view '{view_name}' at {ts}")
                    })?;
                let snapshot_handle = Self::apply_delta_handle_to_view(
                    &mut view,
                    table.clone(),
                    &mut dict_cache,
                    &delta_handle,
                )
                .await
                .with_context(|| format!("apply delta for view '{view_name}' at {ts}"))?;
                let state = self.state_from_handle(&snapshot_handle).await?;
                registry_handle.set_dbsp_state(state);
                registry_handle.publish_version(ts, snapshot_handle.clone());
                mv_latest.insert(view_name.to_string(), (ts, snapshot_handle));
            }
        }

        let bridge_clone = Arc::clone(&self.bridge);
        let view_label = view_name.to_string();
        tokio::spawn(async move {
            let mut cursor = cursor;
            let mut view = view;
            let mut dict_cache = dict_cache;
            loop {
                match cursor.next().await {
                    Ok((ts, delta_handle)) => {
                        let snapshot_handle = match Self::apply_delta_handle_to_view(
                            &mut view,
                            table.clone(),
                            &mut dict_cache,
                            &delta_handle,
                        )
                        .await
                        {
                            Ok(handle) => handle,
                            Err(err) => {
                                eprintln!(
                                    "failed to apply delta for materialized view '{view_label}' at {ts}: {err}"
                                );
                                continue;
                            }
                        };
                        match Self::state_from_handle_with_bridge(&bridge_clone, &snapshot_handle)
                            .await
                        {
                            Ok(state) => {
                                registry_clone.set_dbsp_state(state);
                                registry_clone.publish_version(ts, snapshot_handle);
                                eprintln!(
                                    "materialized view '{view_label}' advanced to version {ts}"
                                );
                            }
                            Err(err) => {
                                eprintln!(
                                    "failed to update materialized view '{view_label}': {err}"
                                );
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "stream for materialized view '{view_label}' closed unexpectedly: {err}"
                        );
                        break;
                    }
                }
            }
        });

        Ok(handle_stream)
    }

    async fn apply_delta_handle_to_view(
        view: &mut DbspView,
        table: Arc<dyn KeyValueTable>,
        dict_cache: &mut HashMap<String, Arc<Dictionary<Vec<u8>>>>,
        delta_handle: &ZSetHandle,
    ) -> Result<ZSetHandle> {
        let deltas = materialize_zset_handle::<Vec<u8>>(table, dict_cache, delta_handle)
            .await
            .context("materialize delta handle for materialized view")?;
        if !deltas.is_empty() {
            view.add_deltas(deltas);
        }
        view.flush()
            .await
            .context("flush materialized view updates")
    }

    async fn state_from_handle(&self, handle: &ZSetHandle) -> Result<DbspPersistedState> {
        Self::state_from_handle_with_bridge(&self.bridge, handle).await
    }

    async fn state_from_handle_with_bridge(
        bridge: &Arc<Mutex<DbspBridge>>,
        handle: &ZSetHandle,
    ) -> Result<DbspPersistedState> {
        let mut guard = bridge.lock().await;
        let handle_view = guard
            .handle_view_for(&handle.ns, handle.version)
            .await
            .context("open handle view for materialized view state")?;
        let (dict, table, namespace, version) = handle_view.into_parts();
        Ok(DbspPersistedState::new(dict, table, namespace, version))
    }
}

#[derive(Default, Clone)]
struct GraphNamespace {
    graph_id: String,
}

impl GraphNamespace {
    fn set_graph_id(&mut self, graph_id: impl Into<String>) {
        self.graph_id = graph_id.into();
    }
}

pub struct BuildInputs<'a> {
    pub graph_id: &'a str,
    pub view_name: &'a str,
    pub plan: &'a CircuitPlan,
    pub mv_registry: Arc<MaterializedViewRegistry>,
    pub outer_handle_streams: &'a HashMap<String, DeltaHandleStream>,
}

pub struct BuildOutputs {
    pub node_streams: HashMap<usize, DeltaHandleStream>,
    pub mv_latest: HashMap<String, (i64, ZSetHandle)>,
    pub required_sources: BTreeSet<String>,
}

async fn log_handle_rows(
    label: &str,
    handle: &ZSetHandle,
    bridge: &Arc<Mutex<DbspBridge>>,
) -> Result<()> {
    let mut guard = bridge.lock().await;
    let handle_view = guard
        .handle_view_for(&handle.ns, handle.version)
        .await
        .context("open handle view for logging")?;
    let materialized = handle_view.materialize().await?;
    let total = materialized.len();
    let mut rows = Vec::new();
    for (row, diff) in materialized.into_iter().take(3) {
        let decoded = decode_projected_row_key(&row);
        rows.push((decoded, diff));
    }
    eprintln!("{label}: row_count={}, first_rows={:?}", total, rows);
    Ok(())
}

fn first_input(node: &CircuitNode, label: &str) -> Result<usize> {
    node.inputs
        .first()
        .copied()
        .with_context(|| anyhow!("{label} node missing required input"))
}

fn join_inputs(node: &CircuitNode) -> Result<(usize, usize)> {
    if node.inputs.len() < 2 {
        bail!("join node requires two inputs");
    }
    Ok((node.inputs[0], node.inputs[1]))
}

fn eval_predicate(
    predicate: &DbspPredicate,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<bool> {
    let value = eval_df_expr(predicate.expression().expr(), row, schema)?;
    scalar_to_bool(&value)
}

fn eval_projection(
    expressions: &[DbspProjectExpr],
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<Vec<ScalarValue>> {
    expressions
        .iter()
        .map(|expr| eval_df_expr(expr.expression().expr(), row, schema))
        .collect()
}

fn resolve_join_key_indices(
    keys: &[DbspJoinKey],
    left_schema: &RowSchema,
    right_schema: &RowSchema,
) -> Result<Vec<(usize, usize)>> {
    let mut indices = Vec::with_capacity(keys.len());
    for key in keys {
        let left_name = match key.left_expression().expr() {
            DfExpr::Column(column) => column.name.clone(),
            other => {
                bail!("join key expression must be a column on the left, found {other:?}");
            }
        };
        let right_name = match key.right_expression().expr() {
            DfExpr::Column(column) => column.name.clone(),
            other => {
                bail!("join key expression must be a column on the right, found {other:?}");
            }
        };
        let left_idx = left_schema
            .field_index(&left_name)
            .ok_or_else(|| anyhow!("left join key column '{left_name}' must exist"))?;
        let right_idx = right_schema
            .field_index(&right_name)
            .ok_or_else(|| anyhow!("right join key column '{right_name}' must exist"))?;
        indices.push((left_idx, right_idx));
    }
    Ok(indices)
}

fn eval_expression(expr: &DbspExpression, row: &[ScalarValue], schema: &RowSchema) -> Result<bool> {
    let value = eval_df_expr(expr.expr(), row, schema)?;
    scalar_to_bool(&value)
}

fn eval_df_expr(expr: &DfExpr, row: &[ScalarValue], schema: &RowSchema) -> Result<ScalarValue> {
    match expr {
        DfExpr::Alias(alias) => eval_df_expr(alias.expr.as_ref(), row, schema),
        DfExpr::Column(column) => {
            let idx = resolve_column(schema, column)?;
            row.get(idx)
                .cloned()
                .ok_or_else(|| anyhow!("column index {idx} out of bounds"))
        }
        DfExpr::Literal(value, _) => Ok(value.clone()),
        DfExpr::BinaryExpr(binary) => {
            let left = eval_df_expr(binary.left.as_ref(), row, schema)?;
            let right = eval_df_expr(binary.right.as_ref(), row, schema)?;
            eval_binary(binary.op, left, right)
        }
        DfExpr::Not(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = scalar_to_bool_opt(&value)?.map(|val| !val);
            Ok(ScalarValue::Boolean(result))
        }
        DfExpr::Negative(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let negated = match value {
                ScalarValue::Int64(v) => ScalarValue::Int64(v.map(|val| -val)),
                ScalarValue::TimestampMillisecond(v, tz) => {
                    ScalarValue::TimestampMillisecond(v.map(|val| -val), tz)
                }
                other => bail!("unsupported type for negation: {other:?}"),
            };
            Ok(negated)
        }
        DfExpr::IsNull(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(value.is_null())))
        }
        DfExpr::IsNotNull(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!value.is_null())))
        }
        DfExpr::IsTrue(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = matches!(scalar_to_bool_opt(&value)?, Some(true));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::IsNotTrue(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = !matches!(scalar_to_bool_opt(&value)?, Some(true));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::IsFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = matches!(scalar_to_bool_opt(&value)?, Some(false));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::IsNotFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = !matches!(scalar_to_bool_opt(&value)?, Some(false));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::Like(like) => {
            let value = eval_df_expr(like.expr.as_ref(), row, schema)?;
            let pattern_value = eval_df_expr(like.pattern.as_ref(), row, schema)?;
            let text = match value {
                ScalarValue::Utf8(Some(text)) => text,
                _ => bail!("LIKE expects utf8 input"),
            };
            let pattern = match pattern_value {
                ScalarValue::Utf8(Some(pattern)) => pattern,
                _ => bail!("LIKE pattern must be utf8 literal"),
            };
            Ok(ScalarValue::Boolean(Some(matches_like(&text, &pattern))))
        }
        DfExpr::Cast(cast) => {
            let value = eval_df_expr(cast.expr.as_ref(), row, schema)?;
            match &cast.data_type {
                datafusion::arrow::datatypes::DataType::Timestamp(_, _) => {
                    let number = scalar_to_i64(&value, "cast to timestamp")?;
                    Ok(ScalarValue::TimestampMillisecond(Some(number), None))
                }
                datafusion::arrow::datatypes::DataType::Int64 => {
                    let number = scalar_to_i64(&value, "cast to int64")?;
                    Ok(ScalarValue::Int64(Some(number)))
                }
                other => bail!("unsupported cast target {other:?}"),
            }
        }
        DfExpr::Case(case) => eval_case(case, row, schema),
        other => bail!("unsupported expression: {other:?}"),
    }
}

fn eval_case(case: &Case, row: &[ScalarValue], schema: &RowSchema) -> Result<ScalarValue> {
    if let Some(base) = case.expr.as_ref() {
        let base_value = eval_df_expr(base, row, schema)?;
        for (when, then) in &case.when_then_expr {
            let when_value = eval_df_expr(when, row, schema)?;
            if scalar_equals(&when_value, &base_value)?.unwrap_or(false) {
                return eval_df_expr(then, row, schema);
            }
        }
    } else {
        for (when, then) in &case.when_then_expr {
            let when_value = eval_df_expr(when, row, schema)?;
            if scalar_to_bool(&when_value)? {
                return eval_df_expr(then, row, schema);
            }
        }
    }

    if let Some(else_expr) = case.else_expr.as_ref() {
        eval_df_expr(else_expr, row, schema)
    } else {
        Ok(ScalarValue::Null)
    }
}

fn eval_binary(op: Operator, left: ScalarValue, right: ScalarValue) -> Result<ScalarValue> {
    match op {
        Operator::Eq => Ok(ScalarValue::Boolean(scalar_equals(&left, &right)?)),
        Operator::NotEq => {
            let result = scalar_equals(&left, &right)?.map(|value| !value);
            Ok(ScalarValue::Boolean(result))
        }
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            let ordering = scalar_compare(&left, &right, op)?;
            Ok(ScalarValue::Boolean(ordering))
        }
        Operator::And => {
            let lhs = scalar_to_bool_opt(&left)?;
            let rhs = scalar_to_bool_opt(&right)?;
            let result = match (lhs, rhs) {
                (Some(false), _) => Some(false),
                (Some(true), other) => other,
                (None, Some(false)) => Some(false),
                (None, Some(true)) => None,
                (None, None) => None,
            };
            Ok(ScalarValue::Boolean(result))
        }
        Operator::Or => {
            let lhs = scalar_to_bool_opt(&left)?;
            let rhs = scalar_to_bool_opt(&right)?;
            let result = match (lhs, rhs) {
                (Some(true), _) => Some(true),
                (Some(false), other) => other,
                (None, Some(true)) => Some(true),
                (None, Some(false)) => None,
                (None, None) => None,
            };
            Ok(ScalarValue::Boolean(result))
        }
        Operator::Plus
        | Operator::Minus
        | Operator::Multiply
        | Operator::Divide
        | Operator::Modulo => {
            let lhs = scalar_to_i64(&left, "arithmetic")?;
            let rhs = scalar_to_i64(&right, "arithmetic")?;
            let value = match op {
                Operator::Plus => lhs + rhs,
                Operator::Minus => lhs - rhs,
                Operator::Multiply => lhs * rhs,
                Operator::Divide => lhs / rhs,
                Operator::Modulo => lhs % rhs,
                _ => unreachable!(),
            };
            Ok(ScalarValue::Int64(Some(value)))
        }
        Operator::StringConcat => {
            let lhs = match left {
                ScalarValue::Utf8(Some(value)) => value,
                _ => bail!("string concat expects utf8 operands"),
            };
            let rhs = match right {
                ScalarValue::Utf8(Some(value)) => value,
                _ => bail!("string concat expects utf8 operands"),
            };
            Ok(ScalarValue::Utf8(Some(lhs + &rhs)))
        }
        _ => bail!("unsupported binary operator {op:?}"),
    }
}

fn scalar_compare(lhs: &ScalarValue, rhs: &ScalarValue, op: Operator) -> Result<Option<bool>> {
    if lhs.is_null() || rhs.is_null() {
        return Ok(None);
    }
    let ordering = match (lhs, rhs) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (
            ScalarValue::TimestampMillisecond(Some(l), _),
            ScalarValue::TimestampMillisecond(Some(r), _),
        ) => l.cmp(r),
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => l.cmp(r),
        _ => bail!("unsupported comparison operands: {lhs:?} vs {rhs:?}"),
    };
    let result = match op {
        Operator::Lt => ordering.is_lt(),
        Operator::LtEq => ordering.is_le(),
        Operator::Gt => ordering.is_gt(),
        Operator::GtEq => ordering.is_ge(),
        _ => unreachable!(),
    };
    Ok(Some(result))
}

// SQL comparisons involving NULL yield NULL (unknown).
fn scalar_equals(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<Option<bool>> {
    if lhs.is_null() || rhs.is_null() {
        return Ok(None);
    }
    let result = match (lhs, rhs) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l == r,
        (
            ScalarValue::TimestampMillisecond(Some(l), _),
            ScalarValue::TimestampMillisecond(Some(r), _),
        ) => l == r,
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => l == r,
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => l == r,
        _ => false,
    };
    Ok(Some(result))
}

// SQL predicate contexts treat NULL as false (unknown).
fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    Ok(scalar_to_bool_opt(value)?.unwrap_or(false))
}

fn scalar_to_bool_opt(value: &ScalarValue) -> Result<Option<bool>> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(Some(*v)),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(None),
        other => bail!("expected boolean value, found {other:?}"),
    }
}

fn scalar_to_i64(value: &ScalarValue, context: &str) -> Result<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Ok(*v),
        other => bail!("{context} expects Int64, found {other:?}"),
    }
}

fn matches_like(value: &str, pattern: &str) -> bool {
    if !pattern.contains('%') {
        return value == pattern;
    }
    if let Some(stripped) = pattern.strip_prefix('%') {
        return value.ends_with(stripped);
    }
    if let Some(stripped) = pattern.strip_suffix('%') {
        return value.starts_with(stripped);
    }
    false
}

fn resolve_column(schema: &RowSchema, column: &Column) -> Result<usize> {
    let qualified = column.flat_name();
    if let Some(idx) = schema.field_index(&qualified) {
        return Ok(idx);
    }
    schema
        .field_index(&column.name)
        .ok_or_else(|| anyhow!("column {} not found in schema", column.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::Expr as DfExpr;
    use dbsp::circuit::plan::DbspJoinType;
    use dbsp::circuit::schema::Field;
    use dbsp::circuit::types::DbspScalarType;

    fn schema() -> Arc<RowSchema> {
        RowSchema::try_new(vec![Field::new("id", DbspScalarType::Int64, true)]).expect("schema")
    }

    #[test]
    fn join_key_requires_column_expressions() {
        let schema = schema();
        let left_expr = DfExpr::Literal(ScalarValue::Int64(Some(1)), None);
        let right_expr = DfExpr::Column(Column::new_unqualified("id".to_string()));
        let node = DbspJoinNode::try_new(
            DbspJoinType::Inner,
            Arc::clone(&schema),
            Arc::clone(&schema),
            vec![(left_expr, right_expr)],
            None,
        )
        .expect("join node");

        let err =
            resolve_join_key_indices(&node.keys, schema.as_ref(), schema.as_ref()).unwrap_err();
        assert!(err.to_string().contains("left"));
    }

    #[test]
    fn join_key_rejects_non_column_on_right() {
        let schema = schema();
        let left_expr = DfExpr::Column(Column::new_unqualified("id".to_string()));
        let right_expr = DfExpr::Literal(ScalarValue::Int64(Some(1)), None);
        let node = DbspJoinNode::try_new(
            DbspJoinType::Inner,
            Arc::clone(&schema),
            Arc::clone(&schema),
            vec![(left_expr, right_expr)],
            None,
        )
        .expect("join node");

        let err =
            resolve_join_key_indices(&node.keys, schema.as_ref(), schema.as_ref()).unwrap_err();
        assert!(err.to_string().contains("right"));
    }
}
