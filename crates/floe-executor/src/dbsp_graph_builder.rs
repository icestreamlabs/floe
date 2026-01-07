use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::dbsp_bridge::DbspBridge;
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
use dbsp::stream::StreamCursor;
use dbsp::stream::operations::basic::differentiate_zset_stream_live;
use dbsp::{
    CircuitNode, CircuitPlan, DbspExpression, DbspFilter, DbspJoin, DbspJoinNode, DbspMap,
    DbspNodeKind, DbspPredicate, DbspProjectNode, DbspSelectNode, DbspSourceNode, RowSchema,
    Stream,
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
        outer_streams: &HashMap<String, Stream<ZSetHandle>>,
        built: &mut HashMap<usize, Stream<ZSetHandle>>,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
    ) -> Result<Stream<ZSetHandle>> {
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
        outer_streams: &HashMap<String, Stream<ZSetHandle>>,
    ) -> Result<Stream<ZSetHandle>> {
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
        upstream: Stream<ZSetHandle>,
    ) -> Result<Stream<ZSetHandle>> {
        let delta_upstream = differentiate_zset_stream_live::<Vec<u8>>(&upstream)
            .await
            .context("build live delta stream for filter input")?;
        let predicate = node.predicate().clone();
        let schema = Arc::clone(node.output_schema());
        let filter_pred = move |bytes: &Vec<u8>| -> bool {
            let row = decode_projected_row_key(bytes)
                .expect("encoded row must decode for filter predicate");
            eval_predicate(&predicate, &row, schema.as_ref()).unwrap_or(false)
        };
        let filter = DbspFilter::new::<Vec<u8>, _>(&delta_upstream, filter_pred)
            .await
            .context("initialize DBSP filter")?;
        Ok(filter.stream())
    }

    async fn compile_map(
        &mut self,
        node: &DbspProjectNode,
        upstream: Stream<ZSetHandle>,
    ) -> Result<Stream<ZSetHandle>> {
        let delta_upstream = differentiate_zset_stream_live::<Vec<u8>>(&upstream)
            .await
            .context("build live delta stream for project input")?;
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
        let schema = Arc::clone(node.input_schema());
        let projector = move |bytes: &Vec<u8>| -> Vec<u8> {
            let row =
                decode_projected_row_key(bytes).expect("encoded row must decode for projection");
            let projected = eval_projection(expressions.as_ref(), &row, schema.as_ref())
                .expect("projection evaluation must succeed");
            encode_projected_row_key(&projected).expect("projected row encoding must succeed")
        };
        let map = DbspMap::new::<Vec<u8>, Vec<u8>, _>(&delta_upstream, projector)
            .await
            .context("initialize DBSP map")?;
        Ok(map.stream())
    }

    async fn compile_join(
        &mut self,
        node: &DbspJoinNode,
        left: Stream<ZSetHandle>,
        right: Stream<ZSetHandle>,
    ) -> Result<Stream<ZSetHandle>> {
        let delta_left = differentiate_zset_stream_live::<Vec<u8>>(&left)
            .await
            .context("build live delta stream for join left input")?;
        let delta_right = differentiate_zset_stream_live::<Vec<u8>>(&right)
            .await
            .context("build live delta stream for join right input")?;
        let keys = Arc::new(node.keys.clone());
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let residual = node.residual.clone();
        let output_schema = Arc::clone(&node.output_schema);

        let match_log_limit = Arc::new(AtomicUsize::new(0));
        let compare_log_limit = Arc::new(AtomicUsize::new(0));
        let left_log_limit = Arc::new(AtomicUsize::new(3));
        let right_log_limit = Arc::new(AtomicUsize::new(3));

        let mut left_cursor = StreamCursor::new(delta_left.clone());
        let mut right_cursor = StreamCursor::new(delta_right.clone());
        if let Ok((ts, handle)) = left_cursor.snapshot().await {
            if left_log_limit.fetch_sub(1, Ordering::Relaxed) > 0 {
                eprintln!(
                    "join left snapshot version {ts}, handle {}, schema width {}",
                    handle.version,
                    left_schema.len()
                );
                log_handle_rows("left snapshot", &handle, &self.bridge).await?;
            }
        }
        if let Ok((ts, handle)) = right_cursor.snapshot().await {
            if right_log_limit.fetch_sub(1, Ordering::Relaxed) > 0 {
                eprintln!(
                    "join right snapshot version {ts}, handle {}, schema width {}",
                    handle.version,
                    right_schema.len()
                );
                log_handle_rows("right snapshot", &handle, &self.bridge).await?;
            }
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

        let key_indices: Vec<(usize, usize)> = keys
            .iter()
            .map(|k| {
                let left_name = match k.left_expression().expr() {
                    datafusion::logical_expr::Expr::Column(c) => c.name.clone(),
                    other => panic!("unexpected left join key expression: {other:?}"),
                };
                let right_name = match k.right_expression().expr() {
                    datafusion::logical_expr::Expr::Column(c) => c.name.clone(),
                    other => panic!("unexpected right join key expression: {other:?}"),
                };
                let left_idx = left_schema
                    .field_index(&left_name)
                    .unwrap_or_else(|| panic!("left key column {left_name} must exist"));
                let right_idx = right_schema
                    .field_index(&right_name)
                    .unwrap_or_else(|| panic!("right key column {right_name} must exist"));
                (left_idx, right_idx)
            })
            .collect();

        let predicate = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
            let left_row = decode_projected_row_key(left_bytes)
                .expect("encoded left row must decode for join");
            let right_row = decode_projected_row_key(right_bytes)
                .expect("encoded right row must decode for join");
            let keys_equal = eval_join_keys(
                keys.as_ref(),
                &left_row,
                left_schema.as_ref(),
                &right_row,
                right_schema.as_ref(),
            )
            .unwrap_or(false);
            let seen_compare = compare_log_limit.fetch_add(1, Ordering::Relaxed);
            if seen_compare < 5 {
                let mut logged = Vec::new();
                for (li, ri) in &key_indices {
                    let left_key = left_row.get(*li).cloned();
                    let right_key = right_row.get(*ri).cloned();
                    logged.push((left_key, right_key));
                }
                eprintln!(
                    "join key comparison #{seen_compare}: equal={keys_equal}, pairs={logged:?}"
                );
            } else if seen_compare < 10 && !keys_equal {
                eprintln!(
                    "join key comparison #{seen_compare}: no match for first key pair {:?}",
                    key_indices.get(0).and_then(|(li, ri)| Some((
                        left_row.get(*li).cloned(),
                        right_row.get(*ri).cloned()
                    )))
                );
            }
            if !keys_equal {
                return false;
            }
            let seen = match_log_limit.fetch_add(1, Ordering::Relaxed);
            if seen < 5 {
                eprintln!("join predicate matched on keys");
            }
            if let Some(expr) = residual.as_ref() {
                let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                combined.extend(left_row.into_iter());
                combined.extend(right_row.into_iter());
                eval_expression(expr, &combined, output_schema.as_ref()).unwrap_or(false)
            } else {
                true
            }
        };

        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            let mut combined = decode_projected_row_key(left_bytes)
                .expect("encoded left row must decode for join projection");
            let right_row = decode_projected_row_key(right_bytes)
                .expect("encoded right row must decode for join projection");
            combined.extend(right_row);
            encode_projected_row_key(&combined).expect("combined join row encoding must succeed")
        };

        let join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
            &delta_left,
            &delta_right,
            predicate,
            projector,
        )
        .await
        .context("initialize DBSP join")?;
        // Log the first output handle, if any, to verify join activity.
        let mut join_cursor = StreamCursor::new(join.stream());
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
        upstream: Stream<ZSetHandle>,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
    ) -> Result<Stream<ZSetHandle>> {
        let handle_stream = upstream.clone();
        let mut cursor = StreamCursor::new(upstream);
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

        if let Ok((ts, handle)) = cursor.snapshot().await {
            let handle_version = handle.version;
            let state = self.state_from_handle(&handle).await?;
            registry_handle.set_dbsp_state(state);
            registry_handle.publish_version(ts, handle.clone());
            mv_latest.insert(view_name.to_string(), (ts, handle.clone()));
            eprintln!(
                "materialized view '{view_name}' snapshot at version {ts} (handle {})",
                handle_version
            );
        }

        let registry_clone = registry_handle.clone();
        let bridge_clone = Arc::clone(&self.bridge);
        let view_label = view_name.to_string();
        tokio::spawn(async move {
            let mut cursor = cursor;
            loop {
                match cursor.next().await {
                    Ok((ts, handle)) => {
                        eprintln!(
                            "Cursor for view '{view_label}' observed handle version {} at ts {}",
                            handle.version, ts
                        );
                        let handle_clone = handle.clone();
                        let handle_version = handle.version;
                        match Self::state_from_handle_with_bridge(&bridge_clone, &handle_clone)
                            .await
                        {
                            Ok(state) => {
                                registry_clone.set_dbsp_state(state);
                                registry_clone.publish_version(ts, handle_clone);
                                eprintln!(
                                    "materialized view '{view_label}' advanced to version {ts} (handle {})",
                                    handle_version
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
    pub outer_handle_streams: &'a HashMap<String, Stream<ZSetHandle>>,
}

pub struct BuildOutputs {
    pub node_streams: HashMap<usize, Stream<ZSetHandle>>,
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

fn eval_join_keys(
    keys: &[DbspJoinKey],
    left: &[ScalarValue],
    left_schema: &RowSchema,
    right: &[ScalarValue],
    right_schema: &RowSchema,
) -> Result<bool> {
    for key in keys {
        let left_value = eval_df_expr(key.left_expression().expr(), left, left_schema)?;
        let right_value = eval_df_expr(key.right_expression().expr(), right, right_schema)?;
        if !scalar_equals(&left_value, &right_value)? {
            return Ok(false);
        }
    }
    Ok(true)
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
            Ok(ScalarValue::Boolean(Some(!scalar_to_bool(&value)?)))
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
            Ok(ScalarValue::Boolean(Some(scalar_to_bool(&value)?)))
        }
        DfExpr::IsNotTrue(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!scalar_to_bool(&value)?)))
        }
        DfExpr::IsFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!scalar_to_bool(&value)?)))
        }
        DfExpr::IsNotFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(scalar_to_bool(&value)?)))
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
            if scalar_equals(&when_value, &base_value)? {
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
        Operator::Eq => Ok(ScalarValue::Boolean(Some(scalar_equals(&left, &right)?))),
        Operator::NotEq => Ok(ScalarValue::Boolean(Some(!scalar_equals(&left, &right)?))),
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            let ordering = scalar_compare(&left, &right, op)?;
            Ok(ScalarValue::Boolean(Some(ordering)))
        }
        Operator::And => {
            let lhs = scalar_to_bool(&left)?;
            if !lhs {
                return Ok(ScalarValue::Boolean(Some(false)));
            }
            let rhs = scalar_to_bool(&right)?;
            Ok(ScalarValue::Boolean(Some(lhs && rhs)))
        }
        Operator::Or => {
            let lhs = scalar_to_bool(&left)?;
            if lhs {
                return Ok(ScalarValue::Boolean(Some(true)));
            }
            let rhs = scalar_to_bool(&right)?;
            Ok(ScalarValue::Boolean(Some(lhs || rhs)))
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

fn scalar_compare(lhs: &ScalarValue, rhs: &ScalarValue, op: Operator) -> Result<bool> {
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
    Ok(result)
}

fn scalar_equals(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<bool> {
    let result = match (lhs, rhs) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l == r,
        (
            ScalarValue::TimestampMillisecond(Some(l), _),
            ScalarValue::TimestampMillisecond(Some(r), _),
        ) => l == r,
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => l == r,
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => l == r,
        (ScalarValue::Null, ScalarValue::Null) => true,
        _ => false,
    };
    Ok(result)
}

fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(*v),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(false),
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
    if pattern.starts_with('%') {
        return value.ends_with(&pattern[1..]);
    }
    if pattern.ends_with('%') {
        return value.starts_with(&pattern[..pattern.len() - 1]);
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
