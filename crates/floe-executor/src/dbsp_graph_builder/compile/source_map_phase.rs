use super::*;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionContext;
use datafusion::physical_expr::PhysicalExpr;

impl DbspGraphBuilder {
    pub(crate) async fn compile_source(
        &self,
        source: &DbspSourceNode,
        outer_streams: &HashMap<String, DeltaHandleStream>,
    ) -> Result<DeltaHandleStream> {
        tracing::info!(
            source = %source.table.name,
            "attaching DBSP source node to outer stream"
        );
        let snapshot_stream = outer_streams
            .get(source.table.name)
            .cloned()
            .with_context(|| anyhow!("source '{}' has no handle stream", source.table.name))?;
        Ok(snapshot_stream)
    }

    pub(crate) async fn compile_filter(
        &mut self,
        node: &DbspSelectNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let predicate = node.predicate().clone();
        let schema = Arc::clone(node.output_schema());
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("filter:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });
        let log_graph_id = graph_id.clone();
        let filter_pred = move |bytes: &Vec<u8>| -> bool {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to decode filter row"
                    );
                    return false;
                }
            };
            match eval_predicate(&predicate, &row, schema.as_ref()) {
                Ok(result) => result,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to evaluate filter predicate"
                    );
                    false
                }
            }
        };
        let filter = DbspFilter::new::<Vec<u8>, _>(&upstream, filter_pred, Some(error_handler))
            .await
            .context("initialize DBSP filter")?;
        Ok(filter.stream())
    }

    pub(crate) async fn compile_map(
        &mut self,
        node: &DbspProjectNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
        let schema = Arc::clone(node.input_schema());
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("map:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });
        let log_graph_id = graph_id.clone();
        let projector = move |bytes: &Vec<u8>| -> Vec<u8> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to decode projection row"
                    );
                    return Vec::new();
                }
            };
            let projected = match eval_projection(expressions.as_ref(), &row, schema.as_ref()) {
                Ok(projected) => projected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to evaluate projection"
                    );
                    return Vec::new();
                }
            };
            match encode_projected_row_key(&projected) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to encode projected row"
                    );
                    Vec::new()
                }
            }
        };
        let map = DbspMap::new::<Vec<u8>, Vec<u8>, _>(&upstream, projector, Some(error_handler))
            .await
            .context("initialize DBSP map")?;
        Ok(map.stream())
    }

    pub(crate) async fn compile_filter_map(
        &mut self,
        select: &DbspSelectNode,
        project: &DbspProjectNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let predicate = select.predicate().clone();
        let filter_schema = Arc::clone(select.output_schema());
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(project.expressions().to_vec());
        let project_schema = Arc::clone(project.input_schema());

        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("filter_map:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });
        let vectorized_enabled = std::env::var("FLOE_VECTORIZED_FILTER_MAP")
            .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "False"))
            .unwrap_or(true);

        if vectorized_enabled {
            match VectorizedFilterProjectEvaluator::new(
                &predicate,
                expressions.as_ref(),
                Arc::clone(&project_schema),
            ) {
                Ok(evaluator) => {
                    tracing::info!(
                        graph_id = %graph_id,
                        "using vectorized filter_map execution path"
                    );
                    let evaluator = Arc::new(evaluator);
                    let vectorized_graph_id = graph_id.clone();
                    let transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<HashMap<Vec<u8>, i64>> {
                        evaluator.transform_delta(&vectorized_graph_id, delta_values)
                    };

                    let filter_map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                        &upstream,
                        transform,
                        Some(error_handler.clone()),
                    )
                    .await
                    .context("initialize vectorized DBSP filter_map")?;
                    return Ok(filter_map.stream());
                }
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "vectorized filter_map initialization failed; falling back to scalar path"
                    );
                }
            }
        } else {
            tracing::info!(
                graph_id = %graph_id,
                "vectorized filter_map disabled via FLOE_VECTORIZED_FILTER_MAP"
            );
        }

        let log_graph_id = graph_id.clone();
        let transform = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to decode filter_map row"
                    );
                    return None;
                }
            };

            match eval_predicate(&predicate, &row, filter_schema.as_ref()) {
                Ok(true) => {}
                Ok(false) => return None,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to evaluate filter_map predicate"
                    );
                    return None;
                }
            }

            let projected =
                match eval_projection(expressions.as_ref(), &row, project_schema.as_ref()) {
                    Ok(projected) => projected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %log_graph_id,
                            error = %err,
                            "failed to evaluate filter_map projection"
                        );
                        return None;
                    }
                };

            match encode_projected_row_key(&projected) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to encode filter_map row"
                    );
                    None
                }
            }
        };

        let filter_map =
            DbspFilterMap::new::<Vec<u8>, Vec<u8>, _>(&upstream, transform, Some(error_handler))
                .await
                .context("initialize DBSP filter_map")?;
        Ok(filter_map.stream())
    }
}

#[derive(Clone)]
struct VectorizedFilterProjectEvaluator {
    input_schema: datafusion::arrow::datatypes::SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
    projection_plan: ProjectionPlan,
}

impl VectorizedFilterProjectEvaluator {
    fn new(
        predicate: &dbsp::DbspPredicate,
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let column_projection = column_projection_indices(projections, input_schema.as_ref());
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized filter_map")?;
        let ctx = SessionContext::new();
        let predicate = ctx
            .create_physical_expr(predicate.expression().expr().clone(), &df_schema)
            .context("compile vectorized predicate expression")?;
        let projection_plan = if let Some(indices) = column_projection {
            ProjectionPlan::ColumnIndices(Arc::new(indices))
        } else {
            let projections = projections
                .iter()
                .map(|expr| {
                    ctx.create_physical_expr(expr.expression().expr().clone(), &df_schema)
                        .context("compile vectorized projection expression")
                })
                .collect::<Result<Vec<_>>>()?;
            ProjectionPlan::Physical(Arc::new(projections))
        };
        Ok(Self {
            input_schema,
            predicate,
            projection_plan,
        })
    }

    fn transform_delta(
        &self,
        graph_id: &str,
        delta_values: Vec<(Vec<u8>, i64)>,
    ) -> Result<HashMap<Vec<u8>, i64>> {
        if delta_values.is_empty() {
            return Ok(HashMap::new());
        }

        let mut rows = Vec::with_capacity(delta_values.len());
        let mut weights = Vec::with_capacity(delta_values.len());
        for (encoded, weight) in delta_values {
            if weight == 0 {
                continue;
            }
            match decode_projected_row_key(&encoded) {
                Ok(row) => {
                    rows.push(row);
                    weights.push(weight);
                }
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to decode vectorized filter_map row"
                    );
                }
            }
        }
        if rows.is_empty() {
            return Ok(HashMap::new());
        }

        match &self.projection_plan {
            ProjectionPlan::ColumnIndices(indices) => {
                let batch = rows_to_record_batch_ref(&self.input_schema, &rows)?;
                let selected = self.selected_indices(&batch)?;
                if selected.is_empty() {
                    return Ok(HashMap::new());
                }
                let mut output: HashMap<Vec<u8>, i64> = HashMap::with_capacity(selected.len());
                for idx in selected {
                    let diff = weights.get(idx).copied().unwrap_or(0);
                    if diff == 0 {
                        continue;
                    }
                    let Some(row) = rows.get(idx) else {
                        continue;
                    };
                    let projected_row = indices
                        .iter()
                        .map(|column_idx| row[*column_idx].clone())
                        .collect::<Vec<_>>();
                    let encoded = encode_projected_row_key(&projected_row)?;
                    let entry = output.entry(encoded.clone()).or_insert(0);
                    *entry += diff;
                    if *entry == 0 {
                        output.remove(&encoded);
                    }
                }
                Ok(output)
            }
            ProjectionPlan::Physical(projections) => {
                let batch = rows_to_record_batch_owned(&self.input_schema, rows)?;
                let selected = self.selected_indices(&batch)?;
                if selected.is_empty() {
                    return Ok(HashMap::new());
                }
                let projection_arrays = projections
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        expr.evaluate(&batch)
                            .with_context(|| {
                                format!("evaluate vectorized projection column {idx}")
                            })?
                            .into_array(batch.num_rows())
                            .with_context(|| {
                                format!("materialize vectorized projection column {idx}")
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut output: HashMap<Vec<u8>, i64> = HashMap::with_capacity(selected.len());
                for idx in selected {
                    let diff = weights.get(idx).copied().unwrap_or(0);
                    if diff == 0 {
                        continue;
                    }
                    let mut projected_row = Vec::with_capacity(projection_arrays.len());
                    for array in &projection_arrays {
                        projected_row.push(ScalarValue::try_from_array(array, idx)?);
                    }
                    let encoded = encode_projected_row_key(&projected_row)?;
                    let entry = output.entry(encoded.clone()).or_insert(0);
                    *entry += diff;
                    if *entry == 0 {
                        output.remove(&encoded);
                    }
                }
                Ok(output)
            }
        }
    }

    fn selected_indices(&self, batch: &RecordBatch) -> Result<Vec<usize>> {
        let predicate = self
            .predicate
            .evaluate(batch)
            .context("evaluate vectorized filter_map predicate")?
            .into_array(batch.num_rows())
            .context("materialize vectorized predicate result")?;
        let bool_array = predicate
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow!("vectorized predicate did not evaluate to boolean"))?;
        let mut selected = Vec::new();
        for idx in 0..bool_array.len() {
            if bool_array.is_valid(idx) && bool_array.value(idx) {
                selected.push(idx);
            }
        }
        Ok(selected)
    }
}

#[derive(Clone)]
enum ProjectionPlan {
    ColumnIndices(Arc<Vec<usize>>),
    Physical(Arc<Vec<Arc<dyn PhysicalExpr>>>),
}

fn column_projection_indices(
    projections: &[DbspProjectExpr],
    input_schema: &RowSchema,
) -> Option<Vec<usize>> {
    projections
        .iter()
        .map(|projection| {
            let column_name = match projection.expression().expr() {
                datafusion::logical_expr::Expr::Column(column) => column.name.as_str(),
                datafusion::logical_expr::Expr::Alias(alias) => match alias.expr.as_ref() {
                    datafusion::logical_expr::Expr::Column(column) => column.name.as_str(),
                    _ => return None,
                },
                _ => return None,
            };
            input_schema.field_index(column_name)
        })
        .collect::<Option<Vec<_>>>()
}

fn rows_to_record_batch_ref(
    schema: &datafusion::arrow::datatypes::SchemaRef,
    rows: &[Vec<ScalarValue>],
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    let width = schema.fields().len();
    let mut columns = vec![Vec::with_capacity(rows.len()); width];
    for row in rows {
        if row.len() != width {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                width
            ));
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value.clone());
        }
    }
    let arrays = columns
        .into_iter()
        .enumerate()
        .map(|(idx, values)| {
            ScalarValue::iter_to_array(values)
                .with_context(|| format!("build vectorized input column {idx}"))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays).context("build vectorized input batch")
}

fn rows_to_record_batch_owned(
    schema: &datafusion::arrow::datatypes::SchemaRef,
    rows: Vec<Vec<ScalarValue>>,
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    let width = schema.fields().len();
    let mut columns = vec![Vec::with_capacity(rows.len()); width];
    for row in rows {
        if row.len() != width {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                width
            ));
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value);
        }
    }
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .enumerate()
        .map(|(idx, values)| {
            ScalarValue::iter_to_array(values)
                .with_context(|| format!("build vectorized input column {idx}"))
        })
        .collect::<Result<_>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays).context("build vectorized input batch")
}
