use super::*;

use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::expression::ExpressionEvaluator;
use crate::projection::ProjectionEvaluator;

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

        match VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&schema)) {
            Ok(evaluator) => {
                let evaluator = Arc::new(evaluator);
                tracing::info!(
                    graph_id = %graph_id,
                    "using vectorized filter execution path"
                );
                let vectorized_graph_id = graph_id.clone();
                let transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<
                    Vec<(Vec<u8>, i64)>,
                > { evaluator.transform_delta(&vectorized_graph_id, delta_values) };
                let filter = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                    &upstream,
                    transform,
                    Some(error_handler),
                )
                .await
                .context("initialize vectorized DBSP filter")?;
                return Ok(filter.stream());
            }
            Err(err) => {
                tracing::info!(
                    graph_id = %graph_id,
                    error = %err,
                    "vectorized filter evaluator unavailable; falling back to scalar execution path"
                );
            }
        }

        tracing::info!(
            graph_id = %graph_id,
            "using scalar filter execution path"
        );
        let predicate_eval = Arc::new(ExpressionEvaluator::new(
            Arc::clone(&schema),
            predicate.expression(),
        ));
        let scalar_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                let mut filtered = Vec::with_capacity(delta_values.len());
                for (bytes, weight) in delta_values {
                    let row = decode_projected_row_key(&bytes).with_context(|| {
                        format!("decode source filter row for graph '{scalar_graph_id}'")
                    })?;
                    if predicate_eval.eval_bool(&row).with_context(|| {
                        format!("evaluate source filter predicate for graph '{scalar_graph_id}'")
                    })? {
                        filtered.push((bytes, weight));
                    }
                }
                Ok(filtered)
            };
        let filter = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize scalar DBSP filter")?;
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

        match VectorizedFilterProjectEvaluator::for_map(expressions.as_ref(), Arc::clone(&schema)) {
            Ok(evaluator) => {
                let evaluator = Arc::new(evaluator);
                tracing::info!(
                    graph_id = %graph_id,
                    "using vectorized map execution path"
                );
                let vectorized_graph_id = graph_id.clone();
                let transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<
                    Vec<(Vec<u8>, i64)>,
                > { evaluator.transform_delta(&vectorized_graph_id, delta_values) };
                let map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                    &upstream,
                    transform,
                    Some(error_handler),
                )
                .await
                .context("initialize vectorized DBSP map")?;
                return Ok(map.stream());
            }
            Err(err) => {
                tracing::info!(
                    graph_id = %graph_id,
                    error = %err,
                    "vectorized map evaluator unavailable; falling back to scalar execution path"
                );
            }
        }

        tracing::info!(
            graph_id = %graph_id,
            "using scalar map execution path"
        );
        let projector_eval = Arc::new(ProjectionEvaluator::new(
            Arc::clone(&schema),
            expressions.as_ref(),
        ));
        let scalar_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                let mut projected = Vec::with_capacity(delta_values.len());
                for (bytes, weight) in delta_values {
                    let row = decode_projected_row_key(&bytes).with_context(|| {
                        format!("decode source map row for graph '{scalar_graph_id}'")
                    })?;
                    let mapped = projector_eval.project(&row).with_context(|| {
                        format!("evaluate source map projection for graph '{scalar_graph_id}'")
                    })?;
                    let encoded = encode_projected_row_key(&mapped).with_context(|| {
                        format!("encode source map projection for graph '{scalar_graph_id}'")
                    })?;
                    projected.push((encoded, weight));
                }
                Ok(projected)
            };
        let map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize scalar DBSP map")?;
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
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(project.expressions().to_vec());
        let project_schema = Arc::clone(project.input_schema());

        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("filter_map:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });

        match VectorizedFilterProjectEvaluator::for_filter_map(
            &predicate,
            expressions.as_ref(),
            Arc::clone(&project_schema),
        ) {
            Ok(evaluator) => {
                let evaluator = Arc::new(evaluator);
                tracing::info!(
                    graph_id = %graph_id,
                    "using vectorized filter_map execution path"
                );
                let vectorized_graph_id = graph_id.clone();
                let transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<
                    Vec<(Vec<u8>, i64)>,
                > { evaluator.transform_delta(&vectorized_graph_id, delta_values) };

                let filter_map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                    &upstream,
                    transform,
                    Some(error_handler),
                )
                .await
                .context("initialize vectorized DBSP filter_map")?;
                return Ok(filter_map.stream());
            }
            Err(err) => {
                tracing::info!(
                    graph_id = %graph_id,
                    error = %err,
                    "vectorized filter_map evaluator unavailable; falling back to scalar execution path"
                );
            }
        }

        tracing::info!(
            graph_id = %graph_id,
            "using scalar filter_map execution path"
        );
        let predicate_eval = Arc::new(ExpressionEvaluator::new(
            Arc::clone(&project_schema),
            predicate.expression(),
        ));
        let projector_eval = Arc::new(ProjectionEvaluator::new(
            Arc::clone(&project_schema),
            expressions.as_ref(),
        ));
        let scalar_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                let mut projected = Vec::with_capacity(delta_values.len());
                for (bytes, weight) in delta_values {
                    let row = decode_projected_row_key(&bytes).with_context(|| {
                        format!("decode source filter_map row for graph '{scalar_graph_id}'")
                    })?;
                    if !predicate_eval.eval_bool(&row).with_context(|| {
                        format!(
                            "evaluate source filter_map predicate for graph '{scalar_graph_id}'"
                        )
                    })? {
                        continue;
                    }
                    let mapped = projector_eval.project(&row).with_context(|| {
                        format!(
                            "evaluate source filter_map projection for graph '{scalar_graph_id}'"
                        )
                    })?;
                    let encoded = encode_projected_row_key(&mapped).with_context(|| {
                        format!("encode source filter_map projection for graph '{scalar_graph_id}'")
                    })?;
                    projected.push((encoded, weight));
                }
                Ok(projected)
            };
        let filter_map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize scalar DBSP filter_map")?;
        Ok(filter_map.stream())
    }
}
