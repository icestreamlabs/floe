use super::*;

use crate::dbsp_graph_builder::vectorized_filter_project::{
    VectorizedFilterProjectEvaluator, vectorized_filter_map_enabled,
};

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
        if !vectorized_filter_map_enabled() {
            return Err(anyhow!(
                "vectorized filter execution is required; FLOE_VECTORIZED_FILTER_MAP cannot be disabled"
            ));
        }

        let predicate = node.predicate().clone();
        let schema = Arc::clone(node.output_schema());
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("filter:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });

        let evaluator = Arc::new(
            VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&schema))
                .context("initialize vectorized filter evaluator")?,
        );
        tracing::info!(
            graph_id = %graph_id,
            "using vectorized filter execution path"
        );
        let vectorized_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                evaluator.transform_delta(&vectorized_graph_id, delta_values)
            };
        let filter = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize vectorized DBSP filter")?;
        Ok(filter.stream())
    }

    pub(crate) async fn compile_map(
        &mut self,
        node: &DbspProjectNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        if !vectorized_filter_map_enabled() {
            return Err(anyhow!(
                "vectorized map execution is required; FLOE_VECTORIZED_FILTER_MAP cannot be disabled"
            ));
        }

        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
        let schema = Arc::clone(node.input_schema());
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("map:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });

        let evaluator = Arc::new(
            VectorizedFilterProjectEvaluator::for_map(expressions.as_ref(), Arc::clone(&schema))
                .context("initialize vectorized map evaluator")?,
        );
        tracing::info!(
            graph_id = %graph_id,
            "using vectorized map execution path"
        );
        let vectorized_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                evaluator.transform_delta(&vectorized_graph_id, delta_values)
            };
        let map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize vectorized DBSP map")?;
        Ok(map.stream())
    }

    pub(crate) async fn compile_filter_map(
        &mut self,
        select: &DbspSelectNode,
        project: &DbspProjectNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        if !vectorized_filter_map_enabled() {
            return Err(anyhow!(
                "vectorized filter_map execution is required; FLOE_VECTORIZED_FILTER_MAP cannot be disabled"
            ));
        }

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

        let evaluator = Arc::new(
            VectorizedFilterProjectEvaluator::for_filter_map(
                &predicate,
                expressions.as_ref(),
                Arc::clone(&project_schema),
            )
            .context("initialize vectorized filter_map evaluator")?,
        );
        tracing::info!(
            graph_id = %graph_id,
            "using vectorized filter_map execution path"
        );
        let vectorized_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                evaluator.transform_delta(&vectorized_graph_id, delta_values)
            };

        let filter_map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize vectorized DBSP filter_map")?;
        Ok(filter_map.stream())
    }
}
