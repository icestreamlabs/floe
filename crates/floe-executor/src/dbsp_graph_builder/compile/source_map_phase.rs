use super::*;

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
