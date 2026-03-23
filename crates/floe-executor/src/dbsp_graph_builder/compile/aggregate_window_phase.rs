use super::*;

impl DbspGraphBuilder {
    pub(crate) async fn compile_aggregate(
        &mut self,
        node: &DbspAggregateNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let input_schema = Arc::clone(node.input_schema());
        let group_keys = node.group_keys().to_vec();
        let aggregates = node.aggregates().to_vec();
        let graph_id = self.graph_id().to_string();
        let aggregate_events = task_events.clone();
        let aggregate_label = format!("aggregate:{graph_id}");
        let aggregate_graph_id = graph_id.clone();
        let aggregate_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &aggregate_events,
                &aggregate_graph_id,
                aggregate_label.clone(),
                err,
            );
        });

        let key_schema = Arc::clone(&input_schema);
        let key_graph_id = graph_id.clone();
        let key_extractor = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to decode aggregate row for group key"
                    );
                    return None;
                }
            };
            let mut key_values = Vec::with_capacity(group_keys.len());
            for key_expr in &group_keys {
                let value = match eval_scalar_expression(
                    key_expr.expression(),
                    &row,
                    key_schema.as_ref(),
                ) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %key_graph_id,
                            error = %err,
                            "failed to evaluate aggregate group key expression"
                        );
                        return None;
                    }
                };
                key_values.push(value);
            }
            match encode_projected_row_key(&key_values) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to encode aggregate group key"
                    );
                    None
                }
            }
        };

        let agg_schema = Arc::clone(&input_schema);
        let agg_graph_id = graph_id.clone();
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            let mut decoded = Vec::with_capacity(values.len());
            for (value, weight) in values {
                if *weight == 0 {
                    continue;
                }
                match decode_projected_row_key(value) {
                    Ok(row) => decoded.push((row, *weight)),
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %agg_graph_id,
                            error = %err,
                            "failed to decode aggregate input row"
                        );
                    }
                }
            }
            if decoded.is_empty() {
                return None;
            }

            let outputs = evaluate_aggregate_values(
                &aggregates,
                &decoded,
                agg_schema.as_ref(),
                &agg_graph_id,
                "aggregate",
            );

            match encode_projected_row_key(&outputs) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %agg_graph_id,
                        error = %err,
                        "failed to encode aggregate output"
                    );
                    None
                }
            }
        };

        let aggregate_spec = dbsp::operators::aggregate::AggregateSpec::new(
            format!("aggregate_{graph_id}"),
            aggregator,
        );

        let aggregate = DbspAggregate::new::<Vec<u8>, Vec<u8>, Vec<u8>, _>(
            &upstream,
            key_extractor,
            aggregate_spec,
            Some(aggregate_error_handler),
        )
        .await
        .context("initialize DBSP aggregate")?;

        let project_events = task_events.clone();
        let project_label = format!("aggregate-project:{graph_id}");
        let project_graph_id = graph_id.clone();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.clone();
        let projector = move |pair: &(Vec<u8>, Vec<u8>)| -> Vec<u8> {
            let mut key_values = match decode_projected_row_key(&pair.0) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to decode aggregate group key"
                    );
                    return Vec::new();
                }
            };
            let aggregate_values = match decode_projected_row_key(&pair.1) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to decode aggregate values"
                    );
                    return Vec::new();
                }
            };
            key_values.extend(aggregate_values);
            match encode_projected_row_key(&key_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode aggregate row"
                    );
                    Vec::new()
                }
            }
        };

        let mapped = DbspMap::new::<(Vec<u8>, Vec<u8>), Vec<u8>, _>(
            &aggregate.stream(),
            projector,
            Some(project_error_handler),
        )
        .await
        .context("initialize aggregate output map")?;

        Ok(mapped.stream())
    }

    pub(crate) async fn compile_window_aggregate(
        &mut self,
        node: &DbspWindowAggregateNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let aggregate = &node.aggregate;
        let input_schema = Arc::clone(aggregate.input_schema());
        let group_keys = aggregate.group_keys().to_vec();
        let aggregates = aggregate.aggregates().to_vec();
        let (window_size, window_slide) = match &node.window.policy {
            DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
            DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
            DbspWindowPolicy::Session { gap_ms } => (*gap_ms, *gap_ms),
        };
        let allowed_lateness_ms = node.window.allowed_lateness_ms;

        let graph_id = self.graph_id().to_string();
        let window_events = task_events.clone();
        let window_label = format!("window-aggregate:{graph_id}");
        let window_graph_id = graph_id.clone();
        let window_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&window_events, &window_graph_id, window_label.clone(), err);
        });

        let key_schema = Arc::clone(&input_schema);
        let key_graph_id = graph_id.clone();
        let key_extractor = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to decode window aggregate row for group key"
                    );
                    return None;
                }
            };
            let mut key_values = Vec::with_capacity(group_keys.len());
            for key_expr in &group_keys {
                let value = match eval_scalar_expression(
                    key_expr.expression(),
                    &row,
                    key_schema.as_ref(),
                ) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %key_graph_id,
                            error = %err,
                            "failed to evaluate window aggregate group key expression"
                        );
                        return None;
                    }
                };
                key_values.push(value);
            }
            match encode_projected_row_key(&key_values) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to encode window aggregate group key"
                    );
                    None
                }
            }
        };

        let time_schema = Arc::clone(&input_schema);
        let time_graph_id = graph_id.clone();
        let time_expression = node.window.time_expression.clone();
        let time_extractor = move |bytes: &Vec<u8>| -> Option<i64> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %time_graph_id,
                        error = %err,
                        "failed to decode window aggregate row for time expression"
                    );
                    return None;
                }
            };
            let value = match eval_scalar_expression(&time_expression, &row, time_schema.as_ref()) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %time_graph_id,
                        error = %err,
                        "failed to evaluate window aggregate time expression"
                    );
                    return None;
                }
            };
            scalar_to_i64(&value)
        };

        let agg_schema = Arc::clone(&input_schema);
        let agg_graph_id = graph_id.clone();
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            let mut decoded = Vec::with_capacity(values.len());
            for (value, weight) in values {
                if *weight == 0 {
                    continue;
                }
                match decode_projected_row_key(value) {
                    Ok(row) => decoded.push((row, *weight)),
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %agg_graph_id,
                            error = %err,
                            "failed to decode window aggregate input row"
                        );
                    }
                }
            }
            if decoded.is_empty() {
                return None;
            }

            let outputs = evaluate_aggregate_values(
                &aggregates,
                &decoded,
                agg_schema.as_ref(),
                &agg_graph_id,
                "window aggregate",
            );

            match encode_projected_row_key(&outputs) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %agg_graph_id,
                        error = %err,
                        "failed to encode window aggregate output"
                    );
                    None
                }
            }
        };

        let watermark = Arc::clone(&self.watermark);
        let window_aggregate = DbspWindowAggregate::new::<Vec<u8>, Vec<u8>, Vec<u8>, _, _, _>(
            &upstream,
            key_extractor,
            aggregator,
            time_extractor,
            window_size,
            window_slide,
            allowed_lateness_ms,
            watermark,
            Some(window_error_handler),
        )
        .await
        .context("initialize DBSP window aggregate")?;

        let project_events = task_events.clone();
        let project_label = format!("window-aggregate-project:{graph_id}");
        let project_graph_id = graph_id.clone();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.clone();
        let projector = move |pair: &(WindowKey<Vec<u8>>, Vec<u8>)| -> Vec<u8> {
            let mut key_values = match decode_projected_row_key(&pair.0.key) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to decode window aggregate group key"
                    );
                    return Vec::new();
                }
            };
            let aggregate_values = match decode_projected_row_key(&pair.1) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to decode window aggregate values"
                    );
                    return Vec::new();
                }
            };
            let mut output = Vec::with_capacity(2 + key_values.len() + aggregate_values.len());
            output.push(ScalarValue::TimestampMillisecond(Some(pair.0.start), None));
            output.push(ScalarValue::TimestampMillisecond(Some(pair.0.end), None));
            output.append(&mut key_values);
            output.extend(aggregate_values);
            match encode_projected_row_key(&output) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode window aggregate row"
                    );
                    Vec::new()
                }
            }
        };

        let mapped = DbspMap::new::<(WindowKey<Vec<u8>>, Vec<u8>), Vec<u8>, _>(
            &window_aggregate.stream(),
            projector,
            Some(project_error_handler),
        )
        .await
        .context("initialize window aggregate output map")?;

        Ok(mapped.stream())
    }
}
