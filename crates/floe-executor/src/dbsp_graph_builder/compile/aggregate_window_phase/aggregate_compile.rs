use super::count_eval::{
    build_count_aggregate_slot_kinds, build_count_batch_row_evaluator, build_count_eval_layout,
    build_window_count_batch_row_evaluator, encode_aggregate_values_from_encoded,
    is_simple_count_star_aggregate,
};
use super::incremental_eval::{
    build_incremental_aggregate_batch_row_evaluator, build_incremental_aggregate_slot_kinds,
    direct_group_key_columns, resolved_expression_column_index,
};
use super::*;
use crate::vectorized_keys::VectorizedEncodedKeyExtractor;

impl DbspGraphBuilder {
    pub(crate) async fn compile_aggregate(
        &mut self,
        node_idx: usize,
        node: &DbspAggregateNode,
        mut upstream: DeltaHandleStream,
        append_only_input: bool,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let input_schema = Arc::clone(node.input_schema());
        let state_namespace = self.operator_state_namespace(node_idx, "aggregate");
        let group_keys = node.group_keys().to_vec();
        let aggregates = node.aggregates().to_vec();
        let mut precompute_expressions = Vec::new();
        precompute_expressions.extend(group_keys.iter().map(|key| key.expression().clone()));
        for agg in &aggregates {
            if let Some(filter) = agg.filter() {
                precompute_expressions.push(filter.clone());
            }
            if let Some(expr) = agg.expression() {
                precompute_expressions.push(expr.clone());
            }
        }
        let (precomputed_upstream, eval_schema, expression_columns) = self
            .precompute_aggregate_window_expressions(
                upstream,
                Arc::clone(&input_schema),
                &precompute_expressions,
                task_events,
                "aggregate",
            )
            .await?;
        upstream = precomputed_upstream;
        let direct_group_key_columns = Arc::new(
            direct_group_key_columns(
                &group_keys,
                eval_schema.as_ref(),
                expression_columns.as_ref(),
            )
            .ok_or_else(|| anyhow!("failed to resolve vectorized aggregate group key columns"))?,
        );
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

        if aggregates
            .iter()
            .all(|agg| agg.function() == &DbspAggregateFunction::Count)
        {
            let slot_kinds = build_count_aggregate_slot_kinds(&aggregates);
            let row_evaluator = build_count_batch_row_evaluator(
                Arc::clone(&eval_schema),
                group_keys.clone(),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "aggregate",
            );

            let count_aggregate =
                DbspCountAggregate::new_batch_with_state_namespace_and_append_only_input::<
                    Vec<u8>,
                    Vec<u8>,
                    Vec<u8>,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_evaluator,
                    slot_kinds,
                    append_only_input,
                    Some(aggregate_error_handler),
                )
                .await
                .context("initialize DBSP count aggregate")?;

            let mapped = self
                .map_count_aggregate_output(
                    &graph_id,
                    &count_aggregate.stream(),
                    task_events,
                    "aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        if let Some(slot_kinds) = build_incremental_aggregate_slot_kinds(&aggregates) {
            let row_evaluator = build_incremental_aggregate_batch_row_evaluator(
                Arc::clone(&eval_schema),
                group_keys.clone(),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "aggregate",
            );

            let incremental_aggregate =
                dbsp::DbspIncrementalAggregate::new_batch_with_state_namespace_and_append_only_input::<
                    Vec<u8>,
                    Vec<u8>,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_evaluator,
                    slot_kinds,
                    append_only_input,
                    Some(aggregate_error_handler),
                )
                .await
                .context("initialize DBSP incremental aggregate")?;

            let mapped = self
                .map_incremental_aggregate_output(
                    &graph_id,
                    &incremental_aggregate.stream(),
                    task_events,
                    "aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        let key_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                eval_schema.to_arrow_schema(),
                Arc::clone(&direct_group_key_columns),
            )
            .context("initialize vectorized aggregate key extractor")?,
        );
        let key_graph_id = graph_id.clone();
        let key_error_handler = Arc::clone(&aggregate_error_handler);
        let key_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
            .extract_keyed_deltas(delta_values)
        {
            Ok(keyed) => keyed,
            Err(err) => {
                tracing::warn!(
                    graph_id = %key_graph_id,
                    error = %err,
                    "failed to evaluate vectorized aggregate group keys"
                );
                report_operator_closure_error(
                    &key_error_handler,
                    "failed to evaluate vectorized aggregate group keys",
                    err,
                );
                Vec::new()
            }
        };

        let agg_graph_id = graph_id.clone();
        let agg_error_handler = Arc::clone(&aggregate_error_handler);
        let agg_layout = Arc::new(build_count_eval_layout(
            &aggregates,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        ));
        let agg_input_schema = Arc::clone(&eval_schema);
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            match encode_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                agg_input_schema.as_ref(),
                values,
                &agg_graph_id,
                "aggregate",
            ) {
                Ok(Some(encoded)) => Some(encoded),
                Ok(None) => None,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %agg_graph_id,
                        error = %err,
                        "failed to encode aggregate output"
                    );
                    report_operator_closure_error(
                        &agg_error_handler,
                        "failed to encode aggregate output",
                        err,
                    );
                    None
                }
            }
        };

        let aggregate_spec = dbsp::operators::aggregate::AggregateSpec::new(
            format!("aggregate_{graph_id}"),
            aggregator,
        );

        let aggregate =
            DbspAggregate::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, _>(
                &upstream,
                Some(state_namespace),
                key_extractor,
                aggregate_spec,
                Some(aggregate_error_handler),
            )
            .await
            .context("initialize DBSP aggregate")?;

        let mapped = self
            .map_aggregate_output(
                &graph_id,
                &aggregate.stream(),
                task_events,
                "aggregate-project",
            )
            .await?;

        Ok(mapped)
    }

    pub(crate) async fn compile_window_aggregate(
        &mut self,
        node_idx: usize,
        node: &DbspWindowAggregateNode,
        mut upstream: DeltaHandleStream,
        append_only_input: bool,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let aggregate = &node.aggregate;
        let state_namespace = self.operator_state_namespace(node_idx, "window_aggregate");
        let input_schema = Arc::clone(aggregate.input_schema());
        let group_keys = aggregate.group_keys().to_vec();
        let aggregates = aggregate.aggregates().to_vec();
        let simple_count_star = is_simple_count_star_aggregate(&aggregates);
        let time_expression = node.window.time_expression.clone();
        let mut precompute_expressions = Vec::new();
        precompute_expressions.extend(group_keys.iter().map(|key| key.expression().clone()));
        precompute_expressions.push(time_expression.clone());
        for agg in &aggregates {
            if let Some(filter) = agg.filter() {
                precompute_expressions.push(filter.clone());
            }
            if !simple_count_star && let Some(expr) = agg.expression() {
                precompute_expressions.push(expr.clone());
            }
        }
        let (precomputed_upstream, eval_schema, expression_columns) = self
            .precompute_aggregate_window_expressions(
                upstream,
                Arc::clone(&input_schema),
                &precompute_expressions,
                task_events,
                "window_aggregate",
            )
            .await?;
        upstream = precomputed_upstream;
        let direct_group_key_columns = Arc::new(
            direct_group_key_columns(
                &group_keys,
                eval_schema.as_ref(),
                expression_columns.as_ref(),
            )
            .ok_or_else(|| {
                anyhow!("failed to resolve vectorized window aggregate group key columns")
            })?,
        );
        let allowed_lateness_ms = node.window.allowed_lateness_ms;
        let graph_id = self.graph_id().to_string();
        let window_events = task_events.clone();
        let window_label = format!("window-aggregate:{graph_id}");
        let window_graph_id = graph_id.clone();
        let window_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&window_events, &window_graph_id, window_label.clone(), err);
        });
        let direct_time_column = resolved_expression_column_index(
            &time_expression,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        )
        .ok_or_else(|| anyhow!("failed to resolve vectorized window aggregate time column"))?;
        let watermark = Arc::clone(&self.watermark);
        let vectorized_window_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                eval_schema.to_arrow_schema(),
                Arc::clone(&direct_group_key_columns),
            )
            .context("initialize vectorized window aggregate key extractor")?,
        );

        if let DbspWindowPolicy::Session { gap_ms } = &node.window.policy {
            tracing::info!(
                graph_id = %graph_id,
                "using session window aggregate path"
            );
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let row_graph_id = graph_id.clone();
            let row_error_handler = Arc::clone(&window_error_handler);
            let row_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %row_graph_id,
                        error = %err,
                        "failed to evaluate vectorized session window aggregate keys"
                    );
                    report_operator_closure_error(
                        &row_error_handler,
                        "failed to evaluate vectorized session window aggregate keys",
                        err,
                    );
                    Vec::new()
                }
            };

            let agg_graph_id = graph_id.clone();
            let agg_error_handler = Arc::clone(&window_error_handler);
            let agg_layout = Arc::new(build_count_eval_layout(
                &aggregates,
                eval_schema.as_ref(),
                expression_columns.as_ref(),
            ));
            let agg_input_schema = Arc::clone(&eval_schema);
            let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
                if values.is_empty() {
                    return None;
                }
                match encode_aggregate_values_from_encoded(
                    agg_layout.as_ref(),
                    &aggregates,
                    agg_input_schema.as_ref(),
                    values,
                    &agg_graph_id,
                    "session window aggregate",
                ) {
                    Ok(Some(encoded)) => Some(encoded),
                    Ok(None) => None,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %agg_graph_id,
                            error = %err,
                            "failed to encode session window aggregate output"
                        );
                        report_operator_closure_error(
                            &agg_error_handler,
                            "failed to encode session window aggregate output",
                            err,
                        );
                        None
                    }
                }
            };

            let session_aggregate =
                dbsp::DbspSessionWindowAggregate::new_batch_with_state_namespace::<
                    Vec<u8>,
                    Vec<u8>,
                    Vec<u8>,
                    _,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_extractor,
                    aggregator,
                    *gap_ms,
                    allowed_lateness_ms,
                    watermark,
                    Some(window_error_handler),
                )
                .await
                .context("initialize DBSP session window aggregate")?;

            let mapped = self
                .map_window_aggregate_output(
                    &graph_id,
                    &session_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        let (window_size, window_slide) = match &node.window.policy {
            DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
            DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
            DbspWindowPolicy::Session { .. } => unreachable!("handled above"),
        };

        if simple_count_star {
            tracing::info!(
                graph_id = %graph_id,
                "using window count-star fast path"
            );
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let row_graph_id = graph_id.clone();
            let row_error_handler = Arc::clone(&window_error_handler);
            let row_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %row_graph_id,
                        error = %err,
                        "failed to evaluate vectorized window count-star keys"
                    );
                    report_operator_closure_error(
                        &row_error_handler,
                        "failed to evaluate vectorized window count-star keys",
                        err,
                    );
                    Vec::new()
                }
            };
            let window_count_star_aggregate =
                dbsp::DbspWindowCountStarAggregate::new_batch_with_state_namespace::<
                    Vec<u8>,
                    Vec<u8>,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_extractor,
                    window_size,
                    window_slide,
                    allowed_lateness_ms,
                    watermark,
                    Some(window_error_handler),
                )
                .await
                .context("initialize DBSP window count-star aggregate")?;

            let mapped = self
                .map_window_count_star_aggregate_output(
                    &graph_id,
                    &window_count_star_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        if aggregates
            .iter()
            .all(|agg| agg.function() == &DbspAggregateFunction::Count)
        {
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let window_graph_id = graph_id.clone();
            let window_error_handler_for_keys = Arc::clone(&window_error_handler);
            let window_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %window_graph_id,
                        error = %err,
                        "failed to evaluate vectorized window count aggregate keys"
                    );
                    report_operator_closure_error(
                        &window_error_handler_for_keys,
                        "failed to evaluate vectorized window count aggregate keys",
                        err,
                    );
                    Vec::new()
                }
            };

            let slot_kinds = build_count_aggregate_slot_kinds(&aggregates);
            let row_evaluator = build_window_count_batch_row_evaluator(
                Arc::clone(&eval_schema),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "window aggregate",
            );
            let window_count_aggregate =
                dbsp::DbspWindowCountAggregate::new_batch_with_state_namespace::<
                    Vec<u8>,
                    Vec<u8>,
                    Vec<u8>,
                    _,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    window_extractor,
                    row_evaluator,
                    slot_kinds,
                    window_size,
                    window_slide,
                    allowed_lateness_ms,
                    watermark,
                    Some(Arc::clone(&window_error_handler)),
                )
                .await
                .context("initialize DBSP window count aggregate")?;

            let mapped = self
                .map_window_count_aggregate_output(
                    &graph_id,
                    &window_count_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        if let Some(slot_kinds) = build_incremental_aggregate_slot_kinds(&aggregates) {
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let window_graph_id = graph_id.clone();
            let window_error_handler_for_keys = Arc::clone(&window_error_handler);
            let window_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %window_graph_id,
                        error = %err,
                        "failed to evaluate vectorized window incremental aggregate keys"
                    );
                    report_operator_closure_error(
                        &window_error_handler_for_keys,
                        "failed to evaluate vectorized window incremental aggregate keys",
                        err,
                    );
                    Vec::new()
                }
            };

            let row_evaluator = build_window_incremental_aggregate_batch_row_evaluator(
                Arc::clone(&eval_schema),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "window aggregate",
            );
            let window_incremental_aggregate =
                dbsp::DbspWindowIncrementalAggregate::new_batch_with_state_namespace_and_append_only_input::<
                    Vec<u8>,
                    Vec<u8>,
                    _,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    window_extractor,
                    row_evaluator,
                    slot_kinds,
                    window_size,
                    window_slide,
                    allowed_lateness_ms,
                    watermark,
                    append_only_input,
                    Some(Arc::clone(&window_error_handler)),
                )
                .await
                .context("initialize DBSP window incremental aggregate")?;

            let mapped = self
                .map_window_incremental_aggregate_output(
                    &graph_id,
                    &window_incremental_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        let key_extractor = Arc::clone(&vectorized_window_extractor);
        let key_graph_id = graph_id.clone();
        let key_error_handler = Arc::clone(&window_error_handler);
        let window_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
            .extract_keyed_time_deltas(delta_values, direct_time_column)
        {
            Ok(extracted) => extracted,
            Err(err) => {
                tracing::warn!(
                    graph_id = %key_graph_id,
                    error = %err,
                    "failed to evaluate vectorized window aggregate keys"
                );
                report_operator_closure_error(
                    &key_error_handler,
                    "failed to evaluate vectorized window aggregate keys",
                    err,
                );
                Vec::new()
            }
        };

        let agg_graph_id = graph_id.clone();
        let agg_error_handler = Arc::clone(&window_error_handler);
        let agg_layout = Arc::new(build_count_eval_layout(
            &aggregates,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        ));
        let agg_input_schema = Arc::clone(&eval_schema);
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            match encode_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                agg_input_schema.as_ref(),
                values,
                &agg_graph_id,
                "window aggregate",
            ) {
                Ok(Some(encoded)) => Some(encoded),
                Ok(None) => None,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %agg_graph_id,
                        error = %err,
                        "failed to encode window aggregate output"
                    );
                    report_operator_closure_error(
                        &agg_error_handler,
                        "failed to encode window aggregate output",
                        err,
                    );
                    None
                }
            }
        };

        let window_aggregate = DbspWindowAggregate::new_with_state_namespace_and_batch_extractor::<
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            _,
            _,
        >(
            &upstream,
            Some(state_namespace),
            window_extractor,
            aggregator,
            window_size,
            window_slide,
            allowed_lateness_ms,
            watermark,
            Some(window_error_handler),
        )
        .await
        .context("initialize DBSP window aggregate")?;

        self.map_window_aggregate_output(
            &graph_id,
            &window_aggregate.stream(),
            task_events,
            "window-aggregate-project",
        )
        .await
    }
}
