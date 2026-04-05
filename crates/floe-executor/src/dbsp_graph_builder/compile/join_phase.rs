use super::*;
use crate::dbsp_graph_builder::materialize::TransientMaterializeBatch;
use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::encoding::{
    EncodedRowProjectionColumn, concat_encoded_rows, extract_encoded_row_columns,
    project_joined_encoded_rows,
};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::BTreeSet;

impl DbspGraphBuilder {
    pub(crate) async fn compile_join(
        &mut self,
        node: &DbspJoinNode,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let join_type = node.join_type.clone();
        let residual = node.residual.clone();
        let residual_for_post_filter = residual.clone();
        let output_schema = Arc::clone(&node.output_schema);
        if !matches!(join_type, DbspJoinType::Inner) && residual.is_some() {
            return Err(anyhow!(
                "OUTER joins currently require pure equi-join predicates"
            ));
        }
        let graph_id = self.graph_id().to_string();
        let join_events = task_events.clone();
        let join_graph_id = graph_id.clone();
        let join_label = format!("join:{graph_id}");
        let join_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&join_events, &join_graph_id, join_label.clone(), err);
        });

        let left_log_limit = Arc::new(AtomicUsize::new(3));
        let right_log_limit = Arc::new(AtomicUsize::new(3));

        let mut left_cursor = StreamCursor::new(left.stream());
        let mut right_cursor = StreamCursor::new(right.stream());
        if let Ok((ts, handle)) = left_cursor.snapshot().await
            && left_log_limit.fetch_sub(1, Ordering::Relaxed) > 0
        {
            tracing::debug!(
                graph_id = %graph_id,
                ts,
                handle_version = handle.version,
                schema_width = left_schema.len(),
                "join left snapshot"
            );
            log_handle_rows("left snapshot", &handle, &self.bridge).await?;
        }
        if let Ok((ts, handle)) = right_cursor.snapshot().await
            && right_log_limit.fetch_sub(1, Ordering::Relaxed) > 0
        {
            tracing::debug!(
                graph_id = %graph_id,
                ts,
                handle_version = handle.version,
                schema_width = right_schema.len(),
                "join right snapshot"
            );
            log_handle_rows("right snapshot", &handle, &self.bridge).await?;
        }
        let left_log_limit_clone = Arc::clone(&left_log_limit);
        let left_schema_clone = Arc::clone(&left_schema);
        let bridge_clone = Arc::clone(&self.bridge);
        let left_task_events = task_events.clone();
        let left_graph_id = graph_id.clone();
        let left_task_label = "join-left-logger".to_string();
        let cancel_left = cancel.clone();
        tokio::spawn(async move {
            let mut cursor = left_cursor;
            loop {
                tokio::select! {
                    _ = cancel_left.cancelled() => break,
                    result = cursor.next() => {
                        let (ts, handle) = match result {
                            Ok(next) => next,
                            Err(err) => {
                                report_graph_task_error(
                                    &left_task_events,
                                    &left_graph_id,
                                    left_task_label.clone(),
                                    anyhow!("join left handle stream closed: {err}"),
                                );
                                break;
                            }
                        };
                        if left_log_limit_clone.fetch_sub(1, Ordering::Relaxed) > 0 {
                            tracing::debug!(
                                graph_id = %left_graph_id,
                                ts,
                                handle_version = handle.version,
                                schema_width = left_schema_clone.len(),
                                "join left handle"
                            );
                            if let Err(err) = log_handle_rows("left handle", &handle, &bridge_clone).await {
                                report_graph_task_error(
                                    &left_task_events,
                                    &left_graph_id,
                                    left_task_label.clone(),
                                    anyhow!("failed to log left handle rows: {err}"),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });
        let right_log_limit_clone = Arc::clone(&right_log_limit);
        let right_schema_clone = Arc::clone(&right_schema);
        let bridge_clone = Arc::clone(&self.bridge);
        let right_task_events = task_events.clone();
        let right_graph_id = graph_id.clone();
        let right_task_label = "join-right-logger".to_string();
        let cancel_right = cancel.clone();
        tokio::spawn(async move {
            let mut cursor = right_cursor;
            loop {
                tokio::select! {
                    _ = cancel_right.cancelled() => break,
                    result = cursor.next() => {
                        let (ts, handle) = match result {
                            Ok(next) => next,
                            Err(err) => {
                                report_graph_task_error(
                                    &right_task_events,
                                    &right_graph_id,
                                    right_task_label.clone(),
                                    anyhow!("join right handle stream closed: {err}"),
                                );
                                break;
                            }
                        };
                        if right_log_limit_clone.fetch_sub(1, Ordering::Relaxed) > 0 {
                            tracing::debug!(
                                graph_id = %right_graph_id,
                                ts,
                                handle_version = handle.version,
                                schema_width = right_schema_clone.len(),
                                "join right handle"
                            );
                            if let Err(err) = log_handle_rows("right handle", &handle, &bridge_clone).await
                            {
                                report_graph_task_error(
                                    &right_task_events,
                                    &right_graph_id,
                                    right_task_label.clone(),
                                    anyhow!("failed to log right handle rows: {err}"),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        let mut left_join_input = left;
        let mut right_join_input = right;
        let mut left_join_schema = Arc::clone(&left_schema);
        let mut right_join_schema = Arc::clone(&right_schema);
        let left_key_column_options = node
            .keys
            .iter()
            .map(|key| direct_column_index(key.left_expression(), left_schema.as_ref()))
            .collect::<Vec<_>>();
        let right_key_column_options = node
            .keys
            .iter()
            .map(|key| direct_column_index(key.right_expression(), right_schema.as_ref()))
            .collect::<Vec<_>>();
        let left_key_columns_resolved = if left_key_column_options.iter().any(Option::is_none) {
            let mut items = Vec::with_capacity(left_schema.len() + node.keys.len());
            for field in left_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let mut key_columns = Vec::with_capacity(node.keys.len());
            let mut next_index = left_schema.len();
            for (index, key) in node.keys.iter().enumerate() {
                if let Some(column_idx) = left_key_column_options[index] {
                    key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_join_left_key_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: key.left_expression().expr().clone(),
                    alias: Some(alias),
                });
                key_columns.push(next_index);
                next_index += 1;
            }
            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&left_schema), items)
                .context("build join left key precompute projection")?;
            left_join_schema = Arc::clone(precompute.output_schema());
            left_join_input = self
                .compile_map(&precompute, left_join_input, task_events)
                .await
                .context("initialize join left key precompute map")?;
            key_columns
        } else {
            left_key_column_options
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .expect("left key columns should be direct")
        };
        let right_key_columns_resolved = if right_key_column_options.iter().any(Option::is_none) {
            let mut items = Vec::with_capacity(right_schema.len() + node.keys.len());
            for field in right_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let mut key_columns = Vec::with_capacity(node.keys.len());
            let mut next_index = right_schema.len();
            for (index, key) in node.keys.iter().enumerate() {
                if let Some(column_idx) = right_key_column_options[index] {
                    key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_join_right_key_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: key.right_expression().expr().clone(),
                    alias: Some(alias),
                });
                key_columns.push(next_index);
                next_index += 1;
            }
            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&right_schema), items)
                .context("build join right key precompute projection")?;
            right_join_schema = Arc::clone(precompute.output_schema());
            right_join_input = self
                .compile_map(&precompute, right_join_input, task_events)
                .await
                .context("initialize join right key precompute map")?;
            key_columns
        } else {
            right_key_column_options
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .expect("right key columns should be direct")
        };

        let left_key_columns = Arc::new(left_key_columns_resolved);
        let right_key_columns = Arc::new(right_key_columns_resolved);
        let direct_residual_bool_column = residual.as_ref().and_then(|expr| {
            direct_join_predicate_boolean_column(expr, output_schema.as_ref(), left_schema.len())
        });
        let defer_residual_to_post_filter = matches!(join_type, DbspJoinType::Inner)
            && residual.is_some()
            && direct_residual_bool_column.is_none();
        let (residual_left_required_columns, residual_right_required_columns) =
            if let Some(expr) = residual.as_ref() {
                if direct_residual_bool_column.is_none() {
                    let (left_required, right_required) = required_join_residual_input_columns(
                        expr,
                        output_schema.as_ref(),
                        left_schema.len(),
                    );
                    (
                        Some(Arc::new(left_required)),
                        Some(Arc::new(right_required)),
                    )
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };
        let left_key_columns_for_outer = left_key_columns.clone();
        let right_key_columns_for_outer = right_key_columns.clone();
        let left_graph_id = graph_id.clone();
        let right_graph_id = graph_id.clone();
        let predicate_graph_id = graph_id.clone();
        let projector_graph_id = graph_id.clone();
        let left_predicate_width = left_schema.len();
        let right_predicate_width = right_schema.len();
        let output_schema_for_predicate = Arc::clone(&output_schema);
        let left_output_projection = (left_join_schema.len() != left_schema.len())
            .then(|| Arc::new((0..left_schema.len()).collect::<Vec<_>>()));
        let right_output_projection = (right_join_schema.len() != right_schema.len())
            .then(|| Arc::new((0..right_schema.len()).collect::<Vec<_>>()));

        let left_key = move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            match extract_encoded_row_columns(left_bytes, left_key_columns.as_ref(), true) {
                Ok(selected) => selected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %left_graph_id,
                        error = %err,
                        "failed to extract join left key columns"
                    );
                    None
                }
            }
        };

        let right_key = move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            match extract_encoded_row_columns(right_bytes, right_key_columns.as_ref(), true) {
                Ok(selected) => selected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %right_graph_id,
                        error = %err,
                        "failed to extract join right key columns"
                    );
                    None
                }
            }
        };

        let predicate = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
            let Some(expr) = residual.as_ref() else {
                return true;
            };
            if let Some(column) = direct_residual_bool_column {
                return match eval_direct_join_predicate_boolean_column(
                    column,
                    left_bytes,
                    right_bytes,
                ) {
                    Ok(include) => include,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %predicate_graph_id,
                            error = %err,
                            "failed to evaluate direct join residual boolean column"
                        );
                        false
                    }
                };
            }
            if defer_residual_to_post_filter {
                return true;
            }
            let left_row = match decode_sparse_row_for_columns(
                left_bytes,
                residual_left_required_columns
                    .as_ref()
                    .expect("residual left required columns should be present")
                    .as_ref(),
                left_predicate_width,
            ) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to decode join left row"
                    );
                    return false;
                }
            };
            let right_row = match decode_sparse_row_for_columns(
                right_bytes,
                residual_right_required_columns
                    .as_ref()
                    .expect("residual right required columns should be present")
                    .as_ref(),
                right_predicate_width,
            ) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to decode join right row"
                    );
                    return false;
                }
            };
            let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
            combined.extend(left_row.into_iter());
            combined.extend(right_row.into_iter());
            match eval_expression(expr, &combined, output_schema_for_predicate.as_ref()) {
                Ok(result) => result,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to evaluate join residual"
                    );
                    false
                }
            }
        };

        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            let left_encoded = if let Some(indices) = left_output_projection.as_ref() {
                match extract_encoded_row_columns(left_bytes, indices.as_ref(), false) {
                    Ok(Some(encoded)) => encoded,
                    Ok(None) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            "join left output projection unexpectedly returned null"
                        );
                        return Vec::new();
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            error = %err,
                            "failed to project join left output columns"
                        );
                        return Vec::new();
                    }
                }
            } else {
                left_bytes.clone()
            };
            let right_encoded = if let Some(indices) = right_output_projection.as_ref() {
                match extract_encoded_row_columns(right_bytes, indices.as_ref(), false) {
                    Ok(Some(encoded)) => encoded,
                    Ok(None) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            "join right output projection unexpectedly returned null"
                        );
                        return Vec::new();
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            error = %err,
                            "failed to project join right output columns"
                        );
                        return Vec::new();
                    }
                }
            } else {
                right_bytes.clone()
            };
            match concat_encoded_rows(&left_encoded, &right_encoded) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %projector_graph_id,
                        error = %err,
                        "failed to concatenate join projection rows"
                    );
                    Vec::new()
                }
            }
        };

        let join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left_join_input,
            &right_join_input,
            left_key,
            right_key,
            predicate,
            projector,
            Some(join_error_handler),
        )
        .await
        .context("initialize DBSP join")?;
        // Log the first output handle, if any, to verify join activity.
        let mut join_cursor = StreamCursor::new(join.stream().stream());
        if let Ok((ts, handle)) = join_cursor.snapshot().await {
            tracing::debug!(
                graph_id = %graph_id,
                ts,
                handle_version = handle.version,
                "join output snapshot"
            );
            log_handle_rows("join output snapshot", &handle, &self.bridge).await?;
        }
        let join_stream = join.stream();
        if matches!(join_type, DbspJoinType::Inner) {
            if defer_residual_to_post_filter {
                let residual_expr = residual_for_post_filter
                    .as_ref()
                    .expect("residual expression should be present")
                    .expr()
                    .clone();
                let residual_predicate =
                    dbsp::DbspPredicate::try_new(residual_expr, Arc::clone(&output_schema))
                        .context("analyze deferred join residual predicate")?;
                let residual_evaluator = Arc::new(
                    VectorizedFilterProjectEvaluator::for_filter(
                        &residual_predicate,
                        Arc::clone(&output_schema),
                    )
                    .context("build vectorized deferred join residual evaluator")?,
                );
                let residual_graph_id = graph_id.clone();
                let residual_filter_events = task_events.clone();
                let residual_filter_graph_id = graph_id.clone();
                let residual_filter_label = format!("join-post-filter:{graph_id}");
                let residual_filter_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
                    report_graph_task_error(
                        &residual_filter_events,
                        &residual_filter_graph_id,
                        residual_filter_label.clone(),
                        err,
                    );
                });
                let residual_transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<
                    Vec<(Vec<u8>, i64)>,
                > { residual_evaluator.transform_delta(&residual_graph_id, delta_values) };
                let residual_filter = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                    &join_stream,
                    residual_transform,
                    Some(residual_filter_error_handler),
                )
                .await
                .context("initialize deferred vectorized join residual filter")?;
                return Ok(residual_filter.stream());
            }
            return Ok(join_stream);
        }

        let mut union_inputs = vec![join_stream];

        if matches!(join_type, DbspJoinType::LeftOuter | DbspJoinType::FullOuter) {
            let antijoin_left_key_columns = left_key_columns_for_outer.clone();
            let antijoin_right_key_columns = right_key_columns_for_outer.clone();
            let antijoin_left_graph_id = graph_id.clone();
            let antijoin_right_graph_id = graph_id.clone();

            let antijoin_left_key = move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                match extract_encoded_row_columns(
                    left_bytes,
                    antijoin_left_key_columns.as_ref(),
                    true,
                ) {
                    Ok(selected) => selected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_left_graph_id,
                            error = %err,
                            "failed to extract left outer join anti left key columns"
                        );
                        None
                    }
                }
            };

            let antijoin_right_key = move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                match extract_encoded_row_columns(
                    right_bytes,
                    antijoin_right_key_columns.as_ref(),
                    true,
                ) {
                    Ok(selected) => selected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_right_graph_id,
                            error = %err,
                            "failed to extract left outer join anti right key columns"
                        );
                        None
                    }
                }
            };

            let antijoin_events = task_events.clone();
            let antijoin_graph_id = graph_id.clone();
            let antijoin_label = format!("left-outer-antijoin:{graph_id}");
            let antijoin_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
                report_graph_task_error(
                    &antijoin_events,
                    &antijoin_graph_id,
                    antijoin_label.clone(),
                    err,
                );
            });

            let antijoin = DbspSemiJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
                &left_join_input,
                &right_join_input,
                antijoin_left_key,
                antijoin_right_key,
                SemiJoinMode::Anti,
                Some(antijoin_error_handler),
            )
            .await
            .context("initialize DBSP anti-join for LEFT/FULL OUTER join")?;

            let right_fields = right_schema.fields().to_vec();
            let mut right_nulls = Vec::with_capacity(right_fields.len());
            for field in &right_fields {
                right_nulls.push(null_scalar_for_dbsp_type(&field.data_type));
            }
            let right_null_suffix = encode_projected_row_key(&right_nulls)
                .context("encode left outer null-extension right template row")?;
            let null_extend_graph_id = graph_id.clone();
            let null_extend = move |left_bytes: &Vec<u8>| -> Vec<u8> {
                match concat_encoded_rows(left_bytes, &right_null_suffix) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %null_extend_graph_id,
                            error = %err,
                            "failed to concatenate null-extended left outer row"
                        );
                        Vec::new()
                    }
                }
            };

            let null_extend_events = task_events.clone();
            let null_extend_error_graph_id = graph_id.clone();
            let null_extend_label = format!("left-outer-null-extend:{graph_id}");
            let null_extend_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
                report_graph_task_error(
                    &null_extend_events,
                    &null_extend_error_graph_id,
                    null_extend_label.clone(),
                    err,
                );
            });

            let null_extended_left = DbspMap::new::<Vec<u8>, Vec<u8>, _>(
                &antijoin.stream(),
                null_extend,
                Some(null_extend_error_handler),
            )
            .await
            .context("initialize null-extension map for LEFT/FULL OUTER join")?;

            union_inputs.push(null_extended_left.stream());
        }

        if matches!(
            join_type,
            DbspJoinType::RightOuter | DbspJoinType::FullOuter
        ) {
            let antijoin_left_key_columns = right_key_columns_for_outer.clone();
            let antijoin_right_key_columns = left_key_columns_for_outer.clone();
            let antijoin_left_graph_id = graph_id.clone();
            let antijoin_right_graph_id = graph_id.clone();

            let antijoin_left_key = move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                match extract_encoded_row_columns(
                    right_bytes,
                    antijoin_left_key_columns.as_ref(),
                    true,
                ) {
                    Ok(selected) => selected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_left_graph_id,
                            error = %err,
                            "failed to extract right outer join anti right key columns"
                        );
                        None
                    }
                }
            };

            let antijoin_right_key = move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                match extract_encoded_row_columns(
                    left_bytes,
                    antijoin_right_key_columns.as_ref(),
                    true,
                ) {
                    Ok(selected) => selected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_right_graph_id,
                            error = %err,
                            "failed to extract right outer join anti left key columns"
                        );
                        None
                    }
                }
            };

            let antijoin_events = task_events.clone();
            let antijoin_graph_id = graph_id.clone();
            let antijoin_label = format!("right-outer-antijoin:{graph_id}");
            let antijoin_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
                report_graph_task_error(
                    &antijoin_events,
                    &antijoin_graph_id,
                    antijoin_label.clone(),
                    err,
                );
            });

            let antijoin = DbspSemiJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
                &right_join_input,
                &left_join_input,
                antijoin_left_key,
                antijoin_right_key,
                SemiJoinMode::Anti,
                Some(antijoin_error_handler),
            )
            .await
            .context("initialize DBSP anti-join for RIGHT/FULL OUTER join")?;

            let left_fields = left_schema.fields().to_vec();
            let mut left_nulls = Vec::with_capacity(left_fields.len());
            for field in &left_fields {
                left_nulls.push(null_scalar_for_dbsp_type(&field.data_type));
            }
            let left_null_prefix = encode_projected_row_key(&left_nulls)
                .context("encode right outer null-extension left template row")?;
            let null_extend_graph_id = graph_id.clone();
            let null_extend = move |right_bytes: &Vec<u8>| -> Vec<u8> {
                match concat_encoded_rows(&left_null_prefix, right_bytes) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %null_extend_graph_id,
                            error = %err,
                            "failed to concatenate null-extended right outer row"
                        );
                        Vec::new()
                    }
                }
            };

            let null_extend_events = task_events.clone();
            let null_extend_error_graph_id = graph_id.clone();
            let null_extend_label = format!("right-outer-null-extend:{graph_id}");
            let null_extend_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
                report_graph_task_error(
                    &null_extend_events,
                    &null_extend_error_graph_id,
                    null_extend_label.clone(),
                    err,
                );
            });

            let null_extended_right = DbspMap::new::<Vec<u8>, Vec<u8>, _>(
                &antijoin.stream(),
                null_extend,
                Some(null_extend_error_handler),
            )
            .await
            .context("initialize null-extension map for RIGHT/FULL OUTER join")?;

            union_inputs.push(null_extended_right.stream());
        }

        let outer_union_events = task_events.clone();
        let outer_union_graph_id = graph_id.clone();
        let outer_union_label = format!("outer-join-union:{graph_id}");
        let outer_union_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &outer_union_events,
                &outer_union_graph_id,
                outer_union_label.clone(),
                err,
            );
        });

        let union = DbspUnion::new::<Vec<u8>>(&union_inputs, Some(outer_union_error_handler))
            .await
            .context("initialize OUTER join union")?;
        Ok(union.stream())
    }

    pub(crate) async fn compile_transient_join_root_materialization(
        &mut self,
        node: &DbspJoinNode,
        mut left: DeltaHandleStream,
        mut right: DeltaHandleStream,
        mut left_transient: Option<
            tokio::sync::mpsc::UnboundedReceiver<dbsp::join::TransientJoinInputBatch<Vec<u8>>>,
        >,
        mut right_transient: Option<
            tokio::sync::mpsc::UnboundedReceiver<dbsp::join::TransientJoinInputBatch<Vec<u8>>>,
        >,
        output_projection: Option<Arc<Vec<EncodedRowProjectionColumn>>>,
        output_tx: tokio::sync::mpsc::UnboundedSender<TransientMaterializeBatch>,
        task_events: &GraphTaskSender,
    ) -> Result<()> {
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let join_type = node.join_type.clone();
        let residual = node.residual.clone();
        let output_schema = Arc::clone(&node.output_schema);
        if !matches!(join_type, DbspJoinType::Inner) {
            return Err(anyhow!(
                "transient join-to-mv fast path currently only supports INNER joins"
            ));
        }
        if residual.is_some() && !matches!(join_type, DbspJoinType::Inner) {
            return Err(anyhow!(
                "OUTER joins currently require pure equi-join predicates"
            ));
        }

        let graph_id = self.graph_id().to_string();
        let join_events = task_events.clone();
        let join_graph_id = graph_id.clone();
        let join_label = format!("join:{graph_id}");
        let join_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&join_events, &join_graph_id, join_label.clone(), err);
        });

        let mut left_join_schema = Arc::clone(&left_schema);
        let mut right_join_schema = Arc::clone(&right_schema);
        let left_key_column_options = node
            .keys
            .iter()
            .map(|key| direct_column_index(key.left_expression(), left_schema.as_ref()))
            .collect::<Vec<_>>();
        let right_key_column_options = node
            .keys
            .iter()
            .map(|key| direct_column_index(key.right_expression(), right_schema.as_ref()))
            .collect::<Vec<_>>();
        let left_key_columns_resolved = if left_key_column_options.iter().any(Option::is_none) {
            let mut items = Vec::with_capacity(left_schema.len() + node.keys.len());
            for field in left_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let mut key_columns = Vec::with_capacity(node.keys.len());
            let mut next_index = left_schema.len();
            for (index, key) in node.keys.iter().enumerate() {
                if let Some(column_idx) = left_key_column_options[index] {
                    key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_transient_join_left_key_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: key.left_expression().expr().clone(),
                    alias: Some(alias),
                });
                key_columns.push(next_index);
                next_index += 1;
            }
            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&left_schema), items)
                .context("build transient join left key precompute projection")?;
            left_join_schema = Arc::clone(precompute.output_schema());
            left = self
                .compile_map(&precompute, left, task_events)
                .await
                .context("initialize transient join left key precompute map")?;
            key_columns
        } else {
            left_key_column_options
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .expect("left key columns should be direct")
        };
        let right_key_columns_resolved = if right_key_column_options.iter().any(Option::is_none) {
            let mut items = Vec::with_capacity(right_schema.len() + node.keys.len());
            for field in right_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let mut key_columns = Vec::with_capacity(node.keys.len());
            let mut next_index = right_schema.len();
            for (index, key) in node.keys.iter().enumerate() {
                if let Some(column_idx) = right_key_column_options[index] {
                    key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_transient_join_right_key_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: key.right_expression().expr().clone(),
                    alias: Some(alias),
                });
                key_columns.push(next_index);
                next_index += 1;
            }
            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&right_schema), items)
                .context("build transient join right key precompute projection")?;
            right_join_schema = Arc::clone(precompute.output_schema());
            right = self
                .compile_map(&precompute, right, task_events)
                .await
                .context("initialize transient join right key precompute map")?;
            key_columns
        } else {
            right_key_column_options
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .expect("right key columns should be direct")
        };
        if left_join_schema.len() != left_schema.len()
            || right_join_schema.len() != right_schema.len()
        {
            left_transient = None;
            right_transient = None;
        }

        let left_key_columns = Arc::new(left_key_columns_resolved);
        let right_key_columns = Arc::new(right_key_columns_resolved);
        let direct_residual_bool_column = residual.as_ref().and_then(|expr| {
            direct_join_predicate_boolean_column(expr, output_schema.as_ref(), left_schema.len())
        });
        let (residual_left_required_columns, residual_right_required_columns) =
            if let Some(expr) = residual.as_ref() {
                if direct_residual_bool_column.is_none() {
                    let (left_required, right_required) = required_join_residual_input_columns(
                        expr,
                        output_schema.as_ref(),
                        left_schema.len(),
                    );
                    (
                        Some(Arc::new(left_required)),
                        Some(Arc::new(right_required)),
                    )
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };
        let left_graph_id = graph_id.clone();
        let right_graph_id = graph_id.clone();
        let predicate_graph_id = graph_id.clone();
        let projector_graph_id = graph_id.clone();
        let left_predicate_width = left_schema.len();
        let right_predicate_width = right_schema.len();
        let left_output_projection = (left_join_schema.len() != left_schema.len())
            .then(|| Arc::new((0..left_schema.len()).collect::<Vec<_>>()));
        let right_output_projection = (right_join_schema.len() != right_schema.len())
            .then(|| Arc::new((0..right_schema.len()).collect::<Vec<_>>()));

        let left_key = move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            match extract_encoded_row_columns(left_bytes, left_key_columns.as_ref(), true) {
                Ok(selected) => selected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %left_graph_id,
                        error = %err,
                        "failed to extract join left key columns"
                    );
                    None
                }
            }
        };

        let right_key = move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            match extract_encoded_row_columns(right_bytes, right_key_columns.as_ref(), true) {
                Ok(selected) => selected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %right_graph_id,
                        error = %err,
                        "failed to extract join right key columns"
                    );
                    None
                }
            }
        };

        let predicate = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
            let Some(expr) = residual.as_ref() else {
                return true;
            };
            if let Some(column) = direct_residual_bool_column {
                return match eval_direct_join_predicate_boolean_column(
                    column,
                    left_bytes,
                    right_bytes,
                ) {
                    Ok(include) => include,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %predicate_graph_id,
                            error = %err,
                            "failed to evaluate direct join residual boolean column"
                        );
                        false
                    }
                };
            }
            let left_row = match decode_sparse_row_for_columns(
                left_bytes,
                residual_left_required_columns
                    .as_ref()
                    .expect("residual left required columns should be present")
                    .as_ref(),
                left_predicate_width,
            ) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to decode join left row"
                    );
                    return false;
                }
            };
            let right_row = match decode_sparse_row_for_columns(
                right_bytes,
                residual_right_required_columns
                    .as_ref()
                    .expect("residual right required columns should be present")
                    .as_ref(),
                right_predicate_width,
            ) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to decode join right row"
                    );
                    return false;
                }
            };
            let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
            combined.extend(left_row.into_iter());
            combined.extend(right_row.into_iter());
            match eval_expression(expr, &combined, output_schema.as_ref()) {
                Ok(result) => result,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to evaluate join residual"
                    );
                    false
                }
            }
        };

        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            let left_encoded = if let Some(indices) = left_output_projection.as_ref() {
                match extract_encoded_row_columns(left_bytes, indices.as_ref(), false) {
                    Ok(Some(encoded)) => encoded,
                    Ok(None) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            "transient join left output projection unexpectedly returned null"
                        );
                        return Vec::new();
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            error = %err,
                            "failed to project transient join left output columns"
                        );
                        return Vec::new();
                    }
                }
            } else {
                left_bytes.clone()
            };
            let right_encoded = if let Some(indices) = right_output_projection.as_ref() {
                match extract_encoded_row_columns(right_bytes, indices.as_ref(), false) {
                    Ok(Some(encoded)) => encoded,
                    Ok(None) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            "transient join right output projection unexpectedly returned null"
                        );
                        return Vec::new();
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            error = %err,
                            "failed to project transient join right output columns"
                        );
                        return Vec::new();
                    }
                }
            } else {
                right_bytes.clone()
            };
            if let Some(columns) = output_projection.as_ref() {
                return match project_joined_encoded_rows(
                    &left_encoded,
                    &right_encoded,
                    columns.as_ref(),
                ) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            error = %err,
                            "failed to project join output columns directly"
                        );
                        Vec::new()
                    }
                };
            }
            match concat_encoded_rows(&left_encoded, &right_encoded) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %projector_graph_id,
                        error = %err,
                        "failed to concatenate join projection rows"
                    );
                    Vec::new()
                }
            }
        };

        let observer = Arc::new(move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| {
            let _ = output_tx.send(TransientMaterializeBatch { version, deltas });
        });

        DbspJoin::spawn_transient_with_inputs::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left,
            &right,
            left_transient,
            right_transient,
            true,
            left_key,
            right_key,
            predicate,
            projector,
            observer,
            Some(join_error_handler),
        )
        .await
        .context("initialize transient DBSP join")?;

        Ok(())
    }
}

#[derive(Clone, Copy)]
enum JoinPredicateBooleanColumn {
    Left(usize),
    Right(usize),
}

fn direct_column_index(
    expr: &dbsp::circuit::plan::DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    match expr.expr() {
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        other => direct_column_index_expression(other, schema),
    }
}

fn direct_column_index_expression(expr: &Expr, schema: &RowSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => resolve_direct_column(schema, column),
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        _ => None,
    }
}

fn resolve_direct_column(schema: &RowSchema, column: &Column) -> Option<usize> {
    let qualified = column.flat_name();
    schema
        .field_index(&qualified)
        .or_else(|| schema.field_index(&column.name))
}

fn direct_join_predicate_boolean_column(
    expr: &dbsp::circuit::plan::DbspExpression,
    output_schema: &RowSchema,
    left_width: usize,
) -> Option<JoinPredicateBooleanColumn> {
    let output_index = direct_column_index(expr, output_schema)?;
    let field = output_schema.field(output_index)?;
    if field.data_type != dbsp::circuit::types::DbspScalarType::Bool {
        return None;
    }
    if output_index < left_width {
        Some(JoinPredicateBooleanColumn::Left(output_index))
    } else {
        Some(JoinPredicateBooleanColumn::Right(output_index - left_width))
    }
}

fn required_join_residual_input_columns(
    expr: &dbsp::circuit::plan::DbspExpression,
    output_schema: &RowSchema,
    left_width: usize,
) -> (Vec<usize>, Vec<usize>) {
    let mut left_columns = BTreeSet::new();
    let mut right_columns = BTreeSet::new();
    for column in expr.expr().column_refs() {
        if let Some(output_idx) = resolve_direct_column(output_schema, &column) {
            if output_idx < left_width {
                left_columns.insert(output_idx);
            } else {
                right_columns.insert(output_idx - left_width);
            }
        }
    }
    (
        left_columns.into_iter().collect(),
        right_columns.into_iter().collect(),
    )
}

fn decode_sparse_row_for_columns(
    encoded: &[u8],
    columns: &[usize],
    row_width: usize,
) -> Result<Vec<ScalarValue>> {
    if columns.is_empty() {
        return Ok(vec![ScalarValue::Null; row_width]);
    }
    let selected = extract_encoded_row_columns(encoded, columns, false)?
        .ok_or_else(|| anyhow!("sparse row extraction unexpectedly returned null"))?;
    let values = decode_projected_row_key(&selected)?;
    if values.len() != columns.len() {
        return Err(anyhow!(
            "sparse row extraction expected {} columns but decoded {}",
            columns.len(),
            values.len()
        ));
    }
    let mut row = vec![ScalarValue::Null; row_width];
    for (slot, column_idx) in columns.iter().copied().enumerate() {
        row[column_idx] = values[slot].clone();
    }
    Ok(row)
}

fn eval_direct_join_predicate_boolean_column(
    column: JoinPredicateBooleanColumn,
    left_bytes: &[u8],
    right_bytes: &[u8],
) -> Result<bool> {
    let (source_bytes, source_index) = match column {
        JoinPredicateBooleanColumn::Left(index) => (left_bytes, index),
        JoinPredicateBooleanColumn::Right(index) => (right_bytes, index),
    };
    let selected = extract_encoded_row_columns(source_bytes, &[source_index], false)?
        .ok_or_else(|| anyhow!("direct join predicate boolean column extraction returned null"))?;
    let values = decode_projected_row_key(&selected)?;
    let value = values.first().ok_or_else(|| {
        anyhow!("direct join predicate boolean column extraction returned no value")
    })?;
    Ok(crate::expression::scalar_to_bool(value)?)
}
