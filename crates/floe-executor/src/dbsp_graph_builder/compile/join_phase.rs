use super::*;
use crate::dbsp_graph_builder::materialize::TransientMaterializeBatch;
use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::encoding::{
    EncodedRowProjectionColumn, concat_encoded_rows, extract_encoded_row_columns,
    project_joined_encoded_rows,
};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;

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
        let defer_residual_to_post_filter =
            matches!(join_type, DbspJoinType::Inner) && residual.is_some();
        let left_key_columns_for_outer = left_key_columns.clone();
        let right_key_columns_for_outer = right_key_columns.clone();
        let left_graph_id = graph_id.clone();
        let right_graph_id = graph_id.clone();
        let projector_graph_id = graph_id.clone();
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

        let predicate = |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true };

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

            let right_null_suffix = encode_null_row_template(right_schema.as_ref())
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

            let left_null_prefix = encode_null_row_template(left_schema.as_ref())
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
        let defer_residual_to_post_filter = residual.is_some();
        let deferred_residual_evaluator = if defer_residual_to_post_filter {
            let residual_expr = residual
                .as_ref()
                .expect("residual expression should be present")
                .expr()
                .clone();
            let residual_predicate =
                dbsp::DbspPredicate::try_new(residual_expr, Arc::clone(&output_schema))
                    .context("analyze deferred transient join residual predicate")?;
            Some(Arc::new(
                VectorizedFilterProjectEvaluator::for_filter(
                    &residual_predicate,
                    Arc::clone(&output_schema),
                )
                .context("build vectorized deferred transient join residual evaluator")?,
            ))
        } else {
            None
        };
        let left_graph_id = graph_id.clone();
        let right_graph_id = graph_id.clone();
        let projector_graph_id = graph_id.clone();
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

        let predicate = |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true };

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

        let observer_graph_id = graph_id.clone();
        let observer_events = task_events.clone();
        let observer_label = format!("transient-join-post-filter:{graph_id}");
        let observer = Arc::new(move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| {
            let filtered = if let Some(evaluator) = deferred_residual_evaluator.as_ref() {
                match evaluator.transform_delta(&observer_graph_id, deltas.as_ref().clone()) {
                    Ok(filtered) => filtered,
                    Err(err) => {
                        report_graph_task_error(
                            &observer_events,
                            &observer_graph_id,
                            observer_label.clone(),
                            err,
                        );
                        Vec::new()
                    }
                }
            } else {
                deltas.as_ref().clone()
            };
            let _ = output_tx.send(TransientMaterializeBatch {
                version,
                deltas: Arc::new(filtered),
            });
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

fn encode_null_row_template(schema: &RowSchema) -> Result<Vec<u8>> {
    let count = u32::try_from(schema.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + schema.len());
    encoded.extend_from_slice(&count.to_le_bytes());
    for field in schema.fields() {
        match field.data_type {
            DbspScalarType::Int64 => encoded.push(0x05),
            DbspScalarType::Utf8 => encoded.push(0x06),
            DbspScalarType::TimestampMillis => encoded.push(0x07),
            DbspScalarType::Bool => encoded.push(0x08),
        }
    }
    Ok(encoded)
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
