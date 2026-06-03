use super::shared::{
    direct_column_index, encode_null_row_template, project_encoded_delta_batch,
    strip_semijoin_precomputed_columns,
};
use super::*;
use crate::dbsp_graph_builder::materialize::DeltaTransformFn;
use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::encoding::{
    EncodedRowProjectionColumn, EncodedRowProjectionSource, PreparedJoinedEncodedRowProjection,
    concat_encoded_rows, project_joined_encoded_rows_prepared,
};
use crate::vectorized_keys::VectorizedEncodedKeyExtractor;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;

impl LegacyGraphHarness {
    pub(super) fn build_transient_join_precompute_transform(
        node: &DbspProjectNode,
    ) -> Result<Arc<DeltaTransformFn>> {
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
        let schema = Arc::clone(node.input_schema());
        let evaluator = Arc::new(
            VectorizedFilterProjectEvaluator::for_map(expressions.as_ref(), Arc::clone(&schema))
                .context("build vectorized transient join precompute evaluator")?,
        );
        Ok(Arc::new(move |delta_values| {
            let evaluator = Arc::clone(&evaluator);
            Box::pin(async move {
                evaluator
                    .transform_delta_arrow("transient_join_precompute", delta_values)
                    .await
            })
        }))
    }

    pub(super) fn remap_transient_join_input_batches(
        graph_id: &str,
        side: &'static str,
        mut input: tokio::sync::mpsc::Receiver<
            dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>,
        >,
        transform: Arc<DeltaTransformFn>,
        task_events: &GraphTaskSender,
    ) -> tokio::sync::mpsc::Receiver<dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>> {
        let (tx, rx) =
            tokio::sync::mpsc::channel(dbsp::join::TRANSIENT_JOIN_INPUT_CHANNEL_CAPACITY);
        let graph_id = graph_id.to_string();
        let task_events = task_events.clone();
        let task_label = format!("transient-join-{side}-precompute:{graph_id}");
        tokio::spawn(async move {
            while let Some(batch) = input.recv().await {
                let transformed = match transform(Arc::clone(&batch.deltas)).await {
                    Ok(transformed) => transformed,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        break;
                    }
                };
                if tx
                    .send(dbsp::join::TransientJoinInputBatch {
                        ts: batch.ts,
                        deltas: Arc::new(transformed),
                        closed_keys: Arc::clone(&batch.closed_keys),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        rx
    }

    pub(crate) async fn compile_join(
        &mut self,
        node_idx: usize,
        node: &DbspJoinNode,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        if node.asof.is_some() {
            return self
                .compile_asof_join(node_idx, node, left, right, task_events)
                .await;
        }
        if node.range.is_some() {
            return self
                .compile_range_join(node_idx, node, left, right, task_events)
                .await;
        }
        let join_type = node.join_type.clone();
        let residual = node.residual.clone();
        let residual_for_post_filter = residual.clone();
        let output_schema = Arc::clone(&node.output_schema);
        if !matches!(join_type, DbspJoinType::Inner) && residual.is_some() {
            return Err(anyhow!(
                "non-INNER joins currently require pure equi-join predicates"
            ));
        }
        let graph_id = self.graph_id().to_string();
        let join_state_namespace = self.operator_state_namespace(node_idx, "join");
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
        let left_key_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                left_join_schema.to_arrow_schema(),
                Arc::clone(&left_key_columns),
            )
            .context("build vectorized left join key extractor")?,
        );
        let right_key_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                right_join_schema.to_arrow_schema(),
                Arc::clone(&right_key_columns),
            )
            .context("build vectorized right join key extractor")?,
        );

        if matches!(join_type, DbspJoinType::LeftSemi | DbspJoinType::LeftAnti) {
            let mode = if matches!(join_type, DbspJoinType::LeftSemi) {
                SemiJoinMode::Semi
            } else {
                SemiJoinMode::Anti
            };
            let semijoin_left_graph_id = graph_id.clone();
            let semijoin_right_graph_id = graph_id.clone();
            let semijoin_left_extractor = Arc::clone(&left_key_extractor);
            let semijoin_right_extractor = Arc::clone(&right_key_extractor);
            let semijoin_left_key =
                move |delta_values: &[(Vec<u8>, i64)]| match semijoin_left_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %semijoin_left_graph_id,
                            error = %err,
                            "failed to extract vectorized left semijoin key columns"
                        );
                        Vec::new()
                    }
                };
            let semijoin_right_key =
                move |delta_values: &[(Vec<u8>, i64)]| match semijoin_right_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %semijoin_right_graph_id,
                            error = %err,
                            "failed to extract vectorized right semijoin key columns"
                        );
                        Vec::new()
                    }
                };
            let semijoin_events = task_events.clone();
            let semijoin_graph_id = graph_id.clone();
            let semijoin_label = format!("semijoin:{graph_id}");
            let semijoin_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
                report_graph_task_error(
                    &semijoin_events,
                    &semijoin_graph_id,
                    semijoin_label.clone(),
                    err,
                );
            });
            let semijoin =
                DbspSemiJoin::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
                    &left_join_input,
                    &right_join_input,
                    Some(format!("{join_state_namespace}_left_semijoin")),
                    semijoin_left_key,
                    semijoin_right_key,
                    mode,
                    Some(semijoin_error_handler),
                )
                .await
                .context("initialize DBSP LEFT SEMI/ANTI join")?;
            let semijoin_stream = semijoin.stream();
            if left_join_schema.len() == output_schema.len() {
                return Ok(semijoin_stream);
            }
            return strip_semijoin_precomputed_columns(
                semijoin_stream,
                output_schema.len(),
                &graph_id,
                format!("left-semijoin-output-project:{graph_id}"),
                task_events,
            )
            .await;
        }

        if matches!(join_type, DbspJoinType::RightSemi | DbspJoinType::RightAnti) {
            let mode = if matches!(join_type, DbspJoinType::RightSemi) {
                SemiJoinMode::Semi
            } else {
                SemiJoinMode::Anti
            };
            let semijoin_left_graph_id = graph_id.clone();
            let semijoin_right_graph_id = graph_id.clone();
            let semijoin_left_extractor = Arc::clone(&right_key_extractor);
            let semijoin_right_extractor = Arc::clone(&left_key_extractor);
            let semijoin_left_key =
                move |delta_values: &[(Vec<u8>, i64)]| match semijoin_left_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %semijoin_left_graph_id,
                            error = %err,
                            "failed to extract vectorized right semijoin key columns"
                        );
                        Vec::new()
                    }
                };
            let semijoin_right_key =
                move |delta_values: &[(Vec<u8>, i64)]| match semijoin_right_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %semijoin_right_graph_id,
                            error = %err,
                            "failed to extract vectorized left semijoin key columns"
                        );
                        Vec::new()
                    }
                };
            let semijoin_events = task_events.clone();
            let semijoin_graph_id = graph_id.clone();
            let semijoin_label = format!("right-semijoin:{graph_id}");
            let semijoin_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
                report_graph_task_error(
                    &semijoin_events,
                    &semijoin_graph_id,
                    semijoin_label.clone(),
                    err,
                );
            });
            let semijoin =
                DbspSemiJoin::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
                    &right_join_input,
                    &left_join_input,
                    Some(format!("{join_state_namespace}_right_semijoin")),
                    semijoin_left_key,
                    semijoin_right_key,
                    mode,
                    Some(semijoin_error_handler),
                )
                .await
                .context("initialize DBSP RIGHT SEMI/ANTI join")?;
            let semijoin_stream = semijoin.stream();
            if right_join_schema.len() == output_schema.len() {
                return Ok(semijoin_stream);
            }
            return strip_semijoin_precomputed_columns(
                semijoin_stream,
                output_schema.len(),
                &graph_id,
                format!("right-semijoin-output-project:{graph_id}"),
                task_events,
            )
            .await;
        }

        let defer_residual_to_post_filter =
            matches!(join_type, DbspJoinType::Inner) && residual.is_some();
        let left_graph_id = graph_id.clone();
        let right_graph_id = graph_id.clone();
        let projector_graph_id = graph_id.clone();
        let left_output_projection = (left_join_schema.len() != left_schema.len())
            .then(|| Arc::new((0..left_schema.len()).collect::<Vec<_>>()));
        let right_output_projection = (right_join_schema.len() != right_schema.len())
            .then(|| Arc::new((0..right_schema.len()).collect::<Vec<_>>()));

        let left_key_extractor_for_join = Arc::clone(&left_key_extractor);
        let left_key_error_handler = Arc::clone(&join_error_handler);
        let left_key = move |delta_values: &[(Vec<u8>, i64)]| match left_key_extractor_for_join
            .extract_keyed_deltas(delta_values)
        {
            Ok(keyed) => keyed,
            Err(err) => {
                tracing::warn!(
                    graph_id = %left_graph_id,
                    error = %err,
                    "failed to extract vectorized join left key columns"
                );
                report_operator_closure_error(
                    &left_key_error_handler,
                    "failed to extract vectorized join left key columns",
                    err,
                );
                Vec::new()
            }
        };

        let right_key_extractor_for_join = Arc::clone(&right_key_extractor);
        let right_key_error_handler = Arc::clone(&join_error_handler);
        let right_key = move |delta_values: &[(Vec<u8>, i64)]| match right_key_extractor_for_join
            .extract_keyed_deltas(delta_values)
        {
            Ok(keyed) => keyed,
            Err(err) => {
                tracing::warn!(
                    graph_id = %right_graph_id,
                    error = %err,
                    "failed to extract vectorized join right key columns"
                );
                report_operator_closure_error(
                    &right_key_error_handler,
                    "failed to extract vectorized join right key columns",
                    err,
                );
                Vec::new()
            }
        };

        let predicate = |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true };
        let prepared_output_projection =
            if left_output_projection.is_some() || right_output_projection.is_some() {
                let mut columns = Vec::new();
                if let Some(indices) = left_output_projection.as_ref() {
                    columns.extend(indices.iter().copied().map(|index| {
                        EncodedRowProjectionColumn {
                            source: EncodedRowProjectionSource::Left,
                            index,
                        }
                    }));
                } else {
                    columns.extend((0..left_join_schema.len()).map(|index| {
                        EncodedRowProjectionColumn {
                            source: EncodedRowProjectionSource::Left,
                            index,
                        }
                    }));
                }
                if let Some(indices) = right_output_projection.as_ref() {
                    columns.extend(indices.iter().copied().map(|index| {
                        EncodedRowProjectionColumn {
                            source: EncodedRowProjectionSource::Right,
                            index,
                        }
                    }));
                } else {
                    columns.extend((0..right_join_schema.len()).map(|index| {
                        EncodedRowProjectionColumn {
                            source: EncodedRowProjectionSource::Right,
                            index,
                        }
                    }));
                }
                Some(
                    PreparedJoinedEncodedRowProjection::try_new(&columns)
                        .context("prepare join output projection")?,
                )
            } else {
                None
            };

        let projector_error_handler = Arc::clone(&join_error_handler);
        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            if let Some(plan) = prepared_output_projection.as_ref() {
                return match project_joined_encoded_rows_prepared(left_bytes, right_bytes, plan) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            error = %err,
                            "failed to project join output columns directly"
                        );
                        report_operator_closure_error(
                            &projector_error_handler,
                            "failed to project join output columns directly",
                            err,
                        );
                        Vec::new()
                    }
                };
            }
            match concat_encoded_rows(left_bytes, right_bytes) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %projector_graph_id,
                        error = %err,
                        "failed to concatenate join projection rows"
                    );
                    report_operator_closure_error(
                        &projector_error_handler,
                        "failed to concatenate join projection rows",
                        err,
                    );
                    Vec::new()
                }
            }
        };

        let join = DbspJoin::new_batch_with_state_namespace::<
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            _,
            _,
            _,
            _,
        >(
            &left_join_input,
            &right_join_input,
            Some(join_state_namespace.clone()),
            left_key,
            right_key,
            predicate,
            projector,
            Some(join_error_handler),
        )
        .await
        .context("initialize DBSP join")?;
        // Log the first output handles, if any, to verify join activity.
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
        let join_log_limit = Arc::new(AtomicUsize::new(3));
        let join_log_limit_clone = Arc::clone(&join_log_limit);
        let join_task_events = task_events.clone();
        let join_task_graph_id = graph_id.clone();
        let join_task_label = "join-output-logger".to_string();
        let join_bridge = Arc::clone(&self.bridge);
        let cancel_join_output = cancel.clone();
        tokio::spawn(async move {
            let mut cursor = join_cursor;
            loop {
                tokio::select! {
                    _ = cancel_join_output.cancelled() => break,
                    result = cursor.next() => {
                        let (ts, handle) = match result {
                            Ok(next) => next,
                            Err(err) => {
                                report_graph_task_error(
                                    &join_task_events,
                                    &join_task_graph_id,
                                    join_task_label.clone(),
                                    anyhow!("join output handle stream closed: {err}"),
                                );
                                break;
                            }
                        };
                        if join_log_limit_clone.fetch_sub(1, Ordering::Relaxed) > 0 {
                            tracing::debug!(
                                graph_id = %join_task_graph_id,
                                ts,
                                handle_version = handle.version,
                                "join output handle"
                            );
                            if let Err(err) = log_handle_rows("join output handle", &handle, &join_bridge).await {
                                report_graph_task_error(
                                    &join_task_events,
                                    &join_task_graph_id,
                                    join_task_label.clone(),
                                    anyhow!("failed to log join output handle rows: {err}"),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });
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
                let residual_transform = move |delta_values: Arc<Vec<(Vec<u8>, i64)>>| {
                    let residual_evaluator = Arc::clone(&residual_evaluator);
                    let residual_graph_id = residual_graph_id.clone();
                    async move {
                        residual_evaluator
                            .transform_delta_arrow(&residual_graph_id, delta_values)
                            .await
                    }
                };
                let residual_filter = DbspFilterMap::new_async_batch::<Vec<u8>, Vec<u8>, _, _>(
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
            let antijoin_left_graph_id = graph_id.clone();
            let antijoin_right_graph_id = graph_id.clone();
            let antijoin_left_extractor = Arc::clone(&left_key_extractor);
            let antijoin_right_extractor = Arc::clone(&right_key_extractor);

            let antijoin_left_key =
                move |delta_values: &[(Vec<u8>, i64)]| match antijoin_left_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_left_graph_id,
                            error = %err,
                            "failed to extract vectorized left outer join anti left key columns"
                        );
                        Vec::new()
                    }
                };

            let antijoin_right_key =
                move |delta_values: &[(Vec<u8>, i64)]| match antijoin_right_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_right_graph_id,
                            error = %err,
                            "failed to extract vectorized left outer join anti right key columns"
                        );
                        Vec::new()
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

            let antijoin =
                DbspSemiJoin::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
                    &left_join_input,
                    &right_join_input,
                    Some(format!("{join_state_namespace}_left_outer_antijoin")),
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

            let null_extend_transform =
                move |delta_values: &[(Vec<u8>, i64)]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                    Ok(project_encoded_delta_batch(delta_values, &null_extend))
                };

            let null_extended_left = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                &antijoin.stream(),
                null_extend_transform,
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
            let antijoin_left_graph_id = graph_id.clone();
            let antijoin_right_graph_id = graph_id.clone();
            let antijoin_left_extractor = Arc::clone(&right_key_extractor);
            let antijoin_right_extractor = Arc::clone(&left_key_extractor);

            let antijoin_left_key =
                move |delta_values: &[(Vec<u8>, i64)]| match antijoin_left_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_left_graph_id,
                            error = %err,
                            "failed to extract vectorized right outer join anti right key columns"
                        );
                        Vec::new()
                    }
                };

            let antijoin_right_key =
                move |delta_values: &[(Vec<u8>, i64)]| match antijoin_right_extractor
                    .extract_keyed_deltas(delta_values)
                {
                    Ok(keyed) => keyed,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %antijoin_right_graph_id,
                            error = %err,
                            "failed to extract vectorized right outer join anti left key columns"
                        );
                        Vec::new()
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

            let antijoin =
                DbspSemiJoin::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
                    &right_join_input,
                    &left_join_input,
                    Some(format!("{join_state_namespace}_right_outer_antijoin")),
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

            let null_extend_transform =
                move |delta_values: &[(Vec<u8>, i64)]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                    Ok(project_encoded_delta_batch(delta_values, &null_extend))
                };

            let null_extended_right = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                &antijoin.stream(),
                null_extend_transform,
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
}
