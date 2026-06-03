use super::shared::{
    build_join_state_projection, direct_column_index, remap_join_output_projection,
    remap_join_state_indices,
};
use super::*;
use crate::dbsp_graph_builder::materialize::{
    TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY, TransientMaterializeBatch, TransientMaterializeSender,
};
use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::encoding::{
    EncodedRowProjectionColumn, EncodedRowProjectionSource, PreparedJoinedEncodedRowProjection,
    concat_encoded_rows, project_joined_encoded_rows_prepared,
};
use crate::vectorized_keys::VectorizedEncodedKeyExtractor;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::BTreeSet;

impl DbspGraphBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn compile_transient_join_root_materialization(
        &mut self,
        node: &DbspJoinNode,
        mut left: DeltaHandleStream,
        mut right: DeltaHandleStream,
        mut left_transient: Option<
            tokio::sync::mpsc::Receiver<dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>>,
        >,
        mut right_transient: Option<
            tokio::sync::mpsc::Receiver<dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>>,
        >,
        left_retention: dbsp::JoinInputRetention,
        right_retention: dbsp::JoinInputRetention,
        mut output_projection: Option<Arc<Vec<EncodedRowProjectionColumn>>>,
        output_tx: TransientMaterializeSender,
        task_events: &GraphTaskSender,
        restore_transient_state: bool,
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
            let precompute_transform = Self::build_transient_join_precompute_transform(&precompute)
                .context("build transient join left key precompute transform")?;
            left = self
                .compile_map(&precompute, left, task_events)
                .await
                .context("initialize transient join left key precompute map")?;
            left_transient = left_transient.map(|receiver| {
                Self::remap_transient_join_input_batches(
                    &graph_id,
                    "left",
                    receiver,
                    Arc::clone(&precompute_transform),
                    task_events,
                )
            });
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
            let precompute_transform = Self::build_transient_join_precompute_transform(&precompute)
                .context("build transient join right key precompute transform")?;
            right = self
                .compile_map(&precompute, right, task_events)
                .await
                .context("initialize transient join right key precompute map")?;
            right_transient = right_transient.map(|receiver| {
                Self::remap_transient_join_input_batches(
                    &graph_id,
                    "right",
                    receiver,
                    Arc::clone(&precompute_transform),
                    task_events,
                )
            });
            key_columns
        } else {
            right_key_column_options
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .expect("right key columns should be direct")
        };

        if residual.is_none()
            && let Some(columns) = output_projection.as_ref()
        {
            let mut left_required = BTreeSet::new();
            let mut right_required = BTreeSet::new();
            for column in columns.iter().copied() {
                match column.source {
                    EncodedRowProjectionSource::Left => {
                        left_required.insert(column.index);
                    }
                    EncodedRowProjectionSource::Right => {
                        right_required.insert(column.index);
                    }
                }
            }
            left_required.extend(left_key_columns_resolved.iter().copied());
            right_required.extend(right_key_columns_resolved.iter().copied());
            if left_required.is_empty() || right_required.is_empty() {
                let left_key_columns = Arc::new(left_key_columns_resolved);
                let right_key_columns = Arc::new(right_key_columns_resolved);
                return self
                    .spawn_transient_join_root_materialization(
                        left,
                        right,
                        left_transient,
                        right_transient,
                        left_key_columns,
                        right_key_columns,
                        left_join_schema,
                        right_join_schema,
                        left_schema,
                        right_schema,
                        output_schema,
                        residual,
                        left_retention,
                        right_retention,
                        output_projection,
                        output_tx,
                        task_events,
                        join_error_handler,
                        restore_transient_state,
                    )
                    .await;
            }

            let (left_projection, left_remap) =
                build_join_state_projection(left_join_schema.as_ref(), &left_required)
                    .context("build transient join left state projection")?;
            let (right_projection, right_remap) =
                build_join_state_projection(right_join_schema.as_ref(), &right_required)
                    .context("build transient join right state projection")?;

            let remapped_left_key_columns =
                remap_join_state_indices(&left_key_columns_resolved, &left_remap)
                    .context("remap transient join left key columns")?;
            let remapped_right_key_columns =
                remap_join_state_indices(&right_key_columns_resolved, &right_remap)
                    .context("remap transient join right key columns")?;
            output_projection = Some(Arc::new(
                remap_join_output_projection(columns.as_ref(), &left_remap, &right_remap)
                    .context("remap transient join output projection")?,
            ));

            if let Some(projection) = left_projection {
                tracing::info!(
                    graph_id = %graph_id,
                    input_columns = left_join_schema.len(),
                    output_columns = projection.output_schema().len(),
                    "pruning transient join left state payload"
                );
                let transform = Self::build_transient_join_precompute_transform(&projection)
                    .context("build transient join left state-prune transform")?;
                left = self
                    .compile_map(&projection, left, task_events)
                    .await
                    .context("initialize transient join left state-prune map")?;
                left_transient = left_transient.map(|receiver| {
                    Self::remap_transient_join_input_batches(
                        &graph_id,
                        "left-state-prune",
                        receiver,
                        Arc::clone(&transform),
                        task_events,
                    )
                });
                left_join_schema = Arc::clone(projection.output_schema());
            }

            if let Some(projection) = right_projection {
                tracing::info!(
                    graph_id = %graph_id,
                    input_columns = right_join_schema.len(),
                    output_columns = projection.output_schema().len(),
                    "pruning transient join right state payload"
                );
                let transform = Self::build_transient_join_precompute_transform(&projection)
                    .context("build transient join right state-prune transform")?;
                right = self
                    .compile_map(&projection, right, task_events)
                    .await
                    .context("initialize transient join right state-prune map")?;
                right_transient = right_transient.map(|receiver| {
                    Self::remap_transient_join_input_batches(
                        &graph_id,
                        "right-state-prune",
                        receiver,
                        Arc::clone(&transform),
                        task_events,
                    )
                });
                right_join_schema = Arc::clone(projection.output_schema());
            }

            let left_key_columns = Arc::new(remapped_left_key_columns);
            let right_key_columns = Arc::new(remapped_right_key_columns);
            return self
                .spawn_transient_join_root_materialization(
                    left,
                    right,
                    left_transient,
                    right_transient,
                    left_key_columns,
                    right_key_columns,
                    left_join_schema,
                    right_join_schema,
                    left_schema,
                    right_schema,
                    output_schema,
                    residual,
                    left_retention,
                    right_retention,
                    output_projection,
                    output_tx,
                    task_events,
                    join_error_handler,
                    restore_transient_state,
                )
                .await;
        }

        let left_key_columns = Arc::new(left_key_columns_resolved);
        let right_key_columns = Arc::new(right_key_columns_resolved);
        self.spawn_transient_join_root_materialization(
            left,
            right,
            left_transient,
            right_transient,
            left_key_columns,
            right_key_columns,
            left_join_schema,
            right_join_schema,
            left_schema,
            right_schema,
            output_schema,
            residual,
            left_retention,
            right_retention,
            output_projection,
            output_tx,
            task_events,
            join_error_handler,
            restore_transient_state,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_transient_join_root_materialization(
        &mut self,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
        left_transient: Option<
            tokio::sync::mpsc::Receiver<dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>>,
        >,
        right_transient: Option<
            tokio::sync::mpsc::Receiver<dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>>,
        >,
        left_key_columns: Arc<Vec<usize>>,
        right_key_columns: Arc<Vec<usize>>,
        left_join_schema: Arc<RowSchema>,
        right_join_schema: Arc<RowSchema>,
        left_schema: Arc<RowSchema>,
        right_schema: Arc<RowSchema>,
        output_schema: Arc<RowSchema>,
        residual: Option<dbsp::DbspExpression>,
        left_retention: dbsp::JoinInputRetention,
        right_retention: dbsp::JoinInputRetention,
        output_projection: Option<Arc<Vec<EncodedRowProjectionColumn>>>,
        output_tx: TransientMaterializeSender,
        task_events: &GraphTaskSender,
        join_error_handler: RuntimeErrorHandler,
        restore_transient_state: bool,
    ) -> Result<()> {
        let graph_id = self.graph_id().to_string();
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
        let left_key_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                left_join_schema.to_arrow_schema(),
                Arc::clone(&left_key_columns),
            )
            .context("build vectorized transient left join key extractor")?,
        );
        let right_key_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                right_join_schema.to_arrow_schema(),
                Arc::clone(&right_key_columns),
            )
            .context("build vectorized transient right join key extractor")?,
        );
        let prepared_output_projection = if let Some(columns) = output_projection.as_ref() {
            Some(
                PreparedJoinedEncodedRowProjection::try_new(columns.as_ref())
                    .context("prepare transient join output projection")?,
            )
        } else if left_join_schema.len() != left_schema.len()
            || right_join_schema.len() != right_schema.len()
        {
            let mut columns = Vec::with_capacity(left_schema.len() + right_schema.len());
            columns.extend(
                (0..left_schema.len()).map(|index| EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Left,
                    index,
                }),
            );
            columns.extend(
                (0..right_schema.len()).map(|index| EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Right,
                    index,
                }),
            );
            Some(
                PreparedJoinedEncodedRowProjection::try_new(&columns)
                    .context("prepare transient join trimmed output projection")?,
            )
        } else {
            None
        }
        .map(Arc::new);

        let make_projector = {
            let prepared_output_projection = prepared_output_projection.clone();
            let projector_graph_id = projector_graph_id.clone();
            let projector_error_handler = Arc::clone(&join_error_handler);
            move || {
                let prepared_output_projection = prepared_output_projection.clone();
                let projector_graph_id = projector_graph_id.clone();
                let projector_error_handler = Arc::clone(&projector_error_handler);
                move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
                    if let Some(plan) = prepared_output_projection.as_ref() {
                        return match project_joined_encoded_rows_prepared(
                            left_bytes,
                            right_bytes,
                            plan,
                        ) {
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
                }
            }
        };

        let observer_graph_id = graph_id.clone();
        let observer_events = task_events.clone();
        let observer_label = format!("transient-join-post-filter:{graph_id}");
        let (observer_filter_tx, mut observer_filter_rx) =
            tokio::sync::mpsc::channel::<(i64, Arc<Vec<(Vec<u8>, i64)>>)>(
                TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY,
            );
        let observer_output_tx = output_tx.clone();
        let observer_filter_graph_id = observer_graph_id.clone();
        let observer_filter_events = observer_events.clone();
        let observer_filter_label = observer_label.clone();
        let observer_filter_evaluator = deferred_residual_evaluator.clone();
        tokio::spawn(async move {
            while let Some((version, deltas)) = observer_filter_rx.recv().await {
                let filtered = if let Some(evaluator) = observer_filter_evaluator.as_ref() {
                    match evaluator
                        .transform_delta_arrow(&observer_filter_graph_id, deltas)
                        .await
                    {
                        Ok(filtered) => filtered,
                        Err(err) => {
                            report_graph_task_error(
                                &observer_filter_events,
                                &observer_filter_graph_id,
                                observer_filter_label.clone(),
                                err,
                            );
                            Vec::new()
                        }
                    }
                } else {
                    deltas.as_ref().clone()
                };
                if tracing::enabled!(tracing::Level::DEBUG) {
                    tracing::debug!(
                        graph_id = %observer_filter_graph_id,
                        version,
                        rows = filtered.len(),
                        "transient join output"
                    );
                }
                let _ = observer_output_tx
                    .send(TransientMaterializeBatch {
                        version,
                        deltas: Arc::new(filtered),
                        deltas_consolidated: false,
                    })
                    .await;
            }
        });
        let observer_send_graph_id = observer_graph_id.clone();
        let observer_send_events = observer_events.clone();
        let observer_send_label = observer_label.clone();
        let observer =
            Arc::new(
                move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| match observer_filter_tx
                    .try_send((version, deltas))
                {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        report_graph_task_error(
                            &observer_send_events,
                            &observer_send_graph_id,
                            observer_send_label.clone(),
                            anyhow!("transient join post-filter channel full"),
                        );
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        report_graph_task_error(
                            &observer_send_events,
                            &observer_send_graph_id,
                            observer_send_label.clone(),
                            anyhow!("transient join post-filter channel closed"),
                        );
                    }
                },
            );

        let left_key_error_handler = Arc::clone(&join_error_handler);
        let left_key = move |delta_values: &[(Vec<u8>, i64)]| match left_key_extractor
            .extract_keyed_deltas(delta_values)
        {
            Ok(keyed) => keyed,
            Err(err) => {
                tracing::warn!(
                    graph_id = %left_graph_id,
                    error = %err,
                    "failed to extract vectorized transient join left key columns"
                );
                report_operator_closure_error(
                    &left_key_error_handler,
                    "failed to extract vectorized transient join left key columns",
                    err,
                );
                Vec::new()
            }
        };

        let right_key_error_handler = Arc::clone(&join_error_handler);
        let right_key = move |delta_values: &[(Vec<u8>, i64)]| match right_key_extractor
            .extract_keyed_deltas(delta_values)
        {
            Ok(keyed) => keyed,
            Err(err) => {
                tracing::warn!(
                    graph_id = %right_graph_id,
                    error = %err,
                    "failed to extract vectorized transient join right key columns"
                );
                report_operator_closure_error(
                    &right_key_error_handler,
                    "failed to extract vectorized transient join right key columns",
                    err,
                );
                Vec::new()
            }
        };

        DbspJoin::spawn_transient_with_inputs_and_retention::<
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            _,
            _,
            _,
            _,
        >(
            &left,
            &right,
            left_transient,
            right_transient,
            true,
            restore_transient_state.then(|| format!("{}_transient_join_root", graph_id)),
            left_retention,
            right_retention,
            left_key,
            right_key,
            |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true },
            make_projector(),
            observer,
            Some(join_error_handler),
        )
        .await
        .context("initialize transient DBSP join")?;

        Ok(())
    }
}
