use super::shared::direct_column_index;
use super::*;
use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::encoding::{
    EncodedRowProjectionColumn, EncodedRowProjectionSource, PreparedJoinedEncodedRowProjection,
    concat_encoded_rows, extract_encoded_row_i64_like_column, project_joined_encoded_rows_prepared,
};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;

impl DbspGraphBuilder {
    pub(crate) async fn compile_range_join(
        &mut self,
        node_idx: usize,
        node: &DbspJoinNode,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let range = node
            .range
            .as_ref()
            .context("range join node is missing range bounds")?;
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let output_schema = Arc::clone(&node.output_schema);
        let graph_id = self.graph_id().to_string();
        let range_join_state_namespace = self.operator_state_namespace(node_idx, "range_join");

        let mut left_join_input = left;
        let mut right_join_input = right;
        let mut left_join_schema = Arc::clone(&left_schema);
        let mut right_join_schema = Arc::clone(&right_schema);

        let left_lower_column =
            direct_column_index(range.left_lower_expression(), left_schema.as_ref());
        let left_upper_column =
            direct_column_index(range.left_upper_expression(), left_schema.as_ref());
        let right_key_column =
            direct_column_index(range.right_key_expression(), right_schema.as_ref());

        let (left_lower_column, left_upper_column) = match (left_lower_column, left_upper_column) {
            (Some(left_lower_column), Some(left_upper_column)) => {
                (left_lower_column, left_upper_column)
            }
            (left_lower_column, left_upper_column) => {
                let mut items = Vec::with_capacity(left_schema.len() + 2);
                for field in left_schema.fields() {
                    items.push(dbsp::circuit::plan::ProjectItem {
                        expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                        alias: Some(field.name.clone()),
                    });
                }
                let mut next_index = left_schema.len();
                let lower_column = if let Some(column_idx) = left_lower_column {
                    column_idx
                } else {
                    let alias = "__floe_range_join_left_lower".to_string();
                    items.push(dbsp::circuit::plan::ProjectItem {
                        expr: range.left_lower_expression().expr().clone(),
                        alias: Some(alias),
                    });
                    let column_idx = next_index;
                    next_index += 1;
                    column_idx
                };
                let upper_column = if let Some(column_idx) = left_upper_column {
                    column_idx
                } else {
                    let alias = "__floe_range_join_left_upper".to_string();
                    items.push(dbsp::circuit::plan::ProjectItem {
                        expr: range.left_upper_expression().expr().clone(),
                        alias: Some(alias),
                    });
                    next_index
                };
                let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&left_schema), items)
                    .context("build range join left bound precompute projection")?;
                left_join_schema = Arc::clone(precompute.output_schema());
                left_join_input = self
                    .compile_map(&precompute, left_join_input, task_events)
                    .await
                    .context("initialize range join left bound precompute map")?;
                (lower_column, upper_column)
            }
        };

        let right_key_column = if let Some(column_idx) = right_key_column {
            column_idx
        } else {
            let mut items = Vec::with_capacity(right_schema.len() + 1);
            for field in right_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let right_key_column = right_schema.len();
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: range.right_key_expression().expr().clone(),
                alias: Some("__floe_range_join_right_key".to_string()),
            });
            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&right_schema), items)
                .context("build range join right key precompute projection")?;
            right_join_schema = Arc::clone(precompute.output_schema());
            right_join_input = self
                .compile_map(&precompute, right_join_input, task_events)
                .await
                .context("initialize range join right key precompute map")?;
            right_key_column
        };

        let left_output_projection = (left_join_schema.len() != left_schema.len())
            .then(|| Arc::new((0..left_schema.len()).collect::<Vec<_>>()));
        let right_output_projection = (right_join_schema.len() != right_schema.len())
            .then(|| Arc::new((0..right_schema.len()).collect::<Vec<_>>()));
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
                        .context("prepare range join output projection")?,
                )
            } else {
                None
            };

        let left_range_graph_id = graph_id.clone();
        let left_range = move |delta_values: &[(Vec<u8>, i64)]| {
            let mut out = Vec::new();
            for (row, weight) in delta_values {
                if *weight == 0 {
                    continue;
                }
                let lower = match extract_encoded_row_i64_like_column(row, left_lower_column) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %left_range_graph_id,
                            error = %err,
                            "failed to extract range join lower bound"
                        );
                        continue;
                    }
                };
                let upper = match extract_encoded_row_i64_like_column(row, left_upper_column) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %left_range_graph_id,
                            error = %err,
                            "failed to extract range join upper bound"
                        );
                        continue;
                    }
                };
                if let (Some(lower), Some(upper)) = (lower, upper) {
                    out.push((lower, upper, row.clone(), *weight));
                }
            }
            out
        };

        let right_key_graph_id = graph_id.clone();
        let right_key = move |delta_values: &[(Vec<u8>, i64)]| {
            let mut out = Vec::new();
            for (row, weight) in delta_values {
                if *weight == 0 {
                    continue;
                }
                match extract_encoded_row_i64_like_column(row, right_key_column) {
                    Ok(Some(key)) => out.push((key, row.clone(), *weight)),
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %right_key_graph_id,
                            error = %err,
                            "failed to extract range join right key"
                        );
                    }
                }
            }
            out
        };

        let projector_graph_id = graph_id.clone();
        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            if let Some(plan) = prepared_output_projection.as_ref() {
                return match project_joined_encoded_rows_prepared(left_bytes, right_bytes, plan) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %projector_graph_id,
                            error = %err,
                            "failed to project range join output columns directly"
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
                        "failed to concatenate range join projection rows"
                    );
                    Vec::new()
                }
            }
        };

        let predicate = |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true };
        let range_events = task_events.clone();
        let range_graph_id = graph_id.clone();
        let range_label = format!("range-join:{graph_id}");
        let range_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&range_events, &range_graph_id, range_label.clone(), err);
        });
        let range_join = DbspRangeJoin::new_batch_with_state_namespace::<
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            i64,
            _,
            _,
            _,
            _,
        >(
            &left_join_input,
            &right_join_input,
            Some(range_join_state_namespace),
            left_range,
            right_key,
            predicate,
            projector,
            dbsp::RangeLookupMode::All,
            Some(range_error_handler),
        )
        .await
        .context("initialize DBSP range join")?;

        let range_stream = range_join.stream();
        let Some(residual) = node.residual.as_ref() else {
            return Ok(range_stream);
        };

        let residual_predicate =
            dbsp::DbspPredicate::try_new(residual.expr().clone(), Arc::clone(&output_schema))
                .context("analyze range join residual predicate")?;
        let residual_evaluator = Arc::new(
            VectorizedFilterProjectEvaluator::for_filter(
                &residual_predicate,
                Arc::clone(&output_schema),
            )
            .context("build vectorized range join residual evaluator")?,
        );
        let residual_graph_id = graph_id.clone();
        let residual_filter_events = task_events.clone();
        let residual_filter_graph_id = graph_id.clone();
        let residual_filter_label = format!("range-join-post-filter:{graph_id}");
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
        let filtered = DbspFilterMap::new_async_batch::<Vec<u8>, Vec<u8>, _, _>(
            &range_stream,
            residual_transform,
            Some(residual_filter_error_handler),
        )
        .await
        .context("initialize range join residual filter")?;
        Ok(filtered.stream())
    }
}
