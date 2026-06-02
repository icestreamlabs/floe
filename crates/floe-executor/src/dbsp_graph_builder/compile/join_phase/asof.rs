use super::shared::{
    asof_candidate_residual_schema, asof_composite_key, asof_composite_upper_bound,
    direct_column_index, encode_null_row_template, project_encoded_delta_batch,
    strip_asof_precomputed_columns,
};
use super::*;
use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::encoding::{
    concat_encoded_rows, decode_all_encoded_row_scalars_into, extract_encoded_row_columns,
    extract_encoded_row_columns_and_i64_like_column, extract_encoded_row_i64_like_column,
};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::cmp::Reverse;

impl DbspGraphBuilder {
    pub(crate) async fn compile_asof_join(
        &mut self,
        node_idx: usize,
        node: &DbspJoinNode,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let asof = node
            .asof
            .as_ref()
            .context("ASOF join node is missing timestamp expressions")?;
        if !matches!(
            node.join_type,
            DbspJoinType::Inner | DbspJoinType::LeftOuter
        ) {
            return Err(anyhow!("ASOF joins support INNER or LEFT OUTER semantics"));
        }
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let output_schema = Arc::clone(&node.output_schema);
        let graph_id = self.graph_id().to_string();
        let asof_state_namespace = self.operator_state_namespace(node_idx, "asof_join");

        let original_left_stream = left.clone();
        let mut left_join_input = left;
        let mut right_join_input = right;
        let mut left_join_schema = Arc::clone(&left_schema);
        let mut right_join_schema = Arc::clone(&right_schema);

        let left_key_column_options = node
            .keys
            .iter()
            .map(|key| direct_column_index(key.left_expression(), left_schema.as_ref()))
            .collect::<Vec<_>>();
        let left_timestamp_option =
            direct_column_index(asof.left_timestamp_expression(), left_schema.as_ref());
        let mut left_key_columns = Vec::with_capacity(node.keys.len());
        let left_timestamp_column = if let (Some(timestamp_column), Some(key_columns)) = (
            left_timestamp_option,
            left_key_column_options
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>(),
        ) {
            left_key_columns.extend(key_columns);
            timestamp_column
        } else {
            let mut items = Vec::with_capacity(left_schema.len() + node.keys.len() + 1);
            for field in left_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let mut next_index = left_schema.len();
            for (index, key) in node.keys.iter().enumerate() {
                if let Some(column_idx) = left_key_column_options[index] {
                    left_key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_asof_left_key_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: key.left_expression().expr().clone(),
                    alias: Some(alias),
                });
                left_key_columns.push(next_index);
                next_index += 1;
            }
            let timestamp_column = if let Some(column_idx) = left_timestamp_option {
                column_idx
            } else {
                let timestamp_column = next_index;
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: asof.left_timestamp_expression().expr().clone(),
                    alias: Some("__floe_asof_left_ts".to_string()),
                });
                timestamp_column
            };
            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&left_schema), items)
                .context("build ASOF left timestamp precompute projection")?;
            left_join_schema = Arc::clone(precompute.output_schema());
            left_join_input = self
                .compile_map(&precompute, left_join_input, task_events)
                .await
                .context("initialize ASOF left timestamp precompute map")?;
            timestamp_column
        };

        let right_key_column_options = node
            .keys
            .iter()
            .map(|key| direct_column_index(key.right_expression(), right_schema.as_ref()))
            .collect::<Vec<_>>();
        let right_timestamp_option =
            direct_column_index(asof.right_timestamp_expression(), right_schema.as_ref());
        let mut right_key_columns = Vec::with_capacity(node.keys.len());
        let right_timestamp_column = if let (Some(timestamp_column), Some(key_columns)) = (
            right_timestamp_option,
            right_key_column_options
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>(),
        ) {
            right_key_columns.extend(key_columns);
            timestamp_column
        } else {
            let mut items = Vec::with_capacity(right_schema.len() + node.keys.len() + 1);
            for field in right_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let mut next_index = right_schema.len();
            for (index, key) in node.keys.iter().enumerate() {
                if let Some(column_idx) = right_key_column_options[index] {
                    right_key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_asof_right_key_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: key.right_expression().expr().clone(),
                    alias: Some(alias),
                });
                right_key_columns.push(next_index);
                next_index += 1;
            }
            let timestamp_column = if let Some(column_idx) = right_timestamp_option {
                column_idx
            } else {
                let timestamp_column = next_index;
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: asof.right_timestamp_expression().expr().clone(),
                    alias: Some("__floe_asof_right_ts".to_string()),
                });
                timestamp_column
            };
            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&right_schema), items)
                .context("build ASOF right timestamp precompute projection")?;
            right_join_schema = Arc::clone(precompute.output_schema());
            right_join_input = self
                .compile_map(&precompute, right_join_input, task_events)
                .await
                .context("initialize ASOF right timestamp precompute map")?;
            timestamp_column
        };

        let left_key_columns = Arc::new(left_key_columns);
        let right_key_columns = Arc::new(right_key_columns);
        let left_range_graph_id = graph_id.clone();
        let left_range_key_columns = Arc::clone(&left_key_columns);
        let left_range = move |delta_values: &[(Vec<u8>, i64)]| {
            let mut out = Vec::new();
            for (row, weight) in delta_values {
                if *weight == 0 {
                    continue;
                }
                let extracted = extract_encoded_row_columns_and_i64_like_column(
                    row,
                    left_range_key_columns.as_ref(),
                    left_timestamp_column,
                    true,
                );
                let Some((prefix, left_ts)) = (match extracted {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %left_range_graph_id,
                            error = %err,
                            "failed to extract ASOF left key/timestamp"
                        );
                        continue;
                    }
                }) else {
                    continue;
                };
                out.push((
                    asof_composite_key(&prefix, left_ts),
                    asof_composite_upper_bound(&prefix, left_ts),
                    row.clone(),
                    *weight,
                ));
            }
            out
        };

        let right_key_graph_id = graph_id.clone();
        let right_range_key_columns = Arc::clone(&right_key_columns);
        let right_key = move |delta_values: &[(Vec<u8>, i64)]| {
            let mut out = Vec::new();
            for (row, weight) in delta_values {
                if *weight == 0 {
                    continue;
                }
                match extract_encoded_row_columns_and_i64_like_column(
                    row,
                    right_range_key_columns.as_ref(),
                    right_timestamp_column,
                    true,
                ) {
                    Ok(Some((prefix, right_ts))) => {
                        out.push((asof_composite_key(&prefix, right_ts), row.clone(), *weight))
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %right_key_graph_id,
                            error = %err,
                            "failed to extract ASOF right key/timestamp"
                        );
                    }
                }
            }
            out
        };

        let projector_graph_id = graph_id.clone();
        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            match concat_encoded_rows(left_bytes, right_bytes) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %projector_graph_id,
                        error = %err,
                        "failed to concatenate ASOF candidate rows"
                    );
                    Vec::new()
                }
            }
        };

        let predicate = |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true };
        let range_events = task_events.clone();
        let range_graph_id = graph_id.clone();
        let range_label = format!("asof-candidates:{graph_id}");
        let range_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&range_events, &range_graph_id, range_label.clone(), err);
        });
        let candidates = DbspRangeJoin::new_batch_with_state_namespace::<
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            dbsp::collections::OrderedBytes,
            _,
            _,
            _,
            _,
        >(
            &left_join_input,
            &right_join_input,
            Some(format!("{asof_state_namespace}_candidates")),
            left_range,
            right_key,
            predicate,
            projector,
            dbsp::RangeLookupMode::First,
            Some(range_error_handler),
        )
        .await
        .context("initialize ASOF candidate range join")?;

        let mut candidate_stream = candidates.stream();
        if let Some(residual) = &node.residual {
            let residual_expr = residual.expr().clone();
            let residual_schema = asof_candidate_residual_schema(
                left_schema.as_ref(),
                left_join_schema.as_ref(),
                right_schema.as_ref(),
                right_join_schema.as_ref(),
                output_schema.as_ref(),
            )?;
            let residual_predicate =
                dbsp::DbspPredicate::try_new(residual_expr, Arc::clone(&residual_schema))
                    .context("analyze ASOF residual predicate")?;
            let residual_evaluator = Arc::new(
                VectorizedFilterProjectEvaluator::for_filter(&residual_predicate, residual_schema)
                    .context("build vectorized ASOF residual evaluator")?,
            );
            let residual_graph_id = graph_id.clone();
            let residual_filter_events = task_events.clone();
            let residual_filter_graph_id = graph_id.clone();
            let residual_filter_label = format!("asof-candidate-filter:{graph_id}");
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
                &candidate_stream,
                residual_transform,
                Some(residual_filter_error_handler),
            )
            .await
            .context("initialize vectorized ASOF residual filter")?;
            candidate_stream = residual_filter.stream();
        }

        let left_partition_columns = Arc::new((0..left_schema.len()).collect::<Vec<_>>());
        let right_timestamp_output_column = left_join_schema.len() + right_timestamp_column;
        let right_value_columns = Arc::new(
            (left_join_schema.len()..left_join_schema.len() + right_schema.len())
                .collect::<Vec<_>>(),
        );
        let top1_graph_id = graph_id.clone();
        let key_extractor = move |delta_values: &[(Vec<u8>, i64)]| {
            let mut out = Vec::new();
            let mut decoded_right = Vec::new();
            for (row, weight) in delta_values {
                if *weight == 0 {
                    continue;
                }
                let partition = match extract_encoded_row_columns(
                    row,
                    left_partition_columns.as_ref(),
                    false,
                ) {
                    Ok(Some(partition)) => partition,
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %top1_graph_id,
                            error = %err,
                            "failed to extract ASOF top1 partition"
                        );
                        continue;
                    }
                };
                let order =
                    match extract_encoded_row_i64_like_column(row, right_timestamp_output_column) {
                        Ok(Some(order)) => order,
                        Ok(None) => continue,
                        Err(err) => {
                            tracing::warn!(
                                    graph_id = %top1_graph_id,
                                    error = %err,
                                "failed to extract ASOF top1 order"
                            );
                            continue;
                        }
                    };
                let right_value =
                    match extract_encoded_row_columns(row, right_value_columns.as_ref(), false) {
                        Ok(Some(encoded)) => {
                            if let Err(err) =
                                decode_all_encoded_row_scalars_into(&encoded, &mut decoded_right)
                            {
                                tracing::warn!(
                                    graph_id = %top1_graph_id,
                                    error = %err,
                                    "failed to decode ASOF top1 right tie-break value"
                                );
                                continue;
                            }
                            decoded_right.clone()
                        }
                        Ok(None) => Vec::new(),
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %top1_graph_id,
                                error = %err,
                                "failed to extract ASOF top1 right tie-break value"
                            );
                            continue;
                        }
                    };
                out.push((
                    row.clone(),
                    *weight,
                    Some(partition),
                    Some((Reverse(order), Reverse(right_value))),
                ));
            }
            out
        };

        let top1_events = task_events.clone();
        let top1_graph_id = graph_id.clone();
        let top1_label = format!("asof-top1:{graph_id}");
        let top1_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&top1_events, &top1_graph_id, top1_label.clone(), err);
        });
        let top1 = DbspPartitionedTop1::new_with_batch_key_extractor::<
            Vec<u8>,
            Vec<u8>,
            (
                Reverse<i64>,
                Reverse<Vec<Option<crate::encoding::EncodedRowScalar>>>,
            ),
            _,
        >(&candidate_stream, key_extractor, Some(top1_error_handler))
        .await
        .context("initialize ASOF latest-row top1")?;

        let matched_raw_stream = top1.stream();
        let matched_left_columns = Arc::new((0..left_schema.len()).collect::<Vec<_>>());
        let matched_left_graph_id = graph_id.clone();
        let matched_left_projector = move |row: &Vec<u8>| -> Vec<u8> {
            match extract_encoded_row_columns(row, matched_left_columns.as_ref(), false) {
                Ok(Some(encoded)) => encoded,
                Ok(None) => Vec::new(),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %matched_left_graph_id,
                        error = %err,
                        "failed to project ASOF matched-left row"
                    );
                    Vec::new()
                }
            }
        };
        let matched_left_events = task_events.clone();
        let matched_left_error_graph_id = graph_id.clone();
        let matched_left_label = format!("asof-matched-left:{graph_id}");
        let matched_left_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &matched_left_events,
                &matched_left_error_graph_id,
                matched_left_label.clone(),
                err,
            );
        });
        let matched_left_transform =
            move |delta_values: &[(Vec<u8>, i64)]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                Ok(project_encoded_delta_batch(
                    delta_values,
                    &matched_left_projector,
                ))
            };
        let matched_left = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &matched_raw_stream,
            matched_left_transform,
            Some(matched_left_error_handler),
        )
        .await
        .context("initialize ASOF matched-left projection")?;

        let final_matched_stream = if left_join_schema.len() != left_schema.len()
            || right_join_schema.len() != right_schema.len()
        {
            strip_asof_precomputed_columns(
                matched_raw_stream,
                left_schema.len(),
                left_join_schema.len(),
                right_schema.len(),
                &graph_id,
                format!("asof-output-project:{graph_id}"),
                task_events,
            )
            .await?
        } else {
            matched_raw_stream
        };

        if matches!(node.join_type, DbspJoinType::Inner) {
            return Ok(final_matched_stream);
        }

        let identity_left_key = |delta_values: &[(Vec<u8>, i64)]| {
            delta_values
                .iter()
                .filter(|(_, weight)| *weight != 0)
                .map(|(row, weight)| (row.clone(), row.clone(), *weight))
                .collect::<Vec<_>>()
        };
        let identity_right_key = |delta_values: &[(Vec<u8>, i64)]| {
            delta_values
                .iter()
                .filter(|(_, weight)| *weight != 0)
                .map(|(row, weight)| (row.clone(), row.clone(), *weight))
                .collect::<Vec<_>>()
        };
        let antijoin_events = task_events.clone();
        let antijoin_graph_id = graph_id.clone();
        let antijoin_label = format!("asof-left-antijoin:{graph_id}");
        let antijoin_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &antijoin_events,
                &antijoin_graph_id,
                antijoin_label.clone(),
                err,
            );
        });
        let unmatched_left =
            DbspSemiJoin::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
                &original_left_stream,
                &matched_left.stream(),
                Some(format!("{asof_state_namespace}_unmatched_left")),
                identity_left_key,
                identity_right_key,
                SemiJoinMode::Anti,
                Some(antijoin_error_handler),
            )
            .await
            .context("initialize ASOF unmatched-left anti join")?;

        let right_null_suffix =
            encode_null_row_template(right_schema.as_ref()).context("encode ASOF null RHS row")?;
        let null_extend_graph_id = graph_id.clone();
        let null_extend = move |left_bytes: &Vec<u8>| -> Vec<u8> {
            match concat_encoded_rows(left_bytes, &right_null_suffix) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %null_extend_graph_id,
                        error = %err,
                        "failed to concatenate ASOF null-extended row"
                    );
                    Vec::new()
                }
            }
        };
        let null_extend_events = task_events.clone();
        let null_extend_error_graph_id = graph_id.clone();
        let null_extend_label = format!("asof-null-extend:{graph_id}");
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
            &unmatched_left.stream(),
            null_extend_transform,
            Some(null_extend_error_handler),
        )
        .await
        .context("initialize ASOF null-extension map")?;

        let union_events = task_events.clone();
        let union_graph_id = graph_id.clone();
        let union_label = format!("asof-left-union:{graph_id}");
        let union_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&union_events, &union_graph_id, union_label.clone(), err);
        });
        let union = DbspUnion::new::<Vec<u8>>(
            &[final_matched_stream, null_extended_left.stream()],
            Some(union_error_handler),
        )
        .await
        .context("initialize ASOF LEFT join union")?;
        Ok(union.stream())
    }
}
