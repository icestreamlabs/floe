use super::*;

use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
use crate::encoding::extract_encoded_row_columns;
use crate::expression::{ExpressionEvaluator, scalar_to_bool};
use crate::projection::ProjectionEvaluator;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::BTreeSet;

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

        match VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&schema)) {
            Ok(evaluator) => {
                let evaluator = Arc::new(evaluator);
                tracing::info!(
                    graph_id = %graph_id,
                    "using vectorized filter execution path"
                );
                let vectorized_graph_id = graph_id.clone();
                let transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<
                    Vec<(Vec<u8>, i64)>,
                > { evaluator.transform_delta(&vectorized_graph_id, delta_values) };
                let filter = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                    &upstream,
                    transform,
                    Some(error_handler),
                )
                .await
                .context("initialize vectorized DBSP filter")?;
                return Ok(filter.stream());
            }
            Err(err) => {
                tracing::info!(
                    graph_id = %graph_id,
                    error = %err,
                    "vectorized filter evaluator unavailable; falling back to scalar execution path"
                );
            }
        }

        tracing::info!(
            graph_id = %graph_id,
            "using scalar filter execution path"
        );
        let direct_predicate_bool_column =
            direct_boolean_predicate_column(predicate.expression(), schema.as_ref());
        let predicate_eval = direct_predicate_bool_column.is_none().then(|| {
            Arc::new(ExpressionEvaluator::new(
                Arc::clone(&schema),
                predicate.expression(),
            ))
        });
        let predicate_required_columns = direct_predicate_bool_column.is_none().then(|| {
            Arc::new(required_expression_input_columns(
                predicate.expression(),
                schema.as_ref(),
            ))
        });
        let scalar_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                let mut filtered = Vec::with_capacity(delta_values.len());
                for (bytes, weight) in delta_values {
                    let include = if let Some(predicate_column) = direct_predicate_bool_column {
                        let selected =
                            extract_encoded_row_columns(&bytes, &[predicate_column], false)
                                .with_context(|| {
                                    format!(
                                        "extract source filter predicate column for graph '{scalar_graph_id}'"
                                    )
                                })?
                                .ok_or_else(|| {
                                    anyhow!(
                                        "source filter direct predicate extraction unexpectedly returned null result for graph '{scalar_graph_id}'"
                                    )
                                })?;
                        let selected_values = decode_projected_row_key(&selected).with_context(|| {
                            format!(
                                "decode extracted source filter predicate column for graph '{scalar_graph_id}'"
                            )
                        })?;
                        let predicate_value = selected_values.first().ok_or_else(|| {
                            anyhow!(
                                "source filter direct predicate extraction produced no columns for graph '{scalar_graph_id}'"
                            )
                        })?;
                        scalar_to_bool(predicate_value).with_context(|| {
                            format!(
                                "evaluate extracted source filter predicate for graph '{scalar_graph_id}'"
                            )
                        })?
                    } else {
                        let row = decode_sparse_row_for_columns(
                            &bytes,
                            predicate_required_columns
                                .as_ref()
                                .expect("predicate required columns should be present")
                                .as_ref(),
                            schema.len(),
                        )
                        .with_context(|| {
                            format!("decode source filter row for graph '{scalar_graph_id}'")
                        })?;
                        predicate_eval
                            .as_ref()
                            .expect("predicate evaluator should be present")
                            .eval_bool(&row)
                            .with_context(|| {
                                format!(
                                    "evaluate source filter predicate for graph '{scalar_graph_id}'"
                                )
                            })?
                    };
                    if include {
                        filtered.push((bytes, weight));
                    }
                }
                Ok(filtered)
            };
        let filter = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize scalar DBSP filter")?;
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

        match VectorizedFilterProjectEvaluator::for_map(expressions.as_ref(), Arc::clone(&schema)) {
            Ok(evaluator) => {
                let evaluator = Arc::new(evaluator);
                tracing::info!(
                    graph_id = %graph_id,
                    "using vectorized map execution path"
                );
                let vectorized_graph_id = graph_id.clone();
                let transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<
                    Vec<(Vec<u8>, i64)>,
                > { evaluator.transform_delta(&vectorized_graph_id, delta_values) };
                let map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                    &upstream,
                    transform,
                    Some(error_handler),
                )
                .await
                .context("initialize vectorized DBSP map")?;
                return Ok(map.stream());
            }
            Err(err) => {
                tracing::info!(
                    graph_id = %graph_id,
                    error = %err,
                    "vectorized map evaluator unavailable; falling back to scalar execution path"
                );
            }
        }

        tracing::info!(
            graph_id = %graph_id,
            "using scalar map execution path"
        );
        let direct_projection_columns =
            direct_project_column_indices(expressions.as_ref(), schema.as_ref()).map(Arc::new);
        let projection_required_columns = direct_projection_columns.is_none().then(|| {
            Arc::new(required_projection_input_columns(
                expressions.as_ref(),
                schema.as_ref(),
            ))
        });
        if let Some(columns) = direct_projection_columns {
            tracing::info!(
                graph_id = %graph_id,
                "using scalar map direct projection fast path"
            );
            let scalar_graph_id = graph_id.clone();
            let transform =
                move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                    let mut projected = Vec::with_capacity(delta_values.len());
                    for (bytes, weight) in delta_values {
                        let encoded = extract_encoded_row_columns(&bytes, columns.as_ref(), false)
                            .with_context(|| {
                                format!(
                                    "extract source map projection columns for graph '{scalar_graph_id}'"
                                )
                            })?
                            .ok_or_else(|| {
                                anyhow!(
                                    "source map direct projection unexpectedly returned null result for graph '{scalar_graph_id}'"
                                )
                            })?;
                        projected.push((encoded, weight));
                    }
                    Ok(projected)
                };
            let map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                &upstream,
                transform,
                Some(error_handler),
            )
            .await
            .context("initialize scalar DBSP map direct projection fast path")?;
            return Ok(map.stream());
        }

        let projector_eval = Arc::new(ProjectionEvaluator::new(
            Arc::clone(&schema),
            expressions.as_ref(),
        ));
        let scalar_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                let mut projected = Vec::with_capacity(delta_values.len());
                for (bytes, weight) in delta_values {
                    let row = decode_sparse_row_for_columns(
                        &bytes,
                        projection_required_columns
                            .as_ref()
                            .expect("projection required columns should be present")
                            .as_ref(),
                        schema.len(),
                    )
                    .with_context(|| {
                        format!("decode source map row for graph '{scalar_graph_id}'")
                    })?;
                    let mapped = projector_eval.project(&row).with_context(|| {
                        format!("evaluate source map projection for graph '{scalar_graph_id}'")
                    })?;
                    let encoded = encode_projected_row_key(&mapped).with_context(|| {
                        format!("encode source map projection for graph '{scalar_graph_id}'")
                    })?;
                    projected.push((encoded, weight));
                }
                Ok(projected)
            };
        let map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize scalar DBSP map")?;
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
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(project.expressions().to_vec());
        let project_schema = Arc::clone(project.input_schema());

        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("filter_map:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });

        match VectorizedFilterProjectEvaluator::for_filter_map(
            &predicate,
            expressions.as_ref(),
            Arc::clone(&project_schema),
        ) {
            Ok(evaluator) => {
                let evaluator = Arc::new(evaluator);
                tracing::info!(
                    graph_id = %graph_id,
                    "using vectorized filter_map execution path"
                );
                let vectorized_graph_id = graph_id.clone();
                let transform = move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<
                    Vec<(Vec<u8>, i64)>,
                > { evaluator.transform_delta(&vectorized_graph_id, delta_values) };

                let filter_map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
                    &upstream,
                    transform,
                    Some(error_handler),
                )
                .await
                .context("initialize vectorized DBSP filter_map")?;
                return Ok(filter_map.stream());
            }
            Err(err) => {
                tracing::info!(
                    graph_id = %graph_id,
                    error = %err,
                    "vectorized filter_map evaluator unavailable; falling back to scalar execution path"
                );
            }
        }

        tracing::info!(
            graph_id = %graph_id,
            "using scalar filter_map execution path"
        );
        let direct_projection_columns =
            direct_project_column_indices(expressions.as_ref(), project_schema.as_ref())
                .map(Arc::new);
        let direct_predicate_bool_column =
            direct_boolean_predicate_column(predicate.expression(), project_schema.as_ref());
        let predicate_eval = direct_predicate_bool_column.is_none().then(|| {
            Arc::new(ExpressionEvaluator::new(
                Arc::clone(&project_schema),
                predicate.expression(),
            ))
        });
        let predicate_required_columns = direct_predicate_bool_column.is_none().then(|| {
            Arc::new(required_expression_input_columns(
                predicate.expression(),
                project_schema.as_ref(),
            ))
        });
        let projector_eval = direct_projection_columns.is_none().then(|| {
            Arc::new(ProjectionEvaluator::new(
                Arc::clone(&project_schema),
                expressions.as_ref(),
            ))
        });
        let projection_required_columns = direct_projection_columns.is_none().then(|| {
            Arc::new(required_projection_input_columns(
                expressions.as_ref(),
                project_schema.as_ref(),
            ))
        });
        let scalar_required_columns =
            if direct_predicate_bool_column.is_none() || direct_projection_columns.is_none() {
                Some(Arc::new(union_required_columns(
                    predicate_required_columns
                        .as_ref()
                        .map(|cols| cols.as_slice()),
                    projection_required_columns
                        .as_ref()
                        .map(|cols| cols.as_slice()),
                )))
            } else {
                None
            };
        let scalar_graph_id = graph_id.clone();
        let transform =
            move |delta_values: Vec<(Vec<u8>, i64)>| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                let mut projected = Vec::with_capacity(delta_values.len());
                for (bytes, weight) in delta_values {
                    let mut decoded_row: Option<Vec<ScalarValue>> = None;
                    let include = if let Some(predicate_column) = direct_predicate_bool_column {
                        let selected =
                            extract_encoded_row_columns(&bytes, &[predicate_column], false)
                                .with_context(|| {
                                    format!(
                                        "extract source filter_map predicate column for graph '{scalar_graph_id}'"
                                    )
                                })?
                                .ok_or_else(|| {
                                    anyhow!(
                                        "source filter_map direct predicate extraction unexpectedly returned null result for graph '{scalar_graph_id}'"
                                    )
                                })?;
                        let selected_values = decode_projected_row_key(&selected).with_context(|| {
                            format!(
                                "decode extracted source filter_map predicate column for graph '{scalar_graph_id}'"
                            )
                        })?;
                        let predicate_value = selected_values.first().ok_or_else(|| {
                            anyhow!(
                                "source filter_map direct predicate extraction produced no columns for graph '{scalar_graph_id}'"
                            )
                        })?;
                        scalar_to_bool(predicate_value).with_context(|| {
                            format!(
                                "evaluate extracted source filter_map predicate for graph '{scalar_graph_id}'"
                            )
                        })?
                    } else {
                        decoded_row = Some(
                            decode_sparse_row_for_columns(
                                &bytes,
                                scalar_required_columns
                                    .as_ref()
                                    .expect("scalar required columns should be present")
                                    .as_ref(),
                                project_schema.len(),
                            )
                            .with_context(|| {
                                format!(
                                    "decode source filter_map row for graph '{scalar_graph_id}'"
                                )
                            })?,
                        );
                        predicate_eval
                            .as_ref()
                            .expect("predicate evaluator should be present")
                            .eval_bool(decoded_row.as_ref().expect("decoded row should be present"))
                            .with_context(|| {
                                format!(
                                    "evaluate source filter_map predicate for graph '{scalar_graph_id}'"
                                )
                            })?
                    };
                    if !include {
                        continue;
                    }

                    let encoded = if let Some(columns) = direct_projection_columns.as_ref() {
                        extract_encoded_row_columns(&bytes, columns.as_ref(), false)
                            .with_context(|| {
                                format!(
                                    "extract source filter_map projection columns for graph '{scalar_graph_id}'"
                                )
                            })?
                            .ok_or_else(|| {
                                anyhow!(
                                    "source filter_map direct projection unexpectedly returned null result for graph '{scalar_graph_id}'"
                                )
                            })?
                    } else {
                        let projector_eval = projector_eval
                            .as_ref()
                            .expect("projection evaluator should be present");
                        if decoded_row.is_none() {
                            decoded_row = Some(
                                decode_sparse_row_for_columns(
                                    &bytes,
                                    scalar_required_columns
                                        .as_ref()
                                        .expect("scalar required columns should be present")
                                        .as_ref(),
                                    project_schema.len(),
                                )
                                .with_context(|| {
                                    format!(
                                        "decode source filter_map row for projection for graph '{scalar_graph_id}'"
                                    )
                                })?,
                            );
                        }
                        let mapped = projector_eval
                            .project(decoded_row.as_ref().expect("decoded row should be present"))
                            .with_context(|| {
                            format!(
                                "evaluate source filter_map projection for graph '{scalar_graph_id}'"
                            )
                        })?;
                        encode_projected_row_key(&mapped).with_context(|| {
                            format!(
                                "encode source filter_map projection for graph '{scalar_graph_id}'"
                            )
                        })?
                    };
                    projected.push((encoded, weight));
                }
                Ok(projected)
            };
        let filter_map = DbspFilterMap::new_batch::<Vec<u8>, Vec<u8>, _>(
            &upstream,
            transform,
            Some(error_handler),
        )
        .await
        .context("initialize scalar DBSP filter_map")?;
        Ok(filter_map.stream())
    }
}

fn direct_project_column_indices(
    expressions: &[DbspProjectExpr],
    schema: &RowSchema,
) -> Option<Vec<usize>> {
    expressions
        .iter()
        .map(|expr| direct_column_index(expr.expression(), schema))
        .collect()
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

fn direct_boolean_predicate_column(
    predicate: &dbsp::circuit::plan::DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    let index = direct_column_index(predicate, schema)?;
    let field = schema.field(index)?;
    if field.data_type == DbspScalarType::Bool {
        Some(index)
    } else {
        None
    }
}

fn required_expression_input_columns(
    expr: &dbsp::circuit::plan::DbspExpression,
    schema: &RowSchema,
) -> Vec<usize> {
    let mut columns = BTreeSet::new();
    add_expr_input_columns(expr.expr(), schema, &mut columns);
    columns.into_iter().collect()
}

fn required_projection_input_columns(
    expressions: &[DbspProjectExpr],
    schema: &RowSchema,
) -> Vec<usize> {
    let mut columns = BTreeSet::new();
    for expr in expressions {
        add_expr_input_columns(expr.expression().expr(), schema, &mut columns);
    }
    columns.into_iter().collect()
}

fn add_expr_input_columns(expr: &Expr, schema: &RowSchema, columns: &mut BTreeSet<usize>) {
    for column in expr.column_refs() {
        if let Some(index) = resolve_direct_column(schema, &column) {
            columns.insert(index);
        }
    }
}

fn union_required_columns(left: Option<&[usize]>, right: Option<&[usize]>) -> Vec<usize> {
    let mut columns = BTreeSet::new();
    if let Some(left_columns) = left {
        columns.extend(left_columns.iter().copied());
    }
    if let Some(right_columns) = right {
        columns.extend(right_columns.iter().copied());
    }
    columns.into_iter().collect()
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
