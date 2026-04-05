use super::*;
use crate::encoding::extract_encoded_row_columns;
use anyhow::anyhow;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::BTreeSet;

impl DbspGraphBuilder {
    pub(crate) async fn compile_union(
        &mut self,
        _node: &DbspUnionNode,
        inputs: Vec<DeltaHandleStream>,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let graph_id = self.graph_id().to_string();
        let union_events = task_events.clone();
        let union_label = format!("union:{graph_id}");
        let union_graph_id = graph_id.clone();
        let union_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&union_events, &union_graph_id, union_label.clone(), err);
        });

        let union = DbspUnion::new::<Vec<u8>>(&inputs, Some(union_error_handler))
            .await
            .context("initialize DBSP union")?;
        Ok(union.stream())
    }

    pub(crate) async fn compile_distinct(
        &mut self,
        _node: &DbspDistinctNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let graph_id = self.graph_id().to_string();
        let distinct_events = task_events.clone();
        let distinct_label = format!("distinct:{graph_id}");
        let distinct_graph_id = graph_id.clone();
        let distinct_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &distinct_events,
                &distinct_graph_id,
                distinct_label.clone(),
                err,
            );
        });

        let distinct = DbspDistinct::new::<Vec<u8>>(&upstream, Some(distinct_error_handler))
            .await
            .context("initialize DBSP distinct")?;
        Ok(distinct.stream())
    }

    pub(crate) async fn compile_topn(
        &mut self,
        node: &DbspTopNNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let partition_exprs: Arc<Vec<_>> = Arc::new(node.partition_by().to_vec());
        let order_exprs: Arc<Vec<_>> = Arc::new(node.order_by().to_vec());
        let schema = Arc::clone(node.output_schema());
        let limit = node.limit();
        let offset = node.offset();
        let partitioned = !partition_exprs.is_empty();
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("topn:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });

        let order_specs = Arc::new(
            order_exprs
                .iter()
                .map(|expr| TopNSortSpec {
                    ascending: expr.ascending(),
                    nulls_first: expr.nulls_first(),
                })
                .collect::<Vec<_>>(),
        );
        let direct_partition_columns = partition_exprs
            .iter()
            .map(|expr| direct_column_index(expr, schema.as_ref()))
            .collect::<Option<Vec<_>>>()
            .map(Arc::new);
        let direct_order_columns = order_exprs
            .iter()
            .map(|expr| direct_column_index(expr.expression(), schema.as_ref()))
            .collect::<Option<Vec<_>>>()
            .map(Arc::new);
        let non_direct_required_columns =
            if direct_partition_columns.is_some() && direct_order_columns.is_some() {
                None
            } else {
                Some(Arc::new(required_topn_input_columns(
                    partition_exprs.as_ref(),
                    order_exprs.as_ref(),
                    schema.as_ref(),
                )?))
            };

        let log_graph_id = graph_id.clone();
        let key_schema = Arc::clone(&schema);
        let key_parts = move |bytes: &Vec<u8>| -> (Option<Vec<u8>>, Option<TopNKey>) {
            let mut decoded_row: Option<Vec<ScalarValue>> = None;

            let partition_key = if partition_exprs.is_empty() {
                Some(Vec::new())
            } else if let Some(indices) = direct_partition_columns.as_ref() {
                match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
                    Ok(selected) => selected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %log_graph_id,
                            error = %err,
                            "failed to extract topn partition key columns"
                        );
                        return (None, None);
                    }
                }
            } else {
                if decoded_row.is_none() {
                    decoded_row = match decode_sparse_row_for_columns(
                        bytes,
                        non_direct_required_columns
                            .as_ref()
                            .expect("non-direct required columns should be present")
                            .as_ref(),
                        key_schema.len(),
                    ) {
                        Ok(row) => Some(row),
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %log_graph_id,
                                error = %err,
                                "failed to decode topn row"
                            );
                            return (None, None);
                        }
                    };
                }
                let row = decoded_row.as_ref().expect("decoded row should be present");
                let mut partition_values = Vec::with_capacity(partition_exprs.len());
                for expr in partition_exprs.iter() {
                    let value = match eval_scalar_expression(expr, row, key_schema.as_ref()) {
                        Ok(value) => value,
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %log_graph_id,
                                error = %err,
                                "failed to evaluate topn partition expression"
                            );
                            return (None, None);
                        }
                    };
                    partition_values.push(value);
                }
                match encode_projected_row_key(&partition_values) {
                    Ok(encoded) => Some(encoded),
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %log_graph_id,
                            error = %err,
                            "failed to encode topn partition key"
                        );
                        return (None, None);
                    }
                }
            };

            let mut values = Vec::with_capacity(order_exprs.len());
            if let Some(indices) = direct_order_columns.as_ref() {
                let selected = match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
                    Ok(Some(selected)) => selected,
                    Ok(None) => return (partition_key, None),
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %log_graph_id,
                            error = %err,
                            "failed to extract topn order key columns"
                        );
                        return (partition_key, None);
                    }
                };
                let order_row = match decode_projected_row_key(&selected) {
                    Ok(values) => values,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %log_graph_id,
                            error = %err,
                            "failed to decode extracted topn order key columns"
                        );
                        return (partition_key, None);
                    }
                };
                for value in order_row {
                    match TopNValue::from_scalar(&value) {
                        Ok(value) => values.push(value),
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %log_graph_id,
                                error = %err,
                                "failed to map topn order value"
                            );
                            return (partition_key, None);
                        }
                    }
                }
            } else {
                if decoded_row.is_none() {
                    decoded_row = match decode_sparse_row_for_columns(
                        bytes,
                        non_direct_required_columns
                            .as_ref()
                            .expect("non-direct required columns should be present")
                            .as_ref(),
                        key_schema.len(),
                    ) {
                        Ok(row) => Some(row),
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %log_graph_id,
                                error = %err,
                                "failed to decode topn row"
                            );
                            return (partition_key, None);
                        }
                    };
                }
                let row = decoded_row.as_ref().expect("decoded row should be present");
                for expr in order_exprs.iter() {
                    let value =
                        match eval_scalar_expression(expr.expression(), row, key_schema.as_ref()) {
                            Ok(value) => value,
                            Err(err) => {
                                tracing::warn!(
                                    graph_id = %log_graph_id,
                                    error = %err,
                                    "failed to evaluate topn order expression"
                                );
                                return (partition_key, None);
                            }
                        };
                    match TopNValue::from_scalar(&value) {
                        Ok(value) => values.push(value),
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %log_graph_id,
                                error = %err,
                                "failed to map topn order value"
                            );
                            return (partition_key, None);
                        }
                    }
                }
            }

            (
                partition_key,
                Some(TopNKey::new(
                    Arc::clone(&order_specs),
                    values,
                    bytes.clone(),
                )),
            )
        };

        if limit == 1 && offset == 0 && partitioned {
            let top1 = dbsp::DbspPartitionedTop1::new_with_key_extractor::<
                Vec<u8>,
                Vec<u8>,
                TopNKey,
                _,
            >(&upstream, key_parts, Some(error_handler))
            .await
            .context("initialize DBSP partitioned top1")?;
            return Ok(top1.stream());
        }

        let topn = DbspTopN::new_with_key_extractor::<Vec<u8>, Vec<u8>, TopNKey, _>(
            &upstream,
            key_parts,
            limit,
            offset,
            Some(error_handler),
        )
        .await
        .context("initialize DBSP topn")?;
        Ok(topn.stream())
    }
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

fn required_topn_input_columns(
    partition_exprs: &[dbsp::circuit::plan::DbspExpression],
    order_exprs: &[dbsp::OrderExpr],
    schema: &RowSchema,
) -> Result<Vec<usize>> {
    let mut columns = BTreeSet::new();
    for expr in partition_exprs {
        add_expr_input_columns(expr.expr(), schema, &mut columns)?;
    }
    for expr in order_exprs {
        add_expr_input_columns(expr.expression().expr(), schema, &mut columns)?;
    }
    Ok(columns.into_iter().collect())
}

fn add_expr_input_columns(
    expr: &Expr,
    schema: &RowSchema,
    columns: &mut BTreeSet<usize>,
) -> Result<()> {
    for column in expr.column_refs() {
        let index = schema.field_index(column.name.as_str()).ok_or_else(|| {
            anyhow!(
                "column '{}' was not found while deriving topn required input columns",
                column.name
            )
        })?;
        columns.insert(index);
    }
    Ok(())
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
