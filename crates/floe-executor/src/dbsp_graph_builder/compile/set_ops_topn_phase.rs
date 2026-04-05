use super::*;
use crate::encoding::{EncodedRowScalar, extract_encoded_row_columns, extract_encoded_row_scalar};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;

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
        mut upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let partition_exprs: Arc<Vec<_>> = Arc::new(node.partition_by().to_vec());
        let order_exprs: Arc<Vec<_>> = Arc::new(node.order_by().to_vec());
        let original_schema = Arc::clone(node.output_schema());
        let limit = node.limit();
        let offset = node.offset();
        let partitioned = !partition_exprs.is_empty();
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_events_for_errors = task_events.clone();
        let task_label = format!("topn:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &task_events_for_errors,
                &error_graph_id,
                task_label.clone(),
                err,
            );
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
            .map(|expr| direct_column_index(expr, original_schema.as_ref()))
            .collect::<Vec<_>>();
        let direct_order_columns = order_exprs
            .iter()
            .map(|expr| direct_column_index(expr.expression(), original_schema.as_ref()))
            .collect::<Vec<_>>();

        let mut key_schema = Arc::clone(&original_schema);
        let mut partition_key_columns = Vec::with_capacity(partition_exprs.len());
        let mut order_key_columns = Vec::with_capacity(order_exprs.len());
        let needs_precompute = direct_partition_columns.iter().any(Option::is_none)
            || direct_order_columns.iter().any(Option::is_none);

        if needs_precompute {
            let mut items = Vec::with_capacity(
                original_schema.len() + partition_exprs.len() + order_exprs.len(),
            );
            for field in original_schema.fields() {
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                });
            }
            let mut next_index = original_schema.len();

            for (index, expr) in partition_exprs.iter().enumerate() {
                if let Some(column_idx) = direct_partition_columns[index] {
                    partition_key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_topn_partition_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: expr.expr().clone(),
                    alias: Some(alias),
                });
                partition_key_columns.push(next_index);
                next_index += 1;
            }

            for (index, expr) in order_exprs.iter().enumerate() {
                if let Some(column_idx) = direct_order_columns[index] {
                    order_key_columns.push(column_idx);
                    continue;
                }
                let alias = format!("__floe_topn_order_expr_{index}");
                items.push(dbsp::circuit::plan::ProjectItem {
                    expr: expr.expression().expr().clone(),
                    alias: Some(alias),
                });
                order_key_columns.push(next_index);
                next_index += 1;
            }

            let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&original_schema), items)
                .context("build topn key precompute projection")?;
            key_schema = Arc::clone(precompute.output_schema());
            upstream = self
                .compile_map(&precompute, upstream, &task_events)
                .await
                .context("initialize topn key precompute map")?;
        } else {
            partition_key_columns = direct_partition_columns
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .expect("all direct partition columns should be present");
            order_key_columns = direct_order_columns
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .expect("all direct order columns should be present");
        }

        let partition_key_columns = Arc::new(partition_key_columns);
        let order_key_columns = Arc::new(order_key_columns);
        let order_value_types = Arc::new(
            order_key_columns
                .iter()
                .map(|column_idx| {
                    key_schema
                        .field(*column_idx)
                        .map(|field| field.data_type.clone())
                        .ok_or_else(|| {
                            anyhow!("topn order key column index {column_idx} out of bounds")
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let needs_trim_projection = needs_precompute;

        let log_graph_id = graph_id.clone();
        let order_key_columns_for_log = Arc::clone(&order_key_columns);
        let order_value_types_for_log = Arc::clone(&order_value_types);
        let partition_key_columns_for_log = Arc::clone(&partition_key_columns);
        let key_parts = move |bytes: &Vec<u8>| -> (Option<Vec<u8>>, Option<TopNKey>) {
            let partition_key = if partition_exprs.is_empty() {
                Some(Vec::new())
            } else {
                match extract_encoded_row_columns(
                    bytes,
                    partition_key_columns_for_log.as_ref(),
                    false,
                ) {
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
            };

            let mut values = Vec::with_capacity(order_exprs.len());
            for (column_idx, expected_type) in order_key_columns_for_log
                .iter()
                .zip(order_value_types_for_log.iter())
            {
                let scalar = match extract_encoded_row_scalar(bytes, *column_idx) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %log_graph_id,
                            error = %err,
                            "failed to extract topn order key column"
                        );
                        return (partition_key, None);
                    }
                };
                match topn_value_from_encoded_scalar(scalar, expected_type) {
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
            if !needs_trim_projection {
                return Ok(top1.stream());
            }
            let trim =
                build_trim_projection_node(Arc::clone(&key_schema), Arc::clone(&original_schema))
                    .context("build topn trim projection")?;
            return self
                .compile_map(&trim, top1.stream(), &task_events)
                .await
                .context("initialize topn trim projection map");
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
        if !needs_trim_projection {
            return Ok(topn.stream());
        }
        let trim = build_trim_projection_node(key_schema, Arc::clone(&original_schema))
            .context("build topn trim projection")?;
        self.compile_map(&trim, topn.stream(), &task_events)
            .await
            .context("initialize topn trim projection map")
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

fn build_trim_projection_node(
    topn_schema: Arc<RowSchema>,
    original_schema: Arc<RowSchema>,
) -> Result<dbsp::DbspProjectNode> {
    let items = original_schema
        .fields()
        .iter()
        .map(|field| dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        })
        .collect::<Vec<_>>();
    dbsp::DbspProjectNode::try_new(topn_schema, items)
}

fn topn_value_from_encoded_scalar(
    scalar: Option<EncodedRowScalar>,
    expected_type: &DbspScalarType,
) -> Result<TopNValue> {
    match (scalar, expected_type) {
        (None, _) => Ok(TopNValue::Null),
        (Some(EncodedRowScalar::Int64(value)), DbspScalarType::Int64) => {
            Ok(TopNValue::Int64(value))
        }
        (Some(EncodedRowScalar::TimestampMillis(value)), DbspScalarType::TimestampMillis) => {
            Ok(TopNValue::Timestamp(value))
        }
        (Some(EncodedRowScalar::Utf8(value)), DbspScalarType::Utf8) => Ok(TopNValue::Utf8(value)),
        (Some(EncodedRowScalar::Bool(value)), DbspScalarType::Bool) => Ok(TopNValue::Bool(value)),
        (Some(other), expected) => Err(anyhow!(
            "topn order key type mismatch: expected {expected:?}, decoded {other:?}"
        )),
    }
}
