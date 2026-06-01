use super::*;
use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use anyhow::bail;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
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
        append_only_input: bool,
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

        let distinct = DbspDistinct::new_with_append_only_input::<Vec<u8>>(
            &upstream,
            append_only_input,
            Some(distinct_error_handler),
        )
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
        let order_key_fields = order_key_columns
            .iter()
            .map(|column_idx| {
                key_schema
                    .field(*column_idx)
                    .map(|field| field.name.clone())
                    .unwrap_or_else(|| format!("__missing_{column_idx}"))
            })
            .collect::<Vec<_>>();
        tracing::info!(
            graph_id = %self.graph_id(),
            partition_key_columns = ?partition_key_columns,
            order_key_columns = ?order_key_columns,
            order_key_fields = ?order_key_fields,
            order_specs = ?order_specs,
            "compiled topn node key layout"
        );
        let needs_trim_projection = needs_precompute;

        let key_parts = Arc::new(VectorizedTopNKeyParts::new(
            Arc::clone(&key_schema),
            partition_key_columns,
            order_key_columns,
            order_value_types,
            Arc::clone(&order_specs),
            graph_id.clone(),
            partitioned,
        ));

        if limit == 1 && offset == 0 && partitioned {
            let key_parts_for_top1 = Arc::clone(&key_parts);
            let order_parts_for_top1 = Arc::clone(&key_parts);
            let key_parts_batch =
                move |delta_values: &[(Vec<u8>, i64)]| key_parts_for_top1.extract(delta_values);
            let order_bytes_batch = move |delta_values: &[(Vec<u8>, i64)]| {
                order_parts_for_top1
                    .extract(delta_values)
                    .into_iter()
                    .map(|(row, weight, _, order)| {
                        (row, weight, order.map(|order| order.ordered_bytes()))
                    })
                    .collect()
            };
            let top1 = dbsp::DbspPartitionedTop1::new_with_batch_key_and_order_extractor::<
                Vec<u8>,
                Vec<u8>,
                TopNKey,
                _,
                _,
            >(
                &upstream,
                key_parts_batch,
                order_bytes_batch,
                Some(error_handler),
            )
            .await
            .context("initialize DBSP partitioned top1")?;
            let top1_stream = top1.stream();
            let mut top1_cursor = StreamCursor::new(top1_stream.stream());
            if let Ok((ts, handle)) = top1_cursor.snapshot().await {
                tracing::debug!(
                    graph_id = %graph_id,
                    ts,
                    handle_version = handle.version,
                    "top1 output snapshot"
                );
                log_handle_rows("top1 output snapshot", &handle, &self.bridge).await?;
            }
            let top1_log_limit = Arc::new(AtomicUsize::new(3));
            let top1_log_limit_clone = Arc::clone(&top1_log_limit);
            let top1_task_events = task_events.clone();
            let top1_task_graph_id = graph_id.clone();
            let top1_task_label = "top1-output-logger".to_string();
            let top1_bridge = Arc::clone(&self.bridge);
            tokio::spawn(async move {
                let mut cursor = top1_cursor;
                loop {
                    if top1_log_limit_clone.fetch_sub(1, Ordering::Relaxed) == 0 {
                        break;
                    }
                    let (ts, handle) = match cursor.next().await {
                        Ok(next) => next,
                        Err(err) => {
                            report_graph_task_error(
                                &top1_task_events,
                                &top1_task_graph_id,
                                top1_task_label.clone(),
                                anyhow!("top1 output handle stream closed: {err}"),
                            );
                            break;
                        }
                    };
                    tracing::debug!(
                        graph_id = %top1_task_graph_id,
                        ts,
                        handle_version = handle.version,
                        "top1 output handle"
                    );
                    if let Err(err) =
                        log_handle_rows("top1 output handle", &handle, &top1_bridge).await
                    {
                        report_graph_task_error(
                            &top1_task_events,
                            &top1_task_graph_id,
                            top1_task_label.clone(),
                            anyhow!("failed to log top1 output handle rows: {err}"),
                        );
                        break;
                    }
                }
            });
            if !needs_trim_projection {
                return Ok(top1_stream);
            }
            let trim =
                build_trim_projection_node(Arc::clone(&key_schema), Arc::clone(&original_schema))
                    .context("build topn trim projection")?;
            return self
                .compile_map(&trim, top1_stream, &task_events)
                .await
                .context("initialize topn trim projection map");
        }

        let key_parts_for_topn = Arc::clone(&key_parts);
        let key_parts_batch =
            move |delta_values: &[(Vec<u8>, i64)]| key_parts_for_topn.extract(delta_values);
        let topn = DbspTopN::new_with_batch_key_extractor::<Vec<u8>, Vec<u8>, TopNKey, _>(
            &upstream,
            key_parts_batch,
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

struct VectorizedTopNKeyParts {
    schema: Arc<RowSchema>,
    partition_key_columns: Arc<Vec<usize>>,
    order_key_columns: Arc<Vec<usize>>,
    order_value_types: Arc<Vec<DbspScalarType>>,
    order_specs: Arc<Vec<TopNSortSpec>>,
    graph_id: String,
    partitioned: bool,
}

impl VectorizedTopNKeyParts {
    fn new(
        schema: Arc<RowSchema>,
        partition_key_columns: Arc<Vec<usize>>,
        order_key_columns: Arc<Vec<usize>>,
        order_value_types: Arc<Vec<DbspScalarType>>,
        order_specs: Arc<Vec<TopNSortSpec>>,
        graph_id: String,
        partitioned: bool,
    ) -> Self {
        Self {
            schema,
            partition_key_columns,
            order_key_columns,
            order_value_types,
            order_specs,
            graph_id,
            partitioned,
        }
    }

    fn extract(
        &self,
        delta_values: &[(Vec<u8>, i64)],
    ) -> Vec<(Vec<u8>, i64, Option<Vec<u8>>, Option<TopNKey>)> {
        match self.try_extract(delta_values) {
            Ok(extracted) => extracted,
            Err(err) => {
                tracing::warn!(
                    graph_id = %self.graph_id,
                    error = %err,
                    "failed to evaluate vectorized topn keys"
                );
                Vec::new()
            }
        }
    }

    fn try_extract(
        &self,
        delta_values: &[(Vec<u8>, i64)],
    ) -> Result<Vec<(Vec<u8>, i64, Option<Vec<u8>>, Option<TopNKey>)>> {
        if delta_values.is_empty() {
            return Ok(Vec::new());
        }

        let mut buffer = DeltaBatchBuffer::new(
            self.schema.to_arrow_schema(),
            false,
            DeltaBatchConfig {
                max_rows: usize::MAX,
                max_bytes: usize::MAX,
            },
        )
        .context("create vectorized topn key input delta buffer")?;
        let mut staged_rows = Vec::with_capacity(delta_values.len());
        for (row, weight) in delta_values {
            if *weight == 0 {
                continue;
            }
            if buffer.push(row.clone(), *weight, None)?.is_some() {
                bail!("unbounded vectorized topn key extractor flushed before manual flush");
            }
            staged_rows.push((row.clone(), *weight));
        }
        let Some(batch) = buffer.flush_manual()? else {
            return Ok(Vec::new());
        };

        let mut output = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let (row, weight) = staged_rows
                .get(row_idx)
                .ok_or_else(|| anyhow!("vectorized topn key row index out of bounds"))?;
            let partition_key = if self.partitioned {
                Some(encode_arrow_columns(
                    &batch,
                    self.partition_key_columns.as_ref(),
                    row_idx,
                )?)
            } else {
                Some(Vec::new())
            };
            let mut order_values = Vec::with_capacity(self.order_key_columns.len());
            for (column_idx, expected_type) in self
                .order_key_columns
                .iter()
                .zip(self.order_value_types.iter())
            {
                order_values.push(topn_value_from_arrow(
                    batch.column(*column_idx).as_ref(),
                    row_idx,
                    expected_type,
                )?);
            }
            let order_key = Some(TopNKey::new(
                Arc::clone(&self.order_specs),
                order_values,
                row.clone(),
            ));
            output.push((row.clone(), *weight, partition_key, order_key));
        }
        Ok(output)
    }
}

fn encode_arrow_columns(batch: &RecordBatch, columns: &[usize], row_idx: usize) -> Result<Vec<u8>> {
    let count = u32::try_from(columns.len()).context("too many topn partition columns")?;
    let mut encoded = Vec::with_capacity(4 + columns.len().saturating_mul(16));
    encoded.extend_from_slice(&count.to_le_bytes());
    for column_idx in columns.iter().copied() {
        append_arrow_encoded_value(batch.column(column_idx).as_ref(), row_idx, &mut encoded)?;
    }
    Ok(encoded)
}

fn append_arrow_encoded_value(
    array: &dyn Array,
    row_idx: usize,
    encoded: &mut Vec<u8>,
) -> Result<()> {
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 partition array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x05);
            } else {
                encoded.push(0x01);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 partition array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x06);
            } else {
                encoded.push(0x02);
                let bytes = values.value(row_idx).as_bytes();
                let len = u32::try_from(bytes.len()).context("topn utf8 partition too large")?;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond partition array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x07);
            } else {
                encoded.push(0x03);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean partition array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x08);
            } else {
                encoded.push(0x04);
                encoded.push(u8::from(values.value(row_idx)));
            }
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("expected Date32 partition array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0A);
            } else {
                encoded.push(0x09);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Decimal128(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("expected Decimal128 partition array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0C);
            } else {
                encoded.push(0x0B);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        other => bail!("unsupported Arrow topn partition key type: {other:?}"),
    }
    Ok(())
}

fn topn_value_from_arrow(
    array: &dyn Array,
    row_idx: usize,
    expected_type: &DbspScalarType,
) -> Result<TopNValue> {
    if array.is_null(row_idx) {
        return Ok(TopNValue::Null);
    }
    match (array.data_type(), expected_type) {
        (DataType::Int64, DbspScalarType::Int64) => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 order array"))?;
            Ok(TopNValue::Int64(values.value(row_idx)))
        }
        (DataType::Timestamp(TimeUnit::Millisecond, _), DbspScalarType::TimestampMillis) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond order array"))?;
            Ok(TopNValue::Timestamp(values.value(row_idx)))
        }
        (DataType::Utf8, DbspScalarType::Utf8) => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 order array"))?;
            Ok(TopNValue::Utf8(values.value(row_idx).to_string()))
        }
        (DataType::Boolean, DbspScalarType::Bool) => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean order array"))?;
            Ok(TopNValue::Bool(values.value(row_idx)))
        }
        (actual, expected) => Err(anyhow!(
            "topn order key type mismatch: expected {expected:?}, Arrow column is {actual:?}"
        )),
    }
}
