use super::*;

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

        let log_graph_id = graph_id.clone();
        let key_schema = Arc::clone(&schema);
        let key_parts = move |bytes: &Vec<u8>| -> (Option<Vec<u8>>, Option<TopNKey>) {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to decode topn row"
                    );
                    return (None, None);
                }
            };

            let mut partition_values = Vec::with_capacity(partition_exprs.len());
            if !partition_exprs.is_empty() {
                for expr in partition_exprs.iter() {
                    let value = match eval_scalar_expression(expr, &row, key_schema.as_ref()) {
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
            }
            let partition_key = if partition_exprs.is_empty() {
                Some(Vec::new())
            } else {
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
            for expr in order_exprs.iter() {
                let value =
                    match eval_scalar_expression(expr.expression(), &row, key_schema.as_ref()) {
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
