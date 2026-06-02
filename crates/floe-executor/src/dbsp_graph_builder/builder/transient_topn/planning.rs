use super::direct::{TransientDirectPartitionTopNConfig, TransientDirectTop1Config};
use super::key::{
    TransientDirectTop1PartitionLayout, TransientTopNKeyLayout, encode_arrow_columns,
    projected_arrow_schema,
};
use super::*;
use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use datafusion::arrow::datatypes::SchemaRef;

pub(super) fn build_transient_topn_key_layout(
    topn: &DbspTopNNode,
) -> Result<TransientTopNKeyLayout> {
    let input_schema = Arc::clone(topn.output_schema());
    let direct_partition_columns = topn
        .partition_by()
        .iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), input_schema.as_ref()))
        .collect::<Vec<_>>();
    let direct_order_columns = topn
        .order_by()
        .iter()
        .map(|expr| {
            projection_direct_column_index_expression(
                expr.expression().expr(),
                input_schema.as_ref(),
            )
        })
        .collect::<Vec<_>>();

    if direct_partition_columns.iter().all(Option::is_some)
        && direct_order_columns.iter().all(Option::is_some)
    {
        return Ok(TransientTopNKeyLayout {
            input_schema: Arc::clone(&input_schema),
            partition_columns: Arc::new(
                direct_partition_columns
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn partition columns should be present"),
            ),
            order_columns: Arc::new(
                direct_order_columns
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn order columns should be present"),
            ),
            order_types: Arc::new(
                direct_order_columns
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn order columns should be present")
                    .into_iter()
                    .map(|column_idx| {
                        input_schema
                            .field(column_idx)
                            .map(|field| field.data_type.clone())
                            .expect("transient topn order key column index should be in bounds")
                    })
                    .collect(),
            ),
            precompute_evaluator: None,
        });
    }

    let mut items =
        Vec::with_capacity(input_schema.len() + topn.partition_by().len() + topn.order_by().len());
    for field in input_schema.fields() {
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }

    let mut expression_columns = HashMap::new();
    let mut seen = HashSet::new();
    let mut next_index = input_schema.len();
    let mut partition_columns = Vec::with_capacity(topn.partition_by().len());
    for (index, expr) in topn.partition_by().iter().enumerate() {
        if let Some(column_idx) = direct_partition_columns[index] {
            partition_columns.push(column_idx);
            continue;
        }
        let key = transient_topn_expression_lookup_key(expr.expr());
        if seen.insert(key.clone()) {
            let alias = format!("__floe_transient_topn_partition_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: expr.expr().clone(),
                alias: Some(alias),
            });
            expression_columns.insert(key.clone(), next_index);
            next_index += 1;
        }
        partition_columns.push(
            *expression_columns
                .get(&key)
                .expect("transient topn partition expression column should be registered"),
        );
    }

    let mut order_columns = Vec::with_capacity(topn.order_by().len());
    for (index, expr) in topn.order_by().iter().enumerate() {
        if let Some(column_idx) = direct_order_columns[index] {
            order_columns.push(column_idx);
            continue;
        }
        let key = transient_topn_expression_lookup_key(expr.expression().expr());
        if seen.insert(key.clone()) {
            let alias = format!("__floe_transient_topn_order_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: expr.expression().expr().clone(),
                alias: Some(alias),
            });
            expression_columns.insert(key.clone(), next_index);
            next_index += 1;
        }
        order_columns.push(
            *expression_columns
                .get(&key)
                .expect("transient topn order expression column should be registered"),
        );
    }

    let project_node = DbspProjectNode::try_new(Arc::clone(&input_schema), items)
        .context("build transient topn expression precompute projection")?;
    let evaluator = VectorizedFilterProjectEvaluator::for_map(
        project_node.expressions(),
        Arc::clone(&input_schema),
    )
    .context("initialize transient topn precompute evaluator")?;
    let projected_schema = project_node.output_schema();
    let order_types = order_columns
        .iter()
        .map(|column_idx| {
            projected_schema
                .field(*column_idx)
                .map(|field| field.data_type.clone())
                .ok_or_else(|| {
                    anyhow!("transient topn order key column index {column_idx} out of bounds")
                })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TransientTopNKeyLayout {
        input_schema: Arc::clone(projected_schema),
        partition_columns: Arc::new(partition_columns),
        order_columns: Arc::new(order_columns),
        order_types: Arc::new(order_types),
        precompute_evaluator: Some(Arc::new(evaluator)),
    })
}

fn transient_topn_expression_lookup_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => transient_topn_expression_lookup_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}

pub(super) fn try_build_direct_partitioned_top1_config(
    topn: &DbspTopNNode,
) -> Option<TransientDirectTop1Config> {
    if topn.offset() != 0 || topn.limit() != 1 {
        return None;
    }
    if topn.partition_by().is_empty() || topn.partition_by().len() > 2 || topn.order_by().len() != 1
    {
        return None;
    }

    let schema = topn.output_schema();
    let partition_indices = topn
        .partition_by()
        .iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), schema.as_ref()))
        .collect::<Option<Vec<_>>>()?;

    for partition_idx in &partition_indices {
        let partition_field = schema.field(*partition_idx)?;
        if partition_field.data_type != dbsp::circuit::types::DbspScalarType::Int64
            || partition_field.nullable
        {
            return None;
        }
    }

    let order_idx = projection_direct_column_index_expression(
        topn.order_by()[0].expression().expr(),
        schema.as_ref(),
    )?;
    let order_field = schema.field(order_idx)?;
    if !matches!(
        order_field.data_type,
        dbsp::circuit::types::DbspScalarType::Int64
            | dbsp::circuit::types::DbspScalarType::TimestampMillis
    ) || order_field.nullable
    {
        return None;
    }

    let partition_layout = match partition_indices.as_slice() {
        [partition_idx] => TransientDirectTop1PartitionLayout::One(*partition_idx),
        [first_partition_idx, second_partition_idx] => {
            TransientDirectTop1PartitionLayout::Two([*first_partition_idx, *second_partition_idx])
        }
        _ => return None,
    };

    Some(TransientDirectTop1Config {
        partition_layout,
        order_idx,
        ascending: topn.order_by()[0].ascending(),
    })
}

pub(super) fn try_build_direct_partition_topn_config(
    topn: &DbspTopNNode,
) -> Option<TransientDirectPartitionTopNConfig> {
    if topn.offset() != 0 || topn.limit() == 0 || topn.partition_by().len() != 1 {
        return None;
    }

    let schema = topn.output_schema();
    let partition_idx =
        projection_direct_column_index_expression(topn.partition_by()[0].expr(), schema.as_ref())?;
    let partition_field = schema.field(partition_idx)?;
    if partition_field.data_type != dbsp::circuit::types::DbspScalarType::Int64
        || partition_field.nullable
    {
        return None;
    }

    Some(TransientDirectPartitionTopNConfig { partition_idx })
}

pub(in crate::dbsp_graph_builder::builder) fn build_direct_projection_transform(
    columns: Arc<Vec<usize>>,
    input_schema: Arc<RowSchema>,
) -> Arc<DeltaTransformFn> {
    let input_arrow_schema = input_schema.to_arrow_schema();
    Arc::new(move |deltas| {
        let columns = Arc::clone(&columns);
        let input_arrow_schema = Arc::clone(&input_arrow_schema);
        Box::pin(async move {
            project_encoded_deltas(deltas.as_ref(), columns.as_ref(), input_arrow_schema)
        })
    })
}

pub(in crate::dbsp_graph_builder::builder) fn fold_topn_root_output_projection(
    shape: &mut TransientSourceTopNRootShape,
) {
    if let Some(output_projection) = shape.output_projection.take() {
        shape.transform = compose_optional_delta_transform(
            shape.transform.take(),
            build_direct_projection_transform(
                output_projection,
                Arc::clone(shape.topn.output_schema()),
            ),
        );
    }
}

pub(super) fn project_encoded_deltas(
    deltas: &[(Vec<u8>, i64)],
    columns: &[usize],
    input_schema: SchemaRef,
) -> Result<Vec<(Vec<u8>, i64)>> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }
    let projected_schema = projected_arrow_schema(&input_schema, columns)?;
    let mut buffer = DeltaBatchBuffer::new_projected(
        projected_schema,
        Arc::<[usize]>::from(columns.to_vec()),
        false,
        DeltaBatchConfig {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
        },
    )
    .context("create transient topn projected output batch")?;
    let mut staged_weights = Vec::with_capacity(deltas.len());
    for (encoded, weight) in deltas {
        if *weight == 0 {
            continue;
        }
        if buffer.push_ref(encoded, *weight, None)?.is_some() {
            bail!("unbounded transient topn projection flushed before manual flush");
        }
        staged_weights.push(*weight);
    }
    let Some(batch) = buffer.flush_manual()? else {
        return Ok(Vec::new());
    };
    let projected_positions = (0..columns.len()).collect::<Vec<_>>();
    let mut output = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let weight = staged_weights
            .get(row_idx)
            .copied()
            .ok_or_else(|| anyhow!("transient topn projection row index out of bounds"))?;
        let projected = encode_arrow_columns(&batch, &projected_positions, row_idx)?;
        output.push((projected, weight));
    }
    Ok(output)
}
