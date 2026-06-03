use super::*;

pub(super) fn try_build_transient_join_pipeline_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientJoinPipelineRootMaterialization>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Join(join) => {
            if !matches!(join.join_type, dbsp::DbspJoinType::Inner)
                || !has_single_consumer(plan, root_idx)
            {
                return Ok(None);
            }
            let (left_input_idx, right_input_idx) = join_inputs(root)?;
            let Some(left_source_root) =
                try_build_transient_source_root_materialization(plan, left_input_idx)?
            else {
                return Ok(None);
            };
            let Some(right_source_root) =
                try_build_transient_source_root_materialization(plan, right_input_idx)?
            else {
                return Ok(None);
            };
            Ok(Some(TransientJoinPipelineRootMaterialization {
                left_input_idx,
                right_input_idx,
                left_source_root,
                right_source_root,
                join: join.as_ref().clone(),
                optimized_nodes: vec![root_idx],
                steps: Vec::new(),
            }))
        }
        DbspNodeKind::Aggregate(aggregate) => {
            if build_incremental_aggregate_slot_kinds(aggregate.aggregates()).is_none() {
                return Ok(None);
            }
            let input_idx = first_input(root, "aggregate")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape
                .steps
                .push(TransientJoinPipelineStep::Aggregate(aggregate.clone()));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::TopN(topn) => {
            let input_idx = first_input(root, "topn")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape
                .steps
                .push(TransientJoinPipelineStep::TopN(topn.clone()));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.steps.push(TransientJoinPipelineStep::Transform(
                build_filter_transform(select)?,
            ));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let Some(mut shape) =
                    try_build_transient_join_pipeline_root_materialization(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.steps.push(TransientJoinPipelineStep::Transform(
                    build_filter_map_transform(select, project)?,
                ));
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape
                .steps
                .push(TransientJoinPipelineStep::Transform(build_map_transform(
                    project,
                )?));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

pub(super) fn try_build_transient_source_topn_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceTopNRootMaterialization>> {
    let Some(shape) = try_build_transient_source_topn_root_shape(plan, root_idx)? else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = transient_topn::build_transient_topn_receiver(
        graph_id,
        &shape.topn,
        upstream,
        Arc::clone(&shape.source_root.transform),
        shape.output_projection.clone(),
        cancel,
        task_events,
        state_table,
        "source_topn",
    );
    Ok(Some(TransientSourceTopNRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
        transform: shape.transform,
    }))
}

pub(super) fn compose_delta_transforms(
    first: Arc<DeltaTransformFn>,
    second: Arc<DeltaTransformFn>,
) -> Arc<DeltaTransformFn> {
    Arc::new(move |deltas| {
        let first = Arc::clone(&first);
        let second = Arc::clone(&second);
        Box::pin(async move {
            let deltas = first(deltas).await?;
            second(Arc::new(deltas)).await
        })
    })
}

pub(super) fn identity_delta_transform() -> Arc<DeltaTransformFn> {
    Arc::new(|deltas: Arc<Vec<(Vec<u8>, i64)>>| {
        Box::pin(async move { Ok(deltas.as_ref().clone()) })
    })
}

pub(super) fn compose_optional_delta_transform(
    first: Option<Arc<DeltaTransformFn>>,
    second: Arc<DeltaTransformFn>,
) -> Option<Arc<DeltaTransformFn>> {
    Some(match first {
        Some(first) => compose_delta_transforms(first, second),
        None => second,
    })
}

pub(super) fn find_transient_source_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Source(source) => Ok(Some(TransientSourceRootShape::Source {
            source: source.clone(),
            optimized_nodes: vec![root_idx],
        })),
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(input) = plan.node(input_idx) else {
                return Ok(None);
            };
            let DbspNodeKind::Source(source) = &input.kind else {
                return Ok(None);
            };
            Ok(Some(TransientSourceRootShape::Select {
                source: source.clone(),
                select: select.clone(),
                optimized_nodes: vec![root_idx],
            }))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let Some(source_node) = plan.node(select_input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let DbspNodeKind::Source(source) = &source_node.kind else {
                    return Ok(None);
                };
                return Ok(Some(TransientSourceRootShape::FilterMap {
                    source: source.clone(),
                    select: select.clone(),
                    project: project.clone(),
                    optimized_nodes: vec![root_idx, input_idx],
                }));
            }
            let Some(input) = plan.node(input_idx) else {
                return Ok(None);
            };
            let DbspNodeKind::Source(source) = &input.kind else {
                return Ok(None);
            };
            Ok(Some(TransientSourceRootShape::Project {
                source: source.clone(),
                project: project.clone(),
                optimized_nodes: vec![root_idx],
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn build_filter_transform(node: &DbspSelectNode) -> Result<Arc<DeltaTransformFn>> {
    let predicate = node.predicate().clone();
    let schema = Arc::clone(node.output_schema());
    let evaluator = Arc::new(
        VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&schema))
            .context("build vectorized transient source filter evaluator")?,
    );
    Ok(Arc::new(move |delta_values| {
        let evaluator = Arc::clone(&evaluator);
        Box::pin(async move {
            evaluator
                .transform_delta_arrow("source_batch_journal", delta_values)
                .await
        })
    }))
}

pub(super) fn build_map_transform(node: &DbspProjectNode) -> Result<Arc<DeltaTransformFn>> {
    let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
    let schema = Arc::clone(node.input_schema());
    let evaluator = Arc::new(
        VectorizedFilterProjectEvaluator::for_map(expressions.as_ref(), Arc::clone(&schema))
            .context("build vectorized transient source map evaluator")?,
    );
    Ok(Arc::new(move |delta_values| {
        let evaluator = Arc::clone(&evaluator);
        Box::pin(async move {
            evaluator
                .transform_delta_arrow("source_batch_journal", delta_values)
                .await
        })
    }))
}

pub(super) fn build_filter_map_transform(
    select: &DbspSelectNode,
    project: &DbspProjectNode,
) -> Result<Arc<DeltaTransformFn>> {
    let predicate = select.predicate().clone();
    let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(project.expressions().to_vec());
    let project_schema = Arc::clone(select.output_schema());
    let evaluator = Arc::new(
        VectorizedFilterProjectEvaluator::for_filter_map(
            &predicate,
            expressions.as_ref(),
            Arc::clone(&project_schema),
        )
        .context("build vectorized transient source filter_map evaluator")?,
    );
    Ok(Arc::new(move |delta_values| {
        let evaluator = Arc::clone(&evaluator);
        Box::pin(async move {
            evaluator
                .transform_delta_arrow("source_batch_journal", delta_values)
                .await
        })
    }))
}
