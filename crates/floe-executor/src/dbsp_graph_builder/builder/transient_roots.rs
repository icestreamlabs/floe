use super::*;

#[cfg(test)]
pub(super) fn join_input_unique_on_direct_source_primary_key<'a>(
    plan: &CircuitPlan,
    input_idx: usize,
    key_expressions: impl IntoIterator<Item = &'a DbspExpression>,
    input_schema: &RowSchema,
) -> Result<bool> {
    Ok(join_input_direct_source_primary_key_columns(
        plan,
        input_idx,
        key_expressions,
        input_schema,
    )?
    .is_some())
}

#[cfg(test)]
pub(super) fn join_input_direct_source_primary_key_columns<'a>(
    plan: &CircuitPlan,
    input_idx: usize,
    key_expressions: impl IntoIterator<Item = &'a DbspExpression>,
    input_schema: &RowSchema,
) -> Result<Option<Arc<Vec<usize>>>> {
    let Some(shape) = find_transient_source_root_shape(plan, input_idx)? else {
        return Ok(None);
    };
    let (source, project) = match shape {
        TransientSourceRootShape::Source { source, .. }
        | TransientSourceRootShape::Select { source, .. } => (source, None),
        TransientSourceRootShape::Project {
            source, project, ..
        }
        | TransientSourceRootShape::FilterMap {
            source, project, ..
        } => (source, Some(project)),
    };

    let Some(key_columns) = key_expressions
        .into_iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), input_schema))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let key_columns = if let Some(project) = project.as_ref() {
        key_columns
            .into_iter()
            .map(|column_idx| {
                project
                    .expressions()
                    .get(column_idx)
                    .and_then(|expr| projection_direct_column_index(expr, project.input_schema()))
            })
            .collect::<Option<BTreeSet<_>>>()
    } else {
        Some(key_columns.into_iter().collect::<BTreeSet<_>>())
    };
    let Some(key_columns) = key_columns else {
        return Ok(None);
    };
    let primary_key_columns = source
        .table
        .primary_key()
        .columns()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    if key_columns == primary_key_columns {
        Ok(Some(Arc::new(primary_key_columns.into_iter().collect())))
    } else {
        Ok(None)
    }
}

pub(super) fn try_build_transient_join_input_optimization(
    graph_id: &str,
    plan: &CircuitPlan,
    input_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    closed_key_columns: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
) -> Result<Option<TransientJoinInputOptimization>> {
    let Some(source_root) = try_build_transient_source_root_materialization(plan, input_idx)?
    else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };

    let mut upstream_rx = upstream.subscribe();
    let (tx, receiver) =
        tokio::sync::mpsc::channel(dbsp::join::TRANSIENT_JOIN_INPUT_CHANNEL_CAPACITY);
    let graph_id = graph_id.to_string();
    let input_label = format!("join_input:{input_idx}");
    let task_events = task_events.clone();
    let source_name = source_root.source_name.clone();
    let optimized_nodes = source_root.optimized_nodes.clone();
    let transform = Arc::clone(&source_root.transform);
    let closed_key_transform =
        try_build_transient_join_closed_key_transform(plan, input_idx, closed_key_columns)?;
    let cancel = cancel.clone();
    let debug_transient_join = tracing::enabled!(tracing::Level::DEBUG);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let transformed = match transform(Arc::clone(&batch.deltas)).await {
                        Ok(transformed) => transformed,
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %graph_id,
                                input_idx,
                                source = %batch.source,
                                version = batch.version,
                                error = %err,
                                "stopping transient join input after transform failure"
                            );
                            report_graph_task_error(
                                &task_events,
                                &graph_id,
                                input_label.clone(),
                                err,
                            );
                            break;
                        }
                    };
                        let join_ts = batch.version.saturating_add(1);
                        let closed_keys = match closed_key_transform.as_ref() {
                            Some(transform) => match transform(Arc::clone(&batch.deltas)).await {
                                Ok(closed_keys) => closed_keys,
                                Err(err) => {
                                    tracing::warn!(
                                        graph_id = %graph_id,
                                        input_idx,
                                        source = %batch.source,
                                        version = batch.version,
                                        error = %err,
                                        "stopping transient join input after closed-key transform failure"
                                    );
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        input_label.clone(),
                                        err,
                                    );
                                    break;
                                }
                            },
                            None => Vec::new(),
                        };
                        if debug_transient_join {
                            tracing::debug!(
                                graph_id = %graph_id,
                                input_idx,
                                source = %batch.source,
                                version = batch.version,
                                join_ts,
                                rows = transformed.len(),
                                closed_keys = closed_keys.len(),
                                "transient join input"
                            );
                        }
                        if tx.send(dbsp::join::TransientJoinInputBatch {
                            ts: join_ts,
                            deltas: Arc::new(transformed),
                            closed_keys: Arc::new(closed_keys),
                        }).await.is_err() {
                        tracing::debug!(
                            graph_id = %graph_id,
                            input_idx,
                            source = %batch.source,
                            "transient join input receiver closed"
                        );
                        break;
                    }
                }
            }
        }
        tracing::debug!(
            graph_id = %graph_id,
            input_idx,
            source = %source_name,
            optimized_nodes = ?optimized_nodes,
            label = %input_label,
            "transient join input optimization stopped"
        );
    });

    Ok(Some(TransientJoinInputOptimization {
        source_name: source_root.source_name,
        optimized_nodes: source_root.optimized_nodes,
        receiver,
    }))
}

pub(super) fn try_build_transient_join_closed_key_transform(
    plan: &CircuitPlan,
    input_idx: usize,
    closed_key_columns: Option<Arc<Vec<usize>>>,
) -> Result<Option<Arc<ClosedJoinKeyTransformFn>>> {
    let Some(closed_key_columns) = closed_key_columns else {
        return Ok(None);
    };
    let Some(shape) = find_transient_source_root_shape(plan, input_idx)? else {
        return Ok(None);
    };
    let select = match shape {
        TransientSourceRootShape::Select { select, .. }
        | TransientSourceRootShape::FilterMap { select, .. } => select,
        TransientSourceRootShape::Source { .. } | TransientSourceRootShape::Project { .. } => {
            return Ok(None);
        }
    };
    let filter_transform = build_filter_transform(&select)?;
    let key_extractor = Arc::new(
        VectorizedEncodedKeyExtractor::new(
            select.output_schema().to_arrow_schema(),
            Arc::clone(&closed_key_columns),
        )
        .context("build vectorized transient closed-key extractor")?,
    );
    Ok(Some(Arc::new(move |delta_values| {
        let filter_transform = Arc::clone(&filter_transform);
        let key_extractor = Arc::clone(&key_extractor);
        Box::pin(async move {
            let selected = filter_transform(Arc::clone(&delta_values)).await?;
            let mut selected_keys = BTreeSet::new();
            for (key, _row, weight) in key_extractor.extract_keyed_deltas(&selected)? {
                if weight <= 0 {
                    continue;
                }
                selected_keys.insert(key);
            }

            let mut closed = BTreeMap::new();
            for (key, _row, weight) in key_extractor.extract_keyed_deltas(delta_values.as_ref())? {
                if weight <= 0 {
                    continue;
                }
                if selected_keys.contains(&key) {
                    continue;
                }
                *closed.entry(key).or_insert(0_i64) += weight;
            }
            Ok(closed.into_iter().collect())
        })
    })))
}

pub(super) fn try_build_transient_source_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceRootMaterialization>> {
    if let Some(shape) = find_transient_source_root_shape(plan, root_idx)? {
        let source_name = shape.source_name().to_string();
        let optimized_nodes = match &shape {
            TransientSourceRootShape::Source {
                optimized_nodes, ..
            }
            | TransientSourceRootShape::Select {
                optimized_nodes, ..
            }
            | TransientSourceRootShape::Project {
                optimized_nodes, ..
            }
            | TransientSourceRootShape::FilterMap {
                optimized_nodes, ..
            } => optimized_nodes.clone(),
        };
        let transform = match match shape {
            TransientSourceRootShape::Source { .. } => Ok(identity_delta_transform()),
            TransientSourceRootShape::Select { select, .. } => build_filter_transform(&select),
            TransientSourceRootShape::Project { project, .. } => build_map_transform(&project),
            TransientSourceRootShape::FilterMap {
                select, project, ..
            } => build_filter_map_transform(&select, &project),
        } {
            Ok(transform) => transform,
            Err(err) => {
                tracing::debug!(
                    root_idx,
                    error = %err,
                    "transient source root materialization declined"
                );
                return Ok(None);
            }
        };
        return Ok(Some(TransientSourceRootMaterialization {
            source_name,
            optimized_nodes,
            transform,
        }));
    }

    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) = try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) = try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_filter_transform(select)?,
            );
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
                    try_build_transient_source_root_materialization(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.transform = compose_delta_transforms(
                    Arc::clone(&shape.transform),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) = try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_map_transform(project)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

pub(super) fn try_build_transient_source_topn_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceTopNRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::TopN(topn) => {
            let input_idx = first_input(root, "topn")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceTopNRootShape {
                source_root,
                topn: topn.clone(),
                optimized_nodes,
                transform: None,
                output_projection: None,
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) = try_build_transient_source_topn_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) = try_build_transient_source_topn_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            transient_topn::fold_topn_root_output_projection(&mut shape);
            shape.transform = compose_optional_delta_transform(
                shape.transform.take(),
                build_filter_transform(select)?,
            );
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
                    try_build_transient_source_topn_root_shape(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                transient_topn::fold_topn_root_output_projection(&mut shape);
                shape.transform = compose_optional_delta_transform(
                    shape.transform.take(),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) = try_build_transient_source_topn_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            if let Some(columns) = try_build_direct_row_projection(project) {
                if shape.transform.is_none() {
                    shape.output_projection = Some(compose_direct_row_projection(
                        shape.output_projection.take(),
                        columns,
                    )?);
                } else {
                    shape.transform = compose_optional_delta_transform(
                        shape.transform.take(),
                        transient_topn::build_direct_projection_transform(
                            columns,
                            Arc::clone(project.input_schema()),
                        ),
                    );
                }
            } else {
                transient_topn::fold_topn_root_output_projection(&mut shape);
                shape.transform = compose_optional_delta_transform(
                    shape.transform.take(),
                    build_map_transform(project)?,
                );
            }
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

pub(super) fn try_build_transient_source_aggregate_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceAggregateRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Aggregate(aggregate) => {
            let input_idx = first_input(root, "aggregate")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            if build_incremental_aggregate_slot_kinds(aggregate.aggregates()).is_none() {
                return Ok(None);
            }
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceAggregateRootShape {
                source_root,
                aggregate: aggregate.clone(),
                optimized_nodes,
                transform: identity_delta_transform(),
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) = try_build_transient_source_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) = try_build_transient_source_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_filter_transform(select)?,
            );
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
                    try_build_transient_source_aggregate_root_shape(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.transform = compose_delta_transforms(
                    Arc::clone(&shape.transform),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) = try_build_transient_source_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_map_transform(project)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

pub(super) fn try_build_transient_source_window_count_star_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceWindowCountStarRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::WindowAggregate(window) => {
            if !is_transient_window_count_star_root(window) {
                return Ok(None);
            }
            let input_idx = first_input(root, "window aggregate")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceWindowCountStarRootShape {
                source_root,
                window: window.clone(),
                optimized_nodes,
                transform: None,
                output_projection: None,
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) =
                try_build_transient_source_window_count_star_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) =
                try_build_transient_source_window_count_star_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            fold_window_count_star_output_projection(&mut shape)?;
            shape.transform = compose_optional_delta_transform(
                shape.transform.take(),
                build_filter_transform(select)?,
            );
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
                let Some(mut shape) = try_build_transient_source_window_count_star_root_shape(
                    plan,
                    select_input_idx,
                )?
                else {
                    return Ok(None);
                };
                fold_window_count_star_output_projection(&mut shape)?;
                shape.transform = compose_optional_delta_transform(
                    shape.transform.take(),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) =
                try_build_transient_source_window_count_star_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            if let Some(columns) = try_build_direct_row_projection(project)
                && shape.transform.is_none()
                && shape.output_projection.is_none()
                && try_build_window_count_group_key_count_projection(
                    columns.as_ref(),
                    shape.window.aggregate.group_keys().len(),
                )
                .is_some()
            {
                shape.output_projection =
                    Some(TransientWindowCountOutputProjection::GroupKeyAndCount);
            } else {
                fold_window_count_star_output_projection(&mut shape)?;
                shape.transform = compose_optional_delta_transform(
                    shape.transform.take(),
                    build_map_transform(project)?,
                );
            }
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_build_transient_source_window_count_star_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceWindowCountStarRootMaterialization>> {
    let Some(shape) = try_build_transient_source_window_count_star_root_shape(plan, root_idx)?
    else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = build_transient_window_count_star_receiver(
        graph_id,
        &shape.window,
        upstream,
        Arc::clone(&shape.source_root.transform),
        shape.transform.clone(),
        shape.output_projection,
        watermark,
        cancel,
        task_events,
        state_table,
        "source_window_count_star",
    )
    .await?;
    Ok(Some(TransientSourceWindowCountStarRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
    }))
}

pub(super) fn fold_window_count_star_output_projection(
    shape: &mut TransientSourceWindowCountStarRootShape,
) -> Result<()> {
    if let Some(output_projection) = shape.output_projection.take() {
        let transform = match output_projection {
            TransientWindowCountOutputProjection::GroupKeyAndCount => {
                let input_schema = transient_window_count_full_output_schema(&shape.window)?;
                let aggregate_width = shape.window.aggregate.output_schema().len();
                let columns = Arc::new((2..2 + aggregate_width).collect::<Vec<_>>());
                transient_topn::build_direct_projection_transform(columns, input_schema)
            }
        };
        shape.transform = compose_optional_delta_transform(shape.transform.take(), transform);
    }
    Ok(())
}

pub(super) fn transient_window_count_full_output_schema(
    window: &dbsp::DbspWindowAggregateNode,
) -> Result<Arc<RowSchema>> {
    let mut fields = Vec::with_capacity(window.aggregate.output_schema().len() + 2);
    fields.push(dbsp::Field::new(
        "__floe_window_start",
        DbspScalarType::TimestampMillis,
        false,
    ));
    fields.push(dbsp::Field::new(
        "__floe_window_end",
        DbspScalarType::TimestampMillis,
        false,
    ));
    fields.extend(window.aggregate.output_schema().fields().iter().cloned());
    RowSchema::try_new(fields)
}

pub(super) fn try_build_window_count_group_key_count_projection(
    columns: &[usize],
    group_key_count: usize,
) -> Option<TransientWindowCountOutputProjection> {
    if columns.len() != group_key_count + 1 {
        return None;
    }
    let count_column = group_key_count + 2;
    let expected_group_columns = 2..count_column;
    if columns[..group_key_count]
        .iter()
        .copied()
        .eq(expected_group_columns)
        && columns[group_key_count] == count_column
    {
        Some(TransientWindowCountOutputProjection::GroupKeyAndCount)
    } else {
        None
    }
}

pub(super) fn try_build_transient_source_window_aggregate_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceWindowAggregateRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::WindowAggregate(window) => {
            if !is_transient_window_incremental_root(window) {
                return Ok(None);
            }
            let input_idx = first_input(root, "window aggregate")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceWindowAggregateRootShape {
                source_root,
                window: window.clone(),
                optimized_nodes,
                transform: identity_delta_transform(),
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) =
                try_build_transient_source_window_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) =
                try_build_transient_source_window_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_filter_transform(select)?,
            );
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
                    try_build_transient_source_window_aggregate_root_shape(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.transform = compose_delta_transforms(
                    Arc::clone(&shape.transform),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) =
                try_build_transient_source_window_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_map_transform(project)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}
