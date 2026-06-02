use super::direct::{TransientDirectPartitionTopNProcessor, TransientDirectTop1Processor};
use super::planning::{
    build_transient_topn_key_layout, project_encoded_deltas,
    try_build_direct_partition_topn_config, try_build_direct_partitioned_top1_config,
};
use super::processors::{
    TransientAppendOnlyTopNProcessor, TransientTop1Processor, TransientTopNProcessor,
};
use super::*;

pub(in crate::dbsp_graph_builder::builder) fn build_transient_topn_receiver(
    graph_id: &str,
    topn: &DbspTopNNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_projection: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> TransientMaterializeReceiver {
    // Source roots are ZSet inputs, not a proven append-only contract. Keeping
    // full TopN input state is required to recompute replacement winners after
    // retractions; winner-only compact state is only correct for strictly
    // append-only streams.
    let append_only_input = false;
    let compact_append_only_state = false;
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-topn-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_topn_receiver_from_batches(
        graph_id,
        topn,
        upstream_rx,
        append_only_input,
        compact_append_only_state,
        output_projection,
        cancel,
        task_events,
        state_table,
        state_label,
    )
}

pub(in crate::dbsp_graph_builder::builder) fn build_transient_topn_receiver_from_batches(
    graph_id: &str,
    topn: &DbspTopNNode,
    mut upstream_rx: TransientMaterializeReceiver,
    append_only_input: bool,
    compact_append_only_state: bool,
    output_projection: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> TransientMaterializeReceiver {
    let (tx, rx) =
        mpsc::channel::<TransientMaterializeBatch>(TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY);
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-topn:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let debug_transient_join = tracing::enabled!(tracing::Level::DEBUG);
    let topn_output_schema = topn.output_schema().to_arrow_schema();
    if let Some(config) = try_build_direct_partitioned_top1_config(topn) {
        let mut processor = TransientDirectTop1Processor::new(
            graph_id.clone(),
            topn,
            config,
            compact_append_only_state,
        );
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            tracing::debug!(
                                graph_id = %graph_id,
                                version = batch.version,
                                rows = output_deltas.len(),
                                "transient topn output"
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_partitioned_top1 =
        topn.limit() == 1 && topn.offset() == 0 && !topn.partition_by().is_empty();
    let key_layout = match build_transient_topn_key_layout(topn) {
        Ok(layout) => layout,
        Err(err) => {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return rx;
        }
    };

    if use_partitioned_top1 {
        let mut processor = TransientTop1Processor::new(graph_id.clone(), topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state
                            && let Err(err) = persistent_state.apply_deltas(&input_deltas).await
                        {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state
                            && let Err(err) = persistent_state.apply_deltas(&output_deltas).await
                        {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            tracing::debug!(
                                graph_id = %graph_id,
                                version = batch.version,
                                rows = output_deltas.len(),
                                "transient topn output"
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    if let Some(config) = try_build_direct_partition_topn_config(topn) {
        let mut processor =
            TransientDirectPartitionTopNProcessor::new(graph_id.clone(), config, topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            tracing::debug!(
                                graph_id = %graph_id,
                                version = batch.version,
                                rows = output_deltas.len(),
                                "transient topn output"
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_append_only_partitioned_topn = append_only_input
        && topn.offset() == 0
        && topn.limit() > 1
        && !topn.partition_by().is_empty();

    if use_append_only_partitioned_topn {
        let mut processor =
            TransientAppendOnlyTopNProcessor::new(graph_id.clone(), topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            tracing::debug!(
                                graph_id = %graph_id,
                                version = batch.version,
                                rows = output_deltas.len(),
                                "transient topn output"
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let mut processor =
        TransientTopNProcessor::new(graph_id.clone(), topn, &key_layout, append_only_input);
    let precompute_evaluator = key_layout.precompute_evaluator.clone();
    let output_projection = output_projection.clone();
    let output_projection_schema = Arc::clone(&topn_output_schema);
    let state_table = state_table.clone();
    let state_label = state_label.clone();

    tokio::spawn(async move {
        let mut persistent_state =
            match PersistentTransientInputState::load(state_table, &graph_id, &state_label).await {
                Ok(state) => state,
                Err(err) => {
                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                    return;
                }
            };
        if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return;
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let input_deltas = batch.deltas.as_ref().clone();
                    let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                        match evaluator
                            .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                            .await
                        {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    } else {
                        input_deltas
                    };
                    if !compact_append_only_state
                        && let Err(err) = persistent_state.apply_deltas(&input_deltas).await
                    {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        break;
                    }
                    let output_deltas = match processor.apply_deltas(input_deltas) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if compact_append_only_state
                        && let Err(err) = persistent_state.apply_deltas(&output_deltas).await
                    {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        break;
                    }
                    let output_deltas = match output_projection.as_ref() {
                        Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        },
                        None => output_deltas,
                    };
                    if debug_transient_join {
                        tracing::debug!(
                            graph_id = %graph_id,
                            version = batch.version,
                            rows = output_deltas.len(),
                            "transient topn output"
                        );
                    }
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(output_deltas),
                        deltas_consolidated: false,
                    }).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    rx
}
