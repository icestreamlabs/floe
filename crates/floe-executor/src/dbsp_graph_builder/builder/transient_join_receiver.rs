use super::*;

impl LegacyGraphHarness {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn build_transient_join_pipeline_root_receiver(
        &mut self,
        plan: &CircuitPlan,
        root: &TransientJoinPipelineRootMaterialization,
        outer_handle_streams: &HashMap<String, DeltaHandleStream>,
        outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        built: &mut HashMap<usize, DeltaHandleStream>,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
        mv_retention: StreamRetention,
        persistence_policy: &PersistencePolicy,
        state_table: Option<Arc<dyn KeyValueTable>>,
    ) -> Result<TransientMaterializeReceiver> {
        let left = self
            .compile_node(
                plan,
                root.left_input_idx,
                outer_handle_streams,
                cancel,
                task_events,
                built,
                mv_registry,
                mv_latest,
                mv_retention,
                persistence_policy,
            )
            .await?;
        let right = self
            .compile_node(
                plan,
                root.right_input_idx,
                outer_handle_streams,
                cancel,
                task_events,
                built,
                mv_registry,
                mv_latest,
                mv_retention,
                persistence_policy,
            )
            .await?;
        let left_transient_input = try_build_transient_join_input_optimization(
            self.graph_id(),
            plan,
            root.left_input_idx,
            outer_transient_streams,
            None,
            cancel,
            task_events,
        )?
        .ok_or_else(|| {
            anyhow!(
                "missing transient join input for left source-journal input {}",
                root.left_input_idx
            )
        })?;
        let right_transient_input = try_build_transient_join_input_optimization(
            self.graph_id(),
            plan,
            root.right_input_idx,
            outer_transient_streams,
            None,
            cancel,
            task_events,
        )?
        .ok_or_else(|| {
            anyhow!(
                "missing transient join input for right source-journal input {}",
                root.right_input_idx
            )
        })?;

        let (tx, mut receiver) =
            mpsc::channel::<TransientMaterializeBatch>(TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY);
        // Join pipeline outputs are general ZSets under CDC. Matched input rows
        // must remain available for future retractions and replacement joins.
        let left_retention = dbsp::JoinInputRetention::RetainAll;
        let right_retention = dbsp::JoinInputRetention::RetainAll;
        tracing::info!(
            graph_id = %self.graph_id(),
            left_retention = ?left_retention,
            right_retention = ?right_retention,
            "using transient join pipeline state retention"
        );
        self.compile_transient_join_root_materialization(
            &root.join,
            left,
            right,
            Some(left_transient_input.receiver),
            Some(right_transient_input.receiver),
            left_retention,
            right_retention,
            None,
            tx,
            task_events,
            state_table.is_some(),
        )
        .await?;

        let identity_transform = identity_delta_transform();
        let mut current_output_append_only = false;
        for (step_idx, step) in root.steps.iter().enumerate() {
            receiver = match step {
                TransientJoinPipelineStep::Transform(transform) => {
                    build_transient_transform_receiver(
                        self.graph_id(),
                        format!("transient-join-pipeline-transform:{step_idx}"),
                        receiver,
                        Arc::clone(transform),
                        cancel,
                        task_events,
                    )
                }
                TransientJoinPipelineStep::TopN(topn) => {
                    let next = transient_topn::build_transient_topn_receiver_from_batches(
                        self.graph_id(),
                        topn,
                        receiver,
                        current_output_append_only,
                        false,
                        None,
                        cancel,
                        task_events,
                        state_table.clone(),
                        format!("join_pipeline_topn_{step_idx}"),
                    );
                    current_output_append_only = false;
                    next
                }
                TransientJoinPipelineStep::Aggregate(aggregate) => {
                    let next = build_transient_aggregate_receiver_from_batches(
                        self.graph_id(),
                        aggregate,
                        receiver,
                        Arc::clone(&identity_transform),
                        current_output_append_only,
                        false,
                        cancel,
                        task_events,
                        state_table.clone(),
                        format!("join_pipeline_aggregate_{step_idx}"),
                    )
                    .await?;
                    current_output_append_only = false;
                    next
                }
            };
        }

        Ok(receiver)
    }
}
