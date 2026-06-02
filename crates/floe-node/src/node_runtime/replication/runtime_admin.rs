use super::reconciliation::{
    manual_retry_status_reason, observe_postgres_table_for_reconciliation, reconciliation_outcome,
};
use super::*;

impl ReplicationPipelineRuntime {
    pub(in crate::node_runtime) fn new(
        plans: impl IntoIterator<Item = ReplicationPipelineRuntimePlan>,
        settings: FloeReplicationConfig,
    ) -> anyhow::Result<Self> {
        let mut pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>> =
            HashMap::new();
        let mut kafka_writers_by_pipeline = HashMap::new();
        let mut postgres_writers_by_pipeline = HashMap::new();

        for plan in plans {
            match &plan.target {
                ReplicationPipelineRuntimeTarget::Kafka { brokers, topic } => {
                    kafka_writers_by_pipeline.insert(
                        plan.name.clone(),
                        Arc::new(writers::KafkaReplicationPipelineWriter::new(
                            brokers,
                            topic,
                            plan.buffer_mode,
                            settings.kafka.clone(),
                            settings.perf_log,
                        )?),
                    );
                }
                ReplicationPipelineRuntimeTarget::Postgres { connection, table } => {
                    anyhow::ensure!(
                        plan.format == ReplicationPipelineRuntimeFormat::FloeJson,
                        "replication pipeline '{}' uses a Postgres target, which currently requires format = 'floe_json'",
                        plan.name
                    );
                    postgres_writers_by_pipeline.insert(
                        plan.name.clone(),
                        Arc::new(writers::PostgresReplicationPipelineWriter::new(
                            connection,
                            table,
                            plan.schema.clone(),
                        )?),
                    );
                }
            }
            pipelines_by_source
                .entry(CdcSourceId::new(plan.source_name.clone())?)
                .or_default()
                .push(plan);
        }

        Ok(Self {
            pipelines_by_source,
            kafka_writers_by_pipeline,
            postgres_writers_by_pipeline,
            buffer_cleanup_last_by_pipeline: Mutex::new(HashMap::new()),
            replay_state_by_pipeline: Mutex::new(HashMap::new()),
            backpressure_state_by_pipeline: Mutex::new(HashMap::new()),
            last_target_error_by_pipeline: Mutex::new(HashMap::new()),
            settings,
        })
    }

    pub(in crate::node_runtime) fn has_pipelines_for_source(
        &self,
        source_id: &CdcSourceId,
    ) -> bool {
        self.pipelines_by_source
            .get(source_id)
            .is_some_and(|plans| !plans.is_empty())
    }

    pub(crate) async fn reconcile_pipeline(
        &self,
        storage: &SlateCatalog,
        pipeline_name: &str,
        options: ReplicationPipelineReconciliationOptions,
    ) -> anyhow::Result<Option<ReplicationPipelineReconciliationReport>> {
        anyhow::ensure!(
            options.max_rows > 0,
            "CDC reconciliation max_rows must be greater than zero"
        );
        let Some(plan) = self.plan_by_name(pipeline_name) else {
            return Ok(None);
        };
        let observed_at_unix_ms = current_unix_time_ms();
        let buffer_stats = storage
            .cdc_buffer_store()
            .stats(&plan.name, observed_at_unix_ms)
            .await
            .with_context(|| format!("load CDC buffer stats for pipeline '{}'", plan.name))?;
        let checkpoint = storage
            .replication_pipeline_checkpoint(&plan.name)
            .await
            .with_context(|| {
                format!(
                    "load replication pipeline '{}' checkpoint for reconciliation",
                    plan.name
                )
            })?;
        let checkpoint_position = checkpoint
            .as_ref()
            .map(|checkpoint| encoding::source_position_key(checkpoint.source_position()));
        let checkpoint_lsn_bytes = checkpoint
            .as_ref()
            .and_then(|checkpoint| postgres_position_lsn_bytes(checkpoint.source_position()));
        let target_kind = target_kind(plan).to_string();
        let mut report = ReplicationPipelineReconciliationReport {
            pipeline: plan.name.clone(),
            source: plan.source_name.clone(),
            upstream_table: plan.upstream_table.clone(),
            target_kind,
            target_table: None,
            checkpoint_position,
            checkpoint_lsn_bytes,
            pending_transactions: buffer_stats.pending_transactions(),
            pending_records: buffer_stats.pending_records(),
            max_rows: options.max_rows,
            full_scan: options.full_scan,
            status: "unsupported_target".to_string(),
            source_observation: None,
            target_observation: None,
            drift: Vec::new(),
            next_steps: Vec::new(),
            observed_at_unix_ms,
        };

        let ReplicationPipelineRuntimeTarget::Postgres {
            connection: target_connection,
            table: target_table,
        } = &plan.target
        else {
            report.next_steps.push(
                "Row-count reconciliation is currently available for Postgres replication targets"
                    .to_string(),
            );
            return Ok(Some(report));
        };
        report.target_table = Some(target_table.clone());

        let source_observation = observe_postgres_table_for_reconciliation(
            &plan.source_connection,
            &plan.upstream_table,
            options,
            "source",
        )
        .await?;
        let target_observation = observe_postgres_table_for_reconciliation(
            target_connection,
            target_table,
            options,
            "target",
        )
        .await?;
        let outcome = reconciliation_outcome(
            &plan.upstream_table,
            target_table,
            &source_observation,
            &target_observation,
            buffer_stats.pending_transactions(),
            buffer_stats.pending_records(),
        );
        report.status = outcome.status;
        report.drift = outcome.drift;
        report.next_steps = outcome.next_steps;
        report.source_observation = Some(source_observation);
        report.target_observation = Some(target_observation);
        Ok(Some(report))
    }

    pub(crate) async fn retry_dlq_entry_with_reason(
        &self,
        storage: &SlateCatalog,
        pipeline_name: &str,
        dlq_id: &str,
        operator_reason: Option<String>,
    ) -> anyhow::Result<Option<ReplicationPipelineDlqEntry>> {
        let Some(plan) = self.plan_by_name(pipeline_name) else {
            return Ok(None);
        };
        let Some(entry) = storage
            .replication_pipeline_dlq_entry(pipeline_name, dlq_id)
            .await?
        else {
            return Ok(None);
        };
        self.retry_loaded_dlq_entry(storage, plan, entry, operator_reason.as_deref())
            .await
            .map(Some)
    }

    pub(crate) async fn retry_pending_dlq_entries_with_reason(
        &self,
        storage: &SlateCatalog,
        pipeline_name: &str,
        limit: usize,
        operator_reason: Option<String>,
    ) -> anyhow::Result<Option<ReplicationPipelineDlqRetryBatchOutcome>> {
        anyhow::ensure!(limit > 0, "DLQ retry limit must be greater than zero");
        let Some(plan) = self.plan_by_name(pipeline_name) else {
            return Ok(None);
        };
        let mut entries = storage
            .replication_pipeline_dlq_entries(pipeline_name)
            .await?;
        entries.retain(|entry| entry.status() == ReplicationPipelineDlqStatus::Pending);
        entries.sort_by(|left, right| {
            left.created_at_unix_ms()
                .cmp(&right.created_at_unix_ms())
                .then_with(|| left.dlq_id().cmp(right.dlq_id()))
        });
        entries.truncate(limit);

        let mut outcome = ReplicationPipelineDlqRetryBatchOutcome {
            pipeline: pipeline_name.to_string(),
            requested_limit: limit,
            attempted: 0,
            replayed: Vec::new(),
            failed: Vec::new(),
        };
        for entry in entries {
            outcome.attempted = outcome.attempted.saturating_add(1);
            let dlq_id = entry.dlq_id().to_string();
            match self
                .retry_loaded_dlq_entry(storage, plan, entry, operator_reason.as_deref())
                .await
            {
                Ok(entry) => outcome.replayed.push(entry),
                Err(err) => {
                    let error = err.to_string();
                    let entry = storage
                        .replication_pipeline_dlq_entry(pipeline_name, &dlq_id)
                        .await?;
                    outcome.failed.push(ReplicationPipelineDlqRetryFailure {
                        dlq_id,
                        error,
                        entry,
                    });
                }
            }
        }
        Ok(Some(outcome))
    }

    async fn retry_loaded_dlq_entry(
        &self,
        storage: &SlateCatalog,
        plan: &ReplicationPipelineRuntimePlan,
        entry: ReplicationPipelineDlqEntry,
        operator_reason: Option<&str>,
    ) -> anyhow::Result<ReplicationPipelineDlqEntry> {
        let pipeline_name = entry.pipeline_name().to_string();
        let dlq_id = entry.dlq_id().to_string();
        anyhow::ensure!(
            entry.status() == ReplicationPipelineDlqStatus::Pending,
            "replication pipeline '{}' DLQ entry {} is {}, not pending",
            pipeline_name,
            dlq_id,
            entry.status().as_str()
        );
        let payload_object_key = entry.payload_object_key().ok_or_else(|| {
            anyhow!(
                "replication pipeline '{}' DLQ entry {} has no payload object key",
                pipeline_name,
                dlq_id
            )
        })?;
        let payload = storage
            .replication_pipeline_dlq_payload(payload_object_key)
            .await
            .with_context(|| {
                format!("load replication pipeline '{pipeline_name}' DLQ payload {dlq_id}")
            })?;
        let records = decode_cdc_buffer_records_payload(&payload).with_context(|| {
            format!("decode replication pipeline '{pipeline_name}' DLQ payload {dlq_id}")
        })?;
        let attempted_entry = storage
            .record_replication_pipeline_dlq_retry_attempt(
                &pipeline_name,
                &dlq_id,
                current_unix_time_ms(),
            )
            .await?
            .unwrap_or(entry);
        match self.send_records_to_target(plan, &records).await {
            Ok(_) => {
                crate::metrics::inc_cdc_replication_dlq_replay(&pipeline_name, "success");
                self.clear_last_target_error(&plan.name);
                let replayed = storage
                    .update_replication_pipeline_dlq_entry_status_with_reason(
                        &pipeline_name,
                        &dlq_id,
                        ReplicationPipelineDlqStatus::Replayed,
                        Some(manual_retry_status_reason(
                            "manual retry delivered to target",
                            operator_reason,
                        )),
                        current_unix_time_ms(),
                    )
                    .await?;
                Ok(replayed.unwrap_or(attempted_entry))
            }
            Err(err) => {
                crate::metrics::inc_cdc_replication_dlq_replay(&pipeline_name, "failure");
                self.record_target_write_failure(plan, &err);
                storage
                    .update_replication_pipeline_dlq_entry_status_with_reason(
                        &pipeline_name,
                        &dlq_id,
                        ReplicationPipelineDlqStatus::Pending,
                        Some(manual_retry_status_reason(
                            &format!("manual retry failed: {err:#}"),
                            operator_reason,
                        )),
                        current_unix_time_ms(),
                    )
                    .await?;
                Err(err).with_context(|| {
                    format!("retry replication pipeline '{pipeline_name}' DLQ entry {dlq_id}")
                })
            }
        }
    }

    fn plan_by_name(&self, pipeline_name: &str) -> Option<&ReplicationPipelineRuntimePlan> {
        self.pipelines_by_source
            .values()
            .flatten()
            .find(|plan| plan.name == pipeline_name)
    }
}
