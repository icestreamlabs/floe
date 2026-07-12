use super::*;

pub(super) fn manual_retry_status_reason(base: &str, operator_reason: Option<&str>) -> String {
    let Some(operator_reason) = operator_reason
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
    else {
        return base.to_string();
    };
    format!("{base}; operator_reason={operator_reason}")
}

pub(super) struct ReplicationPipelineReconciliationOutcome {
    pub(super) status: String,
    pub(super) drift: Vec<ReplicationPipelineReconciliationDrift>,
    pub(super) next_steps: Vec<String>,
}

pub(super) async fn observe_postgres_table_for_reconciliation(
    connection: &str,
    table: &str,
    options: ReplicationPipelineReconciliationOptions,
    role: &str,
) -> anyhow::Result<ReplicationPipelineReconciliationObservation> {
    let quoted_table = crate::postgres_sql::quote_postgres_qualified_name(table)?;
    let (client, connection_task) = tokio_postgres::connect(connection, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("connect Postgres {role} for CDC reconciliation"))?;
    let task_role = role.to_string();
    let task_table = table.to_string();
    tokio::spawn(async move {
        if let Err(err) = connection_task.await {
            tracing::debug!(
                role = %task_role,
                table = %task_table,
                error = %err,
                "Postgres reconciliation connection closed"
            );
        }
    });

    let observed_at_unix_ms = current_unix_time_ms();
    let row_count = if options.full_scan {
        let sql = format!("SELECT count(*)::bigint FROM {quoted_table}");
        query_postgres_row_count(&client, &sql).await?
    } else {
        let limit = options.max_rows.saturating_add(1);
        let sql = format!(
            "SELECT count(*)::bigint FROM (SELECT 1 FROM {quoted_table} LIMIT {limit}) AS floe_reconcile_count"
        );
        query_postgres_row_count(&client, &sql).await?
    };
    let limit_exceeded = !options.full_scan && row_count > options.max_rows as u64;

    Ok(ReplicationPipelineReconciliationObservation {
        table: table.to_string(),
        row_count: (!limit_exceeded).then_some(row_count),
        row_count_lower_bound: limit_exceeded.then_some(row_count),
        exact: !limit_exceeded,
        observed_at_unix_ms,
    })
}

pub(super) async fn query_postgres_row_count(
    client: &tokio_postgres::Client,
    sql: &str,
) -> anyhow::Result<u64> {
    let row = client
        .query_one(sql, &[])
        .await
        .context("query Postgres reconciliation row count")?;
    let count: i64 = row.get(0);
    u64::try_from(count).context("Postgres reconciliation row count was negative")
}

pub(super) fn reconciliation_outcome(
    source_table: &str,
    target_table: &str,
    source: &ReplicationPipelineReconciliationObservation,
    target: &ReplicationPipelineReconciliationObservation,
    pending_transactions: usize,
    pending_records: usize,
) -> ReplicationPipelineReconciliationOutcome {
    if !source.exact || !target.exact {
        return ReplicationPipelineReconciliationOutcome {
            status: "bounded".to_string(),
            drift: Vec::new(),
            next_steps: vec![
                "The table exceeded max_rows; rerun with a higher max_rows or full_scan=true for an exact count"
                    .to_string(),
            ],
        };
    }
    if pending_transactions > 0 || pending_records > 0 {
        return ReplicationPipelineReconciliationOutcome {
            status: "pending_target_delivery".to_string(),
            drift: Vec::new(),
            next_steps: vec![
                "The replication pipeline still has pending buffered records; retry reconciliation after it catches up"
                    .to_string(),
            ],
        };
    }
    if source.row_count == target.row_count {
        return ReplicationPipelineReconciliationOutcome {
            status: "ok".to_string(),
            drift: Vec::new(),
            next_steps: vec!["Row counts match at the observed pipeline checkpoint".to_string()],
        };
    }

    ReplicationPipelineReconciliationOutcome {
        status: "drift".to_string(),
        drift: vec![ReplicationPipelineReconciliationDrift {
            kind: "row_count_mismatch".to_string(),
            source_table: source_table.to_string(),
            target_table: target_table.to_string(),
            source_count: source.row_count,
            target_count: target.row_count,
            detail: format!(
                "source row count {:?} does not match target row count {:?}",
                source.row_count, target.row_count
            ),
        }],
        next_steps: vec![
            "Inspect the pipeline DLQ and target error state, then retry or discard DLQ entries before rerunning reconciliation"
                .to_string(),
        ],
    }
}

pub(super) fn record_replication_buffer_append(
    perf_enabled: bool,
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    append_elapsed: Duration,
) {
    crate::metrics::record_cdc_buffer_append(
        &plan.name,
        manifest.record_count(),
        manifest.payload_bytes(),
        append_elapsed.as_millis() as u64,
    );
    log_replication_buffer_append_perf(perf_enabled, plan, manifest, append_elapsed);
}
