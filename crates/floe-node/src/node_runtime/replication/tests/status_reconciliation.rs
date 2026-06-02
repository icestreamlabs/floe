use super::*;

#[tokio::test]
async fn target_checkpoint_state_makes_partial_delivery_explicit() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let records = vec![CdcBufferRecord::new(Some(vec![1]), Some(vec![2]))];
    let prepared = prepare_replication_buffer_append(&plan, &transaction, records).unwrap();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .unwrap();

    let pending = pending_target_state(&plan, &manifest);
    assert_eq!(pending["buffer.status"], "durable");
    assert_eq!(pending["target.delivery.status"], "pending");
    assert_eq!(pending["target.delivery.replay_may_duplicate"], "true");
    assert_eq!(pending["target.kind"], "kafka");
    assert_eq!(pending["source.position.postgres.commit_lsn"], "0/16B6C50");

    let delivered = delivered_target_state(
        &plan,
        &manifest,
        std::collections::BTreeMap::from([
            ("kafka.topic".to_string(), "orders".to_string()),
            ("kafka.partition.0.offset".to_string(), "42".to_string()),
        ]),
    );
    assert_eq!(delivered["buffer.status"], "delivered");
    assert_eq!(delivered["target.delivery.status"], "delivered");
    assert_eq!(delivered["target.delivery.replay_may_duplicate"], "false");
    assert_eq!(delivered["kafka.partition.0.offset"], "42");

    let failed = failed_target_state(&plan, &manifest, &anyhow!("kafka unavailable"));
    assert_eq!(failed["buffer.status"], "durable");
    assert_eq!(failed["target.delivery.status"], "failed");
    assert_eq!(failed["target.delivery.replay_may_duplicate"], "true");
    assert_eq!(failed["target.failure.class"], "retryable");
    assert!(failed["target.last_error"].contains("kafka unavailable"));

    let sensitive = failed_target_state(
        &plan,
        &manifest,
        &anyhow!("connect postgres://floe:secret@localhost/floe failed with password = topsecret"),
    );
    assert!(sensitive["target.last_error"].contains("postgres://[redacted]@localhost/floe"));
    assert!(sensitive["target.last_error"].contains("password = [redacted]"));
    assert!(!sensitive["target.last_error"].contains("secret"));
    assert!(!sensitive["target.last_error"].contains("topsecret"));
}

#[test]
fn reconciliation_outcome_reports_success_and_drift() {
    let source = ReplicationPipelineReconciliationObservation {
        table: "public.orders".to_string(),
        row_count: Some(3),
        row_count_lower_bound: None,
        exact: true,
        observed_at_unix_ms: 10,
    };
    let matching_target = ReplicationPipelineReconciliationObservation {
        table: "public.orders_copy".to_string(),
        row_count: Some(3),
        row_count_lower_bound: None,
        exact: true,
        observed_at_unix_ms: 11,
    };
    let ok = reconciliation_outcome(
        "public.orders",
        "public.orders_copy",
        &source,
        &matching_target,
        0,
        0,
    );
    assert_eq!(ok.status, "ok");
    assert!(ok.drift.is_empty());

    let drift_target = ReplicationPipelineReconciliationObservation {
        row_count: Some(2),
        ..matching_target
    };
    let drift = reconciliation_outcome(
        "public.orders",
        "public.orders_copy",
        &source,
        &drift_target,
        0,
        0,
    );
    assert_eq!(drift.status, "drift");
    assert_eq!(
        drift.drift,
        vec![ReplicationPipelineReconciliationDrift {
            kind: "row_count_mismatch".to_string(),
            source_table: "public.orders".to_string(),
            target_table: "public.orders_copy".to_string(),
            source_count: Some(3),
            target_count: Some(2),
            detail: "source row count Some(3) does not match target row count Some(2)".to_string(),
        }]
    );
}

#[test]
fn reconciliation_outcome_reports_bounded_and_pending_states() {
    let bounded_source = ReplicationPipelineReconciliationObservation {
        table: "public.orders".to_string(),
        row_count: None,
        row_count_lower_bound: Some(101),
        exact: false,
        observed_at_unix_ms: 10,
    };
    let target = ReplicationPipelineReconciliationObservation {
        table: "public.orders_copy".to_string(),
        row_count: Some(100),
        row_count_lower_bound: None,
        exact: true,
        observed_at_unix_ms: 11,
    };
    let bounded = reconciliation_outcome(
        "public.orders",
        "public.orders_copy",
        &bounded_source,
        &target,
        0,
        0,
    );
    assert_eq!(bounded.status, "bounded");
    assert!(bounded.drift.is_empty());

    let source = ReplicationPipelineReconciliationObservation {
        row_count: Some(100),
        row_count_lower_bound: None,
        exact: true,
        ..bounded_source
    };
    let pending = reconciliation_outcome(
        "public.orders",
        "public.orders_copy",
        &source,
        &target,
        1,
        7,
    );
    assert_eq!(pending.status, "pending_target_delivery");
    assert!(pending.drift.is_empty());
}

#[test]
fn target_write_failure_classifies_transient_and_permanent_errors() {
    let table_id = CdcTableId::new("orders").unwrap();
    let kafka_plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    assert_eq!(
        classify_target_write_failure(&kafka_plan, &anyhow!("kafka broker unavailable")),
        TargetFailureClass::Retryable
    );
    assert_eq!(
        classify_target_write_failure(
            &kafka_plan,
            &anyhow!("replication pipeline 'orders_pipe' has no Kafka writer")
        ),
        TargetFailureClass::Permanent
    );

    let mut postgres_plan = test_plan("orders_pg_pipe", table_id, "public.orders");
    postgres_plan.target = ReplicationPipelineRuntimeTarget::Postgres {
        connection: "host=localhost user=floe".to_string(),
        table: "public.orders".to_string(),
    };
    assert_eq!(
        classify_target_write_failure(
            &postgres_plan,
            &anyhow!("connect replication pipeline Postgres target: connection refused")
        ),
        TargetFailureClass::Retryable
    );
    assert_eq!(
        classify_target_write_failure(
            &postgres_plan,
            &anyhow!(
                "upsert CDC row into replication pipeline Postgres target public.orders: permission denied for table orders"
            )
        ),
        TargetFailureClass::Permanent
    );
}

#[tokio::test]
async fn status_snapshots_expose_buffer_checkpoint_replay_and_error_state() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    runtime.set_replay_state(&plan.name, true);
    runtime.set_last_target_error(&plan.name, "kafka unavailable".to_string());
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let prepared = prepare_replication_buffer_append(
        &plan,
        &transaction,
        vec![CdcBufferRecord::new(Some(vec![1]), Some(vec![2]))],
    )
    .unwrap();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .unwrap();
    storage
        .put_replication_pipeline_checkpoint(
            ReplicationPipelineCheckpoint::new(
                &plan.name,
                &plan.source_name,
                manifest.source_position().clone(),
                manifest.transaction_id().cloned(),
                pending_target_state(&plan, &manifest),
                current_unix_time_ms(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    persist_test_dlq_entry(
        &storage,
        &plan,
        "snapshot-entry-1",
        "0/16B6C40",
        "pg-xid-76",
        current_unix_time_ms().saturating_sub(50),
    )
    .await
    .expect("persist pending DLQ entry");

    let snapshots = runtime.status_snapshots(&storage).await.unwrap();
    let snapshot = snapshots.first().expect("snapshot");

    assert_eq!(snapshot.pipeline_name(), "orders_pipe");
    assert_eq!(snapshot.source_name(), "pg_main");
    assert_eq!(snapshot.target_kind(), "kafka");
    assert_eq!(snapshot.pending_transactions(), 1);
    assert_eq!(snapshot.pending_objects(), 1);
    assert_eq!(snapshot.pending_records(), manifest.record_count());
    assert!(snapshot.pending_bytes() > 0);
    assert!(snapshot.oldest_pending_age_ms().is_some());
    assert_eq!(snapshot.dlq_pending_entries(), 1);
    assert_eq!(snapshot.dlq_replayed_entries(), 0);
    assert_eq!(snapshot.dlq_discarded_entries(), 0);
    assert!(snapshot.oldest_dlq_pending_age_ms().is_some());
    assert_eq!(snapshot.missing_payload_objects(), 0);
    assert_eq!(snapshot.orphan_payload_objects(), 0);
    assert_eq!(snapshot.orphan_payload_bytes(), 0);
    assert!(snapshot.replaying());
    assert_eq!(snapshot.last_error(), Some("kafka unavailable"));
    assert_eq!(
        snapshot.checkpoint_position(),
        Some(manifest.source_position())
    );
    let checkpoint_lsn_bytes = PostgresLsn::parse("0/16B6C50").unwrap().as_u64();
    assert_eq!(snapshot.checkpoint_lsn_bytes(), Some(checkpoint_lsn_bytes));
    assert_eq!(
        snapshot
            .checkpoint_transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-77")
    );
    assert_eq!(snapshot.target_state()["target.delivery.status"], "pending");

    let debug_state = Arc::new(tokio::sync::RwLock::new(
        http_ingest::CdcReplicationDebugState::default(),
    ));
    {
        let mut state = debug_state.write().await;
        state
            .postgres_sources
            .push(http_ingest::PostgresCdcDebugSourceState {
                source: "pg_main".to_string(),
                slot: Some("slot_main".to_string()),
                upstream_lsn: Some(PostgresLsn::from_u64(checkpoint_lsn_bytes + 48).to_pg_string()),
                upstream_lsn_bytes: Some(checkpoint_lsn_bytes + 48),
                durable_lsn: Some(PostgresLsn::from_u64(checkpoint_lsn_bytes).to_pg_string()),
                durable_lsn_bytes: Some(checkpoint_lsn_bytes),
                source_lag_bytes: Some(48),
                ..http_ingest::PostgresCdcDebugSourceState::default()
            });
    }
    runtime
        .refresh_debug_state(&storage, &debug_state)
        .await
        .unwrap();
    let debug_state = debug_state.read().await;
    let debug_pipeline = debug_state.pipelines.first().expect("debug pipeline");
    assert_eq!(debug_state.refresh_error, None);
    assert_eq!(debug_pipeline.pipeline, "orders_pipe");
    assert_eq!(debug_pipeline.source, "pg_main");
    assert_eq!(debug_pipeline.target_kind, "kafka");
    assert_eq!(
        debug_pipeline.checkpoint_position.as_deref(),
        Some("pg/0/16B6C50")
    );
    assert_eq!(
        debug_pipeline.checkpoint_lsn_bytes,
        Some(checkpoint_lsn_bytes)
    );
    assert_eq!(debug_pipeline.checkpoint_lag_bytes, Some(48));
    assert_eq!(
        debug_pipeline.checkpoint_transaction_id.as_deref(),
        Some("pg-xid-77")
    );
    assert_eq!(debug_pipeline.pending_transactions, 1);
    assert_eq!(debug_pipeline.pending_objects, 1);
    assert_eq!(debug_pipeline.pending_records, manifest.record_count());
    assert!(debug_pipeline.pending_bytes > 0);
    assert!(debug_pipeline.oldest_pending_age_ms.is_some());
    assert_eq!(debug_pipeline.dlq_pending_entries, 1);
    assert_eq!(debug_pipeline.dlq_replayed_entries, 0);
    assert_eq!(debug_pipeline.dlq_discarded_entries, 0);
    assert!(debug_pipeline.oldest_dlq_pending_age_ms.is_some());
    assert_eq!(debug_pipeline.missing_payload_objects, 0);
    assert_eq!(debug_pipeline.orphan_payload_objects, 0);
    assert_eq!(debug_pipeline.orphan_payload_bytes, 0);
    assert!(debug_pipeline.replaying);
    assert_eq!(
        debug_pipeline.last_error.as_deref(),
        Some("kafka unavailable")
    );
    assert_eq!(
        debug_pipeline.target_state["target.delivery.status"],
        "pending"
    );
}

#[tokio::test]
async fn status_snapshots_track_target_outage_replay_and_recovery() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-88").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "pending"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let prepared = prepare_replication_buffer_append(
        &plan,
        &transaction,
        vec![CdcBufferRecord::new(Some(vec![2]), Some(vec![4]))],
    )
    .unwrap();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .unwrap();

    runtime
        .mark_manifest_delivery_failed(&plan, &storage, &manifest, anyhow!("kafka outage"))
        .await
        .unwrap();
    let failed = runtime.status_snapshots(&storage).await.unwrap();
    let failed = failed.first().expect("failed snapshot");
    assert_eq!(failed.pending_transactions(), 1);
    assert_eq!(failed.pending_records(), manifest.record_count());
    assert_eq!(failed.last_error(), Some("kafka outage"));
    assert!(!failed.replaying());
    assert_eq!(failed.target_state()["target.delivery.status"], "failed");
    assert_eq!(
        failed.target_state()["target.delivery.replay_may_duplicate"],
        "true"
    );

    runtime.set_replay_state(&plan.name, true);
    let replaying = runtime.status_snapshots(&storage).await.unwrap();
    let replaying = replaying.first().expect("replaying snapshot");
    assert!(replaying.replaying());
    assert_eq!(replaying.last_error(), Some("kafka outage"));
    runtime.set_source_backpressure_state(&plan.name, true);
    let backpressured = runtime.status_snapshots(&storage).await.unwrap();
    let backpressured = backpressured.first().expect("backpressured snapshot");
    assert!(backpressured.source_backpressure_active());
    runtime.set_source_backpressure_state(&plan.name, false);

    runtime
        .mark_manifest_delivered(
            &plan,
            &buffer_store,
            &storage,
            &manifest,
            std::collections::BTreeMap::from([
                ("kafka.topic".to_string(), "orders".to_string()),
                ("kafka.partition.0.offset".to_string(), "99".to_string()),
            ]),
        )
        .await
        .unwrap();
    runtime.set_replay_state(&plan.name, false);

    let recovered = runtime.status_snapshots(&storage).await.unwrap();
    let recovered = recovered.first().expect("recovered snapshot");
    assert_eq!(recovered.pending_transactions(), 0);
    assert_eq!(recovered.pending_records(), 0);
    assert_eq!(recovered.pending_bytes(), 0);
    assert_eq!(recovered.oldest_pending_age_ms(), None);
    assert!(!recovered.replaying());
    assert_eq!(recovered.last_error(), None);
    assert_eq!(
        recovered.checkpoint_position(),
        Some(manifest.source_position())
    );
    assert_eq!(
        recovered
            .checkpoint_transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-88")
    );
    assert_eq!(
        recovered.target_state()["target.delivery.status"],
        "delivered"
    );
    assert_eq!(
        recovered.target_state()["target.delivery.replay_may_duplicate"],
        "false"
    );
    assert_eq!(recovered.target_state()["kafka.partition.0.offset"], "99");

    let debug_state = Arc::new(tokio::sync::RwLock::new(
        http_ingest::CdcReplicationDebugState::default(),
    ));
    runtime
        .refresh_debug_state(&storage, &debug_state)
        .await
        .unwrap();
    let debug_state = debug_state.read().await;
    let pipeline = debug_state.pipelines.first().expect("debug pipeline");
    assert_eq!(pipeline.pending_transactions, 0);
    assert_eq!(pipeline.pending_objects, 0);
    assert!(!pipeline.replaying);
    assert!(!pipeline.source_backpressure_active);
    assert_eq!(pipeline.last_error, None);
    assert_eq!(pipeline.target_state["target.delivery.status"], "delivered");
}
