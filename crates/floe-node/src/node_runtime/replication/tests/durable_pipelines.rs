use super::*;

#[tokio::test]
async fn durable_pipeline_buffers_source_progress_when_target_is_down() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    let cancel = CancellationToken::new();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let schemas = HashMap::from([(plan.table_id.clone(), plan.schema.clone())]);
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let first = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-101").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id.clone(),
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let second = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-102").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "paid"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &first, Some(&storage), &cancel)
            .await
            .expect("buffer first transaction"),
        1
    );
    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &second, Some(&storage), &cancel)
            .await
            .expect("buffer second transaction"),
        1
    );

    let buffer_store = storage.cdc_buffer_store();
    let pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending transactions");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].source_position(), first.commit_position());
    assert_eq!(pending[1].source_position(), second.commit_position());

    let source_frontier = buffer_store
        .source_frontier(&plan.name)
        .await
        .expect("source frontier")
        .expect("source frontier");
    assert_eq!(source_frontier.source_position(), second.commit_position());
    assert_eq!(
        source_frontier
            .transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-102")
    );
    assert_eq!(
        buffer_store
            .delivery_frontier(&plan.name)
            .await
            .expect("delivery frontier"),
        None
    );

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), first.commit_position());
    assert_eq!(
        checkpoint.target_state()["target.delivery.status"],
        "failed"
    );
    assert_eq!(
        checkpoint.target_state()["target.delivery.replay_may_duplicate"],
        "true"
    );

    let restarted = test_runtime_with_plan(plan.clone());
    assert_eq!(
        restarted
            .replay_buffered(&storage, &cancel)
            .await
            .expect("replay buffered transactions"),
        0
    );
    let still_pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending after restart replay");
    assert_eq!(still_pending.len(), 2);
}

#[tokio::test]
async fn distinct_pipeline_failure_does_not_skip_unrelated_durable_pipeline() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut fail_fast_plan = test_plan("orders_fail_fast", table_id.clone(), "public.orders");
    fail_fast_plan.buffer_mode = ReplicationPipelineRuntimeBufferMode::NoBuffer;
    fail_fast_plan.error_policy =
        CatalogReplicationErrorPolicy::new(CatalogReplicationErrorPolicyMode::FailFast, None);
    fail_fast_plan.target = ReplicationPipelineRuntimeTarget::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "orders_fail_fast".to_string(),
    };

    let mut durable_plan = test_plan("orders_retry", table_id.clone(), "public.orders");
    durable_plan.target = ReplicationPipelineRuntimeTarget::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "orders_retry".to_string(),
    };

    let runtime = test_runtime_with_plans(vec![fail_fast_plan.clone(), durable_plan.clone()]);
    let cancel = CancellationToken::new();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let schemas = HashMap::from([(table_id.clone(), durable_plan.schema.clone())]);
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let transaction = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-700").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B7000", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(7, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let error = runtime
        .run_transaction(&source_id, &schemas, &transaction, Some(&storage), &cancel)
        .await
        .expect_err("fail-fast target should make the source transaction fail");
    assert!(error.to_string().contains("has no Kafka writer"));

    let buffer_store = storage.cdc_buffer_store();
    let durable_pending = buffer_store
        .pending_transactions(&durable_plan.name, 10)
        .await
        .expect("durable pipeline pending transactions");
    assert_eq!(durable_pending.len(), 1);
    assert_eq!(
        durable_pending[0].source_position(),
        transaction.commit_position()
    );
    assert_eq!(
        durable_pending[0]
            .transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-700")
    );

    let fail_fast_pending = buffer_store
        .pending_transactions(&fail_fast_plan.name, 10)
        .await
        .expect("fail-fast pipeline pending transactions");
    assert!(fail_fast_pending.is_empty());
    assert_eq!(
        storage
            .replication_pipeline_checkpoint(&fail_fast_plan.name)
            .await
            .expect("fail-fast checkpoint"),
        None
    );

    let durable_checkpoint = storage
        .replication_pipeline_checkpoint(&durable_plan.name)
        .await
        .expect("durable checkpoint")
        .expect("durable checkpoint");
    assert_eq!(
        durable_checkpoint.source_position(),
        transaction.commit_position()
    );
    assert_eq!(
        durable_checkpoint.target_state()["target.delivery.status"],
        "failed"
    );
    assert_eq!(
        durable_checkpoint.target_state()["target.delivery.replay_may_duplicate"],
        "true"
    );
}

#[tokio::test]
async fn durable_pipeline_dead_letters_and_advances_when_policy_allows() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.error_policy = CatalogReplicationErrorPolicy::new(
        CatalogReplicationErrorPolicyMode::DeadLetterAndContinue,
        None,
    );
    let runtime = test_runtime_with_plan(plan.clone());
    let cancel = CancellationToken::new();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let schemas = HashMap::from([(plan.table_id.clone(), plan.schema.clone())]);
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let transaction = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-301").unwrap()),
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

    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &transaction, Some(&storage), &cancel)
            .await
            .expect("dead-letter transaction"),
        1
    );

    let buffer_store = storage.cdc_buffer_store();
    let pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending transactions");
    assert!(pending.is_empty());
    let dlq_entries = storage
        .replication_pipeline_dlq_entries(&plan.name)
        .await
        .expect("dlq entries");
    assert_eq!(dlq_entries.len(), 1);
    let dlq_entry = &dlq_entries[0];
    assert_eq!(dlq_entry.source_position(), transaction.commit_position());
    assert_eq!(dlq_entry.error_class(), "kafka_delivery");
    assert_eq!(dlq_entry.payload_format(), Some("kafka_records"));
    let payload = storage
        .replication_pipeline_dlq_payload(dlq_entry.payload_object_key().unwrap())
        .await
        .expect("dlq payload");
    let records =
        floe_storage::decode_cdc_buffer_records_payload(&payload).expect("decode payload");
    assert_eq!(records.len(), 1);

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), transaction.commit_position());
    assert_eq!(
        checkpoint.target_state()["target.delivery.status"],
        "dead_lettered"
    );
    assert_eq!(
        checkpoint.target_state()["target.delivery.replay_may_duplicate"],
        "false"
    );
    assert_eq!(
        checkpoint.target_state()["target.failure.class"],
        "permanent"
    );
    assert_eq!(
        checkpoint.target_state()["target.dlq.id"],
        dlq_entry.dlq_id()
    );
    assert_eq!(
        dlq_entry.target_state()["target.failure.class"],
        "permanent"
    );
    assert!(checkpoint.target_state()["target.last_error"].contains("has no Kafka writer"));
}

#[tokio::test]
async fn replay_dead_letters_pending_buffer_when_policy_allows() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.error_policy = CatalogReplicationErrorPolicy::new(
        CatalogReplicationErrorPolicyMode::DeadLetterAndContinue,
        None,
    );
    let runtime = test_runtime_with_plan(plan.clone());
    let cancel = CancellationToken::new();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-302").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "paid"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let prepared = prepare_replication_buffer_append(
        &plan,
        &transaction,
        vec![CdcBufferRecord::new(Some(vec![2]), Some(vec![3]))],
    )
    .unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .expect("append pending transaction");

    assert_eq!(
        runtime
            .replay_buffered(&storage, &cancel)
            .await
            .expect("dead-letter pending transaction"),
        1
    );
    assert!(
        buffer_store
            .pending_transactions(&plan.name, 10)
            .await
            .expect("pending transactions")
            .is_empty()
    );
    let delivered = buffer_store
        .delivery_frontier(&plan.name)
        .await
        .expect("delivery frontier")
        .expect("delivery frontier");
    assert_eq!(delivered.source_position(), manifest.source_position());

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), manifest.source_position());
    assert_eq!(
        checkpoint.target_state()["target.delivery.status"],
        "dead_lettered"
    );
    assert_eq!(
        checkpoint.target_state()["target.failure.class"],
        "permanent"
    );
    assert_eq!(
        storage
            .replication_pipeline_dlq_entries(&plan.name)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn restart_replays_pending_buffer_to_dlq_and_retries_dlq_entry() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.error_policy = CatalogReplicationErrorPolicy::new(
        CatalogReplicationErrorPolicyMode::DeadLetterAndContinue,
        None,
    );
    let storage = SlateCatalog::in_memory().await.unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-601").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6F00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(6, "ready"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let prepared = prepare_replication_buffer_append(
        &plan,
        &transaction,
        vec![CdcBufferRecord::new(
            Some(br#"{"id":6}"#.to_vec()),
            Some(br#"{"id":6,"status":"ready"}"#.to_vec()),
        )],
    )
    .expect("prepare pending transaction");
    let buffer_store = storage.cdc_buffer_store();
    buffer_store
        .append_transaction(&prepared.append)
        .await
        .expect("append pending transaction before restart");
    assert_eq!(
        buffer_store
            .pending_transactions(&plan.name, 10)
            .await
            .expect("pending before restart")
            .len(),
        1
    );

    let restarted_runtime = test_runtime_with_plan(plan.clone());
    let cancel = CancellationToken::new();
    assert_eq!(
        restarted_runtime
            .replay_buffered(&storage, &cancel)
            .await
            .expect("restart replay should dead-letter pending transaction"),
        1
    );
    assert!(
        buffer_store
            .pending_transactions(&plan.name, 10)
            .await
            .expect("pending after restart replay")
            .is_empty()
    );
    let dlq_entry = storage
        .replication_pipeline_dlq_entries(&plan.name)
        .await
        .expect("dlq entries after restart replay")
        .into_iter()
        .next()
        .expect("dlq entry");
    assert_eq!(dlq_entry.status(), ReplicationPipelineDlqStatus::Pending);
    assert_eq!(dlq_entry.attempt_count(), 1);
    assert_eq!(dlq_entry.source_position(), transaction.commit_position());

    let retry_runtime = test_runtime_with_plan(plan.clone());
    let outcome = retry_runtime
        .retry_pending_dlq_entries_with_reason(&storage, &plan.name, 10, None)
        .await
        .expect("manual retry after restart")
        .expect("pipeline exists");
    assert_eq!(outcome.attempted, 1);
    assert!(outcome.replayed.is_empty());
    assert_eq!(outcome.failed.len(), 1);
    assert_eq!(outcome.failed[0].dlq_id, dlq_entry.dlq_id());
    let retried = storage
        .replication_pipeline_dlq_entry(&plan.name, dlq_entry.dlq_id())
        .await
        .expect("load retried dlq entry")
        .expect("retried dlq entry");
    assert_eq!(retried.status(), ReplicationPipelineDlqStatus::Pending);
    assert_eq!(retried.attempt_count(), 2);
    assert!(
        retried
            .status_reason()
            .expect("retry failure reason")
            .contains("manual retry failed")
    );
}

#[tokio::test]
async fn retry_dlq_entry_records_attempt_when_target_still_fails() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id, "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    let storage = SlateCatalog::in_memory().await.unwrap();
    let dlq_id = "entry-1";
    persist_test_dlq_entry(
        &storage,
        &plan,
        dlq_id,
        "0/16B6E00",
        "pg-xid-401",
        current_unix_time_ms(),
    )
    .await
    .expect("persist entry");

    let err = runtime
        .retry_dlq_entry_with_reason(
            &storage,
            &plan.name,
            dlq_id,
            Some("operator requested replay".to_string()),
        )
        .await
        .expect_err("retry should fail without a writer");
    assert!(err.to_string().contains("retry replication pipeline"));
    let attempted = storage
        .replication_pipeline_dlq_entry(&plan.name, dlq_id)
        .await
        .expect("load entry")
        .expect("entry exists");
    assert_eq!(attempted.status(), ReplicationPipelineDlqStatus::Pending);
    assert_eq!(attempted.attempt_count(), 2);
    let status_reason = attempted.status_reason().expect("retry failure reason");
    assert!(status_reason.contains("manual retry failed"));
    assert!(status_reason.contains("operator_reason=operator requested replay"));
}

#[tokio::test]
async fn retry_pending_dlq_entries_respects_limit_and_skips_resolved_entries() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id, "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    let storage = SlateCatalog::in_memory().await.unwrap();
    persist_test_dlq_entry(
        &storage,
        &plan,
        "entry-1",
        "0/16B6E00",
        "pg-xid-501",
        1_700_000_000_000,
    )
    .await
    .expect("persist first entry");
    persist_test_dlq_entry(
        &storage,
        &plan,
        "entry-2",
        "0/16B6E10",
        "pg-xid-502",
        1_700_000_000_001,
    )
    .await
    .expect("persist second entry");
    persist_test_dlq_entry(
        &storage,
        &plan,
        "entry-3",
        "0/16B6E20",
        "pg-xid-503",
        1_700_000_000_002,
    )
    .await
    .expect("persist third entry");
    persist_test_dlq_entry(
        &storage,
        &plan,
        "entry-4",
        "0/16B6E30",
        "pg-xid-504",
        1_700_000_000_003,
    )
    .await
    .expect("persist fourth entry");
    storage
        .update_replication_pipeline_dlq_entry_status_with_reason(
            &plan.name,
            "entry-4",
            ReplicationPipelineDlqStatus::Discarded,
            Some("operator skipped duplicate".to_string()),
            1_700_000_000_004,
        )
        .await
        .expect("discard entry");

    let outcome = runtime
        .retry_pending_dlq_entries_with_reason(&storage, &plan.name, 2, None)
        .await
        .expect("retry batch")
        .expect("pipeline exists");
    assert_eq!(outcome.attempted, 2);
    assert!(outcome.replayed.is_empty());
    assert_eq!(outcome.failed.len(), 2);
    assert_eq!(outcome.failed[0].dlq_id, "entry-1");
    assert_eq!(outcome.failed[1].dlq_id, "entry-2");

    let first = storage
        .replication_pipeline_dlq_entry(&plan.name, "entry-1")
        .await
        .expect("load first")
        .expect("first exists");
    let second = storage
        .replication_pipeline_dlq_entry(&plan.name, "entry-2")
        .await
        .expect("load second")
        .expect("second exists");
    let third = storage
        .replication_pipeline_dlq_entry(&plan.name, "entry-3")
        .await
        .expect("load third")
        .expect("third exists");
    let fourth = storage
        .replication_pipeline_dlq_entry(&plan.name, "entry-4")
        .await
        .expect("load fourth")
        .expect("fourth exists");
    assert_eq!(first.attempt_count(), 2);
    assert_eq!(second.attempt_count(), 2);
    assert_eq!(third.attempt_count(), 1);
    assert_eq!(fourth.status(), ReplicationPipelineDlqStatus::Discarded);
}

#[tokio::test]
async fn durable_pipeline_stops_source_progress_when_buffer_cap_remains_exceeded() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.buffer_policy = CatalogReplicationBufferPolicy::new(None, None, Some(1), None);
    let runtime = test_runtime_with_plan(plan.clone());
    let cancel = CancellationToken::new();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let schemas = HashMap::from([(plan.table_id.clone(), plan.schema.clone())]);
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let first = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-201").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id.clone(),
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let second = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-202").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "paid"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &first, Some(&storage), &cancel)
            .await
            .expect("buffer first transaction"),
        1
    );
    let error = runtime
        .run_transaction(&source_id, &schemas, &second, Some(&storage), &cancel)
        .await
        .expect_err("second transaction should trip the pending object cap");
    assert!(error.to_string().contains("durable buffer limit exceeded"));

    let buffer_store = storage.cdc_buffer_store();
    let pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending transactions");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].transaction_id().map(CdcTransactionId::as_str),
        Some("pg-xid-201")
    );
    let source_frontier = buffer_store
        .source_frontier(&plan.name)
        .await
        .expect("source frontier")
        .expect("source frontier");
    assert_eq!(source_frontier.source_position(), first.commit_position());
    assert_eq!(
        source_frontier
            .transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-201")
    );

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), first.commit_position());
    assert_eq!(
        checkpoint.transaction_id().map(CdcTransactionId::as_str),
        Some("pg-xid-201")
    );

    let snapshots = runtime.status_snapshots(&storage).await.unwrap();
    let snapshot = snapshots.first().expect("snapshot");
    assert_eq!(snapshot.pending_transactions(), 1);
    assert_eq!(snapshot.pending_objects(), 1);
    assert_eq!(snapshot.pending_records(), pending[0].record_count());
    assert!(snapshot.source_backpressure_active());
}
