use super::*;

#[tokio::test]
async fn applier_returns_feedback_lsn_only_after_table_apply() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-applier-apply").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());

    let relation_outcome = applier
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .await
        .expect("relation");
    assert!(relation_outcome.apply_result().is_none());
    assert_eq!(relation_outcome.feedback_lsn(), None);

    applier.accept_event(begin(58)).await.expect("begin");
    applier
        .accept_event(xlog(insert_message(RELATION_ID, 9, "open")))
        .await
        .expect("insert");
    let outcome = applier
        .accept_event(commit(60))
        .await
        .expect("commit apply");

    assert_eq!(outcome.feedback_lsn(), Some(PostgresLsn::from_u64(60)));
    let apply_result = outcome.apply_result().expect("apply result");
    assert!(!apply_result.already_committed());
    assert_eq!(
        table_store
            .load_checkpoint(&source_id)
            .await
            .expect("load checkpoint"),
        Some(apply_result.checkpoint().clone())
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(9)]).expect("key")
            )
            .await
            .expect("load row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(9)),
                Some(RowValue::Utf8("open".to_string()))
            ])
            .expect("row")
        )
    );

    let lag = outcome.lag_snapshot();
    assert_eq!(lag.source_id(), &source_id);
    assert_eq!(lag.upstream_wal_end(), Some(PostgresLsn::from_u64(60)));
    assert_eq!(lag.durable_lsn(), Some(PostgresLsn::from_u64(60)));
    assert_eq!(lag.source_lag_bytes(), Some(0));
    assert_eq!(lag.table_lags().len(), 1);
    assert_eq!(
        lag.table_lags()[0].table_id(),
        &CdcTableId::new("orders").expect("table id")
    );
    assert_eq!(
        lag.table_lags()[0].last_applied_lsn(),
        Some(PostgresLsn::from_u64(60))
    );
    assert_eq!(lag.table_lags()[0].table_lag_bytes(), Some(0));
}

#[tokio::test]
async fn applier_moves_primary_key_updates_between_keys() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-applier-primary-key-update").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());

    applier
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .await
        .expect("relation");
    applier.accept_event(begin(65)).await.expect("begin insert");
    applier
        .accept_event(xlog(insert_message(RELATION_ID, 1, "open")))
        .await
        .expect("insert");
    applier
        .accept_event(commit(100))
        .await
        .expect("commit insert");

    applier.accept_event(begin(66)).await.expect("begin update");
    applier
        .accept_event(xlog(update_key_message(RELATION_ID, 1, 2, "paid")))
        .await
        .expect("primary-key update");
    let outcome = applier
        .accept_event(commit(120))
        .await
        .expect("commit update");

    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &id_status_key(1)
            )
            .await
            .expect("load old key"),
        None
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &id_status_key(2)
            )
            .await
            .expect("load new key"),
        Some(id_status_row(2, "paid"))
    );
    let deltas = outcome.apply_result().expect("apply result").table_deltas()[0].deltas();
    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0].diff(), -1);
    assert_eq!(deltas[0].row(), &id_status_row(1, "open"));
    assert_eq!(deltas[1].diff(), 1);
    assert_eq!(deltas[1].row(), &id_status_row(2, "paid"));
    assert_eq!(outcome.feedback_lsn(), Some(PostgresLsn::from_u64(120)));
}

#[tokio::test]
async fn applier_does_not_persist_or_feedback_when_table_apply_fails() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-applier-apply-fails").await;
    let mut applier = PostgresCdcEventApplier::with_router(
        source_id.clone(),
        table_store.clone(),
        HashMap::new(),
        router(),
    );

    applier
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .await
        .expect("relation");
    applier.accept_event(begin(59)).await.expect("begin");
    applier
        .accept_event(xlog(insert_message(RELATION_ID, 10, "open")))
        .await
        .expect("insert");
    let err = applier
        .accept_event(commit(70))
        .await
        .expect_err("missing schema should fail apply");
    assert!(format!("{err:#}").contains("unknown table"));
    assert_eq!(
        table_store
            .load_checkpoint(&source_id)
            .await
            .expect("load checkpoint"),
        None
    );
}

#[tokio::test]
async fn applier_ignores_idle_events_and_feedbacks_after_commit() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-feedback").await;
    let mut applier = PostgresCdcEventApplier::new(source_id, table_store, orders_schemas());
    let mut feedbacks = Vec::new();
    for event in [
        PostgresReplicationEvent::KeepAlive {
            wal_end: PostgresLsn::from_u64(11),
            reply_requested: true,
            server_time_micros: 1,
        },
        xlog(relation_message(RELATION_ID, "orders")),
        begin(60),
        xlog(insert_message(RELATION_ID, 11, "open")),
        PostgresReplicationEvent::Message {
            transactional: false,
            lsn: PostgresLsn::from_u64(12),
            prefix: "noop".to_string(),
            content: Bytes::new(),
        },
        commit(80),
    ] {
        let outcome = applier.accept_event(event).await.expect("accept event");
        if let Some(feedback_lsn) = outcome.feedback_lsn() {
            feedbacks.push(feedback_lsn);
        }
    }

    assert_eq!(feedbacks, vec![PostgresLsn::from_u64(80)]);
}

#[tokio::test]
async fn applier_exposes_shared_source_and_per_table_lag() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-lag-snapshot").await;
    let orders = schema_for(RELATION_ID, "orders", "orders");
    let customers = schema_for(OTHER_RELATION_ID, "customers", "customers");
    let schemas = HashMap::from([
        (orders.table_id().clone(), orders),
        (customers.table_id().clone(), customers),
    ]);
    let mut router = PostgresTableRouter::new();
    router.insert(
        UpstreamTableRef::new("public", "orders").expect("orders upstream"),
        CdcTableId::new("orders").expect("orders id"),
    );
    router.insert(
        UpstreamTableRef::new("public", "customers").expect("customers upstream"),
        CdcTableId::new("customers").expect("customers id"),
    );
    let mut applier =
        PostgresCdcEventApplier::with_router(source_id.clone(), table_store, schemas, router);

    applier
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .await
        .expect("orders relation");
    applier
        .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
        .await
        .expect("customers relation");
    applier.accept_event(begin(63)).await.expect("begin");
    applier
        .accept_event(xlog(insert_message(RELATION_ID, 20, "open")))
        .await
        .expect("orders insert");
    let applied = applier
        .accept_event(commit(100))
        .await
        .expect("commit apply");

    assert_eq!(applied.lag_snapshot().source_lag_bytes(), Some(0));
    let idle = applier
        .accept_event(PostgresReplicationEvent::KeepAlive {
            wal_end: PostgresLsn::from_u64(150),
            reply_requested: false,
            server_time_micros: 200,
        })
        .await
        .expect("keepalive");
    let lag = idle.lag_snapshot();
    assert_eq!(lag.source_id(), &source_id);
    assert_eq!(lag.upstream_wal_end(), Some(PostgresLsn::from_u64(150)));
    assert_eq!(lag.durable_lsn(), Some(PostgresLsn::from_u64(100)));
    assert_eq!(lag.source_lag_bytes(), Some(50));

    let table_lags = lag.table_lags();
    assert_eq!(table_lags.len(), 2);
    assert_eq!(
        table_lags[0].table_id(),
        &CdcTableId::new("customers").expect("customers id")
    );
    assert_eq!(table_lags[0].last_applied_lsn(), None);
    assert_eq!(table_lags[0].table_lag_bytes(), None);
    assert_eq!(
        table_lags[1].table_id(),
        &CdcTableId::new("orders").expect("orders id")
    );
    assert_eq!(
        table_lags[1].last_applied_lsn(),
        Some(PostgresLsn::from_u64(100))
    );
    assert_eq!(table_lags[1].table_lag_bytes(), Some(50));
}
