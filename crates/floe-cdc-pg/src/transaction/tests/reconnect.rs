use super::*;

#[tokio::test]
async fn reconnect_loop_reloads_checkpoint_as_next_start_lsn() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-reconnect").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let first_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
            FakeStep::Event(begin(61)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 12, "open"))),
            FakeStep::Event(commit(90)),
            FakeStep::Error("disconnect"),
        ],
        Arc::clone(&feedbacks),
    );
    let second_stream = FakeStream::new([FakeStep::End], Arc::clone(&feedbacks));
    let factory = FakeFactory::new([first_stream, second_stream]);

    run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect("run reconnect loop");

    assert_eq!(
        *feedbacks.lock().expect("feedback lock"),
        vec![PostgresLsn::from_u64(90)]
    );
    let configs = factory.configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].start_lsn(), None);
    assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(90)));
}

#[tokio::test]
async fn reconnect_loop_replays_inflight_wal_transaction_from_durable_checkpoint() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-inflight-reconnect").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let first_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
            FakeStep::Event(begin(61)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 12, "committed"))),
            FakeStep::Event(commit(90)),
            FakeStep::Event(begin(63)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 14, "inflight"))),
            FakeStep::Error("disconnect before commit"),
        ],
        Arc::clone(&feedbacks),
    );
    let second_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
            FakeStep::Event(begin(63)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 14, "replayed"))),
            FakeStep::Event(commit(120)),
            FakeStep::End,
        ],
        Arc::clone(&feedbacks),
    );
    let factory = FakeFactory::new([first_stream, second_stream]);

    run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect("run reconnect loop");

    assert_eq!(
        *feedbacks.lock().expect("feedback lock"),
        vec![PostgresLsn::from_u64(90), PostgresLsn::from_u64(120)]
    );
    let configs = factory.configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].start_lsn(), None);
    assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(90)));
    let checkpoint = table_store
        .load_checkpoint(&source_id)
        .await
        .expect("load checkpoint")
        .expect("checkpoint");
    assert_eq!(
        PostgresLsn::from_source_position(checkpoint.position()).expect("checkpoint lsn"),
        PostgresLsn::from_u64(120)
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(12)]).expect("key")
            )
            .await
            .expect("load committed row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(12)),
                Some(RowValue::Utf8("committed".to_string())),
            ])
            .expect("row")
        )
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(14)]).expect("key")
            )
            .await
            .expect("load replayed row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(14)),
                Some(RowValue::Utf8("replayed".to_string())),
            ])
            .expect("row")
        )
    );
}

#[tokio::test]
async fn reconnect_loop_resumes_after_compatible_schema_change() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-schema-reconnect").await;
    let relation = relation_message_with_columns(
        RELATION_ID,
        "orders",
        &[
            ("id", PG_INT8_OID, true),
            ("status", PG_TEXT_OID, false),
            ("note", PG_TEXT_OID, false),
        ],
    );
    let PgOutputMessage::Relation(observed_relation) =
        decode_pgoutput_message(relation.clone()).expect("decode relation")
    else {
        panic!("expected relation");
    };
    let observed_schema = observed_relation
        .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
        .expect("observed schema");
    let mut applier = PostgresCdcEventApplier::with_schema_policy(
        source_id.clone(),
        table_store.clone(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let first_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation)),
            FakeStep::Event(begin(62)),
            FakeStep::Event(xlog(insert_message_with_values(
                RELATION_ID,
                &["13".to_string(), "open".to_string(), "ignored".to_string()],
            ))),
            FakeStep::Event(commit(120)),
            FakeStep::Error("disconnect after schema change"),
        ],
        Arc::clone(&feedbacks),
    );
    let second_stream = FakeStream::new([FakeStep::End], Arc::clone(&feedbacks));
    let factory = FakeFactory::new([first_stream, second_stream]);

    run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect("run reconnect loop");

    assert_eq!(
        *feedbacks.lock().expect("feedback lock"),
        vec![PostgresLsn::from_u64(120)]
    );
    let configs = factory.configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].start_lsn(), None);
    assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(120)));
    let checkpoint = table_store
        .load_checkpoint(&source_id)
        .await
        .expect("load checkpoint")
        .expect("checkpoint");
    assert_eq!(
        checkpoint.schema_versions().get("orders").copied(),
        Some(observed_schema.stable_fingerprint())
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(13)]).expect("key")
            )
            .await
            .expect("load row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(13)),
                Some(RowValue::Utf8("open".to_string())),
            ])
            .expect("row")
        )
    );
}

#[tokio::test]
async fn reconnect_loop_errors_after_max_reconnects() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-reconnect-exhausted").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let factory = FakeFactory::new([
        FakeStream::new([FakeStep::Error("disconnect 1")], Arc::clone(&feedbacks)),
        FakeStream::new([FakeStep::Error("disconnect 2")], feedbacks),
    ]);

    let err = run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect_err("reconnects should be exhausted");
    assert!(format!("{err:#}").contains("failed after 1 reconnect"));
}
