use super::*;

#[test]
fn assembles_decoded_changes_into_transaction_batch() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let mut assembler = PostgresTransactionAssembler::new(source_id.clone(), router());

    assert!(
        assembler
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .expect("relation metadata")
            .is_none()
    );
    assembler.accept_event(begin(55)).expect("begin");
    assembler
        .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
        .expect("insert");
    let transaction = assembler
        .accept_event(commit(30))
        .expect("commit")
        .expect("transaction");

    assert_eq!(transaction.source_id(), &source_id);
    assert_eq!(
        transaction.transaction_id().expect("txid").as_str(),
        "pg-xid-55"
    );
    assert_eq!(
        transaction.commit_position(),
        &CdcSourcePosition::Postgres {
            commit_lsn: "0/1E".to_string(),
            event_lsn: None
        }
    );
    assert_eq!(transaction.change_batches().len(), 1);
    assert_eq!(
        transaction.change_batches()[0].table_id(),
        &CdcTableId::new("orders").expect("table id")
    );
    assert_eq!(
        transaction.change_batches()[0].changes(),
        &[CdcChange::Insert {
            row: CdcRow::new([
                Some(RowValue::Int64(7)),
                Some(RowValue::Utf8("open".to_string())),
            ])
            .expect("row")
        }]
    );
}

#[test]
fn compatible_column_additions_can_be_projected_to_catalog_schema() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        source_id,
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let relation = relation_message_with_columns(
        RELATION_ID,
        "orders",
        &[
            ("id", PG_INT8_OID, true),
            ("status", PG_TEXT_OID, false),
            ("note", PG_TEXT_OID, false),
        ],
    );

    assembler
        .accept_event(xlog(relation.clone()))
        .expect("compatible relation");
    let observations = assembler.drain_schema_evolution_observations();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(observation.table_id().as_str(), "orders");
    assert_eq!(observation.upstream_table().schema(), "public");
    assert_eq!(observation.upstream_table().table(), "orders");
    assert_eq!(
        observation.policy(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible
    );
    assert_eq!(
        observation.outcome(),
        PostgresSchemaEvolutionOutcome::CompatibleAddition
    );
    assert_eq!(observation.added_columns(), &["note".to_string()]);
    assert_eq!(observation.reason(), None);
    assembler.accept_event(begin(57)).expect("begin");
    assembler
        .accept_event(xlog(insert_message_with_values(
            RELATION_ID,
            &["7".to_string(), "open".to_string(), "ignored".to_string()],
        )))
        .expect("insert");
    let transaction = assembler
        .accept_event(commit(41))
        .expect("commit")
        .expect("transaction");

    assert_eq!(
        transaction.change_batches()[0].changes(),
        &[CdcChange::Insert {
            row: CdcRow::new([
                Some(RowValue::Int64(7)),
                Some(RowValue::Utf8("open".to_string())),
            ])
            .expect("row")
        }]
    );
    let PgOutputMessage::Relation(observed_relation) =
        decode_pgoutput_message(relation).expect("decode relation")
    else {
        panic!("expected relation");
    };
    let observed_schema = observed_relation
        .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
        .expect("observed schema");
    assert_eq!(
        transaction.schema_versions().get("orders").copied(),
        Some(observed_schema.stable_fingerprint())
    );
}

#[test]
fn fail_fast_schema_policy_rejects_compatible_additions() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::FailFast,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[
                ("id", PG_INT8_OID, true),
                ("status", PG_TEXT_OID, false),
                ("note", PG_TEXT_OID, false),
            ],
        )))
        .expect_err("compatible addition should fail under fail-fast");

    assert!(format!("{err:#}").contains("compatible column additions"));
    let observations = assembler.drain_schema_evolution_observations();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(
        observation.policy(),
        PostgresSchemaEvolutionPolicy::FailFast
    );
    assert_eq!(
        observation.outcome(),
        PostgresSchemaEvolutionOutcome::RejectedCompatibleAddition
    );
    assert_eq!(observation.added_columns(), &["note".to_string()]);
    assert_eq!(
        observation.reason(),
        Some("compatible column additions rejected by fail-fast policy")
    );
}

#[test]
fn schema_policy_rejects_incompatible_type_changes() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[("id", PG_INT8_OID, true), ("status", PG_INT8_OID, false)],
        )))
        .expect_err("type change should fail");

    assert!(format!("{err:#}").contains("type changed"));
    let observations = assembler.drain_schema_evolution_observations();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(
        observation.outcome(),
        PostgresSchemaEvolutionOutcome::Incompatible
    );
    assert_eq!(observation.added_columns(), &[] as &[String]);
    assert!(
        observation
            .reason()
            .expect("reason")
            .contains("type changed")
    );
}

#[test]
fn schema_policy_rejects_dropped_columns() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[("id", PG_INT8_OID, true)],
        )))
        .expect_err("dropped column should fail");

    assert!(format!("{err:#}").contains("column count decreased"));
}

#[test]
fn schema_policy_rejects_reordered_columns() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[("status", PG_TEXT_OID, false), ("id", PG_INT8_OID, true)],
        )))
        .expect_err("reordered columns should fail");

    assert!(format!("{err:#}").contains("column 0 changed"));
}

#[test]
fn schema_policy_rejects_primary_key_changes() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[("id", PG_INT8_OID, false), ("status", PG_TEXT_OID, true)],
        )))
        .expect_err("primary key change should fail");

    assert!(format!("{err:#}").contains("primary key changed"));
}

#[test]
fn schema_policy_rejects_replica_identity_changes() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    assembler
        .accept_event(xlog(relation_message_with_identity_and_column_specs(
            RELATION_ID,
            "orders",
            b'd',
            &[("id", PG_INT8_OID, true), ("status", PG_TEXT_OID, false)],
        )))
        .expect("initial relation");

    let err = assembler
        .accept_event(xlog(relation_message_with_identity_and_column_specs(
            RELATION_ID,
            "orders",
            b'f',
            &[("id", PG_INT8_OID, true), ("status", PG_TEXT_OID, false)],
        )))
        .expect_err("replica identity change should fail");

    assert!(format!("{err:#}").contains("replica identity changed"));
    let observations = assembler.drain_schema_evolution_observations();
    assert_eq!(observations.len(), 1);
    assert_eq!(
        observations[0].outcome(),
        PostgresSchemaEvolutionOutcome::Incompatible
    );
}

#[test]
fn in_flight_transaction_decodes_each_relation_schema_version() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        source_id,
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );

    assembler.accept_event(begin(70)).expect("begin");
    assembler
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .expect("initial relation");
    assembler
        .accept_event(xlog(insert_message(RELATION_ID, 1, "before")))
        .expect("insert before schema change");
    let evolved_relation = relation_message_with_columns(
        RELATION_ID,
        "orders",
        &[
            ("id", PG_INT8_OID, true),
            ("status", PG_TEXT_OID, false),
            ("note", PG_TEXT_OID, false),
        ],
    );
    assembler
        .accept_event(xlog(evolved_relation.clone()))
        .expect("compatible relation");
    assembler
        .accept_event(xlog(insert_message_with_values(
            RELATION_ID,
            &[
                "2".to_string(),
                "after".to_string(),
                "projected".to_string(),
            ],
        )))
        .expect("insert after schema change");
    let transaction = assembler
        .accept_event(commit(140))
        .expect("commit")
        .expect("transaction");

    assert_eq!(
        transaction.change_batches()[0].changes(),
        &[
            CdcChange::Insert {
                row: id_status_row(1, "before")
            },
            CdcChange::Insert {
                row: id_status_row(2, "after")
            },
        ]
    );
    let PgOutputMessage::Relation(observed_relation) =
        decode_pgoutput_message(evolved_relation).expect("decode relation")
    else {
        panic!("expected relation");
    };
    let observed_schema = observed_relation
        .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
        .expect("observed schema");
    assert_eq!(
        transaction.schema_versions().get("orders").copied(),
        Some(observed_schema.stable_fingerprint())
    );
}

#[test]
fn schema_history_is_bounded_for_repeated_relation_versions() {
    let table_id = CdcTableId::new("orders").expect("table id");
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );

    for added in 0..(POSTGRES_SCHEMA_HISTORY_LIMIT + 8) {
        let mut borrowed = vec![("id", PG_INT8_OID, true), ("status", PG_TEXT_OID, false)];
        borrowed.extend((0..added).map(|idx| {
            let name: &'static str = Box::leak(format!("note_{idx}").into_boxed_str());
            (name, PG_TEXT_OID, false)
        }));
        assembler
            .accept_event(xlog(relation_message_with_columns(
                RELATION_ID,
                "orders",
                &borrowed,
            )))
            .expect("compatible relation");
    }

    assert_eq!(
        assembler.schema_history_len_for_test(&table_id),
        POSTGRES_SCHEMA_HISTORY_LIMIT
    );
}
