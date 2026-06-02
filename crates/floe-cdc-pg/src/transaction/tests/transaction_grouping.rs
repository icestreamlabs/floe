use super::*;

#[test]
fn groups_multiple_tables_in_one_source_transaction() {
    let mut router = router();
    router.insert(
        UpstreamTableRef::new("public", "customers").expect("upstream"),
        CdcTableId::new("customers").expect("table id"),
    );
    let mut assembler =
        PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router);

    assembler
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .expect("orders relation");
    assembler
        .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
        .expect("customers relation");
    assembler.accept_event(begin(56)).expect("begin");
    assembler
        .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
        .expect("orders insert");
    assembler
        .accept_event(xlog(insert_message(OTHER_RELATION_ID, 8, "active")))
        .expect("customers insert");

    let transaction = assembler
        .accept_event(commit(40))
        .expect("commit")
        .expect("transaction");
    let tables: Vec<&str> = transaction
        .change_batches()
        .iter()
        .map(|batch| batch.table_id().as_str())
        .collect();
    assert_eq!(tables, vec!["orders", "customers"]);
}

#[test]
fn preserves_multi_row_order_within_one_source_transaction() {
    let mut assembler =
        PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router());

    assembler
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .expect("relation");
    assembler.accept_event(begin(64)).expect("begin");
    assembler
        .accept_event(xlog(insert_message(RELATION_ID, 1, "first")))
        .expect("first insert");
    assembler
        .accept_event(xlog(insert_message(RELATION_ID, 2, "second")))
        .expect("second insert");
    assembler
        .accept_event(xlog(update_key_message(RELATION_ID, 1, 3, "third")))
        .expect("primary-key update");

    let transaction = assembler
        .accept_event(commit(43))
        .expect("commit")
        .expect("transaction");

    assert_eq!(transaction.change_batches().len(), 1);
    let observed = transaction.change_batches()[0]
        .changes()
        .iter()
        .map(|change| match change {
            CdcChange::Insert { row } => {
                let id = row.values()[0].as_ref().expect("id");
                let status = row.values()[1].as_ref().expect("status");
                format!("insert:{id:?}:{status:?}")
            }
            CdcChange::Update { key, after, .. } => {
                let key = key.as_ref().expect("key").values()[0].clone();
                let id = after.values()[0].as_ref().expect("id");
                let status = after.values()[1].as_ref().expect("status");
                format!("update:{key:?}->{id:?}:{status:?}")
            }
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            "insert:Int64(1):Utf8(\"first\")",
            "insert:Int64(2):Utf8(\"second\")",
            "update:Int64(1)->Int64(3):Utf8(\"third\")",
        ]
    );
}

#[test]
fn groups_multi_relation_truncate_in_one_source_transaction() {
    let mut router = router();
    router.insert(
        UpstreamTableRef::new("public", "customers").expect("upstream"),
        CdcTableId::new("customers").expect("table id"),
    );
    let mut assembler =
        PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router);

    assembler
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .expect("orders relation");
    assembler
        .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
        .expect("customers relation");
    assembler.accept_event(begin(62)).expect("begin");
    assembler
        .accept_event(xlog(truncate_message([RELATION_ID, OTHER_RELATION_ID])))
        .expect("truncate");

    let transaction = assembler
        .accept_event(commit(45))
        .expect("commit")
        .expect("transaction");
    assert_eq!(transaction.change_batches().len(), 2);
    assert_eq!(
        transaction.change_batches()[0].changes(),
        &[CdcChange::Truncate]
    );
    assert_eq!(
        transaction.change_batches()[1].changes(),
        &[CdcChange::Truncate]
    );
}

#[test]
fn ignores_unrouted_tables_and_empty_transactions() {
    let mut assembler =
        PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router());
    assembler
        .accept_event(xlog(relation_message(OTHER_RELATION_ID, "unmapped")))
        .expect("relation");
    assembler.accept_event(begin(57)).expect("begin");
    assembler
        .accept_event(xlog(insert_message(OTHER_RELATION_ID, 7, "ignored")))
        .expect("ignored insert");
    assert!(
        assembler
            .accept_event(commit(50))
            .expect("commit")
            .is_none()
    );
}

#[test]
fn rejects_dml_outside_transaction_boundary() {
    let mut assembler =
        PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router());
    assembler
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .expect("relation");
    let err = assembler
        .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
        .expect_err("dml outside transaction should fail");
    assert!(format!("{err:#}").contains("outside a transaction"));
}
