use super::*;

#[test]
fn parse_basic() {
    let sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_person";
    let mv = parse_materialized_view(sql).expect("parse mv");
    assert_eq!(mv.name(), "mv");
    assert_eq!(mv.query(), "SELECT * FROM nexmark_person");
    assert!(!mv.if_not_exists());
}

#[test]
fn parse_if_not_exists() {
    let sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1";
    let mv = parse_materialized_view(sql).expect("parse mv");
    assert!(mv.if_not_exists());
    assert_eq!(mv.query(), "SELECT 1");
}

#[test]
fn parse_with_clause() {
    let sql = "CREATE MATERIALIZED VIEW mv WITH (foo = 'bar') AS SELECT 1";
    let mv = parse_materialized_view(sql).expect("parse mv");
    assert_eq!(mv.name(), "mv");
    assert_eq!(mv.query(), "SELECT 1");
}

#[test]
fn reject_missing_as() {
    let sql = "CREATE MATERIALIZED VIEW mv SELECT 1";
    let err = parse_materialized_view(sql).unwrap_err();
    assert!(
        err.to_string()
            .contains("failed to parse materialized view statement")
    );
}

#[test]
fn reject_empty_query() {
    let sql = "CREATE MATERIALIZED VIEW mv AS";
    let err = parse_materialized_view(sql).unwrap_err();
    assert!(
        err.to_string()
            .contains("failed to parse materialized view statement")
    );
}

#[test]
fn reject_multiple_statements() {
    let sql = "CREATE MATERIALIZED VIEW mv AS SELECT 1; CREATE MATERIALIZED VIEW mv2 AS SELECT 2";
    let err = parse_materialized_view(sql).unwrap_err();
    assert!(err.to_string().contains("multiple statements"));
}

#[test]
fn parse_quoted_identifier() {
    let sql = "CREATE MATERIALIZED VIEW \"MyView\" AS SELECT 1";
    let mv = parse_materialized_view(sql).expect("parse mv");
    assert_eq!(mv.name(), "MyView");
}

#[test]
fn parse_postgres_style_qualified_materialized_view_name() {
    let sql =
        "CREATE MATERIALIZED VIEW IF NOT EXISTS public.\"MyView\" AS SELECT \"dateTime\" FROM bid";
    let mv = parse_materialized_view(sql).expect("parse mv");
    assert_eq!(mv.name(), "public.MyView");
    assert_eq!(mv.query(), "SELECT \"dateTime\" FROM bid");
    assert!(mv.if_not_exists());
}

#[test]
fn parse_create_sink_statement() {
    let stmt = parse_floe_statement(
        "CREATE SINK out_bid FROM mv_bid WITH (type = 'http', url = 'http://localhost:8080', batch_size = 32, with_snapshot = true, as_of = 42)",
    )
    .expect("parse sink");
    match stmt {
        FloeStatement::CreateSink(definition) => {
            assert_eq!(definition.name(), "out_bid");
            assert_eq!(definition.mv_name(), "mv_bid");
            assert!(definition.with_snapshot());
            assert_eq!(definition.as_of(), Some(42));
            assert_eq!(
                definition.connector(),
                &SinkConnector::Http {
                    url: "http://localhost:8080".to_string(),
                    batch_size: Some(32),
                }
            );
        }
        other => panic!("expected CREATE SINK statement, got {other:?}"),
    }
}

#[test]
fn parse_create_table_statement() {
    let stmt = parse_floe_statement(
        "CREATE TABLE bids (id BIGINT PRIMARY KEY, price BIGINT NOT NULL, channel TEXT)",
    )
    .expect("parse table");
    match stmt {
        FloeStatement::CreateTable(definition) => {
            assert_eq!(definition.name(), "bids");
            assert_eq!(definition.columns().len(), 3);
            let id = &definition.columns()[0];
            assert_eq!(id.name(), "id");
            assert_eq!(id.data_type(), &SqlColumnType::Int64);
            assert!(!id.nullable());
            assert!(id.primary_key());
        }
        other => panic!("expected CREATE TABLE statement, got {other:?}"),
    }
}

#[test]
fn parse_create_table_rejects_unsupported_type() {
    let err = parse_floe_statement("CREATE TABLE bids (id UUID PRIMARY KEY)").expect_err("error");
    assert!(
        err.to_string()
            .contains("unsupported type 'UUID' for column 'id'")
    );
}

#[test]
fn parse_floe_program_preserves_statement_order() {
    let program = r#"
        CREATE MATERIALIZED VIEW mv_bid AS SELECT auction FROM bid;
        CREATE SINK sink_bid FROM mv_bid WITH (connector = 'file', path = '/tmp/out.jsonl', append = true);
        TAIL mv_bid WITH SNAPSHOT;
    "#;
    let statements = parse_floe_program(program).expect("parse program");
    assert_eq!(statements.len(), 3);
    assert!(matches!(
        statements.first(),
        Some(FloeStatement::CreateMaterializedView(_))
    ));
    assert!(matches!(
        statements.get(1),
        Some(FloeStatement::CreateSink(_))
    ));
    assert!(matches!(
        statements.last(),
        Some(FloeStatement::Tail { .. })
    ));
}

#[test]
fn parse_floe_statement_rejects_multi_statement_input() {
    let err = parse_floe_statement("TAIL mv; TAIL mv2").unwrap_err();
    assert!(err.to_string().contains("exactly one statement"));
}

#[test]
fn parse_tail_variants() {
    let stmt = parse_floe_statement("TAIL mv_orders").expect("parse tail");
    assert_eq!(
        stmt,
        FloeStatement::Tail {
            mv_name: "mv_orders".to_string(),
            with_snapshot: false,
            as_of: None,
        }
    );

    let stmt = parse_floe_statement("TAIL mv_orders WITH SNAPSHOT").expect("parse tail snapshot");
    assert_eq!(
        stmt,
        FloeStatement::Tail {
            mv_name: "mv_orders".to_string(),
            with_snapshot: true,
            as_of: None,
        }
    );

    let stmt = parse_floe_statement("TAIL mv_orders AS OF 42").expect("parse tail as of");
    assert_eq!(
        stmt,
        FloeStatement::Tail {
            mv_name: "mv_orders".to_string(),
            with_snapshot: false,
            as_of: Some(42),
        }
    );

    let stmt = parse_floe_statement("TAIL mv_orders WITH SNAPSHOT AS OF 42")
        .expect("parse tail snapshot as of");
    assert_eq!(
        stmt,
        FloeStatement::Tail {
            mv_name: "mv_orders".to_string(),
            with_snapshot: true,
            as_of: Some(42),
        }
    );
}
