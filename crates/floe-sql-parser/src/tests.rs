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
fn parse_create_postgres_cdc_source_statement() {
    let stmt = parse_floe_statement(
        "CREATE SOURCE pg_main WITH (
            connector = 'postgres-cdc',
            connection = 'postgres://postgres:postgres@localhost/postgres',
            slot.name = 'floe_slot',
            publication.name = 'floe_pub',
            include_schema_in_source = true,
            schema.evolution = 'ignore-compatible'
        )",
    )
    .expect("parse source");
    match stmt {
        FloeStatement::CreateSource(definition) => {
            assert_eq!(definition.name(), "pg_main");
            assert_eq!(
                definition.connector(),
                &SourceConnector::PostgresCdc(
                    PostgresCdcSourceOptions::new_with_schema_evolution_policy(
                        "postgres://postgres:postgres@localhost/postgres",
                        "floe_slot",
                        Some("floe_pub".to_string()),
                        Some(true),
                        PostgresCdcSchemaEvolutionPolicy::IgnoreCompatible,
                    )
                    .expect("options")
                )
            );
        }
        other => panic!("expected CREATE SOURCE statement, got {other:?}"),
    }
}

#[test]
fn parse_create_replication_pipeline_statement() {
    let stmt = parse_floe_statement(
        "CREATE REPLICATION PIPELINE pg_orders_to_kafka
         FROM pg_main TABLE 'public.orders'
         INTO KAFKA WITH (
            brokers = 'localhost:9092',
            topic = 'orders_cdc',
            format = 'debezium-json',
            durable_buffer = true,
            buffer.max_pending_bytes = 1048576,
            buffer.max_pending_records = 100000,
            buffer.max_pending_objects = 64,
            buffer.max_pending_age_ms = 60000,
            tombstones = true,
            transaction_metadata = true
         )",
    )
    .expect("parse replication pipeline");
    match stmt {
        FloeStatement::CreateReplicationPipeline(definition) => {
            assert_eq!(definition.name(), "pg_orders_to_kafka");
            assert_eq!(definition.source_name(), "pg_main");
            assert_eq!(definition.upstream_table(), "public.orders");
            assert_eq!(definition.format(), ReplicationPipelineFormat::DebeziumJson);
            assert_eq!(definition.buffer_mode(), ReplicationBufferMode::Durable);
            assert_eq!(
                definition.buffer_policy().max_pending_bytes(),
                Some(1_048_576)
            );
            assert_eq!(
                definition.buffer_policy().max_pending_records(),
                Some(100_000)
            );
            assert_eq!(
                definition.buffer_policy().max_pending_transactions(),
                Some(64)
            );
            assert_eq!(
                definition.buffer_policy().max_pending_age_ms(),
                Some(60_000)
            );
            assert!(definition.emit_tombstones());
            assert!(definition.include_transaction_metadata());
            assert_eq!(
                definition.target(),
                &ReplicationPipelineTarget::Kafka {
                    brokers: "localhost:9092".to_string(),
                    topic: "orders_cdc".to_string(),
                }
            );
        }
        other => panic!("expected CREATE REPLICATION PIPELINE statement, got {other:?}"),
    }
}

#[test]
fn parse_create_replication_pipeline_defaults() {
    let stmt = parse_floe_statement(
        "CREATE REPLICATION PIPELINE p FROM pg_main TABLE public.orders INTO KAFKA WITH (
            brokers = 'localhost:9092',
            topic = 'orders_cdc'
        )",
    )
    .expect("parse replication pipeline");
    let FloeStatement::CreateReplicationPipeline(definition) = stmt else {
        panic!("expected replication pipeline");
    };
    assert_eq!(definition.format(), ReplicationPipelineFormat::FloeJson);
    assert_eq!(definition.buffer_mode(), ReplicationBufferMode::Durable);
    assert!(!definition.emit_tombstones());
    assert!(!definition.include_transaction_metadata());
}

#[test]
fn parse_create_replication_pipeline_floe_json_format() {
    let stmt = parse_floe_statement(
        "CREATE REPLICATION PIPELINE p FROM pg_main TABLE public.orders INTO KAFKA WITH (
            brokers = 'localhost:9092',
            topic = 'orders_cdc',
            format = 'floe-json'
        )",
    )
    .expect("parse replication pipeline");
    let FloeStatement::CreateReplicationPipeline(definition) = stmt else {
        panic!("expected replication pipeline");
    };
    assert_eq!(definition.format(), ReplicationPipelineFormat::FloeJson);
}

#[test]
fn parse_create_replication_pipeline_arrow_ipc_format() {
    let stmt = parse_floe_statement(
        "CREATE REPLICATION PIPELINE p FROM pg_main TABLE public.orders INTO KAFKA WITH (
            brokers = 'localhost:9092',
            topic = 'orders_cdc',
            format = 'arrow-ipc'
        )",
    )
    .expect("parse replication pipeline");
    let FloeStatement::CreateReplicationPipeline(definition) = stmt else {
        panic!("expected replication pipeline");
    };
    assert_eq!(definition.format(), ReplicationPipelineFormat::ArrowIpc);
}

#[test]
fn parse_create_replication_pipeline_postgres_target() {
    let stmt = parse_floe_statement(
        "CREATE REPLICATION PIPELINE p FROM pg_main TABLE public.orders INTO POSTGRES WITH (
            connection = 'postgres://postgres:postgres@localhost/postgres',
            table = 'public.orders_copy'
        )",
    )
    .expect("parse replication pipeline");
    let FloeStatement::CreateReplicationPipeline(definition) = stmt else {
        panic!("expected replication pipeline");
    };
    assert_eq!(
        definition.target(),
        &ReplicationPipelineTarget::Postgres {
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
        }
    );
    assert_eq!(definition.format(), ReplicationPipelineFormat::FloeJson);
}

#[test]
fn parse_create_replication_pipeline_without_durable_buffer() {
    let stmt = parse_floe_statement(
        "CREATE REPLICATION PIPELINE p FROM pg_main TABLE public.orders INTO KAFKA WITH (
            brokers = 'localhost:9092',
            topic = 'orders_cdc',
            durable_buffer = false
        )",
    )
    .expect("parse replication pipeline");
    let FloeStatement::CreateReplicationPipeline(definition) = stmt else {
        panic!("expected replication pipeline");
    };
    assert_eq!(definition.buffer_mode(), ReplicationBufferMode::NoBuffer);
}

#[test]
fn parse_create_replication_pipeline_rejects_unknown_format() {
    let err = parse_floe_statement(
        "CREATE REPLICATION PIPELINE p FROM pg_main TABLE public.orders INTO KAFKA WITH (
            brokers = 'localhost:9092',
            topic = 'orders_cdc',
            format = 'avro'
        )",
    )
    .expect_err("unknown format");
    assert!(
        err.to_string()
            .contains("unsupported replication pipeline format")
    );
}

#[test]
fn parse_create_postgres_cdc_source_from_connection_parts() {
    let stmt = parse_floe_statement(
        "CREATE SOURCE pg_main WITH (
            type = 'postgres_cdc',
            hostname = 'localhost',
            port = '55433',
            username = 'postgres',
            password = 'postgres',
            database.name = 'postgres',
            slot = 'floe_slot'
        )",
    )
    .expect("parse source");
    match stmt {
        FloeStatement::CreateSource(definition) => {
            let SourceConnector::PostgresCdc(options) = definition.connector();
            assert_eq!(
                options.connection(),
                "host=localhost port=55433 user=postgres dbname=postgres password=postgres"
            );
            assert_eq!(options.slot(), "floe_slot");
            assert_eq!(options.publication(), None);
        }
        other => panic!("expected CREATE SOURCE statement, got {other:?}"),
    }
}

#[test]
fn parse_create_table_statement() {
    let stmt = parse_floe_statement(
        "CREATE TABLE bids (id BIGINT PRIMARY KEY, price NUMERIC(15,2) NOT NULL, channel TEXT, shipdate DATE)",
    )
    .expect("parse table");
    match stmt {
        FloeStatement::CreateTable(definition) => {
            assert_eq!(definition.name(), "bids");
            assert_eq!(definition.columns().len(), 4);
            let id = &definition.columns()[0];
            assert_eq!(id.name(), "id");
            assert_eq!(id.data_type(), &SqlColumnType::Int64);
            assert!(!id.nullable());
            assert!(id.primary_key());
            assert_eq!(definition.columns()[1].data_type(), &SqlColumnType::Numeric);
            assert_eq!(
                definition.columns()[3].data_type(),
                &SqlColumnType::DateDays
            );
        }
        other => panic!("expected CREATE TABLE statement, got {other:?}"),
    }
}

#[test]
fn parse_source_backed_create_table_statement() {
    let stmt = parse_floe_statement(
        "CREATE TABLE orders (
            id BIGINT PRIMARY KEY,
            amount BIGINT NOT NULL,
            status TEXT
        ) FROM pg_main TABLE 'public.orders'",
    )
    .expect("parse source-backed table");
    match stmt {
        FloeStatement::CreateTable(definition) => {
            assert_eq!(definition.name(), "orders");
            assert_eq!(definition.columns().len(), 3);
            let source = definition.source().expect("source binding");
            assert_eq!(source.source_name(), "pg_main");
            assert_eq!(source.upstream_table(), "public.orders");
        }
        other => panic!("expected CREATE TABLE statement, got {other:?}"),
    }
}

#[test]
fn parse_source_backed_create_table_requires_primary_key() {
    let err = parse_floe_statement(
        "CREATE TABLE orders (
            id BIGINT,
            amount BIGINT NOT NULL
        ) FROM pg_main TABLE 'public.orders'",
    )
    .expect_err("source-backed table without primary key should fail");

    assert!(
        err.to_string()
            .contains("table orders must declare exactly one primary key column")
    );
}

#[test]
fn query_table_reference_extraction_handles_joins_subqueries_and_ctes() {
    let refs = referenced_table_names_in_query(
        "WITH recent AS (
            SELECT * FROM public.orders
        )
        SELECT *
        FROM recent r
        JOIN customers c ON r.customer_id = c.id
        JOIN (SELECT * FROM public.payments) p ON p.order_id = r.id",
    )
    .expect("table refs");

    assert_eq!(
        refs,
        ["customers", "public.orders", "public.payments"]
            .into_iter()
            .map(ToString::to_string)
            .collect()
    );
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
