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
fn parse_create_postgres_sink_statement() {
    let stmt = parse_floe_statement(
        "CREATE SINK out_orders FROM mv_orders WITH (
            connector = 'postgres',
            connection = 'postgres://postgres:postgres@localhost/postgres',
            table = 'public.orders_copy',
            type = 'upsert',
            primary_key = 'tenant_id,id',
            with_snapshot = true
        )",
    )
    .expect("parse sink");
    match stmt {
        FloeStatement::CreateSink(definition) => {
            assert_eq!(definition.name(), "out_orders");
            assert_eq!(definition.mv_name(), "mv_orders");
            assert!(definition.with_snapshot());
            assert_eq!(
                definition.connector(),
                &SinkConnector::Postgres {
                    connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
                    table: "public.orders_copy".to_string(),
                    mode: Some("upsert".to_string()),
                    primary_key: vec!["tenant_id".to_string(), "id".to_string()],
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
            schema.evolution = 'ignore-compatible',
            slot.create = false,
            publication.create = true
        )",
    )
    .expect("parse source");
    match stmt {
        FloeStatement::CreateSource(definition) => {
            assert_eq!(definition.name(), "pg_main");
            assert_eq!(
                definition.connector(),
                &SourceConnector::PostgresCdc(
                    PostgresCdcSourceOptions::new_with_setup_policy(
                        "postgres://postgres:postgres@localhost/postgres",
                        "floe_slot",
                        Some("floe_pub".to_string()),
                        Some(true),
                        PostgresCdcSchemaEvolutionPolicy::IgnoreCompatible,
                        false,
                        true,
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
            transaction_metadata = true,
            error.policy = 'dead-letter-and-continue',
            error.max_retries = 3
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
                definition.error_policy().mode(),
                ReplicationErrorPolicyMode::DeadLetterAndContinue
            );
            assert_eq!(definition.error_policy().max_retries(), Some(3));
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
    assert_eq!(
        definition.error_policy().mode(),
        ReplicationErrorPolicyMode::RetryWithBackoff
    );
    assert_eq!(definition.error_policy().max_retries(), None);
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
            let SourceConnector::PostgresCdc(options) = definition.connector() else {
                panic!("expected Postgres CDC source");
            };
            assert_eq!(
                options.connection(),
                "host=localhost port=55433 user=postgres dbname=postgres password=postgres"
            );
            assert_eq!(options.slot(), "floe_slot");
            assert_eq!(options.publication(), None);
            assert!(options.auto_create_slot());
            assert!(options.auto_create_publication());
        }
        other => panic!("expected CREATE SOURCE statement, got {other:?}"),
    }
}

#[test]
fn parse_create_kafka_source_statement() {
    let stmt = parse_floe_statement(
        "CREATE SOURCE bids WITH (
            connector = 'kafka',
            brokers = 'localhost:9092',
            topics = 'nexmark_bid,nexmark_bid_retry',
            group_id = 'floe_sql',
            default_source = 'nexmark_bid',
            poll_ms = 5,
            max_messages_per_tick = 1024,
            format = 'debezium-json'
        )",
    )
    .expect("parse source");

    let FloeStatement::CreateSource(definition) = stmt else {
        panic!("expected CREATE SOURCE statement");
    };
    assert_eq!(definition.name(), "bids");
    assert_eq!(
        definition.connector(),
        &SourceConnector::Kafka(
            KafkaSourceOptions::new(
                "localhost:9092",
                vec!["nexmark_bid".to_string(), "nexmark_bid_retry".to_string()],
                Some("floe_sql".to_string()),
                Some("nexmark_bid".to_string()),
                Some(5),
                Some(1024),
                Some("debezium_json".to_string()),
            )
            .expect("options")
        )
    );
}

#[test]
fn parse_create_source_with_inline_schema_and_format_encode() {
    let stmt = parse_floe_statement(
        "CREATE SOURCE orders (
            id BIGINT PRIMARY KEY,
            amount NUMERIC(15,2),
            status TEXT,
            created_at TIMESTAMP
        )
        WITH (
            connector = 'kafka',
            brokers = 'localhost:9092',
            topic = 'orders'
        )
        FORMAT PLAIN ENCODE JSON",
    )
    .expect("parse source");

    let FloeStatement::CreateSource(definition) = stmt else {
        panic!("expected CREATE SOURCE statement");
    };
    assert_eq!(definition.name(), "orders");
    assert_eq!(definition.columns().len(), 4);
    assert_eq!(definition.columns()[0].name(), "id");
    assert!(definition.columns()[0].primary_key());
    assert!(!definition.columns()[0].nullable());
    assert_eq!(
        definition.columns()[1].data_type(),
        &SqlColumnType::Decimal128 {
            precision: 15,
            scale: 2,
        }
    );
    assert_eq!(
        definition.connector(),
        &SourceConnector::Kafka(
            KafkaSourceOptions::new(
                "localhost:9092",
                vec!["orders".to_string()],
                None,
                None,
                None,
                None,
                Some("floe_json".to_string()),
            )
            .expect("options")
        )
    );
}

#[test]
fn parse_create_source_if_not_exists_with_risingwave_kafka_options() {
    let stmt = parse_floe_statement(
        "CREATE SOURCE IF NOT EXISTS orders (
            id BIGINT,
            amount BIGINT,
            PRIMARY KEY (id)
        )
        WITH (
            connector = 'kafka',
            topic = 'orders',
            properties.bootstrap.server = 'localhost:9092',
            scan.startup.mode = 'earliest',
            properties.fetch.wait.max.ms = '1',
            properties.fetch.queue.backoff.ms = '1',
            properties.fetch.min.bytes = '1'
        )
        FORMAT PLAIN ENCODE JSON",
    )
    .expect("parse source");

    let FloeStatement::CreateSource(definition) = stmt else {
        panic!("expected CREATE SOURCE statement");
    };
    assert!(definition.if_not_exists());
    assert_eq!(definition.columns().len(), 2);
    assert!(definition.columns()[0].primary_key());
    assert_eq!(
        definition.connector(),
        &SourceConnector::Kafka(
            KafkaSourceOptions::new(
                "localhost:9092",
                vec!["orders".to_string()],
                None,
                None,
                None,
                None,
                Some("floe_json".to_string()),
            )
            .expect("options")
        )
    );
}

#[test]
fn parse_create_source_rejects_not_null_columns() {
    let err = parse_floe_statement(
        "CREATE SOURCE orders (
            id BIGINT PRIMARY KEY,
            status TEXT NOT NULL
        )
        WITH (
            connector = 'kafka',
            brokers = 'localhost:9092',
            topic = 'orders'
        )
        FORMAT PLAIN ENCODE JSON",
    )
    .expect_err("NOT NULL source column should fail");
    assert!(
        err.to_string()
            .contains("CREATE SOURCE schemas do not support NOT NULL")
    );
}

#[test]
fn parse_create_source_rejects_risingwave_clauses_floe_does_not_support() {
    let cases = [
        (
            "CREATE SOURCE orders (
                id BIGINT,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts
            )
            WITH (connector = 'kafka', brokers = 'localhost:9092', topic = 'orders')
            FORMAT PLAIN ENCODE JSON",
            "WATERMARK clauses are not supported",
        ),
        (
            "CREATE SOURCE orders (
                id BIGINT
            )
            INCLUDE KEY AS key
            WITH (connector = 'kafka', brokers = 'localhost:9092', topic = 'orders')
            FORMAT PLAIN ENCODE JSON",
            "INCLUDE clauses are not supported",
        ),
        (
            "CREATE SOURCE orders (
                *,
                gen_id BIGINT AS id + 1
            )
            WITH (connector = 'kafka', brokers = 'localhost:9092', topic = 'orders')
            FORMAT PLAIN ENCODE JSON",
            "'*' schemas require external schema discovery",
        ),
        (
            "CREATE SOURCE orders (
                id BIGINT AS id + 1
            )
            WITH (connector = 'kafka', brokers = 'localhost:9092', topic = 'orders')
            FORMAT PLAIN ENCODE JSON",
            "generated/default columns are not supported",
        ),
    ];

    for (sql, expected) in cases {
        let err = parse_floe_statement(sql).expect_err("unsupported source clause should fail");
        assert!(
            err.to_string().contains(expected),
            "expected error containing {expected:?}, got {err}"
        );
    }
}

#[test]
fn parse_create_source_rejects_unsupported_risingwave_kafka_options() {
    let err = parse_floe_statement(
        "CREATE SOURCE orders (
            id BIGINT PRIMARY KEY
        )
        WITH (
            connector = 'kafka',
            topic = 'orders',
            properties.bootstrap.server = 'localhost:9092',
            scan.startup.mode = 'latest'
        )
        FORMAT PLAIN ENCODE JSON",
    )
    .expect_err("unsupported startup mode should fail");
    assert!(
        err.to_string()
            .contains("supports only scan.startup.mode = 'earliest'")
    );
}

#[test]
fn parse_create_source_rejects_unsupported_format_encode() {
    let err = parse_floe_statement(
        "CREATE SOURCE orders (
            id BIGINT PRIMARY KEY
        )
        WITH (
            connector = 'kafka',
            brokers = 'localhost:9092',
            topic = 'orders'
        )
        FORMAT UPSERT ENCODE JSON",
    )
    .expect_err("UPSERT source format should fail");
    assert!(err.to_string().contains("unsupported source FORMAT"));
}

#[test]
fn parse_create_file_http_generator_and_object_store_sources() {
    let statements = parse_floe_program(
        "
        CREATE SOURCE file_bid WITH (
            connector = 'file',
            path = '/tmp/events.jsonl',
            default_source = 'nexmark_bid'
        );
        CREATE SOURCE http_bid WITH (
            connector = 'http',
            host = '127.0.0.1',
            port = 8080,
            default_source = 'nexmark_bid'
        );
        CREATE SOURCE gen WITH (
            connector = 'generator',
            events_per_second = 12.5,
            max_events = 100
        );
        CREATE SOURCE object_bid WITH (
            connector = 'object-store',
            url = 's3://bucket/events/',
            default_source = 'nexmark_bid'
        );
        ",
    )
    .expect("parse sources");

    assert!(matches!(
        statements[0],
        FloeStatement::CreateSource(CreateSourceDefinition { .. })
    ));
    let FloeStatement::CreateSource(file) = &statements[0] else {
        panic!("expected file source");
    };
    assert_eq!(
        file.connector(),
        &SourceConnector::File(
            FileSourceOptions::new("/tmp/events.jsonl", Some("nexmark_bid".to_string()))
                .expect("options")
        )
    );
    let FloeStatement::CreateSource(http) = &statements[1] else {
        panic!("expected http source");
    };
    assert_eq!(
        http.connector(),
        &SourceConnector::Http(
            HttpSourceOptions::new(
                Some("127.0.0.1".to_string()),
                8080,
                Some("nexmark_bid".to_string())
            )
            .expect("options")
        )
    );
    let FloeStatement::CreateSource(generator) = &statements[2] else {
        panic!("expected generator source");
    };
    assert_eq!(
        generator.connector(),
        &SourceConnector::Generator(
            GeneratorSourceOptions::new(Some(12.5), Some(100)).expect("options")
        )
    );
    let FloeStatement::CreateSource(object_store) = &statements[3] else {
        panic!("expected object store source");
    };
    assert_eq!(
        object_store.connector(),
        &SourceConnector::ObjectStore(
            ObjectStoreSourceOptions::new("s3://bucket/events/", Some("nexmark_bid".to_string()))
                .expect("options")
        )
    );
}

#[test]
fn parse_create_sink_statement_preserves_runtime_options() {
    let stmt = parse_floe_statement(
        "CREATE SINK out_bid FROM mv_bid WITH (
            type = 'kafka',
            brokers = 'localhost:9092',
            topic = 'mv_bid',
            batch_rows = 100,
            batch_bytes = 65536,
            queue_capacity = 32,
            retry_max_attempts = 7,
            retry_base_ms = 10,
            retry_max_backoff_ms = 500,
            transactional_id = 'tx-out-bid',
            checkpoint_topic = 'floe-checkpoints',
            checkpoint_partition = 2
        )",
    )
    .expect("parse sink");

    let FloeStatement::CreateSink(definition) = stmt else {
        panic!("expected CREATE SINK statement");
    };
    assert_eq!(definition.options().batch_rows, Some(100));
    assert_eq!(definition.options().batch_bytes, Some(65_536));
    assert_eq!(definition.options().queue_capacity, Some(32));
    assert_eq!(definition.options().retry_max_attempts, Some(7));
    assert_eq!(definition.options().retry_base_ms, Some(10));
    assert_eq!(definition.options().retry_max_backoff_ms, Some(500));
    assert_eq!(
        definition.options().transactional_id.as_deref(),
        Some("tx-out-bid")
    );
    assert_eq!(
        definition.options().checkpoint_topic.as_deref(),
        Some("floe-checkpoints")
    );
    assert_eq!(definition.options().checkpoint_partition, Some(2));
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
            assert_eq!(
                definition.columns()[1].data_type(),
                &SqlColumnType::Decimal128 {
                    precision: 15,
                    scale: 2
                }
            );
            assert_eq!(
                definition.columns()[3].data_type(),
                &SqlColumnType::DateDays
            );
        }
        other => panic!("expected CREATE TABLE statement, got {other:?}"),
    }
}

#[test]
fn parse_numeric_precision_and_unbounded_numeric_types() {
    let stmt = parse_floe_statement(
        "CREATE TABLE metrics (id BIGINT PRIMARY KEY, exact_amount NUMERIC(12,2), whole_amount NUMERIC(12), freeform NUMERIC)",
    )
    .expect("parse numeric table");
    match stmt {
        FloeStatement::CreateTable(definition) => {
            assert_eq!(
                definition.columns()[1].data_type(),
                &SqlColumnType::Decimal128 {
                    precision: 12,
                    scale: 2
                }
            );
            assert_eq!(
                definition.columns()[2].data_type(),
                &SqlColumnType::Decimal128 {
                    precision: 12,
                    scale: 0
                }
            );
            assert_eq!(definition.columns()[3].data_type(), &SqlColumnType::Numeric);
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
        SUBSCRIBE mv_bid WITH SNAPSHOT;
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
        Some(FloeStatement::Subscribe { .. })
    ));
}

#[test]
fn parse_floe_statement_rejects_multi_statement_input() {
    let err = parse_floe_statement("SUBSCRIBE mv; SUBSCRIBE mv2").unwrap_err();
    assert!(err.to_string().contains("exactly one statement"));
}

#[test]
fn parse_floe_statement_rejects_tail() {
    let err = parse_floe_statement("TAIL mv_orders").expect_err("tail should not parse");
    assert!(
        err.to_string()
            .contains("unsupported SQL statement: TAIL mv_orders")
    );
}

#[test]
fn parse_subscribe_variants() {
    let stmt = parse_floe_statement("SUBSCRIBE mv_orders").expect("parse subscribe");
    assert_eq!(
        stmt,
        FloeStatement::Subscribe {
            mv_name: "mv_orders".to_string(),
            with_snapshot: false,
            as_of: None,
        }
    );

    let stmt = parse_floe_statement("SUBSCRIBE mv_orders WITH SNAPSHOT AS OF 42")
        .expect("parse subscribe");
    assert_eq!(
        stmt,
        FloeStatement::Subscribe {
            mv_name: "mv_orders".to_string(),
            with_snapshot: true,
            as_of: Some(42),
        }
    );
}
