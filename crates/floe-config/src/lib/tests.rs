use super::*;
use floe_sql_parser::{SinkConnector, SinkDefinition};
use std::path::PathBuf;

fn write_temp_config(extension: &str, contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    path.push(format!(
        "floe-config-test-{}-{suffix}.{extension}",
        std::process::id()
    ));
    std::fs::write(&path, contents).expect("write temp config");
    path
}

#[test]
fn normalize_assigns_unique_names() {
    let configs = vec![
        ConnectorConfig::Kafka {
            name: None,
            brokers: "localhost:9092".to_string(),
            topics: vec!["a".to_string()],
            group_id: None,
            default_source: None,
            poll_ms: None,
            max_messages_per_tick: None,
            format: None,
        },
        ConnectorConfig::Kafka {
            name: None,
            brokers: "localhost:9092".to_string(),
            topics: vec!["b".to_string()],
            group_id: None,
            default_source: None,
            poll_ms: None,
            max_messages_per_tick: None,
            format: None,
        },
    ];
    let specs = normalize_connectors(configs).expect("normalize");
    assert_eq!(specs[0].name, "kafka");
    assert_eq!(specs[1].name, "kafka_2");
}

#[test]
fn load_config_accepts_toml() {
    let input = r#"
            [[connectors]]
            type = "generator"
            events_per_second = 12.5
            max_events = 100
        "#;
    let config: NodeConfig = toml::from_str(input).expect("parse toml");
    assert_eq!(config.connectors.len(), 1);
}

#[test]
fn load_config_accepts_json_file() {
    let path = write_temp_config(
        "json",
        r#"{"connectors":[{"type":"generator","events_per_second":12.5,"max_events":100}]}"#,
    );

    let config = load_config(&path).expect("load json config");
    std::fs::remove_file(&path).expect("remove temp config");

    assert_eq!(config.connectors.len(), 1);
}

#[test]
fn load_config_accepts_yaml_file() {
    let path = write_temp_config(
        "yaml",
        r#"
connectors:
  - type: generator
    events_per_second: 12.5
    max_events: 100
"#,
    );

    let config = load_config(&path).expect("load yaml config");
    std::fs::remove_file(&path).expect("remove temp config");

    assert_eq!(config.connectors.len(), 1);
}

#[test]
fn load_config_rejects_unknown_extension() {
    let path = write_temp_config("conf", "connectors = []");

    let err = load_config(&path).expect_err("unknown config extension should fail");
    std::fs::remove_file(&path).expect("remove temp config");

    assert!(err.to_string().contains("unsupported config extension"));
}

#[test]
fn parse_toml_config_accepts_multiline_sql() {
    let input = r#"
            [[materialized_views]]
            name = "mv_orders"
            query = '''
            CREATE MATERIALIZED VIEW mv_orders AS
            SELECT customer_id, count(*) AS order_count
            FROM orders
            GROUP BY customer_id
            '''
        "#;

    let config = parse_toml_config(input).expect("parse toml config");

    assert_eq!(config.materialized_views.len(), 1);
    assert!(
        config.materialized_views[0]
            .query
            .contains("GROUP BY customer_id")
    );
}

#[test]
fn maps_sql_sink_definition_to_runtime_config() {
    let definition = SinkDefinition::new(
        "out_http",
        "mv_bid",
        SinkConnector::Http {
            url: "http://localhost:8080".to_string(),
            batch_size: Some(16),
        },
        true,
        Some(7),
    );
    let spec = sink_spec_from_sql(&definition).expect("map sink");
    match spec.config {
        SinkConfig::Http {
            name,
            url,
            mv,
            with_snapshot,
            as_of,
            batch_size,
            ..
        } => {
            assert_eq!(name.as_deref(), Some("out_http"));
            assert_eq!(url, "http://localhost:8080");
            assert_eq!(mv, "mv_bid");
            assert_eq!(with_snapshot, Some(true));
            assert_eq!(as_of, Some(7));
            assert_eq!(batch_size, Some(16));
        }
        other => panic!("expected HTTP sink config, got {other:?}"),
    }
}

#[test]
fn maps_sql_kafka_debezium_sink_options_to_runtime_config() {
    let definition = SinkDefinition::new(
        "out_orders",
        "mv_orders",
        SinkConnector::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders".to_string(),
            format: Some("debezium_json".to_string()),
            key_columns: vec!["tenant_id".to_string(), "id".to_string()],
        },
        false,
        None,
    );
    let spec = sink_spec_from_sql(&definition).expect("map sink");
    match spec.config {
        SinkConfig::Kafka {
            name,
            brokers,
            topic,
            mv,
            format,
            key_columns,
            ..
        } => {
            assert_eq!(name.as_deref(), Some("out_orders"));
            assert_eq!(brokers, "localhost:9092");
            assert_eq!(topic, "orders");
            assert_eq!(mv, "mv_orders");
            assert_eq!(format.as_deref(), Some("debezium_json"));
            assert_eq!(
                key_columns,
                Some(vec!["tenant_id".to_string(), "id".to_string()])
            );
        }
        other => panic!("expected Kafka sink config, got {other:?}"),
    }
}

#[test]
fn maps_sql_postgres_sink_options_to_runtime_config() {
    let definition = SinkDefinition::new(
        "out_orders",
        "mv_orders",
        SinkConnector::Postgres {
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
            mode: Some("upsert".to_string()),
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
        },
        true,
        Some(9),
    );
    let spec = sink_spec_from_sql(&definition).expect("map sink");
    match spec.config {
        SinkConfig::Postgres {
            name,
            connection,
            table,
            mv,
            mode,
            primary_key,
            with_snapshot,
            as_of,
            ..
        } => {
            assert_eq!(name.as_deref(), Some("out_orders"));
            assert_eq!(
                connection,
                "postgres://postgres:postgres@localhost/postgres"
            );
            assert_eq!(table, "public.orders_copy");
            assert_eq!(mv, "mv_orders");
            assert_eq!(mode.as_deref(), Some("upsert"));
            assert_eq!(
                primary_key,
                Some(vec!["tenant_id".to_string(), "id".to_string()])
            );
            assert_eq!(with_snapshot, Some(true));
            assert_eq!(as_of, Some(9));
        }
        other => panic!("expected Postgres sink config, got {other:?}"),
    }
}

#[test]
fn validation_rejects_empty_kafka_topics() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::Kafka {
            name: Some("kafka_ingest".to_string()),
            brokers: "localhost:9092".to_string(),
            topics: vec![],
            group_id: Some("floe".to_string()),
            default_source: Some("nexmark_bid".to_string()),
            poll_ms: Some(100),
            max_messages_per_tick: Some(64),
            format: None,
        }],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("connectors[0].topics must not be empty")
    );
}

#[test]
fn validation_rejects_invalid_object_store_url() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::ObjectStore {
            name: None,
            url: "not a url".to_string(),
            default_source: Some("nexmark_bid".to_string()),
        }],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("connectors[0].url must be a valid URL")
    );
}

#[test]
fn validation_rejects_unknown_kafka_format() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::Kafka {
            name: Some("kafka_ingest".to_string()),
            brokers: "localhost:9092".to_string(),
            topics: vec!["events".to_string()],
            group_id: Some("floe".to_string()),
            default_source: Some("nexmark_bid".to_string()),
            poll_ms: Some(100),
            max_messages_per_tick: Some(64),
            format: Some("bad_format".to_string()),
        }],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("connectors[0].format must be one of")
    );
}

#[test]
fn validation_rejects_non_positive_watermark_idle_source_ms() {
    let config = NodeConfig {
        runtime: RuntimeConfig {
            watermark_idle_source_ms: Some(0),
            ..RuntimeConfig::default()
        },
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("runtime.watermark_idle_source_ms must be greater than 0")
    );
}

#[test]
fn validation_rejects_non_positive_mv_flush_max_pending_deltas() {
    let config = NodeConfig {
        runtime: RuntimeConfig {
            mv_flush: MvFlushConfig {
                max_pending_deltas: Some(0),
                ..MvFlushConfig::default()
            },
            ..RuntimeConfig::default()
        },
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("runtime.mv_flush.max_pending_deltas must be greater than 0")
    );
}

#[test]
fn validation_rejects_invalid_http_sink_url() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::Generator {
            name: None,
            events_per_second: Some(1.0),
            max_events: None,
        }],
        sinks: vec![SinkConfig::Http {
            name: Some("sink_http".to_string()),
            url: "://missing-scheme".to_string(),
            mv: "mv_bid".to_string(),
            with_snapshot: Some(true),
            as_of: None,
            batch_size: Some(1),
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
        }],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(err.to_string().contains("sinks[0].url must be a valid URL"));
}

#[test]
fn validation_rejects_negative_kafka_checkpoint_partition() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::Generator {
            name: None,
            events_per_second: Some(1.0),
            max_events: None,
        }],
        sinks: vec![SinkConfig::Kafka {
            name: Some("sink_kafka".to_string()),
            brokers: "localhost:9092".to_string(),
            topic: "out".to_string(),
            mv: "mv_bid".to_string(),
            format: None,
            key_columns: None,
            with_snapshot: Some(false),
            as_of: None,
            batch_rows: Some(1),
            batch_bytes: None,
            queue_capacity: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
            transactional_id: Some("tx-1".to_string()),
            checkpoint_topic: Some("out_checkpoint".to_string()),
            checkpoint_partition: Some(-1),
        }],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("sinks[0].checkpoint_partition must be >= 0")
    );
}

#[test]
fn validation_requires_key_columns_for_debezium_kafka_sink() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::Generator {
            name: None,
            events_per_second: Some(1.0),
            max_events: None,
        }],
        sinks: vec![SinkConfig::Kafka {
            name: Some("sink_kafka".to_string()),
            brokers: "localhost:9092".to_string(),
            topic: "out".to_string(),
            mv: "mv_bid".to_string(),
            format: Some("debezium_json".to_string()),
            key_columns: None,
            with_snapshot: Some(false),
            as_of: None,
            batch_rows: Some(1),
            batch_bytes: None,
            queue_capacity: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
            transactional_id: None,
            checkpoint_topic: None,
            checkpoint_partition: None,
        }],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(err.to_string().contains("sinks[0].key_columns is required"));
}

#[test]
fn validation_requires_primary_key_for_postgres_upsert_sink() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::Generator {
            name: None,
            events_per_second: Some(1.0),
            max_events: None,
        }],
        sinks: vec![SinkConfig::Postgres {
            name: Some("sink_pg".to_string()),
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
            mv: "mv_orders".to_string(),
            mode: Some("upsert".to_string()),
            primary_key: None,
            with_snapshot: Some(false),
            as_of: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
        }],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(err.to_string().contains("sinks[0].primary_key is required"));
}

#[test]
fn validation_rejects_non_positive_replication_encoding_rows_per_record() {
    let config = NodeConfig {
        replication: ReplicationConfig {
            encoding: ReplicationEncodingConfig {
                arrow_ipc_rows_per_record: 0,
                ..ReplicationEncodingConfig::default()
            },
            ..ReplicationConfig::default()
        },
        ..NodeConfig::default()
    };

    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("replication.encoding.arrow_ipc_rows_per_record must be greater than 0")
    );
}

#[test]
fn validation_rejects_invalid_postgres_cdc_snapshot_watermark() {
    let config = NodeConfig {
        postgres_cdc: PostgresCdcConfig {
            snapshot: PostgresCdcSnapshotConfig {
                wal_buffer_high_watermark_percent: 0,
                ..PostgresCdcSnapshotConfig::default()
            },
            ..PostgresCdcConfig::default()
        },
        ..NodeConfig::default()
    };

    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(err.to_string().contains(
        "postgres_cdc.snapshot.wal_buffer_high_watermark_percent must be between 1 and 100"
    ));
}

#[test]
fn load_config_accepts_materialized_views_and_runtime_sections() {
    let input = r#"
            [[connectors]]
            type = "generator"

            [[materialized_views]]
            name = "mv_cfg"
            query = "SELECT * FROM nexmark_bid"

            [runtime]
            ingest_batch_size = 128
            mv_retain_last = 5
            admin_port = 8082
            pgwire_addr = "127.0.0.1:6543"
            pgwire_enabled = false
            subscribe_channel_capacity = 512
            subscribe_max_catchup_versions = 64

            [runtime.mv_snapshot]
            max_pending_batches = 2048
            max_pending_rows = 500000
            max_delay_ms = 2000

            [storage]
            await_durable = true
            data_dir = "/tmp/floe-data"
            source_journal = "auto"
            slatedb_close_timeout_ms = 1000
            zset_compaction_max_chain_len = 64

            [maintenance]
            paused = true
            inspect_namespace = ["mv::mv_cfg"]
        "#;
    let config: NodeConfig = toml::from_str(input).expect("parse toml");
    assert_eq!(config.materialized_views.len(), 1);
    assert_eq!(config.runtime.ingest_batch_size, Some(128));
    assert_eq!(config.runtime.mv_snapshot.max_pending_batches, Some(2048));
    assert_eq!(config.runtime.mv_snapshot.max_pending_rows, Some(500000));
    assert_eq!(config.runtime.mv_snapshot.max_delay_ms, Some(2000));
    assert_eq!(config.runtime.admin_port, Some(8082));
    assert_eq!(
        config.runtime.pgwire_addr.as_deref(),
        Some("127.0.0.1:6543")
    );
    assert_eq!(config.runtime.pgwire_enabled, Some(false));
    assert_eq!(config.runtime.subscribe_channel_capacity, Some(512));
    assert_eq!(config.runtime.subscribe_max_catchup_versions, Some(64));
    assert_eq!(config.storage.await_durable, Some(true));
    assert_eq!(config.storage.data_dir.as_deref(), Some("/tmp/floe-data"));
    assert_eq!(
        config.storage.source_journal,
        Some(SourceJournalConfig::Auto)
    );
    assert_eq!(config.storage.slatedb_close_timeout_ms, Some(1000));
    assert_eq!(config.maintenance.paused, Some(true));
}

#[test]
fn load_config_accepts_object_store_storage_section() {
    let input = r#"
            [storage]
            object_store_from_env = true
            object_store_env_file = "/tmp/object-store.env"
            slatedb_name = "floe-test"
        "#;
    let config: NodeConfig = toml::from_str(input).expect("parse toml");
    validate_node_config(&config).expect("valid object-store config");
    assert!(config.storage.object_store_from_env);
    assert_eq!(
        config.storage.object_store_env_file.as_deref(),
        Some("/tmp/object-store.env")
    );
    assert_eq!(config.storage.slatedb_name.as_deref(), Some("floe-test"));
}

#[test]
fn load_config_accepts_replication_buffer_cleanup_section() {
    let input = r#"
            [replication.buffer_cleanup]
            delivered_retention_ms = 1000
            orphan_retention_ms = 5000
            cleanup_interval_ms = 250
        "#;

    let config = parse_toml_config(input).expect("parse toml");

    assert_eq!(
        config.replication.buffer_cleanup.delivered_retention_ms,
        1000
    );
    assert_eq!(config.replication.buffer_cleanup.orphan_retention_ms, 5000);
    assert_eq!(config.replication.buffer_cleanup.cleanup_interval_ms, 250);
}

#[test]
fn load_config_accepts_replication_buffer_limits_section() {
    let input = r#"
            [replication.buffer_limits]
            max_pending_bytes = 123
            max_pending_records = 456
            max_pending_transactions = 7
            max_pending_age_ms = 89
        "#;

    let config = parse_toml_config(input).expect("parse toml");

    assert_eq!(config.replication.buffer_limits.max_pending_bytes, 123);
    assert_eq!(config.replication.buffer_limits.max_pending_records, 456);
    assert_eq!(config.replication.buffer_limits.max_pending_transactions, 7);
    assert_eq!(config.replication.buffer_limits.max_pending_age_ms, 89);
}

#[test]
fn replication_buffer_defaults_are_object_store_oriented() {
    let cleanup = ReplicationBufferCleanupConfig::default();
    assert_eq!(
        cleanup.delivered_retention_ms,
        DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS
    );
    assert_eq!(
        cleanup.orphan_retention_ms,
        DEFAULT_REPLICATION_BUFFER_ORPHAN_RETENTION_MS
    );
    assert_eq!(
        cleanup.cleanup_interval_ms,
        DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS
    );
    assert!(cleanup.cleanup_interval_ms >= 500);
    assert!(cleanup.delivered_retention_ms >= 500);
    assert!(cleanup.orphan_retention_ms >= cleanup.cleanup_interval_ms);

    let limits = ReplicationBufferLimitsConfig::default();
    assert_eq!(
        limits.max_pending_bytes,
        DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES
    );
    assert!(limits.max_pending_bytes >= 1024 * 1024);

    let encoding = ReplicationEncodingConfig::default();
    assert_eq!(
        encoding.arrow_ipc_rows_per_record,
        DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD
    );
    assert_eq!(
        encoding.snapshot_batches_per_chunk,
        DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK
    );
    assert!(encoding.arrow_ipc_rows_per_record >= 1024);
    assert!(encoding.snapshot_batches_per_chunk >= 1);
}

#[test]
fn load_config_accepts_replication_kafka_section() {
    let input = r#"
            [replication.kafka]
            message_max_bytes = 2000000
            acks = "all"
            enable_idempotence = true
            batch_size = 300000
            batch_num_messages = 400000
            linger_ms = 2
            queue_max_messages = 500000
            queue_max_kbytes = 600000
            message_send_max_retries = 3
        "#;

    let config = parse_toml_config(input).expect("parse toml");

    assert_eq!(config.replication.kafka.message_max_bytes, 2_000_000);
    assert_eq!(config.replication.kafka.acks, "all");
    assert!(config.replication.kafka.enable_idempotence);
    assert_eq!(config.replication.kafka.batch_size, 300_000);
    assert_eq!(config.replication.kafka.batch_num_messages, 400_000);
    assert_eq!(config.replication.kafka.linger_ms, 2);
    assert_eq!(config.replication.kafka.queue_max_messages, 500_000);
    assert_eq!(config.replication.kafka.queue_max_kbytes, 600_000);
    assert_eq!(config.replication.kafka.message_send_max_retries, 3);
}

#[test]
fn load_config_accepts_replication_encoding_section() {
    let input = r#"
            [replication]
            perf_log = true

            [replication.encoding]
            arrow_ipc_rows_per_record = 2048
            snapshot_batches_per_chunk = 4
            arrow_ipc_compression = "lz4_frame"
            kafka_metadata_headers = true
        "#;

    let config = parse_toml_config(input).expect("parse toml");

    assert!(config.replication.perf_log);
    assert_eq!(config.replication.encoding.arrow_ipc_rows_per_record, 2048);
    assert_eq!(config.replication.encoding.snapshot_batches_per_chunk, 4);
    assert_eq!(
        config.replication.encoding.arrow_ipc_compression,
        Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
    );
    assert!(config.replication.encoding.kafka_metadata_headers);
}

#[test]
fn load_config_accepts_postgres_cdc_snapshot_section() {
    let input = r#"
            [postgres_cdc.snapshot]
            rows_per_batch = 8192
            max_workers = 4
            intra_table_chunks = 8
            adaptive_concurrency = false
            min_workers = 2
            wal_buffer_high_watermark_percent = 80
            wal_buffer_low_watermark_percent = 20
            slow_scan_ms = 12000
            controller_interval_ms = 250
            perf_log = true
        "#;

    let config = parse_toml_config(input).expect("parse toml");
    let snapshot = config.postgres_cdc.snapshot;

    assert_eq!(snapshot.rows_per_batch, 8192);
    assert_eq!(snapshot.max_workers, 4);
    assert_eq!(snapshot.intra_table_chunks, 8);
    assert!(!snapshot.adaptive_concurrency);
    assert_eq!(snapshot.min_workers, 2);
    assert_eq!(snapshot.wal_buffer_high_watermark_percent, 80);
    assert_eq!(snapshot.wal_buffer_low_watermark_percent, 20);
    assert_eq!(snapshot.slow_scan_ms, 12_000);
    assert_eq!(snapshot.controller_interval_ms, 250);
    assert!(snapshot.perf_log);
}

#[test]
fn load_config_accepts_postgres_cdc_reconnect_section() {
    let input = r#"
            [postgres_cdc.reconnect]
            max_reconnects = 3
            retry_base_ms = 250
            retry_max_backoff_ms = 4000
        "#;

    let config = parse_toml_config(input).expect("parse toml");
    let reconnect = config.postgres_cdc.reconnect;

    assert_eq!(reconnect.max_reconnects, 3);
    assert_eq!(reconnect.retry_base_ms, 250);
    assert_eq!(reconnect.retry_max_backoff_ms, 4_000);
}

#[test]
fn validation_rejects_invalid_postgres_cdc_reconnect_backoff() {
    let config = NodeConfig {
        postgres_cdc: PostgresCdcConfig {
            reconnect: PostgresCdcReconnectConfig {
                retry_base_ms: 5_000,
                retry_max_backoff_ms: 1_000,
                ..PostgresCdcReconnectConfig::default()
            },
            ..PostgresCdcConfig::default()
        },
        ..NodeConfig::default()
    };

    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(err.to_string().contains(
            "postgres_cdc.reconnect.retry_max_backoff_ms must be >= postgres_cdc.reconnect.retry_base_ms"
        ));
}

#[test]
fn load_config_accepts_postgres_cdc_setup_policy() {
    let input = r#"
            [[connectors]]
            type = "postgres_cdc"
            connection = "postgres://postgres:postgres@localhost/postgres"
            slot = "floe_slot"
            publication = "floe_pub"
            auto_create_slot = false
            auto_create_publication = true
        "#;

    let config = parse_toml_config(input).expect("parse toml");
    let ConnectorConfig::PostgresCdc {
        auto_create_slot,
        auto_create_publication,
        ..
    } = &config.connectors[0]
    else {
        panic!("expected postgres cdc connector");
    };

    assert_eq!(*auto_create_slot, Some(false));
    assert_eq!(*auto_create_publication, Some(true));
}

#[test]
fn replication_arrow_ipc_compression_config_parses_alias_values() {
    assert_eq!(
        ReplicationArrowIpcCompressionConfig::parse("lz4"),
        Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
    );
    assert_eq!(
        ReplicationArrowIpcCompressionConfig::parse("lz4-frame"),
        Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
    );
    assert_eq!(ReplicationArrowIpcCompressionConfig::parse("none"), None);
    assert_eq!(ReplicationArrowIpcCompressionConfig::parse("bogus"), None);
}

#[test]
fn validation_rejects_duplicate_materialized_view_names() {
    let config = NodeConfig {
        connectors: vec![ConnectorConfig::Generator {
            name: None,
            events_per_second: Some(1.0),
            max_events: None,
        }],
        materialized_views: vec![
            MaterializedViewConfig {
                name: "mv_dup".to_string(),
                query: "SELECT 1".to_string(),
                if_not_exists: false,
            },
            MaterializedViewConfig {
                name: "mv_dup".to_string(),
                query: "SELECT 2".to_string(),
                if_not_exists: false,
            },
        ],
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("duplicate materialized view name 'mv_dup'")
    );
}

#[test]
fn validation_rejects_non_positive_mv_snapshot_max_pending_batches() {
    let config = NodeConfig {
        runtime: RuntimeConfig {
            mv_snapshot: MvSnapshotConfig {
                max_pending_batches: Some(0),
                ..MvSnapshotConfig::default()
            },
            ..RuntimeConfig::default()
        },
        ..NodeConfig::default()
    };
    let err = validate_node_config(&config).expect_err("validation should fail");
    assert!(
        err.to_string()
            .contains("runtime.mv_snapshot.max_pending_batches must be greater than 0")
    );
}

#[test]
fn materialized_view_definitions_from_config_maps_fields() {
    let config_views = vec![MaterializedViewConfig {
        name: "mv_cfg".to_string(),
        query: "SELECT * FROM nexmark_bid".to_string(),
        if_not_exists: true,
    }];
    let definitions = materialized_view_definitions_from_config(&config_views);
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name(), "mv_cfg");
    assert_eq!(definitions[0].query(), "SELECT * FROM nexmark_bid");
    assert!(definitions[0].if_not_exists());
}
