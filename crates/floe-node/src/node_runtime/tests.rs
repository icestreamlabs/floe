use super::*;
use crate::node_runtime::orchestration::{
    merge_catalog_source_connectors, postgres_cdc_runtime_plan, source_journal_required_sources,
    validate_materialized_views_do_not_query_raw_cdc_sources,
};
use floe_sql_parser::parse_floe_statement;
use serde_json::json;

fn default_run_args() -> cli::RunArgs {
    cli::RunArgs {
        events_per_second: DEFAULT_EVENTS_PER_SECOND,
        max_events: None,
        mv_query: None,
        config: None,
        dry_run: false,
        slatedb_config: None,
        slatedb_env_prefix: None,
        slatedb_flush_interval_ms: None,
        slatedb_l0_sst_size_bytes: None,
        slatedb_max_unflushed_bytes: None,
        slatedb_compaction_max_sst_bytes: None,
        slatedb_compaction_max_concurrent: None,
        slatedb_await_durable: None,
        slatedb_cache_dir: None,
        slatedb_cache_max_bytes: None,
        slatedb_cache_part_bytes: None,
        slatedb_cache_puts: false,
        mv_retain_last: DEFAULT_MV_RETAIN_LAST,
        zset_compaction_max_chain_len: DEFAULT_ZSET_COMPACTION_MAX_CHAIN_LEN,
        zset_compaction_max_segments: DEFAULT_ZSET_COMPACTION_MAX_SEGMENTS,
        zset_compaction_backoff_ticks: DEFAULT_ZSET_COMPACTION_BACKOFF_TICKS,
        zset_compaction_max_concurrent_jobs: DEFAULT_ZSET_COMPACTION_MAX_CONCURRENT_JOBS,
        zset_gc_grace_period_ms: DEFAULT_ZSET_GC_GRACE_PERIOD_MS,
        maintenance_paused: false,
        maintenance_inspect_namespace: Vec::new(),
        maintenance_compact_namespace: Vec::new(),
        maintenance_gc_namespace: Vec::new(),
        output_consolidation_mode: cli::OutputConsolidationMode::AllColumns,
        input_file: None,
        input_source: None,
        kafka_brokers: None,
        kafka_topics: Vec::new(),
        kafka_group_id: DEFAULT_KAFKA_GROUP_ID.to_string(),
        kafka_default_source: None,
        kafka_poll_ms: DEFAULT_KAFKA_POLL_MS,
        kafka_max_messages: DEFAULT_KAFKA_MAX_MESSAGES,
        ingest_queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
        ingest_batch_size: DEFAULT_INGEST_BATCH_SIZE,
        ingest_batch_per_source: DEFAULT_INGEST_BATCH_PER_SOURCE,
        ingest_batch_per_connector: DEFAULT_INGEST_BATCH_PER_CONNECTOR,
        http_host: DEFAULT_HTTP_HOST.to_string(),
        http_port: None,
        http_source: None,
    }
}

fn event(source: &str, id: i64) -> core_source::SourceEvent {
    core_source::SourceEvent::new(source, json!({ "id": id }))
}

fn queued_event(source: &str, id: i64) -> QueuedSourceEvent {
    QueuedSourceEvent {
        event: event(source, id),
        commit_ack: None,
    }
}

#[test]
fn build_batch_limits_per_connector() {
    let pending_events = core_source::PendingEventCounter::default();
    pending_events.record_enqueue(4);
    let mut queues = vec![
        ConnectorQueue {
            id: 0,
            name: "a".to_string(),
            pending: VecDeque::from([queued_event("s1", 1), queued_event("s1", 2)]),
        },
        ConnectorQueue {
            id: 1,
            name: "b".to_string(),
            pending: VecDeque::from([queued_event("s2", 3), queued_event("s2", 4)]),
        },
    ];

    let source_id_by_name = HashMap::from([("s1".to_string(), 0usize), ("s2".to_string(), 1usize)]);
    let selection = build_batch(
        &mut queues,
        &source_id_by_name,
        2,
        0,
        10,
        10,
        1,
        &pending_events,
    );
    assert_eq!(selection.batch.len(), 2);
    assert_eq!(selection.batch[0].source_id, Some(0));
    assert_eq!(selection.batch[1].source_id, Some(1));
    assert_eq!(selection.per_connector_counts, vec![1, 1]);
    assert_eq!(queues[0].pending.len(), 1);
    assert_eq!(queues[1].pending.len(), 1);
    assert_eq!(pending_events.pending(), 2);
}

#[test]
fn build_batch_limits_per_source() {
    let pending_events = core_source::PendingEventCounter::default();
    pending_events.record_enqueue(3);
    let mut queues = vec![ConnectorQueue {
        id: 0,
        name: "a".to_string(),
        pending: VecDeque::from([
            queued_event("s1", 1),
            queued_event("s1", 2),
            queued_event("s1", 3),
        ]),
    }];

    let source_id_by_name = HashMap::from([("s1".to_string(), 0usize)]);
    let selection = build_batch(
        &mut queues,
        &source_id_by_name,
        1,
        0,
        10,
        1,
        10,
        &pending_events,
    );
    assert_eq!(selection.batch.len(), 1);
    assert_eq!(selection.batch[0].source_id, Some(0));
    assert_eq!(queues[0].pending.len(), 2);
    assert_eq!(pending_events.pending(), 2);
}

#[test]
fn merge_sql_sinks_validates_mv_reference() {
    let mut sink_specs = Vec::new();
    let sql_sink_specs = vec![SinkSpec {
        name: "sink_missing".to_string(),
        config: SinkConfig::File {
            name: Some("sink_missing".to_string()),
            path: "/tmp/out.jsonl".to_string(),
            mv: "missing_mv".to_string(),
            with_snapshot: Some(false),
            as_of: None,
            append: Some(true),
            effectively_once: None,
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
        },
    }];
    let materialized_view_map = HashMap::new();

    let err = merge_sql_sinks(&mut sink_specs, sql_sink_specs, &materialized_view_map)
        .expect_err("expected unknown mv validation error");
    assert!(
        err.to_string()
            .contains("references unknown materialized view 'missing_mv'")
    );
}

#[test]
fn merge_sql_sinks_rejects_duplicate_names() {
    let mut sink_specs = vec![SinkSpec {
        name: "sink_dup".to_string(),
        config: SinkConfig::File {
            name: Some("sink_dup".to_string()),
            path: "/tmp/first.jsonl".to_string(),
            mv: "mv_a".to_string(),
            with_snapshot: Some(false),
            as_of: None,
            append: Some(true),
            effectively_once: None,
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
        },
    }];
    let sql_sink_specs = vec![SinkSpec {
        name: "sink_dup".to_string(),
        config: SinkConfig::Http {
            name: Some("sink_dup".to_string()),
            url: "http://localhost:8080".to_string(),
            mv: "mv_a".to_string(),
            with_snapshot: Some(true),
            as_of: None,
            batch_size: Some(1),
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
        },
    }];
    let mut materialized_view_map = HashMap::new();
    materialized_view_map.insert(
        "mv_a".to_string(),
        MaterializedViewDefinition::new("mv_a", "SELECT 1", false),
    );

    let err = merge_sql_sinks(&mut sink_specs, sql_sink_specs, &materialized_view_map)
        .expect_err("expected duplicate sink name error");
    assert!(err.to_string().contains("duplicate sink name 'sink_dup'"));
}

#[test]
fn runtime_failure_records_first_error_only() {
    let state = Arc::new(StdMutex::new(None::<String>));
    record_runtime_failure(&state, "first".to_string());
    record_runtime_failure(&state, "second".to_string());
    assert_eq!(
        state.lock().expect("runtime failure lock").as_deref(),
        Some("first")
    );
}

#[test]
fn collect_mv_versions_for_commit_uses_logical_overlay_versions() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let handle = registry.register("mv_overlay".to_string());
    handle.publish_logical_version(7);

    let mut last_versions = HashMap::new();
    let committed = collect_mv_versions_for_commit(&registry, &mut last_versions);
    assert_eq!(
        committed,
        vec![MaterializedViewTickVersion {
            view: "mv_overlay".to_string(),
            version: 7,
        }]
    );

    let committed_again = collect_mv_versions_for_commit(&registry, &mut last_versions);
    assert!(
        committed_again.is_empty(),
        "logical version should only commit once"
    );
}

#[test]
fn kafka_offset_commit_state_preserves_idle_topic_offsets() {
    let mut committed = HashMap::new();
    advance_kafka_offset_commit_state(
        &mut committed,
        &HashMap::from([
            ((Arc::<str>::from("auction"), 0_i32), 41_i64),
            ((Arc::<str>::from("person"), 0_i32), 17_i64),
        ]),
    );

    advance_kafka_offset_commit_state(
        &mut committed,
        &HashMap::from([((Arc::<str>::from("person"), 0_i32), 22_i64)]),
    );

    assert_eq!(
        build_kafka_offset_commit(2, &committed),
        KafkaOffsetCommit {
            tick_id: 2,
            offsets: vec![
                KafkaTopicPartitionOffset {
                    topic: "auction".to_string(),
                    partition: 0,
                    offset: 41,
                },
                KafkaTopicPartitionOffset {
                    topic: "person".to_string(),
                    partition: 0,
                    offset: 22,
                },
            ],
        }
    );
}

#[test]
fn postgres_cdc_commit_state_preserves_idle_slots() {
    let mut committed = HashMap::new();
    advance_postgres_cdc_commit_state(
        &mut committed,
        &HashMap::from([
            ("slot_a".to_string(), (16_u64, "0/10".to_string())),
            ("slot_b".to_string(), (32_u64, "0/20".to_string())),
        ]),
    );

    advance_postgres_cdc_commit_state(
        &mut committed,
        &HashMap::from([("slot_b".to_string(), (48_u64, "0/30".to_string()))]),
    );

    assert_eq!(
        build_postgres_cdc_commit(3, &committed),
        PostgresCdcCommit {
            tick_id: 3,
            slots: vec![
                PostgresSlotCommit {
                    slot: "slot_a".to_string(),
                    lsn: "0/10".to_string(),
                },
                PostgresSlotCommit {
                    slot: "slot_b".to_string(),
                    lsn: "0/30".to_string(),
                },
            ],
        }
    );
}

#[test]
fn cli_connector_creation_flags_collects_explicit_connector_inputs() {
    let mut args = default_run_args();
    args.config = Some("node.toml".to_string());
    args.input_file = Some("/tmp/events.jsonl".to_string());
    args.kafka_brokers = Some("localhost:9092".to_string());
    args.kafka_topics = vec!["nexmark_bid".to_string()];
    args.http_port = Some(8080);
    let flags = cli_connector_creation_flags(&args);
    assert_eq!(
        flags,
        vec![
            "--http-port",
            "--kafka-brokers",
            "--kafka-topics",
            "--input-file"
        ]
    );
}

#[test]
fn log_operator_hints_handles_empty_materialized_views() {
    let connectors = vec![config::ConnectorSpec {
        name: "generator".to_string(),
        config: ConnectorConfig::Generator {
            name: None,
            events_per_second: Some(10.0),
            max_events: None,
        },
    }];
    let available_sources = BTreeSet::from(["nexmark_bid".to_string()]);
    log_operator_hints(&connectors, &available_sources, &[], &[]);
}

#[test]
fn log_startup_banner_handles_mixed_connectors() {
    let args = default_run_args();
    let connectors = vec![
        config::ConnectorSpec {
            name: "generator".to_string(),
            config: ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(10.0),
                max_events: None,
            },
        },
        config::ConnectorSpec {
            name: "http".to_string(),
            config: ConnectorConfig::Http {
                name: None,
                host: Some("127.0.0.1".to_string()),
                port: 8080,
                default_source: Some("nexmark_bid".to_string()),
            },
        },
    ];
    log_startup_banner(&args, &connectors);
}

#[test]
fn apply_runtime_config_defaults_uses_config_when_cli_values_are_defaults() {
    let mut args = default_run_args();
    let config = NodeConfig {
        runtime: config::RuntimeConfig {
            events_per_second: Some(25.0),
            max_events: Some(123),
            output_consolidation_mode: Some(OutputConsolidationModeConfig::Key),
            ingest_queue_capacity: Some(2048),
            ingest_batch_size: Some(512),
            ingest_batch_per_source: Some(128),
            ingest_batch_per_connector: Some(96),
            mv_retain_last: Some(7),
            http_host: Some("0.0.0.0".to_string()),
            kafka_group_id: Some("cfg-group".to_string()),
            kafka_poll_ms: Some(250),
            kafka_max_messages: Some(1024),
            watermark_idle_source_ms: Some(45_000),
            mv_flush: config::MvFlushConfig::default(),
            mv_snapshot: config::MvSnapshotConfig::default(),
        },
        storage: config::StorageConfig {
            await_durable: Some(true),
            slatedb_config: Some("/tmp/slatedb.toml".to_string()),
            slatedb_env_prefix: Some("CFG_".to_string()),
            zset_compaction_max_chain_len: Some(99),
            zset_compaction_max_segments: Some(500),
            zset_compaction_backoff_ticks: Some(8),
            zset_compaction_max_concurrent_jobs: Some(4),
            zset_gc_grace_period_ms: Some(1_000),
            source_journal: None,
        },
        maintenance: config::MaintenanceConfig {
            paused: Some(true),
            inspect_namespace: vec!["ns.inspect".to_string()],
            compact_namespace: vec!["ns.compact".to_string()],
            gc_namespace: vec!["ns.gc".to_string()],
        },
        ..NodeConfig::default()
    };

    apply_runtime_config_defaults(&mut args, &config);

    assert_eq!(args.events_per_second, 25.0);
    assert_eq!(args.max_events, Some(123));
    assert_eq!(
        args.output_consolidation_mode,
        cli::OutputConsolidationMode::Key
    );
    assert_eq!(args.ingest_queue_capacity, 2048);
    assert_eq!(args.ingest_batch_size, 512);
    assert_eq!(args.ingest_batch_per_source, 128);
    assert_eq!(args.ingest_batch_per_connector, 96);
    assert_eq!(args.mv_retain_last, 7);
    assert_eq!(args.http_host, "0.0.0.0");
    assert_eq!(args.kafka_group_id, "cfg-group");
    assert_eq!(args.kafka_poll_ms, 250);
    assert_eq!(args.kafka_max_messages, 1024);
    assert_eq!(args.slatedb_await_durable, Some(true));
    assert_eq!(args.slatedb_config.as_deref(), Some("/tmp/slatedb.toml"));
    assert_eq!(args.slatedb_env_prefix.as_deref(), Some("CFG_"));
    assert_eq!(args.zset_compaction_max_chain_len, 99);
    assert_eq!(args.zset_compaction_max_segments, 500);
    assert_eq!(args.zset_compaction_backoff_ticks, 8);
    assert_eq!(args.zset_compaction_max_concurrent_jobs, 4);
    assert_eq!(args.zset_gc_grace_period_ms, 1_000);
    assert!(args.maintenance_paused);
    assert_eq!(
        args.maintenance_inspect_namespace,
        vec!["ns.inspect".to_string()]
    );
    assert_eq!(
        args.maintenance_compact_namespace,
        vec!["ns.compact".to_string()]
    );
    assert_eq!(args.maintenance_gc_namespace, vec!["ns.gc".to_string()]);
}

#[test]
fn source_journal_auto_skips_replayable_connector_sources() {
    let kafka = SourceDefinition::new(
        "kafka_source",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source")
    .with_property("connector.kafka.type", "kafka");
    let http = SourceDefinition::new(
        "http_source",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source")
    .with_property("connector.http.type", "http");
    let file = SourceDefinition::new(
        "file_source",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source")
    .with_property("connector.file.type", "file");
    let mut registry = SourceRegistry::new();
    registry.register(kafka);
    registry.register(http);
    registry.register(file);
    let transient = BTreeSet::from([
        "file_source".to_string(),
        "http_source".to_string(),
        "kafka_source".to_string(),
    ]);

    assert_eq!(
        source_journal_required_sources(&registry, &transient, SourceJournalConfig::Auto),
        BTreeSet::from(["file_source".to_string(), "http_source".to_string()])
    );
    assert_eq!(
        source_journal_required_sources(&registry, &transient, SourceJournalConfig::Full),
        transient
    );
    assert!(
        source_journal_required_sources(&registry, &transient, SourceJournalConfig::None)
            .is_empty()
    );
}

#[test]
fn apply_runtime_config_defaults_preserves_explicit_cli_values() {
    let mut args = default_run_args();
    args.events_per_second = 77.0;
    args.output_consolidation_mode = cli::OutputConsolidationMode::Key;
    args.ingest_batch_size = 999;
    args.maintenance_paused = true;
    args.slatedb_await_durable = Some(true);

    let config = NodeConfig {
        runtime: config::RuntimeConfig {
            events_per_second: Some(25.0),
            output_consolidation_mode: Some(OutputConsolidationModeConfig::AllColumns),
            ingest_batch_size: Some(128),
            ..config::RuntimeConfig::default()
        },
        storage: config::StorageConfig {
            await_durable: Some(false),
            ..config::StorageConfig::default()
        },
        maintenance: config::MaintenanceConfig {
            paused: Some(false),
            ..config::MaintenanceConfig::default()
        },
        ..NodeConfig::default()
    };

    apply_runtime_config_defaults(&mut args, &config);

    assert_eq!(args.events_per_second, 77.0);
    assert_eq!(
        args.output_consolidation_mode,
        cli::OutputConsolidationMode::Key
    );
    assert_eq!(args.ingest_batch_size, 999);
    assert!(args.maintenance_paused);
    assert_eq!(args.slatedb_await_durable, Some(true));
}

#[test]
fn mv_flush_coalescing_config_maps_optional_fields() {
    let config = config::MvFlushConfig {
        enabled: Some(true),
        max_pending_deltas: Some(8),
        max_pending_versions: Some(16),
        max_pending_rows: Some(1_000),
        max_pending_bytes: Some(2_000),
        max_delay_ms: Some(250),
        flush_on_catchup_boundary: Some(false),
        flush_on_shutdown: Some(false),
    };

    let mapped = mv_flush_coalescing_config(&config);
    assert!(mapped.enabled);
    assert_eq!(mapped.max_pending_deltas, 8);
    assert_eq!(mapped.max_pending_versions, Some(16));
    assert_eq!(mapped.max_pending_rows, Some(1_000));
    assert_eq!(mapped.max_pending_bytes, Some(2_000));
    assert_eq!(mapped.max_delay_ms, Some(250));
    assert!(!mapped.flush_on_catchup_boundary);
    assert!(!mapped.flush_on_shutdown);
}

#[test]
fn mv_flush_coalescing_defaults_to_disabled() {
    let mapped = mv_flush_coalescing_config(&config::MvFlushConfig::default());
    assert!(!mapped.enabled);
    assert_eq!(mapped.max_pending_deltas, 1);
    assert_eq!(mapped.max_pending_versions, None);
    assert_eq!(mapped.max_pending_rows, None);
    assert_eq!(mapped.max_pending_bytes, None);
    assert_eq!(mapped.max_delay_ms, None);
}

#[test]
fn mv_snapshot_config_maps_optional_fields() {
    let config = config::MvSnapshotConfig {
        max_pending_batches: Some(2_048),
        max_pending_rows: Some(500_000),
        max_delay_ms: Some(2_000),
    };

    let mapped = mv_snapshot_config(&config);
    assert_eq!(mapped.max_pending_batches, 2_048);
    assert_eq!(mapped.max_pending_rows, 500_000);
    assert_eq!(mapped.max_delay_ms, 2_000);
}

#[test]
fn mv_snapshot_config_defaults_to_overlay_snapshot_defaults() {
    let mapped = mv_snapshot_config(&config::MvSnapshotConfig::default());
    assert_eq!(mapped.max_pending_batches, 16_384);
    assert_eq!(mapped.max_pending_rows, 1_000_000);
    assert_eq!(mapped.max_delay_ms, 10_000);
}

#[test]
fn table_definition_from_sql_preserves_primary_key_and_nullability() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, note TEXT, enabled BOOL NOT NULL, created_at TIMESTAMP)",
    )
    .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    assert_eq!(table.name(), "orders");
    assert_eq!(table.columns().len(), 4);
    assert_eq!(table.primary_key_index(), 0);
    assert!(!table.columns()[0].nullable());
    assert!(table.columns()[1].nullable());
    assert!(!table.columns()[2].nullable());
    assert!(table.columns()[3].nullable());
}

#[test]
fn source_definition_from_table_sets_pk_property() {
    let statement =
        parse_floe_statement("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, active BOOL)")
            .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    assert_eq!(source.name(), "users");
    assert_eq!(source.property(SOURCE_PRIMARY_KEY_PROPERTY), Some("id"));
    assert!(source_definition_has_primary_key(&source));
    assert!(!source.columns()[0].nullable());
    assert!(source.columns()[1].nullable());
}

#[test]
fn catalog_source_definition_from_sql_preserves_postgres_cdc_options() {
    let statement = parse_floe_statement(
        "CREATE SOURCE pg_main WITH (
            connector = 'postgres-cdc',
            connection = 'postgres://postgres:postgres@localhost/postgres',
            slot.name = 'floe_slot',
            publication.name = 'floe_pub'
        )",
    )
    .expect("parse create source");
    let FloeStatement::CreateSource(definition) = statement else {
        panic!("expected create source statement");
    };
    let source = catalog_source_definition_from_sql(&definition).expect("catalog source");
    assert_eq!(source.name(), "pg_main");
    let CatalogSourceConnector::PostgresCdc(postgres) = source.connector();
    assert_eq!(
        postgres.connection(),
        "postgres://postgres:postgres@localhost/postgres"
    );
    assert_eq!(postgres.slot(), "floe_slot");
    assert_eq!(postgres.publication(), Some("floe_pub"));
}

#[test]
fn materialized_view_validation_rejects_raw_cdc_source_references() {
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    let views = vec![
        MaterializedViewDefinition::new("mv_orders", "SELECT * FROM pg_main", false),
        MaterializedViewDefinition::new("mv_join", "SELECT * FROM orders", false),
    ];

    let err = validate_materialized_views_do_not_query_raw_cdc_sources(
        &HashMap::from([(source.name().to_string(), source)]),
        &views,
    )
    .expect_err("raw CDC source should be rejected");

    assert!(err.to_string().contains("reads raw CDC source 'pg_main'"));
    assert!(
        err.to_string()
            .contains("CREATE TABLE ... FROM pg_main TABLE")
    );
}

#[test]
fn materialized_view_validation_accepts_cdc_table_references() {
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    let views = vec![MaterializedViewDefinition::new(
        "mv_orders",
        "SELECT * FROM orders",
        false,
    )];

    validate_materialized_views_do_not_query_raw_cdc_sources(
        &HashMap::from([(source.name().to_string(), source)]),
        &views,
    )
    .expect("CDC table reference should be allowed");
}

#[test]
fn source_backed_table_definition_from_sql_preserves_binding() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT)
         FROM pg_main TABLE 'public.orders'",
    )
    .expect("parse source-backed table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let binding = source_backed_table_definition_from_sql(&definition)
        .expect("source-backed table")
        .expect("binding");
    assert_eq!(binding.table_name(), "orders");
    assert_eq!(binding.source_name(), "pg_main");
    assert_eq!(binding.upstream_table(), "public.orders");
}

#[test]
fn replication_pipeline_definition_from_sql_preserves_postgres_target() {
    let statement = parse_floe_statement(
        "CREATE REPLICATION PIPELINE pg_orders_to_postgres
         FROM pg_main TABLE public.orders
         INTO POSTGRES WITH (
            connection = 'postgres://postgres:postgres@localhost/postgres',
            table = 'public.orders_copy'
         )",
    )
    .expect("parse pipeline");
    let FloeStatement::CreateReplicationPipeline(definition) = statement else {
        panic!("expected replication pipeline statement");
    };

    let pipeline = replication_pipeline_definition_from_sql(&definition).expect("catalog pipeline");

    assert_eq!(pipeline.name(), "pg_orders_to_postgres");
    assert_eq!(pipeline.source_name(), "pg_main");
    assert_eq!(pipeline.upstream_table(), "public.orders");
    assert_eq!(
        pipeline.format(),
        CatalogReplicationPipelineFormat::FloeJson
    );
    assert_eq!(
        pipeline.target(),
        &CatalogReplicationPipelineTarget::Postgres {
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
        }
    );
}

#[test]
fn catalog_postgres_source_connector_merges_include_tables() {
    let mut connector_specs = Vec::new();
    let mut catalog_sources = HashMap::new();
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    catalog_sources.insert(source.name().to_string(), source);
    let mut source_tables = HashMap::new();
    let binding =
        SourceBackedTableDefinition::new("orders", "pg_main", "public.orders").expect("binding");
    source_tables.insert(binding.table_name().to_string(), binding);

    merge_catalog_source_connectors(
        &mut connector_specs,
        &catalog_sources,
        &source_tables,
        &HashMap::new(),
    )
    .expect("merge connector");

    assert_eq!(connector_specs.len(), 1);
    assert_eq!(connector_specs[0].name, "pg_main");
    let ConnectorConfig::PostgresCdc {
        connection,
        slot,
        publication,
        include_tables,
        include_schema_in_source,
        ..
    } = &connector_specs[0].config
    else {
        panic!("expected postgres cdc connector");
    };
    assert_eq!(
        connection,
        "postgres://postgres:postgres@localhost/postgres"
    );
    assert_eq!(slot, "floe_slot");
    assert_eq!(publication.as_deref(), Some("floe_pub"));
    assert_eq!(
        include_tables.as_deref(),
        Some(&["public.orders".to_string()][..])
    );
    assert_eq!(include_schema_in_source.as_ref().copied(), Some(false));
}

#[test]
fn catalog_postgres_source_connector_merges_pipeline_tables() {
    let mut connector_specs = Vec::new();
    let mut catalog_sources = HashMap::new();
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    catalog_sources.insert(source.name().to_string(), source);
    let pipeline = CatalogReplicationPipelineDefinition::new(
        "pg_orders_to_kafka",
        "pg_main",
        "public.orders",
        CatalogReplicationPipelineTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders_cdc".to_string(),
        },
        CatalogReplicationPipelineFormat::DebeziumJson,
        CatalogReplicationBufferMode::Durable,
        CatalogReplicationBufferPolicy::default(),
        false,
        false,
    )
    .expect("pipeline");
    let mut pipelines = HashMap::new();
    pipelines.insert(pipeline.name().to_string(), pipeline);

    merge_catalog_source_connectors(
        &mut connector_specs,
        &catalog_sources,
        &HashMap::new(),
        &pipelines,
    )
    .expect("merge connector");

    let ConnectorConfig::PostgresCdc { include_tables, .. } = &connector_specs[0].config else {
        panic!("expected postgres cdc connector");
    };
    assert_eq!(
        include_tables.as_deref(),
        Some(&["public.orders".to_string()][..])
    );
}

#[tokio::test]
async fn postgres_cdc_runtime_plan_builds_cdc_schema_from_source_primary_key() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, note TEXT)",
    )
    .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    let mut registry = SourceRegistry::new();
    registry.register(source);
    let include_tables = vec!["public.orders".to_string()];

    let plan = postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::new(),
    )
    .await
    .expect("runtime plan")
    .expect("native runtime plan");

    assert_eq!(plan.source_id.as_str(), "pg_main");
    let schema = plan
        .schemas
        .get(&CdcTableId::new("orders").expect("table id"))
        .expect("orders schema");
    assert_eq!(schema.upstream_table().schema(), "public");
    assert_eq!(schema.upstream_table().table(), "orders");
    assert_eq!(schema.primary_key().columns(), &["id".to_string()]);
    assert_eq!(schema.columns().len(), 3);
    assert!(!schema.columns()[0].nullable());
}

#[tokio::test]
async fn postgres_cdc_runtime_plan_accepts_postgres_replication_target() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, note TEXT)",
    )
    .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    let mut registry = SourceRegistry::new();
    registry.register(source);
    let include_tables = vec!["public.orders".to_string()];
    let pipeline = CatalogReplicationPipelineDefinition::new(
        "pg_orders_to_postgres",
        "pg_main",
        "public.orders",
        CatalogReplicationPipelineTarget::Postgres {
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
        },
        CatalogReplicationPipelineFormat::FloeJson,
        CatalogReplicationBufferMode::Durable,
        CatalogReplicationBufferPolicy::default(),
        false,
        false,
    )
    .expect("pipeline");

    let plan = postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::from([(pipeline.name().to_string(), pipeline)]),
    )
    .await
    .expect("runtime plan")
    .expect("native runtime plan");

    assert_eq!(plan.replication_pipelines.len(), 1);
    let target = &plan.replication_pipelines[0].target;
    assert!(matches!(
        target,
        ReplicationPipelineRuntimeTarget::Postgres { connection, table }
            if connection == "postgres://postgres:postgres@localhost/postgres"
                && table == "public.orders_copy"
    ));
}

#[tokio::test]
async fn postgres_cdc_runtime_plan_falls_back_without_primary_key() {
    let source = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source");
    let mut registry = SourceRegistry::new();
    registry.register(source);
    let include_tables = vec!["orders".to_string()];

    let plan = postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::new(),
    )
    .await
    .expect("runtime plan");

    assert!(plan.is_none());
}

#[test]
fn resolve_output_consolidation_mode_defaults_to_key_when_pk_present() {
    let statement = parse_floe_statement("CREATE TABLE users (id BIGINT PRIMARY KEY)")
        .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    let mut registry = SourceRegistry::new();
    registry.register(source);

    assert_eq!(
        resolve_output_consolidation_mode(cli::OutputConsolidationMode::AllColumns, &registry),
        cli::OutputConsolidationMode::Key
    );
}

#[test]
fn resolve_output_consolidation_mode_keeps_all_columns_without_pk() {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions().expect("generator definitions"));

    assert_eq!(
        resolve_output_consolidation_mode(cli::OutputConsolidationMode::AllColumns, &registry),
        cli::OutputConsolidationMode::AllColumns
    );
}

#[test]
fn lookup_decoder_for_source_rejects_unknown_source() {
    let decoders: HashMap<String, SourceRowDecoder> = HashMap::new();
    let err = lookup_decoder_for_source(&decoders, "missing_source")
        .expect_err("unknown source should fail");
    assert!(
        err.to_string()
            .contains("received event for unknown source 'missing_source'")
    );
}

#[test]
fn build_postgres_cdc_commit_orders_slots() {
    let mut slots = HashMap::new();
    slots.insert("z_slot".to_string(), (10_u64, "0/0000000A".to_string()));
    slots.insert("a_slot".to_string(), (3_u64, "0/00000003".to_string()));
    let commit = build_postgres_cdc_commit(7, &slots);
    assert_eq!(commit.tick_id, 7);
    assert_eq!(commit.slots.len(), 2);
    assert_eq!(commit.slots[0].slot, "a_slot");
    assert_eq!(commit.slots[1].slot, "z_slot");
}

#[test]
fn event_postgres_lsn_extracts_slot_and_value() {
    let token = core_source::SourceResumeToken::PostgresCdc {
        slot: Some("cdc_slot".to_string()),
        lsn: "16/B3738".to_string(),
        txid: None,
    };
    let (slot, value, lsn) =
        event_postgres_lsn(Some(&token)).expect("postgres resume token should parse");
    assert_eq!(slot, "cdc_slot");
    assert_eq!(lsn, "16/B3738");
    assert_eq!(value, parse_postgres_lsn("16/B3738").expect("parse lsn"));
}

#[test]
fn event_resume_offset_extracts_postgres_lsn() {
    let token = core_source::SourceResumeToken::PostgresCdc {
        slot: Some("slot_a".to_string()),
        lsn: "0/0000002A".to_string(),
        txid: Some(5),
    };
    assert_eq!(
        event_resume_offset(Some(&token)),
        Some((0, parse_postgres_lsn("0/0000002A").expect("parse lsn")))
    );
}

#[test]
fn compute_global_watermark_uses_min_of_active_sources() {
    let now = Instant::now();
    let mut source_watermarks = HashMap::new();
    source_watermarks.insert("s1".to_string(), 5_000);
    source_watermarks.insert("s2".to_string(), 3_000);

    let mut source_last_seen = HashMap::new();
    source_last_seen.insert("s1".to_string(), now);
    source_last_seen.insert("s2".to_string(), now);

    assert_eq!(
        compute_global_watermark(
            &source_watermarks,
            &source_last_seen,
            now,
            Duration::from_secs(30),
        ),
        Some(3_000)
    );
}

#[test]
fn compute_global_watermark_skips_idle_sources() {
    let now = Instant::now();
    let mut source_watermarks = HashMap::new();
    source_watermarks.insert("active".to_string(), 9_000);
    source_watermarks.insert("idle".to_string(), 1_000);

    let mut source_last_seen = HashMap::new();
    source_last_seen.insert("active".to_string(), now);
    source_last_seen.insert("idle".to_string(), now - Duration::from_secs(60));

    assert_eq!(
        compute_global_watermark(
            &source_watermarks,
            &source_last_seen,
            now,
            Duration::from_secs(30),
        ),
        Some(9_000)
    );
}

#[test]
fn advance_global_watermark_is_monotonic() {
    assert_eq!(advance_global_watermark(5_000, Some(4_000)), 5_000);
    assert_eq!(advance_global_watermark(5_000, Some(7_000)), 7_000);
    assert_eq!(advance_global_watermark(5_000, None), 5_000);
}
