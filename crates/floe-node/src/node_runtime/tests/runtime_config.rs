use super::*;

#[test]
fn explicit_cli_connector_does_not_add_generator() {
    let mut args = default_run_args();
    args.kafka_brokers = Some("localhost:9092".to_string());
    args.kafka_topics = vec!["events".to_string()];

    let connectors = connectors_from_cli(&args);

    assert_eq!(connectors.len(), 1);
    assert!(matches!(connectors[0], ConnectorConfig::Kafka { .. }));
}

#[test]
fn default_generator_is_created_separately_from_explicit_connectors() {
    let mut args = default_run_args();
    args.events_per_second = 25.0;
    args.max_events = Some(100);

    assert!(connectors_from_cli(&args).is_empty());
    assert!(matches!(
        default_generator_connector_from_cli(&args),
        ConnectorConfig::Generator {
            events_per_second: Some(25.0),
            max_events: Some(100),
            ..
        }
    ));
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
        kafka_metadata_journal_required_sources(&registry, &transient, SourceJournalConfig::Auto),
        BTreeSet::from(["kafka_source".to_string()])
    );
    assert_eq!(
        source_journal_required_sources(&registry, &transient, SourceJournalConfig::Full),
        transient
    );
    assert!(
        kafka_metadata_journal_required_sources(&registry, &transient, SourceJournalConfig::Full)
            .is_empty()
    );
    assert!(
        source_journal_required_sources(&registry, &transient, SourceJournalConfig::None)
            .is_empty()
    );
    assert!(
        kafka_metadata_journal_required_sources(&registry, &transient, SourceJournalConfig::None)
            .is_empty()
    );
}

#[test]
fn source_journal_auto_row_journals_durable_table_sources() {
    let mut source_journal_sources = BTreeSet::new();
    let mut kafka_metadata_sources = BTreeSet::from([
        "kafka_events".to_string(),
        "orders".to_string(),
        "unused_table".to_string(),
    ]);
    let durable_tables = BTreeSet::from(["orders".to_string(), "unused_table".to_string()]);
    let required_sources = BTreeSet::from(["kafka_events".to_string(), "orders".to_string()]);

    apply_durable_table_source_journal_policy(
        &mut source_journal_sources,
        &mut kafka_metadata_sources,
        &durable_tables,
        &required_sources,
        SourceJournalConfig::Auto,
    );

    assert_eq!(
        source_journal_sources,
        BTreeSet::from(["orders".to_string()])
    );
    assert_eq!(
        kafka_metadata_sources,
        BTreeSet::from(["kafka_events".to_string(), "unused_table".to_string()])
    );
}

#[test]
fn apply_runtime_config_defaults_preserves_explicit_cli_values() {
    let mut args = default_run_args();
    args.maintenance_paused = true;
    args.slatedb_await_durable = Some(true);
    let overrides = RunArgOverrides::from_ids([
        "events_per_second",
        "ingest_batch_size",
        "maintenance_paused",
    ]);

    let config = NodeConfig {
        runtime: config::RuntimeConfig {
            events_per_second: Some(25.0),
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

    apply_runtime_config_defaults(&mut args, &config, &overrides);

    assert_eq!(args.events_per_second, DEFAULT_EVENTS_PER_SECOND);
    assert_eq!(args.ingest_batch_size, DEFAULT_INGEST_BATCH_SIZE);
    assert!(args.maintenance_paused);
    assert_eq!(args.slatedb_await_durable, Some(true));
}

#[test]
fn slatedb_new_storage_overrides_apply_to_settings() {
    let mut args = default_run_args();
    args.slatedb_max_wal_flushes_before_l0_flush = Some(17);
    args.slatedb_l0_max_ssts = Some(19);
    args.slatedb_l0_max_ssts_per_key = Some(23);
    args.slatedb_cache_max_open_file_handles = Some(29);

    assert!(slatedb_overrides_present(&args));

    let mut settings = Settings::default();
    apply_slatedb_overrides(&mut settings, &args);

    assert_eq!(settings.max_wal_flushes_before_l0_flush, 17);
    assert_eq!(settings.l0_max_ssts, 19);
    assert_eq!(settings.l0_max_ssts_per_key, 23);
    assert_eq!(
        settings.object_store_cache_options.max_open_file_handles,
        29
    );
}
