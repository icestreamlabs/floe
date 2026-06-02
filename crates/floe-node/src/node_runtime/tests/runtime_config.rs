use super::*;

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
fn apply_runtime_config_defaults_preserves_explicit_cli_values() {
    let mut args = default_run_args();
    args.events_per_second = 77.0;
    args.ingest_batch_size = 999;
    args.maintenance_paused = true;
    args.slatedb_await_durable = Some(true);

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

    apply_runtime_config_defaults(&mut args, &config);

    assert_eq!(args.events_per_second, 77.0);
    assert_eq!(args.ingest_batch_size, 999);
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
    assert_eq!(mapped.max_pending_rows, 4_000_000);
    assert_eq!(mapped.max_delay_ms, 10_000);
}
