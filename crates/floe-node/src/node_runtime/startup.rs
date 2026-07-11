use super::*;

pub(super) fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

pub(super) fn apply_runtime_config_defaults(
    args: &mut cli::RunArgs,
    config: &NodeConfig,
    cli_overrides: &RunArgOverrides,
) {
    let runtime = &config.runtime;
    let storage = &config.storage;
    let maintenance = &config.maintenance;

    if !cli_overrides.contains("events_per_second")
        && let Some(events_per_second) = runtime.events_per_second
    {
        args.events_per_second = events_per_second;
    }
    if args.max_events.is_none() {
        args.max_events = runtime.max_events;
    }
    if args.pgwire_addr.is_none() {
        args.pgwire_addr = runtime.pgwire_addr.clone();
    }
    if !cli_overrides.contains("disable_pgwire")
        && let Some(enabled) = runtime.pgwire_enabled
    {
        args.disable_pgwire = !enabled;
    }
    if args.admin_port.is_none() {
        args.admin_port = runtime.admin_port;
    }
    if args.watermark_idle_source_ms.is_none() {
        args.watermark_idle_source_ms = runtime.watermark_idle_source_ms;
    }
    if args.subscribe_channel_capacity.is_none() {
        args.subscribe_channel_capacity = runtime.subscribe_channel_capacity;
    }
    if args.subscribe_max_catchup_versions.is_none() {
        args.subscribe_max_catchup_versions = runtime.subscribe_max_catchup_versions;
    }
    if !cli_overrides.contains("ingest_queue_capacity")
        && let Some(capacity) = runtime.ingest_queue_capacity
    {
        args.ingest_queue_capacity = capacity;
    }
    if !cli_overrides.contains("ingest_batch_size")
        && let Some(batch_size) = runtime.ingest_batch_size
    {
        args.ingest_batch_size = batch_size;
    }
    if !cli_overrides.contains("ingest_batch_per_source")
        && let Some(limit) = runtime.ingest_batch_per_source
    {
        args.ingest_batch_per_source = limit;
    }
    if !cli_overrides.contains("ingest_batch_per_connector")
        && let Some(limit) = runtime.ingest_batch_per_connector
    {
        args.ingest_batch_per_connector = limit;
    }
    if !cli_overrides.contains("mv_retain_last")
        && let Some(retain_last) = runtime.mv_retain_last
    {
        args.mv_retain_last = retain_last;
    }
    if !cli_overrides.contains("http_host")
        && let Some(host) = runtime.http_host.as_ref()
    {
        args.http_host = host.clone();
    }
    if !cli_overrides.contains("kafka_group_id")
        && let Some(group_id) = runtime.kafka_group_id.as_ref()
    {
        args.kafka_group_id = group_id.clone();
    }
    if !cli_overrides.contains("kafka_poll_ms")
        && let Some(poll_ms) = runtime.kafka_poll_ms
    {
        args.kafka_poll_ms = poll_ms;
    }
    if !cli_overrides.contains("kafka_max_messages")
        && let Some(max_messages) = runtime.kafka_max_messages
    {
        args.kafka_max_messages = max_messages;
    }

    if args.slatedb_await_durable.is_none() {
        args.slatedb_await_durable = storage.await_durable;
    }
    if args.data_dir.is_none() {
        args.data_dir = storage.data_dir.clone();
    }
    if !cli_overrides.contains("object_store_from_env") {
        args.object_store_from_env = storage.object_store_from_env;
    }
    if args.object_store_env_file.is_none() {
        args.object_store_env_file = storage.object_store_env_file.clone();
    }
    if args.slatedb_name.is_none() {
        args.slatedb_name = storage.slatedb_name.clone();
    }
    if args.slatedb_config.is_none() {
        args.slatedb_config = storage.slatedb_config.clone();
    }
    if args.slatedb_env_prefix.is_none() {
        args.slatedb_env_prefix = storage.slatedb_env_prefix.clone();
    }
    if args.slatedb_close_timeout_ms.is_none() {
        args.slatedb_close_timeout_ms = storage.slatedb_close_timeout_ms;
    }
    if !cli_overrides.contains("zset_compaction_max_chain_len")
        && let Some(max_chain_len) = storage.zset_compaction_max_chain_len
    {
        args.zset_compaction_max_chain_len = max_chain_len;
    }
    if !cli_overrides.contains("zset_compaction_max_segments")
        && let Some(max_segments) = storage.zset_compaction_max_segments
    {
        args.zset_compaction_max_segments = max_segments;
    }
    if !cli_overrides.contains("zset_compaction_backoff_ticks")
        && let Some(backoff_ticks) = storage.zset_compaction_backoff_ticks
    {
        args.zset_compaction_backoff_ticks = backoff_ticks;
    }
    if !cli_overrides.contains("zset_compaction_max_concurrent_jobs")
        && let Some(max_jobs) = storage.zset_compaction_max_concurrent_jobs
    {
        args.zset_compaction_max_concurrent_jobs = max_jobs;
    }
    if !cli_overrides.contains("zset_gc_grace_period_ms")
        && let Some(grace_ms) = storage.zset_gc_grace_period_ms
    {
        args.zset_gc_grace_period_ms = grace_ms;
    }

    if !cli_overrides.contains("maintenance_paused")
        && let Some(paused) = maintenance.paused
    {
        args.maintenance_paused = paused;
    }
    if args.maintenance_inspect_namespace.is_empty() {
        args.maintenance_inspect_namespace = maintenance.inspect_namespace.clone();
    }
    if args.maintenance_compact_namespace.is_empty() {
        args.maintenance_compact_namespace = maintenance.compact_namespace.clone();
    }
    if args.maintenance_gc_namespace.is_empty() {
        args.maintenance_gc_namespace = maintenance.gc_namespace.clone();
    }
}

pub(super) async fn upsert_materialized_view_definition(
    materialized_view_map: &mut HashMap<String, MaterializedViewDefinition>,
    definition: MaterializedViewDefinition,
    storage: Option<&Arc<floe_storage::SlateCatalog>>,
    source: &str,
) -> anyhow::Result<()> {
    let name = definition.name().to_string();
    if definition.if_not_exists() && materialized_view_map.contains_key(&name) {
        tracing::info!(
            view = %name,
            source = %source,
            "materialized view already exists; skipping due to IF NOT EXISTS"
        );
        return Ok(());
    }
    if let Some(storage) = storage {
        let metadata = MaterializedViewMetadata::new(
            definition.name(),
            definition.query(),
            definition.if_not_exists(),
        );
        storage
            .upsert_materialized_view(metadata)
            .await
            .with_context(|| {
                format!(
                    "persist materialized view definition for '{}' from {}",
                    definition.name(),
                    source
                )
            })?;
    }
    materialized_view_map.insert(name, definition);
    Ok(())
}

pub(super) fn load_slatedb_settings(args: &cli::RunArgs) -> anyhow::Result<Option<Settings>> {
    let mut settings = if let Some(path) = args.slatedb_config.clone() {
        Some(
            Settings::from_file(&path)
                .map_err(|err| anyhow!("failed to load SlateDB settings from {path}: {err}"))?,
        )
    } else if let Some(prefix) = args
        .slatedb_env_prefix
        .as_deref()
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
    {
        if env_has_prefix(prefix) {
            Some(Settings::from_env(prefix).map_err(|err| {
                anyhow!("failed to load SlateDB settings from env prefix '{prefix}': {err}")
            })?)
        } else {
            None
        }
    } else {
        None
    };

    if settings.is_none() && slatedb_overrides_present(args) {
        settings = Some(Settings::default());
    }
    if let Some(settings) = settings.as_mut() {
        apply_slatedb_overrides(settings, args);
    }
    Ok(settings)
}

pub(super) fn env_has_prefix(prefix: &str) -> bool {
    std::env::vars().any(|(key, _)| key.starts_with(prefix))
}

pub(super) fn slatedb_overrides_present(args: &cli::RunArgs) -> bool {
    args.slatedb_flush_interval_ms.is_some()
        || args.slatedb_l0_sst_size_bytes.is_some()
        || args.slatedb_max_wal_flushes_before_l0_flush.is_some()
        || args.slatedb_l0_max_ssts.is_some()
        || args.slatedb_l0_max_ssts_per_key.is_some()
        || args.slatedb_max_unflushed_bytes.is_some()
        || args.slatedb_compaction_max_sst_bytes.is_some()
        || args.slatedb_compaction_max_concurrent.is_some()
        || args.slatedb_cache_dir.is_some()
        || args.slatedb_cache_max_bytes.is_some()
        || args.slatedb_cache_part_bytes.is_some()
        || args.slatedb_cache_puts
        || args.slatedb_cache_max_open_file_handles.is_some()
}

pub(super) fn apply_slatedb_overrides(settings: &mut Settings, args: &cli::RunArgs) {
    if let Some(interval_ms) = args.slatedb_flush_interval_ms {
        settings.flush_interval = if interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(interval_ms))
        };
    }
    if let Some(bytes) = args.slatedb_l0_sst_size_bytes {
        settings.l0_sst_size_bytes = bytes;
    }
    if let Some(flushes) = args.slatedb_max_wal_flushes_before_l0_flush {
        settings.max_wal_flushes_before_l0_flush = flushes;
    }
    if let Some(max_ssts) = args.slatedb_l0_max_ssts {
        settings.l0_max_ssts = max_ssts;
    }
    if let Some(max_ssts_per_key) = args.slatedb_l0_max_ssts_per_key {
        settings.l0_max_ssts_per_key = max_ssts_per_key;
    }
    if let Some(bytes) = args.slatedb_max_unflushed_bytes {
        settings.max_unflushed_bytes = bytes;
    }
    if let Some(max_sst_size) = args.slatedb_compaction_max_sst_bytes {
        let compactor = settings
            .compactor_options
            .get_or_insert_with(CompactorOptions::default);
        compactor.max_sst_size = max_sst_size;
    }
    if let Some(max_concurrent) = args.slatedb_compaction_max_concurrent {
        let compactor = settings
            .compactor_options
            .get_or_insert_with(CompactorOptions::default);
        compactor.max_concurrent_compactions = max_concurrent;
    }
    if let Some(dir) = args.slatedb_cache_dir.as_ref() {
        settings.object_store_cache_options.root_folder = Some(PathBuf::from(dir));
    }
    if let Some(max_bytes) = args.slatedb_cache_max_bytes {
        settings.object_store_cache_options.max_cache_size_bytes = Some(max_bytes);
    }
    if let Some(part_bytes) = args.slatedb_cache_part_bytes {
        settings.object_store_cache_options.part_size_bytes = part_bytes;
    }
    if args.slatedb_cache_puts {
        settings.object_store_cache_options.cache_puts = true;
    }
    if let Some(max_open_file_handles) = args.slatedb_cache_max_open_file_handles {
        settings.object_store_cache_options.max_open_file_handles = max_open_file_handles;
    }
}

pub(super) fn connectors_from_cli(args: &cli::RunArgs) -> Vec<ConnectorConfig> {
    let mut connectors = Vec::new();
    if let Some(port) = args.http_port {
        connectors.push(ConnectorConfig::Http {
            name: None,
            host: Some(args.http_host.clone()),
            port,
            default_source: args.http_source.clone(),
        });
    }
    if let Some(brokers) = args.kafka_brokers.clone() {
        connectors.push(ConnectorConfig::Kafka {
            name: None,
            brokers,
            topics: args.kafka_topics.clone(),
            group_id: Some(args.kafka_group_id.clone()),
            default_source: args.kafka_default_source.clone(),
            poll_ms: Some(args.kafka_poll_ms),
            max_messages_per_tick: Some(args.kafka_max_messages),
            format: None,
        });
    }
    if let Some(path) = args.input_file.clone() {
        connectors.push(ConnectorConfig::File {
            name: None,
            path,
            default_source: args.input_source.clone(),
        });
    }
    connectors
}

pub(super) fn default_generator_connector_from_cli(args: &cli::RunArgs) -> ConnectorConfig {
    ConnectorConfig::Generator {
        name: None,
        events_per_second: Some(args.events_per_second),
        max_events: args.max_events,
    }
}

pub(super) fn merge_sql_connectors(
    connector_specs: &mut Vec<config::ConnectorSpec>,
    sql_connector_specs: Vec<config::ConnectorSpec>,
) -> anyhow::Result<()> {
    let mut connector_names: BTreeSet<String> = connector_specs
        .iter()
        .map(|spec| spec.name.clone())
        .collect();
    for connector in sql_connector_specs {
        if !connector_names.insert(connector.name.clone()) {
            return Err(anyhow!("duplicate connector name '{}'", connector.name));
        }
        connector_specs.push(connector);
    }
    Ok(())
}

pub(super) fn cli_connector_creation_flags(args: &cli::RunArgs) -> Vec<&'static str> {
    let mut flags = Vec::new();
    if args.http_port.is_some() {
        flags.push("--http-port");
    }
    if args.kafka_brokers.is_some() {
        flags.push("--kafka-brokers");
    }
    if !args.kafka_topics.is_empty() {
        flags.push("--kafka-topics");
    }
    if args.input_file.is_some() {
        flags.push("--input-file");
    }
    flags
}

pub(super) fn log_startup_banner(args: &cli::RunArgs, connectors: &[config::ConnectorSpec]) {
    let pgwire_addr = args.pgwire_addr.as_deref().unwrap_or(DEFAULT_PGWIRE_ADDR);
    let storage_mode = if args.object_store_from_env {
        format!(
            "object-store({})",
            args.slatedb_name.as_deref().unwrap_or("floe")
        )
    } else if let Some(dir) = args.data_dir.as_deref() {
        format!("filesystem({dir})")
    } else {
        "in-memory".to_string()
    };
    let connector_names: Vec<&str> = connectors
        .iter()
        .map(|connector| connector.name.as_str())
        .collect();
    let http_addrs: Vec<String> = connectors
        .iter()
        .filter_map(|connector| match &connector.config {
            ConnectorConfig::Http { host, port, .. } => Some(format!(
                "{}:{}",
                host.as_deref().unwrap_or(args.http_host.as_str()),
                port
            )),
            _ => None,
        })
        .collect();

    tracing::info!(
        storage_mode = %storage_mode,
        pgwire_addr = %pgwire_addr,
        pgwire_enabled = !args.disable_pgwire,
        http_addrs = ?http_addrs,
        connectors = ?connector_names,
        mv_retain_last = args.mv_retain_last,
        zset_compaction_max_chain_len = args.zset_compaction_max_chain_len,
        zset_compaction_max_segments = args.zset_compaction_max_segments,
        zset_compaction_backoff_ticks = args.zset_compaction_backoff_ticks,
        zset_compaction_max_concurrent_jobs = args.zset_compaction_max_concurrent_jobs,
        zset_gc_grace_period_ms = args.zset_gc_grace_period_ms,
        "startup banner"
    );
}

pub(super) fn sink_mv_name(config: &SinkConfig) -> &str {
    match config {
        SinkConfig::Kafka { mv, .. }
        | SinkConfig::File { mv, .. }
        | SinkConfig::Http { mv, .. }
        | SinkConfig::Postgres { mv, .. } => mv,
    }
}

pub(super) fn merge_sql_sinks(
    sink_specs: &mut Vec<SinkSpec>,
    sql_sink_specs: Vec<SinkSpec>,
    materialized_view_map: &HashMap<String, MaterializedViewDefinition>,
) -> anyhow::Result<()> {
    let mut sink_names: BTreeSet<String> =
        sink_specs.iter().map(|spec| spec.name.clone()).collect();
    for sink in sql_sink_specs {
        let mv_name = sink_mv_name(&sink.config);
        if !materialized_view_map.contains_key(mv_name) {
            return Err(anyhow!(
                "sink '{}' references unknown materialized view '{}'",
                sink.name,
                mv_name
            ));
        }
        if !sink_names.insert(sink.name.clone()) {
            return Err(anyhow!("duplicate sink name '{}'", sink.name));
        }
        sink_specs.push(sink);
    }
    Ok(())
}

pub(super) fn log_operator_hints(
    connectors: &[config::ConnectorSpec],
    available_sources: &BTreeSet<String>,
    materialized_views: &[MaterializedViewDefinition],
    sinks: &[SinkSpec],
    args: &cli::RunArgs,
) {
    let connector_names: Vec<&str> = connectors
        .iter()
        .map(|connector| connector.name.as_str())
        .collect();
    let sink_names: Vec<&str> = sinks.iter().map(|sink| sink.name.as_str()).collect();
    let mv_names: Vec<&str> = materialized_views.iter().map(|mv| mv.name()).collect();
    let pgwire_addr = args.pgwire_addr.as_deref().unwrap_or(DEFAULT_PGWIRE_ADDR);
    let pgwire_enabled = !args.disable_pgwire;

    tracing::info!(
        pgwire_addr = %pgwire_addr,
        pgwire_enabled,
        connectors = ?connector_names,
        sources = ?available_sources,
        materialized_views = ?mv_names,
        sinks = ?sink_names,
        "runtime topology"
    );

    if !pgwire_enabled {
        return;
    }

    for mv_name in mv_names {
        tracing::info!(
            mv = %mv_name,
            subscribe_sql = %format!("psql postgresql://postgres@{pgwire_addr}/postgres -c \"COPY (SUBSCRIBE {mv_name} WITH SNAPSHOT) TO STDOUT\""),
            pgwire_addr = %pgwire_addr,
            "subscribe hint"
        );
    }
}
