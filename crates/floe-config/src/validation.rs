use super::*;

pub(crate) fn validate_node_config(config: &NodeConfig) -> Result<()> {
    let mut seen_mv_names = HashSet::new();
    for (index, mv) in config.materialized_views.iter().enumerate() {
        ensure_non_empty(&mv.name, &format!("materialized_views[{index}].name"))?;
        ensure_non_empty(&mv.query, &format!("materialized_views[{index}].query"))?;
        if !seen_mv_names.insert(mv.name.clone()) {
            bail!(
                "duplicate materialized view name '{}' in materialized_views[{index}]",
                mv.name
            );
        }
    }
    for (index, connector) in config.connectors.iter().enumerate() {
        validate_connector(connector, index)?;
    }
    for (index, sink) in config.sinks.iter().enumerate() {
        validate_sink(sink, index)?;
    }
    validate_runtime_config(&config.runtime)?;
    validate_storage_config(&config.storage)?;
    validate_maintenance_config(&config.maintenance)?;
    validate_replication_config(&config.replication)?;
    validate_postgres_cdc_config(&config.postgres_cdc)?;
    Ok(())
}

fn validate_runtime_config(runtime: &RuntimeConfig) -> Result<()> {
    if let Some(rate) = runtime.events_per_second
        && rate <= 0.0
    {
        bail!("runtime.events_per_second must be greater than 0");
    }
    if let Some(max_events) = runtime.max_events
        && max_events == 0
    {
        bail!("runtime.max_events must be greater than 0");
    }
    ensure_optional_positive_usize(
        runtime.ingest_queue_capacity,
        "runtime.ingest_queue_capacity",
    )?;
    ensure_optional_positive_usize(runtime.ingest_batch_size, "runtime.ingest_batch_size")?;
    ensure_optional_positive_usize(
        runtime.ingest_batch_per_source,
        "runtime.ingest_batch_per_source",
    )?;
    ensure_optional_positive_usize(
        runtime.ingest_batch_per_connector,
        "runtime.ingest_batch_per_connector",
    )?;
    ensure_optional_non_empty(runtime.http_host.as_deref(), "runtime.http_host")?;
    ensure_optional_non_empty(runtime.kafka_group_id.as_deref(), "runtime.kafka_group_id")?;
    ensure_optional_positive_u64(runtime.kafka_poll_ms, "runtime.kafka_poll_ms")?;
    ensure_optional_positive_usize(runtime.kafka_max_messages, "runtime.kafka_max_messages")?;
    ensure_optional_positive_u64(
        runtime.watermark_idle_source_ms,
        "runtime.watermark_idle_source_ms",
    )?;
    ensure_optional_positive_usize(
        runtime.tail_channel_capacity,
        "runtime.tail_channel_capacity",
    )?;
    ensure_optional_positive_i64(
        runtime.tail_max_catchup_versions,
        "runtime.tail_max_catchup_versions",
    )?;
    ensure_optional_positive_usize(
        runtime.transient_segment_max_nodes,
        "runtime.transient_segment_max_nodes",
    )?;
    ensure_optional_nonnegative_i32(
        runtime.transient_segment_min_score,
        "runtime.transient_segment_min_score",
    )?;
    ensure_optional_positive_u16(runtime.admin_port, "runtime.admin_port")?;
    ensure_optional_non_empty(runtime.pgwire_addr.as_deref(), "runtime.pgwire_addr")?;
    ensure_optional_positive_usize(
        runtime.mv_flush.max_pending_deltas,
        "runtime.mv_flush.max_pending_deltas",
    )?;
    ensure_optional_positive_usize(
        runtime.mv_flush.max_pending_versions,
        "runtime.mv_flush.max_pending_versions",
    )?;
    ensure_optional_positive_usize(
        runtime.mv_flush.max_pending_rows,
        "runtime.mv_flush.max_pending_rows",
    )?;
    ensure_optional_positive_usize(
        runtime.mv_flush.max_pending_bytes,
        "runtime.mv_flush.max_pending_bytes",
    )?;
    ensure_optional_positive_u64(
        runtime.mv_flush.max_delay_ms,
        "runtime.mv_flush.max_delay_ms",
    )?;
    ensure_optional_positive_usize(
        runtime.mv_snapshot.max_pending_batches,
        "runtime.mv_snapshot.max_pending_batches",
    )?;
    ensure_optional_positive_usize(
        runtime.mv_snapshot.max_pending_rows,
        "runtime.mv_snapshot.max_pending_rows",
    )?;
    ensure_optional_positive_u64(
        runtime.mv_snapshot.max_delay_ms,
        "runtime.mv_snapshot.max_delay_ms",
    )?;
    Ok(())
}

fn validate_storage_config(storage: &StorageConfig) -> Result<()> {
    if storage.object_store_from_env && storage.data_dir.is_some() {
        bail!("storage.data_dir cannot be set when storage.object_store_from_env is true");
    }
    if !storage.object_store_from_env && storage.object_store_env_file.is_some() {
        bail!("storage.object_store_env_file requires storage.object_store_from_env = true");
    }
    if !storage.object_store_from_env && storage.slatedb_name.is_some() {
        bail!("storage.slatedb_name requires storage.object_store_from_env = true");
    }
    ensure_optional_non_empty(storage.data_dir.as_deref(), "storage.data_dir")?;
    ensure_optional_non_empty(
        storage.object_store_env_file.as_deref(),
        "storage.object_store_env_file",
    )?;
    ensure_optional_non_empty(storage.slatedb_name.as_deref(), "storage.slatedb_name")?;
    ensure_optional_non_empty(storage.slatedb_config.as_deref(), "storage.slatedb_config")?;
    ensure_optional_non_empty(
        storage.slatedb_env_prefix.as_deref(),
        "storage.slatedb_env_prefix",
    )?;
    ensure_optional_positive_u64(
        storage.slatedb_close_timeout_ms,
        "storage.slatedb_close_timeout_ms",
    )?;
    ensure_optional_positive_usize(
        storage.zset_compaction_max_chain_len,
        "storage.zset_compaction_max_chain_len",
    )?;
    ensure_optional_positive_usize(
        storage.zset_compaction_max_segments,
        "storage.zset_compaction_max_segments",
    )?;
    ensure_optional_positive_usize(
        storage.zset_compaction_max_concurrent_jobs,
        "storage.zset_compaction_max_concurrent_jobs",
    )?;
    ensure_optional_positive_u64(
        storage.zset_gc_grace_period_ms,
        "storage.zset_gc_grace_period_ms",
    )?;
    Ok(())
}

fn validate_maintenance_config(maintenance: &MaintenanceConfig) -> Result<()> {
    for (index, namespace) in maintenance.inspect_namespace.iter().enumerate() {
        ensure_non_empty(
            namespace,
            &format!("maintenance.inspect_namespace[{index}]"),
        )?;
    }
    for (index, namespace) in maintenance.compact_namespace.iter().enumerate() {
        ensure_non_empty(
            namespace,
            &format!("maintenance.compact_namespace[{index}]"),
        )?;
    }
    for (index, namespace) in maintenance.gc_namespace.iter().enumerate() {
        ensure_non_empty(namespace, &format!("maintenance.gc_namespace[{index}]"))?;
    }
    Ok(())
}

fn validate_replication_config(replication: &ReplicationConfig) -> Result<()> {
    ensure_positive_usize(
        replication.kafka.message_max_bytes,
        "replication.kafka.message_max_bytes",
    )?;
    ensure_non_empty(&replication.kafka.acks, "replication.kafka.acks")?;
    ensure_positive_usize(replication.kafka.batch_size, "replication.kafka.batch_size")?;
    ensure_positive_usize(
        replication.kafka.batch_num_messages,
        "replication.kafka.batch_num_messages",
    )?;
    ensure_positive_usize(
        replication.encoding.arrow_ipc_rows_per_record,
        "replication.encoding.arrow_ipc_rows_per_record",
    )?;
    ensure_positive_usize(
        replication.encoding.snapshot_batches_per_chunk,
        "replication.encoding.snapshot_batches_per_chunk",
    )?;
    Ok(())
}

fn validate_postgres_cdc_config(postgres_cdc: &PostgresCdcConfig) -> Result<()> {
    let snapshot = &postgres_cdc.snapshot;
    ensure_positive_usize(
        snapshot.rows_per_batch,
        "postgres_cdc.snapshot.rows_per_batch",
    )?;
    ensure_positive_usize(snapshot.max_workers, "postgres_cdc.snapshot.max_workers")?;
    ensure_positive_usize(
        snapshot.intra_table_chunks,
        "postgres_cdc.snapshot.intra_table_chunks",
    )?;
    ensure_positive_usize(snapshot.min_workers, "postgres_cdc.snapshot.min_workers")?;
    if !(1..=100).contains(&snapshot.wal_buffer_high_watermark_percent) {
        bail!("postgres_cdc.snapshot.wal_buffer_high_watermark_percent must be between 1 and 100");
    }
    if snapshot.wal_buffer_low_watermark_percent > 100 {
        bail!("postgres_cdc.snapshot.wal_buffer_low_watermark_percent must be <= 100");
    }
    ensure_positive_u64(snapshot.slow_scan_ms, "postgres_cdc.snapshot.slow_scan_ms")?;
    ensure_positive_u64(
        snapshot.controller_interval_ms,
        "postgres_cdc.snapshot.controller_interval_ms",
    )?;
    Ok(())
}

fn validate_connector(connector: &ConnectorConfig, index: usize) -> Result<()> {
    match connector {
        ConnectorConfig::Kafka {
            name,
            brokers,
            topics,
            group_id,
            default_source,
            poll_ms: _,
            max_messages_per_tick,
            format,
        } => {
            ensure_non_empty(brokers, &format!("connectors[{index}].brokers"))?;
            if topics.is_empty() {
                bail!("connectors[{index}].topics must not be empty");
            }
            for (topic_index, topic) in topics.iter().enumerate() {
                ensure_non_empty(topic, &format!("connectors[{index}].topics[{topic_index}]"))?;
            }
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_optional_non_empty(
                group_id.as_deref(),
                &format!("connectors[{index}].group_id"),
            )?;
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
            ensure_optional_positive_usize(
                *max_messages_per_tick,
                &format!("connectors[{index}].max_messages_per_tick"),
            )?;
            if let Some(format) = format {
                let normalized = format.to_ascii_lowercase();
                if normalized != "floe_json" && normalized != "debezium_json" {
                    bail!("connectors[{index}].format must be one of: floe_json, debezium_json");
                }
            }
        }
        ConnectorConfig::File {
            name,
            path,
            default_source,
        } => {
            ensure_non_empty(path, &format!("connectors[{index}].path"))?;
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
        }
        ConnectorConfig::Http {
            name,
            host,
            port,
            default_source,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_optional_non_empty(host.as_deref(), &format!("connectors[{index}].host"))?;
            if *port == 0 {
                bail!("connectors[{index}].port must be greater than 0");
            }
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
        }
        ConnectorConfig::Generator {
            name,
            events_per_second,
            max_events,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            if let Some(rate) = events_per_second
                && *rate <= 0.0
            {
                bail!("connectors[{index}].events_per_second must be greater than 0");
            }
            if let Some(limit) = max_events
                && *limit == 0
            {
                bail!("connectors[{index}].max_events must be greater than 0");
            }
        }
        ConnectorConfig::ObjectStore {
            name,
            url,
            default_source,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_non_empty(url, &format!("connectors[{index}].url"))?;
            Url::parse(url).with_context(|| {
                format!("connectors[{index}].url must be a valid URL (found '{url}')")
            })?;
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
        }
        ConnectorConfig::PostgresCdc {
            name,
            connection,
            slot,
            publication,
            include_tables,
            include_schema_in_source: _,
            schema_evolution_policy: _,
            auto_create_slot: _,
            auto_create_publication: _,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_non_empty(connection, &format!("connectors[{index}].connection"))?;
            connection
                .parse::<tokio_postgres::Config>()
                .with_context(|| {
                    format!(
                        "connectors[{index}].connection must be a valid Postgres connection string (found '{connection}')"
                    )
                })?;
            ensure_non_empty(slot, &format!("connectors[{index}].slot"))?;
            ensure_optional_non_empty(
                publication.as_deref(),
                &format!("connectors[{index}].publication"),
            )?;
            if let Some(tables) = include_tables {
                if tables.is_empty() {
                    bail!("connectors[{index}].include_tables must not be empty");
                }
                for (table_index, table) in tables.iter().enumerate() {
                    ensure_non_empty(
                        table,
                        &format!("connectors[{index}].include_tables[{table_index}]"),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_sink(sink: &SinkConfig, index: usize) -> Result<()> {
    match sink {
        SinkConfig::Kafka {
            name,
            brokers,
            topic,
            mv,
            format,
            key_columns,
            with_snapshot: _,
            as_of: _,
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
            transactional_id,
            checkpoint_topic,
            checkpoint_partition,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("sinks[{index}].name"))?;
            ensure_non_empty(brokers, &format!("sinks[{index}].brokers"))?;
            ensure_non_empty(topic, &format!("sinks[{index}].topic"))?;
            ensure_non_empty(mv, &format!("sinks[{index}].mv"))?;
            if let Some(format) = format {
                let normalized = normalize_sink_format(format);
                if !matches!(normalized.as_str(), "json" | "debezium_json") {
                    bail!(
                        "sinks[{index}].format must be one of json, debezium_json (found '{format}')"
                    );
                }
                if normalized == "debezium_json" && key_columns.as_ref().is_none_or(Vec::is_empty) {
                    bail!("sinks[{index}].key_columns is required for Debezium Kafka sinks");
                }
            }
            if let Some(key_columns) = key_columns {
                validate_key_columns(key_columns, &format!("sinks[{index}].key_columns"))?;
            }
            ensure_optional_positive_usize(*batch_rows, &format!("sinks[{index}].batch_rows"))?;
            ensure_optional_positive_usize(*batch_bytes, &format!("sinks[{index}].batch_bytes"))?;
            ensure_optional_positive_usize(
                *queue_capacity,
                &format!("sinks[{index}].queue_capacity"),
            )?;
            ensure_optional_positive_usize(
                *retry_max_attempts,
                &format!("sinks[{index}].retry_max_attempts"),
            )?;
            ensure_optional_positive_u64(*retry_base_ms, &format!("sinks[{index}].retry_base_ms"))?;
            ensure_optional_positive_u64(
                *retry_max_backoff_ms,
                &format!("sinks[{index}].retry_max_backoff_ms"),
            )?;
            ensure_optional_non_empty(
                transactional_id.as_deref(),
                &format!("sinks[{index}].transactional_id"),
            )?;
            ensure_optional_non_empty(
                checkpoint_topic.as_deref(),
                &format!("sinks[{index}].checkpoint_topic"),
            )?;
            if let Some(partition) = checkpoint_partition
                && *partition < 0
            {
                bail!("sinks[{index}].checkpoint_partition must be >= 0");
            }
        }
        SinkConfig::File {
            name,
            path,
            mv,
            with_snapshot: _,
            as_of: _,
            append: _,
            batch_rows,
            batch_bytes,
            queue_capacity,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("sinks[{index}].name"))?;
            ensure_non_empty(path, &format!("sinks[{index}].path"))?;
            ensure_non_empty(mv, &format!("sinks[{index}].mv"))?;
            ensure_optional_positive_usize(*batch_rows, &format!("sinks[{index}].batch_rows"))?;
            ensure_optional_positive_usize(*batch_bytes, &format!("sinks[{index}].batch_bytes"))?;
            ensure_optional_positive_usize(
                *queue_capacity,
                &format!("sinks[{index}].queue_capacity"),
            )?;
        }
        SinkConfig::Http {
            name,
            url,
            mv,
            with_snapshot: _,
            as_of: _,
            batch_size,
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("sinks[{index}].name"))?;
            ensure_non_empty(url, &format!("sinks[{index}].url"))?;
            Url::parse(url).with_context(|| {
                format!("sinks[{index}].url must be a valid URL (found '{url}')")
            })?;
            ensure_non_empty(mv, &format!("sinks[{index}].mv"))?;
            ensure_optional_positive_usize(*batch_size, &format!("sinks[{index}].batch_size"))?;
            ensure_optional_positive_usize(*batch_rows, &format!("sinks[{index}].batch_rows"))?;
            ensure_optional_positive_usize(*batch_bytes, &format!("sinks[{index}].batch_bytes"))?;
            ensure_optional_positive_usize(
                *queue_capacity,
                &format!("sinks[{index}].queue_capacity"),
            )?;
            ensure_optional_positive_usize(
                *retry_max_attempts,
                &format!("sinks[{index}].retry_max_attempts"),
            )?;
            ensure_optional_positive_u64(*retry_base_ms, &format!("sinks[{index}].retry_base_ms"))?;
            ensure_optional_positive_u64(
                *retry_max_backoff_ms,
                &format!("sinks[{index}].retry_max_backoff_ms"),
            )?;
        }
        SinkConfig::Postgres {
            name,
            connection,
            table,
            mv,
            mode,
            primary_key,
            with_snapshot: _,
            as_of: _,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("sinks[{index}].name"))?;
            ensure_non_empty(connection, &format!("sinks[{index}].connection"))?;
            connection
                .parse::<tokio_postgres::Config>()
                .with_context(|| {
                    format!("sinks[{index}].connection must be a valid Postgres connection string")
                })?;
            ensure_non_empty(table, &format!("sinks[{index}].table"))?;
            ensure_non_empty(mv, &format!("sinks[{index}].mv"))?;
            let normalized_mode = mode
                .as_deref()
                .map(normalize_sink_format)
                .unwrap_or_else(|| "upsert".to_string());
            if !matches!(normalized_mode.as_str(), "upsert" | "append_only") {
                bail!(
                    "sinks[{index}].mode must be one of upsert, append_only (found '{}')",
                    mode.as_deref().unwrap_or("upsert")
                );
            }
            if normalized_mode == "upsert" && primary_key.as_ref().is_none_or(Vec::is_empty) {
                bail!("sinks[{index}].primary_key is required for Postgres upsert sinks");
            }
            if let Some(primary_key) = primary_key {
                validate_key_columns(primary_key, &format!("sinks[{index}].primary_key"))?;
            }
            ensure_optional_positive_usize(
                *retry_max_attempts,
                &format!("sinks[{index}].retry_max_attempts"),
            )?;
            ensure_optional_positive_u64(*retry_base_ms, &format!("sinks[{index}].retry_base_ms"))?;
            ensure_optional_positive_u64(
                *retry_max_backoff_ms,
                &format!("sinks[{index}].retry_max_backoff_ms"),
            )?;
        }
    }
    Ok(())
}

fn ensure_non_empty(value: &str, field_path: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field_path} must not be empty");
    }
    Ok(())
}

fn ensure_optional_non_empty(value: Option<&str>, field_path: &str) -> Result<()> {
    if let Some(value) = value {
        ensure_non_empty(value, field_path)?;
    }
    Ok(())
}

fn normalize_sink_format(value: &str) -> String {
    value.to_ascii_lowercase().replace('-', "_")
}

fn validate_key_columns(columns: &[String], field_path: &str) -> Result<()> {
    if columns.is_empty() {
        bail!("{field_path} must not be empty");
    }
    let mut seen = HashSet::new();
    for (index, column) in columns.iter().enumerate() {
        ensure_non_empty(column, &format!("{field_path}[{index}]"))?;
        if !seen.insert(column.as_str()) {
            bail!("{field_path} contains duplicate column '{column}'");
        }
    }
    Ok(())
}

fn ensure_optional_positive_usize(value: Option<usize>, field_path: &str) -> Result<()> {
    if let Some(value) = value
        && value == 0
    {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}

fn ensure_optional_positive_u64(value: Option<u64>, field_path: &str) -> Result<()> {
    if let Some(value) = value
        && value == 0
    {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}

fn ensure_optional_positive_u16(value: Option<u16>, field_path: &str) -> Result<()> {
    if let Some(value) = value
        && value == 0
    {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}

fn ensure_optional_positive_i64(value: Option<i64>, field_path: &str) -> Result<()> {
    if let Some(value) = value
        && value <= 0
    {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}

fn ensure_optional_nonnegative_i32(value: Option<i32>, field_path: &str) -> Result<()> {
    if let Some(value) = value
        && value < 0
    {
        bail!("{field_path} must be greater than or equal to 0");
    }
    Ok(())
}

fn ensure_positive_usize(value: usize, field_path: &str) -> Result<()> {
    if value == 0 {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}

fn ensure_positive_u64(value: u64, field_path: &str) -> Result<()> {
    if value == 0 {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}
