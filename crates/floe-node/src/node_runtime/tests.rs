use super::*;
use crate::node_runtime::orchestration::{
    PostgresCdcRuntimeReconnectPolicy, kafka_metadata_journal_required_sources,
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
        data_dir: None,
        object_store_from_env: false,
        object_store_env_file: None,
        slatedb_name: None,
        pgwire_addr: None,
        disable_pgwire: false,
        admin_port: None,
        pre_tick_commit_delay_ms: None,
        watermark_idle_source_ms: None,
        subscribe_channel_capacity: None,
        subscribe_max_catchup_versions: None,
        transient_segment_max_nodes: None,
        transient_segment_min_score: None,
        slatedb_config: None,
        slatedb_env_prefix: None,
        slatedb_flush_interval_ms: None,
        slatedb_l0_sst_size_bytes: None,
        slatedb_max_wal_flushes_before_l0_flush: None,
        slatedb_l0_max_ssts: None,
        slatedb_l0_max_ssts_per_key: None,
        slatedb_max_unflushed_bytes: None,
        slatedb_compaction_max_sst_bytes: None,
        slatedb_compaction_max_concurrent: None,
        slatedb_await_durable: None,
        slatedb_cache_dir: None,
        slatedb_cache_max_bytes: None,
        slatedb_cache_part_bytes: None,
        slatedb_cache_puts: false,
        slatedb_cache_max_open_file_handles: None,
        slatedb_close_timeout_ms: None,
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

fn event(source: &str, id: i64) -> core_source::AppendIngestEvent {
    core_source::AppendIngestEvent::new(source, json!({ "id": id }))
}

fn queued_event(source: &str, id: i64) -> QueuedAppendIngestEvent {
    QueuedAppendIngestEvent {
        event: event(source, id),
        commit_ack: None,
    }
}

#[path = "tests/config_validation.rs"]
mod config_validation;
#[path = "tests/postgres_cdc_reconnect.rs"]
mod postgres_cdc_reconnect;
#[path = "tests/postgres_runtime_plan.rs"]
mod postgres_runtime_plan;
#[path = "tests/runtime_config.rs"]
mod runtime_config;
#[path = "tests/sql_catalog.rs"]
mod sql_catalog;
#[path = "tests/watermarks.rs"]
mod watermarks;
