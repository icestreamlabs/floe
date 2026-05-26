use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
    register_histogram, register_histogram_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec,
};

static INGEST_QUEUE_DEPTH: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_ingest_queue_depth",
        "Number of events buffered between connectors and the executor"
    )
    .expect("register floe_ingest_queue_depth")
});

static INGEST_DECODE_LATENCY_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_ingest_decode_latency_ms",
        "Time spent decoding a batch of source events in milliseconds",
    ))
    .expect("register floe_ingest_decode_latency_ms")
});

static INGEST_TICK_LATENCY_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_ingest_tick_latency_ms",
        "Time spent advancing source frontiers per ingestion tick in milliseconds",
    ))
    .expect("register floe_ingest_tick_latency_ms")
});

static INGEST_TICK_PHASE_LATENCY_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_ingest_tick_phase_latency_ms",
        "Time spent in ingest tick phases in milliseconds",
        &["phase"]
    )
    .expect("register floe_ingest_tick_phase_latency_ms")
});

static INGEST_TICKS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_ingest_ticks_total",
        "Total number of successful ingest ticks",
        &["result"]
    )
    .expect("register floe_ingest_ticks_total")
});

static LAST_COMMITTED_TICK: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_last_committed_tick",
        "Most recently committed ingestion tick id"
    )
    .expect("register floe_last_committed_tick")
});

static CHECKPOINT_AGE_SECONDS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_checkpoint_age_seconds",
        "Seconds elapsed since the latest committed tick checkpoint"
    )
    .expect("register floe_checkpoint_age_seconds")
});

static SOURCE_OFFSET_LAG: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_source_offset_lag",
        "Difference between latest observed source offset and last committed offset",
        &["source", "partition"]
    )
    .expect("register floe_source_offset_lag")
});

static WATERMARK_LAG_MS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_watermark_lag_ms",
        "Difference between wall-clock time and current global watermark in milliseconds",
    )
    .expect("register floe_watermark_lag_ms")
});

static SOURCE_WATERMARK_MS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_source_watermark_ms",
        "Latest observed watermark timestamp (ms) per source",
        &["source"]
    )
    .expect("register floe_source_watermark_ms")
});

static GLOBAL_WATERMARK_MS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_global_watermark_ms",
        "Latest propagated global watermark timestamp (ms)",
    )
    .expect("register floe_global_watermark_ms")
});

static MV_FRESHNESS_SECONDS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_mv_freshness_seconds",
        "Seconds since each materialized view last advanced to a new committed version",
        &["view"]
    )
    .expect("register floe_mv_freshness_seconds")
});

static SINK_QUEUE_DEPTH: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_sink_queue_depth",
        "Number of records currently buffered in a sink queue",
        &["sink"]
    )
    .expect("register floe_sink_queue_depth")
});

static SINK_VERSION_LAG: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_sink_version_lag",
        "Difference between latest enqueued and latest flushed MV version per sink",
        &["sink"]
    )
    .expect("register floe_sink_version_lag")
});

static SINK_FAILURES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_sink_failures_total",
        "Total sink emission failures by sink and transport",
        &["sink", "transport"]
    )
    .expect("register floe_sink_failures_total")
});

static SINK_RETRIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_sink_retries_total",
        "Total sink retry attempts by sink and transport",
        &["sink", "transport"]
    )
    .expect("register floe_sink_retries_total")
});

static RUNTIME_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_runtime_errors_total",
        "Total runtime errors by component",
        &["component"]
    )
    .expect("register floe_runtime_errors_total")
});

static POSTGRES_CDC_UPSTREAM_LSN: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_upstream_lsn",
        "Latest observed upstream Postgres WAL LSN per CDC source and slot",
        &["source", "slot"]
    )
    .expect("register floe_postgres_cdc_upstream_lsn")
});

static POSTGRES_CDC_DURABLE_LSN: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_durable_lsn",
        "Latest SlateDB-durable Postgres CDC LSN per source and slot",
        &["source", "slot"]
    )
    .expect("register floe_postgres_cdc_durable_lsn")
});

static POSTGRES_CDC_SOURCE_LAG_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_source_lag_bytes",
        "Byte lag between latest observed upstream Postgres WAL LSN and durable Floe CDC LSN",
        &["source", "slot"]
    )
    .expect("register floe_postgres_cdc_source_lag_bytes")
});

static POSTGRES_CDC_TABLE_LAST_APPLIED_LSN: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_table_last_applied_lsn",
        "Latest SlateDB-durable Postgres CDC LSN applied to each CDC table",
        &["source", "slot", "table"]
    )
    .expect("register floe_postgres_cdc_table_last_applied_lsn")
});

static POSTGRES_CDC_TABLE_LAG_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_table_lag_bytes",
        "Byte lag between latest observed upstream Postgres WAL LSN and each CDC table's durable applied LSN",
        &["source", "slot", "table"]
    )
    .expect("register floe_postgres_cdc_table_lag_bytes")
});

static POSTGRES_CDC_SCHEMA_EVOLUTION_POLICY: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_schema_evolution_policy",
        "Configured Postgres CDC schema evolution policy per source; the active policy label is set to 1",
        &["source", "policy"]
    )
    .expect("register floe_postgres_cdc_schema_evolution_policy")
});

static POSTGRES_CDC_SCHEMA_EVOLUTION_EVENTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_postgres_cdc_schema_evolution_events_total",
        "Observed Postgres CDC schema evolution events by source, table, outcome, and policy",
        &["source", "table", "outcome", "policy"]
    )
    .expect("register floe_postgres_cdc_schema_evolution_events_total")
});

static POSTGRES_CDC_SCHEMA_EVOLUTION_LAST_OBSERVED_UNIX_MS: LazyLock<IntGaugeVec> = LazyLock::new(
    || {
        register_int_gauge_vec!(
            "floe_postgres_cdc_schema_evolution_last_observed_unix_ms",
            "Unix timestamp in milliseconds for the latest observed Postgres CDC schema evolution event",
            &["source", "table", "outcome", "policy"]
        )
        .expect("register floe_postgres_cdc_schema_evolution_last_observed_unix_ms")
    },
);

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_TARGET: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_snapshot_concurrency_target",
        "Current adaptive Postgres CDC initial snapshot scan concurrency target",
        &["source", "slot"]
    )
    .expect("register floe_postgres_cdc_snapshot_concurrency_target")
});

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ACTIVE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_snapshot_concurrency_active",
        "Current active Postgres CDC initial snapshot scan workers",
        &["source", "slot"]
    )
    .expect("register floe_postgres_cdc_snapshot_concurrency_active")
});

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_MAX: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_snapshot_concurrency_max",
        "Maximum configured Postgres CDC initial snapshot scan workers",
        &["source", "slot"]
    )
    .expect("register floe_postgres_cdc_snapshot_concurrency_max")
});

static POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_FILL_PERCENT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
            "floe_postgres_cdc_snapshot_wal_buffer_fill_percent",
            "Percent fill of the in-memory WAL buffer used while Postgres CDC initial snapshot scans are running",
            &["source", "slot"]
        )
        .expect("register floe_postgres_cdc_snapshot_wal_buffer_fill_percent")
});

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ADJUSTMENTS_TOTAL: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        register_int_counter_vec!(
            "floe_postgres_cdc_snapshot_concurrency_adjustments_total",
            "Adaptive Postgres CDC initial snapshot concurrency target changes",
            &["source", "slot", "direction", "reason"]
        )
        .expect("register floe_postgres_cdc_snapshot_concurrency_adjustments_total")
    });

static CDC_BUFFER_PENDING_TRANSACTIONS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_transactions",
        "Number of pending transactions in each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_pending_transactions")
});

static CDC_BUFFER_PENDING_OBJECTS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_objects",
        "Number of pending payload objects in each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_pending_objects")
});

static CDC_BUFFER_PENDING_RECORDS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_records",
        "Number of pending records in each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_pending_records")
});

static CDC_BUFFER_PENDING_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_bytes",
        "Approximate pending payload bytes in each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_pending_bytes")
});

static CDC_BUFFER_OLDEST_PENDING_AGE_MS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_oldest_pending_age_ms",
        "Age in milliseconds of the oldest pending transaction in each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_oldest_pending_age_ms")
});

static CDC_BUFFER_OBJECT_OPS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_object_ops_total",
        "CDC replication buffer object-store operations",
        &["pipeline", "operation"]
    )
    .expect("register floe_cdc_buffer_object_ops_total")
});

static CDC_BUFFER_APPENDED_RECORDS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_appended_records_total",
        "Number of records appended to each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_appended_records_total")
});

static CDC_BUFFER_APPENDED_BYTES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_appended_bytes_total",
        "Approximate payload bytes appended to each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_appended_bytes_total")
});

static CDC_BUFFER_APPEND_LATENCY_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_buffer_append_latency_ms",
        "Time spent appending a transaction to each CDC replication buffer in milliseconds",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_append_latency_ms")
});

static CDC_BUFFER_FORCED_FLUSHES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_forced_flushes_total",
        "Number of explicit CDC replication buffer flushes",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_forced_flushes_total")
});

static CDC_BUFFER_FLUSH_LATENCY_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_buffer_flush_latency_ms",
        "Time spent flushing each CDC replication buffer in milliseconds",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_flush_latency_ms")
});

static CDC_BUFFER_DRAIN_ATTEMPTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_drain_attempts_total",
        "Number of CDC replication buffer drain attempts before accepting more source data",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_drain_attempts_total")
});

static CDC_BUFFER_SOURCE_BACKPRESSURE_ACTIVE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_source_backpressure_active",
        "Whether a CDC replication buffer is applying source backpressure because pending limits remain exceeded",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_source_backpressure_active")
});

static CDC_BUFFER_REPLAYED_RECORDS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_replayed_records_total",
        "Number of records replayed from each CDC replication buffer",
        &["pipeline"]
    )
    .expect("register floe_cdc_buffer_replayed_records_total")
});

static CDC_BUFFER_REPLAY_LATENCY_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_buffer_replay_latency_ms",
        "Time spent replaying records from each CDC replication buffer in milliseconds",
        &["pipeline", "phase"]
    )
    .expect("register floe_cdc_buffer_replay_latency_ms")
});

static CDC_REPLICATION_REPLAYING: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_replication_replaying",
        "Whether a CDC replication pipeline is actively replaying buffered records",
        &["pipeline"]
    )
    .expect("register floe_cdc_replication_replaying")
});

static CDC_REPLICATION_TARGET_ERROR: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_replication_target_error",
        "Whether the last CDC replication target delivery attempt failed",
        &["pipeline"]
    )
    .expect("register floe_cdc_replication_target_error")
});

static CDC_REPLICATION_TARGET_FAILURES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_replication_target_failures_total",
        "CDC replication target delivery failures by pipeline, target kind, and failure class",
        &["pipeline", "target_kind", "class"]
    )
    .expect("register floe_cdc_replication_target_failures_total")
});

static CDC_REPLICATION_TARGET_WRITE_LATENCY_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_replication_target_write_latency_ms",
        "Time spent delivering CDC replication records to a target in milliseconds",
        &["pipeline", "target_kind", "result"]
    )
    .expect("register floe_cdc_replication_target_write_latency_ms")
});

static CDC_REPLICATION_TARGET_WRITE_BATCH_RECORDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_replication_target_write_batch_records",
        "CDC replication target write batch size in records",
        &["pipeline", "target_kind", "result"]
    )
    .expect("register floe_cdc_replication_target_write_batch_records")
});

static CDC_REPLICATION_TARGET_WRITE_RECORDS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_replication_target_write_records_total",
        "Total CDC replication records delivered or attempted by pipeline, target kind, and result",
        &["pipeline", "target_kind", "result"]
    )
    .expect("register floe_cdc_replication_target_write_records_total")
});

static CDC_REPLICATION_DLQ_REPLAYS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_replication_dlq_replays_total",
        "Manual CDC replication DLQ replay attempts by pipeline and result",
        &["pipeline", "result"]
    )
    .expect("register floe_cdc_replication_dlq_replays_total")
});

static CDC_REPLICATION_DLQ_ENTRIES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_replication_dlq_entries",
        "CDC replication DLQ entries by pipeline and status",
        &["pipeline", "status"]
    )
    .expect("register floe_cdc_replication_dlq_entries")
});

static CDC_REPLICATION_DLQ_OLDEST_PENDING_AGE_MS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_replication_dlq_oldest_pending_age_ms",
        "Oldest pending CDC replication DLQ entry age in milliseconds",
        &["pipeline"]
    )
    .expect("register floe_cdc_replication_dlq_oldest_pending_age_ms")
});

static POSTGRES_CDC_METRIC_STATE: LazyLock<Mutex<PostgresCdcMetricState>> =
    LazyLock::new(|| Mutex::new(PostgresCdcMetricState::default()));

#[derive(Debug, Default)]
struct PostgresCdcMetricState {
    upstream_lsn_by_source: HashMap<PostgresSourceMetricKey, u64>,
    durable_lsn_by_source: HashMap<PostgresSourceMetricKey, u64>,
    table_applied_lsn: HashMap<PostgresTableMetricKey, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PostgresSourceMetricKey {
    source: String,
    slot: String,
}

impl PostgresSourceMetricKey {
    fn new(source: &str, slot: &str) -> Self {
        Self {
            source: source.to_string(),
            slot: slot.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PostgresTableMetricKey {
    source: String,
    slot: String,
    table: String,
}

impl PostgresTableMetricKey {
    fn new(source: &str, slot: &str, table: &str) -> Self {
        Self {
            source: source.to_string(),
            slot: slot.to_string(),
            table: table.to_string(),
        }
    }
}

pub(crate) fn record_ingest_queue_depth(depth: usize) {
    INGEST_QUEUE_DEPTH.set(depth as i64);
}

pub(crate) fn observe_decode_latency_ms(latency_ms: u64) {
    INGEST_DECODE_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn observe_tick_latency_ms(latency_ms: u64) {
    INGEST_TICK_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn observe_tick_phase_latency_ms(phase: &str, latency_ms: u64) {
    INGEST_TICK_PHASE_LATENCY_MS
        .with_label_values(&[phase])
        .observe(latency_ms as f64);
}

pub(crate) fn inc_ingest_tick(result: &str) {
    INGEST_TICKS_TOTAL.with_label_values(&[result]).inc();
}

pub(crate) fn record_last_committed_tick(tick: u64) {
    LAST_COMMITTED_TICK.set(i64::try_from(tick).unwrap_or(i64::MAX));
}

pub(crate) fn record_checkpoint_age_seconds(age_seconds: u64) {
    CHECKPOINT_AGE_SECONDS.set(i64::try_from(age_seconds).unwrap_or(i64::MAX));
}

pub(crate) fn record_source_offset_lag(source: &str, partition: u32, lag: u64) {
    let partition = partition.to_string();
    SOURCE_OFFSET_LAG
        .with_label_values(&[source, partition.as_str()])
        .set(i64::try_from(lag).unwrap_or(i64::MAX));
}

pub(crate) fn record_watermark_lag_ms(lag_ms: u64) {
    WATERMARK_LAG_MS.set(i64::try_from(lag_ms).unwrap_or(i64::MAX));
}

pub(crate) fn record_source_watermark_ms(source: &str, watermark_ms: i64) {
    SOURCE_WATERMARK_MS
        .with_label_values(&[source])
        .set(watermark_ms.max(0));
}

pub(crate) fn record_global_watermark_ms(watermark_ms: i64) {
    GLOBAL_WATERMARK_MS.set(watermark_ms.max(0));
}

pub(crate) fn record_mv_freshness_seconds(view: &str, age_seconds: u64) {
    MV_FRESHNESS_SECONDS
        .with_label_values(&[view])
        .set(i64::try_from(age_seconds).unwrap_or(i64::MAX));
}

pub(crate) fn record_sink_queue_depth(sink: &str, depth: usize) {
    SINK_QUEUE_DEPTH
        .with_label_values(&[sink])
        .set(depth as i64);
}

pub(crate) fn record_sink_version_lag(sink: &str, lag: i64) {
    SINK_VERSION_LAG.with_label_values(&[sink]).set(lag.max(0));
}

pub(crate) fn inc_sink_failure(sink: &str, transport: &str) {
    SINK_FAILURES.with_label_values(&[sink, transport]).inc();
}

pub(crate) fn inc_sink_retry(sink: &str, transport: &str) {
    SINK_RETRIES.with_label_values(&[sink, transport]).inc();
}

pub(crate) fn inc_runtime_error(component: &str) {
    RUNTIME_ERRORS.with_label_values(&[component]).inc();
}

pub(crate) fn record_postgres_cdc_upstream_lsn(source: &str, slot: &str, lsn: u64) {
    let mut state = POSTGRES_CDC_METRIC_STATE
        .lock()
        .expect("Postgres CDC metric state poisoned");
    let key = PostgresSourceMetricKey::new(source, slot);
    let upstream_lsn = record_max_lsn(&mut state.upstream_lsn_by_source, key.clone(), lsn);
    POSTGRES_CDC_UPSTREAM_LSN
        .with_label_values(&[source, slot])
        .set(i64_from_u64(upstream_lsn));

    if let Some(durable_lsn) = state.durable_lsn_by_source.get(&key).copied() {
        POSTGRES_CDC_SOURCE_LAG_BYTES
            .with_label_values(&[source, slot])
            .set(i64_from_u64(upstream_lsn.saturating_sub(durable_lsn)));
    }

    for (table_key, applied_lsn) in &state.table_applied_lsn {
        if table_key.source == source && table_key.slot == slot {
            POSTGRES_CDC_TABLE_LAG_BYTES
                .with_label_values(&[source, slot, table_key.table.as_str()])
                .set(i64_from_u64(upstream_lsn.saturating_sub(*applied_lsn)));
        }
    }
}

pub(crate) fn record_postgres_cdc_durable_lsn(source: &str, slot: &str, lsn: u64) {
    let mut state = POSTGRES_CDC_METRIC_STATE
        .lock()
        .expect("Postgres CDC metric state poisoned");
    let key = PostgresSourceMetricKey::new(source, slot);
    let durable_lsn = record_max_lsn(&mut state.durable_lsn_by_source, key.clone(), lsn);
    POSTGRES_CDC_DURABLE_LSN
        .with_label_values(&[source, slot])
        .set(i64_from_u64(durable_lsn));

    if let Some(upstream_lsn) = state.upstream_lsn_by_source.get(&key).copied() {
        POSTGRES_CDC_SOURCE_LAG_BYTES
            .with_label_values(&[source, slot])
            .set(i64_from_u64(upstream_lsn.saturating_sub(durable_lsn)));
    }
}

pub(crate) fn record_postgres_cdc_table_applied_lsn(
    source: &str,
    slot: &str,
    table: &str,
    lsn: u64,
) {
    let mut state = POSTGRES_CDC_METRIC_STATE
        .lock()
        .expect("Postgres CDC metric state poisoned");
    let key = PostgresTableMetricKey::new(source, slot, table);
    let applied_lsn = record_max_lsn(&mut state.table_applied_lsn, key, lsn);
    POSTGRES_CDC_TABLE_LAST_APPLIED_LSN
        .with_label_values(&[source, slot, table])
        .set(i64_from_u64(applied_lsn));

    let source_key = PostgresSourceMetricKey::new(source, slot);
    if let Some(upstream_lsn) = state.upstream_lsn_by_source.get(&source_key).copied() {
        POSTGRES_CDC_TABLE_LAG_BYTES
            .with_label_values(&[source, slot, table])
            .set(i64_from_u64(upstream_lsn.saturating_sub(applied_lsn)));
    }
}

pub(crate) fn record_postgres_cdc_schema_evolution_policy(source: &str, active_policy: &str) {
    for policy in [
        "fail_fast",
        "ignore_compatible",
        "apply_compatible_additions",
    ] {
        POSTGRES_CDC_SCHEMA_EVOLUTION_POLICY
            .with_label_values(&[source, policy])
            .set(i64::from(policy == active_policy));
    }
}

pub(crate) fn record_postgres_cdc_schema_evolution_observation(
    source: &str,
    table: &str,
    outcome: &str,
    policy: &str,
    observed_at_unix_ms: u64,
) {
    POSTGRES_CDC_SCHEMA_EVOLUTION_EVENTS_TOTAL
        .with_label_values(&[source, table, outcome, policy])
        .inc();
    POSTGRES_CDC_SCHEMA_EVOLUTION_LAST_OBSERVED_UNIX_MS
        .with_label_values(&[source, table, outcome, policy])
        .set(i64_from_u64(observed_at_unix_ms));
}

pub(crate) fn record_postgres_cdc_snapshot_concurrency(
    source: &str,
    slot: &str,
    target: usize,
    active: usize,
    max: usize,
) {
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_TARGET
        .with_label_values(&[source, slot])
        .set(i64_from_usize(target));
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ACTIVE
        .with_label_values(&[source, slot])
        .set(i64_from_usize(active));
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_MAX
        .with_label_values(&[source, slot])
        .set(i64_from_usize(max));
}

pub(crate) fn record_postgres_cdc_snapshot_wal_buffer_fill(
    source: &str,
    slot: &str,
    pending: usize,
    capacity: usize,
) {
    let fill_percent = if capacity == 0 {
        0
    } else {
        pending.saturating_mul(100) / capacity
    };
    POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_FILL_PERCENT
        .with_label_values(&[source, slot])
        .set(i64_from_usize(fill_percent.min(100)));
}

pub(crate) fn inc_postgres_cdc_snapshot_concurrency_adjustment(
    source: &str,
    slot: &str,
    direction: &str,
    reason: &str,
) {
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ADJUSTMENTS_TOTAL
        .with_label_values(&[source, slot, direction, reason])
        .inc();
}

pub(crate) fn record_cdc_buffer_pending(
    pipeline: &str,
    transactions: usize,
    records: usize,
    bytes: usize,
    oldest_age_ms: Option<u64>,
) {
    CDC_BUFFER_PENDING_TRANSACTIONS
        .with_label_values(&[pipeline])
        .set(i64::try_from(transactions).unwrap_or(i64::MAX));
    CDC_BUFFER_PENDING_OBJECTS
        .with_label_values(&[pipeline])
        .set(i64::try_from(transactions).unwrap_or(i64::MAX));
    CDC_BUFFER_PENDING_RECORDS
        .with_label_values(&[pipeline])
        .set(i64::try_from(records).unwrap_or(i64::MAX));
    CDC_BUFFER_PENDING_BYTES
        .with_label_values(&[pipeline])
        .set(i64::try_from(bytes).unwrap_or(i64::MAX));
    CDC_BUFFER_OLDEST_PENDING_AGE_MS
        .with_label_values(&[pipeline])
        .set(oldest_age_ms.map(i64_from_u64).unwrap_or(0));
}

pub(crate) fn inc_cdc_buffer_object_op(pipeline: &str, operation: &str, count: usize) {
    if count == 0 {
        return;
    }
    CDC_BUFFER_OBJECT_OPS_TOTAL
        .with_label_values(&[pipeline, operation])
        .inc_by(u64::try_from(count).unwrap_or(u64::MAX));
}

pub(crate) fn record_cdc_buffer_append(
    pipeline: &str,
    records: usize,
    bytes: usize,
    latency_ms: u64,
) {
    CDC_BUFFER_APPENDED_RECORDS_TOTAL
        .with_label_values(&[pipeline])
        .inc_by(u64::try_from(records).unwrap_or(u64::MAX));
    CDC_BUFFER_APPENDED_BYTES_TOTAL
        .with_label_values(&[pipeline])
        .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    CDC_BUFFER_APPEND_LATENCY_MS
        .with_label_values(&[pipeline])
        .observe(latency_ms as f64);
}

pub(crate) fn inc_cdc_buffer_forced_flush(pipeline: &str) {
    CDC_BUFFER_FORCED_FLUSHES_TOTAL
        .with_label_values(&[pipeline])
        .inc();
}

pub(crate) fn observe_cdc_buffer_flush_latency_ms(pipeline: &str, latency_ms: u64) {
    CDC_BUFFER_FLUSH_LATENCY_MS
        .with_label_values(&[pipeline])
        .observe(latency_ms as f64);
}

pub(crate) fn inc_cdc_buffer_drain_attempt(pipeline: &str) {
    CDC_BUFFER_DRAIN_ATTEMPTS_TOTAL
        .with_label_values(&[pipeline])
        .inc();
}

pub(crate) fn record_cdc_buffer_source_backpressure_active(pipeline: &str, active: bool) {
    CDC_BUFFER_SOURCE_BACKPRESSURE_ACTIVE
        .with_label_values(&[pipeline])
        .set(if active { 1 } else { 0 });
}

pub(crate) fn record_cdc_buffer_replay(pipeline: &str, delivered_records: usize, latency_ms: u64) {
    CDC_BUFFER_REPLAYED_RECORDS_TOTAL
        .with_label_values(&[pipeline])
        .inc_by(u64::try_from(delivered_records).unwrap_or(u64::MAX));
    CDC_BUFFER_REPLAY_LATENCY_MS
        .with_label_values(&[pipeline, "total"])
        .observe(latency_ms as f64);
}

pub(crate) fn observe_cdc_buffer_replay_phase_latency_ms(
    pipeline: &str,
    phase: &str,
    latency_ms: u64,
) {
    CDC_BUFFER_REPLAY_LATENCY_MS
        .with_label_values(&[pipeline, phase])
        .observe(latency_ms as f64);
}

pub(crate) fn record_cdc_replication_replaying(pipeline: &str, replaying: bool) {
    CDC_REPLICATION_REPLAYING
        .with_label_values(&[pipeline])
        .set(if replaying { 1 } else { 0 });
}

pub(crate) fn record_cdc_replication_target_error(pipeline: &str, failed: bool) {
    CDC_REPLICATION_TARGET_ERROR
        .with_label_values(&[pipeline])
        .set(if failed { 1 } else { 0 });
}

pub(crate) fn inc_cdc_replication_target_failure(
    pipeline: &str,
    target_kind: &str,
    failure_class: &str,
) {
    CDC_REPLICATION_TARGET_FAILURES_TOTAL
        .with_label_values(&[pipeline, target_kind, failure_class])
        .inc();
}

pub(crate) fn record_cdc_replication_target_write(
    pipeline: &str,
    target_kind: &str,
    result: &str,
    records: usize,
    latency_ms: u64,
) {
    let label_values = &[pipeline, target_kind, result];
    CDC_REPLICATION_TARGET_WRITE_LATENCY_MS
        .with_label_values(label_values)
        .observe(latency_ms as f64);
    CDC_REPLICATION_TARGET_WRITE_BATCH_RECORDS
        .with_label_values(label_values)
        .observe(records as f64);
    CDC_REPLICATION_TARGET_WRITE_RECORDS_TOTAL
        .with_label_values(label_values)
        .inc_by(u64::try_from(records).unwrap_or(u64::MAX));
}

pub(crate) fn inc_cdc_replication_dlq_replay(pipeline: &str, result: &str) {
    CDC_REPLICATION_DLQ_REPLAYS_TOTAL
        .with_label_values(&[pipeline, result])
        .inc();
}

pub(crate) fn record_cdc_replication_dlq_stats(
    pipeline: &str,
    pending: usize,
    replayed: usize,
    discarded: usize,
    oldest_pending_age_ms: Option<u64>,
) {
    CDC_REPLICATION_DLQ_ENTRIES
        .with_label_values(&[pipeline, "pending"])
        .set(i64::try_from(pending).unwrap_or(i64::MAX));
    CDC_REPLICATION_DLQ_ENTRIES
        .with_label_values(&[pipeline, "replayed"])
        .set(i64::try_from(replayed).unwrap_or(i64::MAX));
    CDC_REPLICATION_DLQ_ENTRIES
        .with_label_values(&[pipeline, "discarded"])
        .set(i64::try_from(discarded).unwrap_or(i64::MAX));
    CDC_REPLICATION_DLQ_OLDEST_PENDING_AGE_MS
        .with_label_values(&[pipeline])
        .set(
            oldest_pending_age_ms
                .map(|age_ms| i64::try_from(age_ms).unwrap_or(i64::MAX))
                .unwrap_or(0),
        );
}

pub(crate) fn init() {
    let _ = &*INGEST_QUEUE_DEPTH;
    let _ = &*INGEST_DECODE_LATENCY_MS;
    let _ = &*INGEST_TICK_LATENCY_MS;
    let _ = &*INGEST_TICK_PHASE_LATENCY_MS;
    let _ = &*INGEST_TICKS_TOTAL;
    let _ = &*LAST_COMMITTED_TICK;
    let _ = &*CHECKPOINT_AGE_SECONDS;
    let _ = &*SOURCE_OFFSET_LAG;
    let _ = &*WATERMARK_LAG_MS;
    let _ = &*SOURCE_WATERMARK_MS;
    let _ = &*GLOBAL_WATERMARK_MS;
    let _ = &*MV_FRESHNESS_SECONDS;
    let _ = &*SINK_QUEUE_DEPTH;
    let _ = &*SINK_VERSION_LAG;
    let _ = &*SINK_FAILURES;
    let _ = &*SINK_RETRIES;
    let _ = &*RUNTIME_ERRORS;
    let _ = &*POSTGRES_CDC_UPSTREAM_LSN;
    let _ = &*POSTGRES_CDC_DURABLE_LSN;
    let _ = &*POSTGRES_CDC_SOURCE_LAG_BYTES;
    let _ = &*POSTGRES_CDC_TABLE_LAST_APPLIED_LSN;
    let _ = &*POSTGRES_CDC_TABLE_LAG_BYTES;
    let _ = &*POSTGRES_CDC_SCHEMA_EVOLUTION_POLICY;
    let _ = &*POSTGRES_CDC_SCHEMA_EVOLUTION_EVENTS_TOTAL;
    let _ = &*POSTGRES_CDC_SCHEMA_EVOLUTION_LAST_OBSERVED_UNIX_MS;
    let _ = &*POSTGRES_CDC_SNAPSHOT_CONCURRENCY_TARGET;
    let _ = &*POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ACTIVE;
    let _ = &*POSTGRES_CDC_SNAPSHOT_CONCURRENCY_MAX;
    let _ = &*POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_FILL_PERCENT;
    let _ = &*POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ADJUSTMENTS_TOTAL;
    let _ = &*CDC_BUFFER_PENDING_TRANSACTIONS;
    let _ = &*CDC_BUFFER_PENDING_OBJECTS;
    let _ = &*CDC_BUFFER_PENDING_RECORDS;
    let _ = &*CDC_BUFFER_PENDING_BYTES;
    let _ = &*CDC_BUFFER_OLDEST_PENDING_AGE_MS;
    let _ = &*CDC_BUFFER_OBJECT_OPS_TOTAL;
    let _ = &*CDC_BUFFER_APPENDED_RECORDS_TOTAL;
    let _ = &*CDC_BUFFER_APPENDED_BYTES_TOTAL;
    let _ = &*CDC_BUFFER_APPEND_LATENCY_MS;
    let _ = &*CDC_BUFFER_FORCED_FLUSHES_TOTAL;
    let _ = &*CDC_BUFFER_FLUSH_LATENCY_MS;
    let _ = &*CDC_BUFFER_DRAIN_ATTEMPTS_TOTAL;
    let _ = &*CDC_BUFFER_SOURCE_BACKPRESSURE_ACTIVE;
    let _ = &*CDC_BUFFER_REPLAYED_RECORDS_TOTAL;
    let _ = &*CDC_BUFFER_REPLAY_LATENCY_MS;
    let _ = &*CDC_REPLICATION_REPLAYING;
    let _ = &*CDC_REPLICATION_TARGET_ERROR;
    let _ = &*CDC_REPLICATION_TARGET_FAILURES_TOTAL;
    let _ = &*CDC_REPLICATION_TARGET_WRITE_LATENCY_MS;
    let _ = &*CDC_REPLICATION_TARGET_WRITE_BATCH_RECORDS;
    let _ = &*CDC_REPLICATION_TARGET_WRITE_RECORDS_TOTAL;
    let _ = &*CDC_REPLICATION_DLQ_REPLAYS_TOTAL;
    let _ = &*CDC_REPLICATION_DLQ_ENTRIES;
    let _ = &*CDC_REPLICATION_DLQ_OLDEST_PENDING_AGE_MS;
    let _ = &*POSTGRES_CDC_METRIC_STATE;
}

fn record_max_lsn<K>(values: &mut HashMap<K, u64>, key: K, lsn: u64) -> u64
where
    K: Eq + std::hash::Hash,
{
    let entry = values.entry(key).or_insert(lsn);
    *entry = (*entry).max(lsn);
    *entry
}

fn i64_from_u64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn i64_from_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use prometheus::{Encoder, TextEncoder};

    use super::*;

    #[test]
    fn postgres_cdc_metrics_record_source_and_table_lag() {
        let source = "pg_metrics_test";
        let slot = "slot_metrics_test";
        let table = "orders_metrics_test";

        record_postgres_cdc_upstream_lsn(source, slot, 150);
        record_postgres_cdc_durable_lsn(source, slot, 100);
        record_postgres_cdc_table_applied_lsn(source, slot, table, 90);
        record_postgres_cdc_schema_evolution_policy(source, "ignore_compatible");
        record_postgres_cdc_schema_evolution_observation(
            source,
            table,
            "compatible_addition",
            "ignore_compatible",
            123_456,
        );
        record_postgres_cdc_snapshot_concurrency(source, slot, 2, 1, 4);
        record_postgres_cdc_snapshot_wal_buffer_fill(source, slot, 3, 4);
        inc_postgres_cdc_snapshot_concurrency_adjustment(
            source,
            slot,
            "decrease",
            "wal_buffer_high",
        );

        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder
            .encode(&prometheus::gather(), &mut buffer)
            .expect("encode metrics");
        let body = String::from_utf8(buffer).expect("metrics utf8");

        assert!(body.contains(
            "floe_postgres_cdc_source_lag_bytes{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 50"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_table_lag_bytes{slot=\"slot_metrics_test\",source=\"pg_metrics_test\",table=\"orders_metrics_test\"} 60"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_schema_evolution_policy{policy=\"ignore_compatible\",source=\"pg_metrics_test\"} 1"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_schema_evolution_policy{policy=\"fail_fast\",source=\"pg_metrics_test\"} 0"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_schema_evolution_events_total{outcome=\"compatible_addition\",policy=\"ignore_compatible\",source=\"pg_metrics_test\",table=\"orders_metrics_test\"} 1"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_schema_evolution_last_observed_unix_ms{outcome=\"compatible_addition\",policy=\"ignore_compatible\",source=\"pg_metrics_test\",table=\"orders_metrics_test\"} 123456"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_snapshot_concurrency_target{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 2"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_snapshot_concurrency_active{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 1"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_snapshot_concurrency_max{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 4"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_snapshot_wal_buffer_fill_percent{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 75"
        ));
        assert!(body.contains(
            "floe_postgres_cdc_snapshot_concurrency_adjustments_total{direction=\"decrease\",reason=\"wal_buffer_high\",slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 1"
        ));
    }

    #[test]
    fn cdc_replication_metrics_record_replay_and_target_error_state() {
        let pipeline = "pipe_metrics_test";

        record_cdc_buffer_pending(pipeline, 2, 10, 2048, Some(100));
        inc_cdc_buffer_object_op(pipeline, "create", 2);
        inc_cdc_buffer_object_op(pipeline, "get", 1);
        inc_cdc_buffer_object_op(pipeline, "delete", 1);
        record_cdc_buffer_append(pipeline, 10, 2048, 7);
        inc_cdc_buffer_forced_flush(pipeline);
        observe_cdc_buffer_flush_latency_ms(pipeline, 3);
        inc_cdc_buffer_drain_attempt(pipeline);
        record_cdc_buffer_replay(pipeline, 4, 11);
        observe_cdc_buffer_replay_phase_latency_ms(pipeline, "target_delivery", 5);
        record_cdc_buffer_source_backpressure_active(pipeline, true);
        record_cdc_replication_replaying(pipeline, true);
        record_cdc_replication_target_error(pipeline, true);
        inc_cdc_replication_target_failure(pipeline, "kafka", "retryable");
        record_cdc_replication_target_write(pipeline, "kafka", "failure", 7, 13);
        inc_cdc_replication_dlq_replay(pipeline, "success");
        inc_cdc_replication_dlq_replay(pipeline, "failure");
        record_cdc_replication_dlq_stats(pipeline, 2, 3, 4, Some(5));

        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder
            .encode(&prometheus::gather(), &mut buffer)
            .expect("encode metrics");
        let body = String::from_utf8(buffer).expect("metrics utf8");

        assert!(body.contains("floe_cdc_replication_replaying{pipeline=\"pipe_metrics_test\"} 1"));
        assert!(
            body.contains("floe_cdc_replication_target_error{pipeline=\"pipe_metrics_test\"} 1")
        );
        assert!(body.contains(
            "floe_cdc_replication_target_failures_total{class=\"retryable\",pipeline=\"pipe_metrics_test\",target_kind=\"kafka\"} 1"
        ));
        assert!(body.contains(
            "floe_cdc_replication_target_write_latency_ms_count{pipeline=\"pipe_metrics_test\",result=\"failure\",target_kind=\"kafka\"} 1"
        ));
        assert!(body.contains(
            "floe_cdc_replication_target_write_batch_records_sum{pipeline=\"pipe_metrics_test\",result=\"failure\",target_kind=\"kafka\"} 7"
        ));
        assert!(body.contains(
            "floe_cdc_replication_target_write_records_total{pipeline=\"pipe_metrics_test\",result=\"failure\",target_kind=\"kafka\"} 7"
        ));
        assert!(body.contains(
            "floe_cdc_replication_dlq_replays_total{pipeline=\"pipe_metrics_test\",result=\"success\"} 1"
        ));
        assert!(body.contains(
            "floe_cdc_replication_dlq_replays_total{pipeline=\"pipe_metrics_test\",result=\"failure\"} 1"
        ));
        assert!(body.contains(
            "floe_cdc_replication_dlq_entries{pipeline=\"pipe_metrics_test\",status=\"pending\"} 2"
        ));
        assert!(body.contains(
            "floe_cdc_replication_dlq_entries{pipeline=\"pipe_metrics_test\",status=\"replayed\"} 3"
        ));
        assert!(body.contains(
            "floe_cdc_replication_dlq_entries{pipeline=\"pipe_metrics_test\",status=\"discarded\"} 4"
        ));
        assert!(body.contains(
            "floe_cdc_replication_dlq_oldest_pending_age_ms{pipeline=\"pipe_metrics_test\"} 5"
        ));
        assert!(body.contains("floe_cdc_buffer_pending_objects{pipeline=\"pipe_metrics_test\"} 2"));
        assert!(body.contains(
            "floe_cdc_buffer_object_ops_total{operation=\"create\",pipeline=\"pipe_metrics_test\"} 2"
        ));
        assert!(body.contains(
            "floe_cdc_buffer_object_ops_total{operation=\"get\",pipeline=\"pipe_metrics_test\"} 1"
        ));
        assert!(body.contains(
            "floe_cdc_buffer_object_ops_total{operation=\"delete\",pipeline=\"pipe_metrics_test\"} 1"
        ));
        assert!(
            body.contains(
                "floe_cdc_buffer_appended_records_total{pipeline=\"pipe_metrics_test\"} 10"
            )
        );
        assert!(
            body.contains(
                "floe_cdc_buffer_appended_bytes_total{pipeline=\"pipe_metrics_test\"} 2048"
            )
        );
        assert!(
            body.contains(
                "floe_cdc_buffer_append_latency_ms_count{pipeline=\"pipe_metrics_test\"} 1"
            )
        );
        assert!(
            body.contains("floe_cdc_buffer_forced_flushes_total{pipeline=\"pipe_metrics_test\"} 1")
        );
        assert!(
            body.contains(
                "floe_cdc_buffer_flush_latency_ms_count{pipeline=\"pipe_metrics_test\"} 1"
            )
        );
        assert!(
            body.contains("floe_cdc_buffer_drain_attempts_total{pipeline=\"pipe_metrics_test\"} 1")
        );
        assert!(
            body.contains(
                "floe_cdc_buffer_replayed_records_total{pipeline=\"pipe_metrics_test\"} 4"
            )
        );
        assert!(body.contains(
            "floe_cdc_buffer_replay_latency_ms_count{phase=\"total\",pipeline=\"pipe_metrics_test\"} 1"
        ));
        assert!(body.contains(
            "floe_cdc_buffer_replay_latency_ms_count{phase=\"target_delivery\",pipeline=\"pipe_metrics_test\"} 1"
        ));
        assert!(body.contains(
            "floe_cdc_buffer_source_backpressure_active{pipeline=\"pipe_metrics_test\"} 1"
        ));

        record_cdc_replication_replaying(pipeline, false);
        record_cdc_replication_target_error(pipeline, false);
        record_cdc_buffer_source_backpressure_active(pipeline, false);
    }
}
