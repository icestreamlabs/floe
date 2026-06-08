use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
    register_histogram, register_histogram_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec,
};

struct OptionalMetricValue<T> {
    metric: Option<T>,
}

trait OptionalIntGauge {
    fn set(&self, value: i64);
}

trait OptionalIntGaugeVec {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<IntGauge>;
}

trait OptionalIntCounterVec {
    fn with_label_values(
        &self,
        label_values: &[&str],
    ) -> OptionalMetricValue<prometheus::IntCounter>;
}

trait OptionalHistogram {
    fn observe(&self, value: f64);
}

trait OptionalHistogramVec {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<Histogram>;
}

impl OptionalIntGauge for LazyLock<Option<IntGauge>> {
    fn set(&self, value: i64) {
        if let Some(metric) = self.as_ref() {
            metric.set(value);
        }
    }
}

impl OptionalIntGauge for OptionalMetricValue<IntGauge> {
    fn set(&self, value: i64) {
        if let Some(metric) = &self.metric {
            metric.set(value);
        }
    }
}

impl OptionalIntGaugeVec for LazyLock<Option<IntGaugeVec>> {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<IntGauge> {
        OptionalMetricValue {
            metric: self
                .as_ref()
                .and_then(|metric| metric.get_metric_with_label_values(label_values).ok()),
        }
    }
}

impl OptionalIntCounterVec for LazyLock<Option<IntCounterVec>> {
    fn with_label_values(
        &self,
        label_values: &[&str],
    ) -> OptionalMetricValue<prometheus::IntCounter> {
        OptionalMetricValue {
            metric: self
                .as_ref()
                .and_then(|metric| metric.get_metric_with_label_values(label_values).ok()),
        }
    }
}

impl OptionalMetricValue<prometheus::IntCounter> {
    fn inc(&self) {
        if let Some(metric) = &self.metric {
            metric.inc();
        }
    }

    fn inc_by(&self, value: u64) {
        if let Some(metric) = &self.metric {
            metric.inc_by(value);
        }
    }
}

impl OptionalHistogram for LazyLock<Option<Histogram>> {
    fn observe(&self, value: f64) {
        if let Some(metric) = self.as_ref() {
            metric.observe(value);
        }
    }
}

impl OptionalHistogram for OptionalMetricValue<Histogram> {
    fn observe(&self, value: f64) {
        if let Some(metric) = &self.metric {
            metric.observe(value);
        }
    }
}

impl OptionalHistogramVec for LazyLock<Option<HistogramVec>> {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<Histogram> {
        OptionalMetricValue {
            metric: self
                .as_ref()
                .and_then(|metric| metric.get_metric_with_label_values(label_values).ok()),
        }
    }
}

static INGEST_QUEUE_DEPTH: LazyLock<Option<IntGauge>> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_ingest_queue_depth",
        "Number of events buffered between connectors and the executor"
    )
    .ok()
});

static INGEST_DECODE_LATENCY_MS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_ingest_decode_latency_ms",
        "Time spent decoding a batch of append ingest events in milliseconds",
    ))
    .ok()
});

static INGEST_TICK_LATENCY_MS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_ingest_tick_latency_ms",
        "Time spent advancing source frontiers per ingestion tick in milliseconds",
    ))
    .ok()
});

static INGEST_TICK_PHASE_LATENCY_MS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_ingest_tick_phase_latency_ms",
        "Time spent in ingest tick phases in milliseconds",
        &["phase"]
    )
    .ok()
});

static INGEST_TICKS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_ingest_ticks_total",
        "Total number of successful ingest ticks",
        &["result"]
    )
    .ok()
});

static LAST_COMMITTED_TICK: LazyLock<Option<IntGauge>> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_last_committed_tick",
        "Most recently committed ingestion tick id"
    )
    .ok()
});

static CHECKPOINT_AGE_SECONDS: LazyLock<Option<IntGauge>> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_checkpoint_age_seconds",
        "Seconds elapsed since the latest committed tick checkpoint"
    )
    .ok()
});

static SOURCE_OFFSET_LAG: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_source_offset_lag",
        "Difference between latest observed source offset and last committed offset",
        &["source", "partition"]
    )
    .ok()
});

static WATERMARK_LAG_MS: LazyLock<Option<IntGauge>> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_watermark_lag_ms",
        "Difference between wall-clock time and current global watermark in milliseconds",
    )
    .ok()
});

static SOURCE_WATERMARK_MS: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_source_watermark_ms",
        "Latest observed watermark timestamp (ms) per source",
        &["source"]
    )
    .ok()
});

static GLOBAL_WATERMARK_MS: LazyLock<Option<IntGauge>> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_global_watermark_ms",
        "Latest propagated global watermark timestamp (ms)",
    )
    .ok()
});

static MV_FRESHNESS_SECONDS: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_mv_freshness_seconds",
        "Seconds since each materialized view last advanced to a new committed version",
        &["view"]
    )
    .ok()
});

static SINK_QUEUE_DEPTH: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_sink_queue_depth",
        "Number of records currently buffered in a sink queue",
        &["sink"]
    )
    .ok()
});

static SINK_VERSION_LAG: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_sink_version_lag",
        "Difference between latest enqueued and latest flushed MV version per sink",
        &["sink"]
    )
    .ok()
});

static SINK_FAILURES: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_sink_failures_total",
        "Total sink emission failures by sink and transport",
        &["sink", "transport"]
    )
    .ok()
});

static SINK_RETRIES: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_sink_retries_total",
        "Total sink retry attempts by sink and transport",
        &["sink", "transport"]
    )
    .ok()
});

static RUNTIME_ERRORS: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_runtime_errors_total",
        "Total runtime errors by component",
        &["component"]
    )
    .ok()
});

static POSTGRES_CDC_UPSTREAM_LSN: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_upstream_lsn",
        "Latest observed upstream Postgres WAL LSN per CDC source and slot",
        &["source", "slot"]
    )
    .ok()
});

static POSTGRES_CDC_DURABLE_LSN: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_durable_lsn",
        "Latest SlateDB-durable Postgres CDC LSN per source and slot",
        &["source", "slot"]
    )
    .ok()
});

static POSTGRES_CDC_SOURCE_LAG_BYTES: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_source_lag_bytes",
        "Byte lag between latest observed upstream Postgres WAL LSN and durable Floe CDC LSN",
        &["source", "slot"]
    )
    .ok()
});

static POSTGRES_CDC_SOURCE_CONNECTED: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_source_connected",
        "Whether the Postgres CDC replication stream is currently connected",
        &["source", "slot"]
    )
    .ok()
});

static POSTGRES_CDC_RECONNECTS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_postgres_cdc_reconnects_total",
        "Postgres CDC source reconnect attempts by source, slot, and result",
        &["source", "slot", "result"]
    )
    .ok()
});

static POSTGRES_CDC_TABLE_LAST_APPLIED_LSN: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_table_last_applied_lsn",
        "Latest SlateDB-durable Postgres CDC LSN applied to each CDC table",
        &["source", "slot", "table"]
    )
    .ok()
});

static POSTGRES_CDC_TABLE_LAG_BYTES: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_table_lag_bytes",
        "Byte lag between latest observed upstream Postgres WAL LSN and each CDC table's durable applied LSN",
        &["source", "slot", "table"]
    )
    .ok()
});

static POSTGRES_CDC_SCHEMA_EVOLUTION_POLICY: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_schema_evolution_policy",
        "Configured Postgres CDC schema evolution policy per source; the active policy label is set to 1",
        &["source", "policy"]
    )
    .ok()
});

static POSTGRES_CDC_SCHEMA_EVOLUTION_EVENTS_TOTAL: LazyLock<Option<IntCounterVec>> =
    LazyLock::new(|| {
        register_int_counter_vec!(
            "floe_postgres_cdc_schema_evolution_events_total",
            "Observed Postgres CDC schema evolution events by source, table, outcome, and policy",
            &["source", "table", "outcome", "policy"]
        )
        .ok()
    });

static POSTGRES_CDC_SCHEMA_EVOLUTION_LAST_OBSERVED_UNIX_MS: LazyLock<Option<IntGaugeVec>> =
    LazyLock::new(|| {
        register_int_gauge_vec!(
            "floe_postgres_cdc_schema_evolution_last_observed_unix_ms",
            "Unix timestamp in milliseconds for the latest observed Postgres CDC schema evolution event",
            &["source", "table", "outcome", "policy"]
        )
        .ok()
    });

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_TARGET: LazyLock<Option<IntGaugeVec>> =
    LazyLock::new(|| {
        register_int_gauge_vec!(
            "floe_postgres_cdc_snapshot_concurrency_target",
            "Current adaptive Postgres CDC initial snapshot scan concurrency target",
            &["source", "slot"]
        )
        .ok()
    });

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ACTIVE: LazyLock<Option<IntGaugeVec>> =
    LazyLock::new(|| {
        register_int_gauge_vec!(
            "floe_postgres_cdc_snapshot_concurrency_active",
            "Current active Postgres CDC initial snapshot scan workers",
            &["source", "slot"]
        )
        .ok()
    });

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_MAX: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_postgres_cdc_snapshot_concurrency_max",
        "Maximum configured Postgres CDC initial snapshot scan workers",
        &["source", "slot"]
    )
    .ok()
});

static POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_FILL_PERCENT: LazyLock<Option<IntGaugeVec>> = LazyLock::new(
    || {
        register_int_gauge_vec!(
            "floe_postgres_cdc_snapshot_wal_buffer_fill_percent",
            "Percent fill of the in-memory WAL buffer used while Postgres CDC initial snapshot scans are running",
            &["source", "slot"]
        )
        .ok()
    },
);

static POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ADJUSTMENTS_TOTAL: LazyLock<Option<IntCounterVec>> =
    LazyLock::new(|| {
        register_int_counter_vec!(
            "floe_postgres_cdc_snapshot_concurrency_adjustments_total",
            "Adaptive Postgres CDC initial snapshot concurrency target changes",
            &["source", "slot", "direction", "reason"]
        )
        .ok()
    });

static CDC_BUFFER_PENDING_TRANSACTIONS: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_transactions",
        "Number of pending transactions in each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_PENDING_OBJECTS: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_objects",
        "Number of pending payload objects in each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_PENDING_RECORDS: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_records",
        "Number of pending records in each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_PENDING_BYTES: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_pending_bytes",
        "Approximate pending payload bytes in each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_OLDEST_PENDING_AGE_MS: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_oldest_pending_age_ms",
        "Age in milliseconds of the oldest pending transaction in each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_CAP_UTILIZATION_PERCENT: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_cap_utilization_percent",
        "Percent utilization of configured CDC replication buffer caps",
        &["pipeline", "limit"]
    )
    .ok()
});

static CDC_BUFFER_INTEGRITY_OBJECTS: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_integrity_objects",
        "CDC replication buffer payload object integrity counts by state",
        &["pipeline", "state"]
    )
    .ok()
});

static CDC_BUFFER_ORPHAN_PAYLOAD_BYTES: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_orphan_payload_bytes",
        "Total bytes in orphaned CDC replication buffer payload objects",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_OBJECT_OPS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_object_ops_total",
        "CDC replication buffer object-store operations",
        &["pipeline", "operation"]
    )
    .ok()
});

static CDC_BUFFER_APPENDED_RECORDS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_appended_records_total",
        "Number of records appended to each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_APPENDED_BYTES_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_appended_bytes_total",
        "Approximate payload bytes appended to each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_APPEND_LATENCY_MS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_buffer_append_latency_ms",
        "Time spent appending a transaction to each CDC replication buffer in milliseconds",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_CLEANUP_TRANSACTIONS_TOTAL: LazyLock<Option<IntCounterVec>> =
    LazyLock::new(|| {
        register_int_counter_vec!(
            "floe_cdc_buffer_cleanup_transactions_total",
            "Number of delivered CDC replication buffer transactions removed by cleanup",
            &["pipeline"]
        )
        .ok()
    });

static CDC_BUFFER_CLEANUP_RECORDS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_cleanup_records_total",
        "Number of delivered CDC replication buffer records removed by cleanup",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_CLEANUP_BYTES_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_cleanup_bytes_total",
        "Number of delivered CDC replication buffer payload bytes removed by cleanup",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_CLEANUP_LATENCY_MS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_buffer_cleanup_latency_ms",
        "Time spent cleaning delivered CDC replication buffer payloads in milliseconds",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_FORCED_FLUSHES_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_forced_flushes_total",
        "Number of explicit CDC replication buffer flushes",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_FLUSH_LATENCY_MS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_buffer_flush_latency_ms",
        "Time spent flushing each CDC replication buffer in milliseconds",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_DRAIN_ATTEMPTS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_drain_attempts_total",
        "Number of CDC replication buffer drain attempts before accepting more source data",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_SOURCE_BACKPRESSURE_ACTIVE: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_buffer_source_backpressure_active",
        "Whether a CDC replication buffer is applying source backpressure because pending limits remain exceeded",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_REPLAYED_RECORDS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_buffer_replayed_records_total",
        "Number of records replayed from each CDC replication buffer",
        &["pipeline"]
    )
    .ok()
});

static CDC_BUFFER_REPLAY_LATENCY_MS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_cdc_buffer_replay_latency_ms",
        "Time spent replaying records from each CDC replication buffer in milliseconds",
        &["pipeline", "phase"]
    )
    .ok()
});

static CDC_REPLICATION_REPLAYING: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_replication_replaying",
        "Whether a CDC replication pipeline is actively replaying buffered records",
        &["pipeline"]
    )
    .ok()
});

static CDC_REPLICATION_TARGET_ERROR: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_replication_target_error",
        "Whether the last CDC replication target delivery attempt failed",
        &["pipeline"]
    )
    .ok()
});

static CDC_REPLICATION_TARGET_FAILURES_TOTAL: LazyLock<Option<IntCounterVec>> =
    LazyLock::new(|| {
        register_int_counter_vec!(
            "floe_cdc_replication_target_failures_total",
            "CDC replication target delivery failures by pipeline, target kind, and failure class",
            &["pipeline", "target_kind", "class"]
        )
        .ok()
    });

static CDC_REPLICATION_TARGET_WRITE_LATENCY_MS: LazyLock<Option<HistogramVec>> =
    LazyLock::new(|| {
        register_histogram_vec!(
            "floe_cdc_replication_target_write_latency_ms",
            "Time spent delivering CDC replication records to a target in milliseconds",
            &["pipeline", "target_kind", "result"]
        )
        .ok()
    });

static CDC_REPLICATION_TARGET_WRITE_BATCH_RECORDS: LazyLock<Option<HistogramVec>> =
    LazyLock::new(|| {
        register_histogram_vec!(
            "floe_cdc_replication_target_write_batch_records",
            "CDC replication target write batch size in records",
            &["pipeline", "target_kind", "result"]
        )
        .ok()
    });

static CDC_REPLICATION_TARGET_WRITE_RECORDS_TOTAL: LazyLock<Option<IntCounterVec>> =
    LazyLock::new(|| {
        register_int_counter_vec!(
        "floe_cdc_replication_target_write_records_total",
        "Total CDC replication records delivered or attempted by pipeline, target kind, and result",
        &["pipeline", "target_kind", "result"]
    )
    .ok()
    });

static CDC_REPLICATION_DLQ_REPLAYS_TOTAL: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_cdc_replication_dlq_replays_total",
        "Manual CDC replication DLQ replay attempts by pipeline and result",
        &["pipeline", "result"]
    )
    .ok()
});

static CDC_REPLICATION_DLQ_ENTRIES: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "floe_cdc_replication_dlq_entries",
        "CDC replication DLQ entries by pipeline and status",
        &["pipeline", "status"]
    )
    .ok()
});

static CDC_REPLICATION_DLQ_OLDEST_PENDING_AGE_MS: LazyLock<Option<IntGaugeVec>> =
    LazyLock::new(|| {
        register_int_gauge_vec!(
            "floe_cdc_replication_dlq_oldest_pending_age_ms",
            "Oldest pending CDC replication DLQ entry age in milliseconds",
            &["pipeline"]
        )
        .ok()
    });

#[path = "metrics/postgres_cdc.rs"]
mod postgres_cdc;
#[cfg(test)]
#[path = "metrics/tests.rs"]
mod tests;

use self::postgres_cdc::init_postgres_cdc_metrics;
pub(crate) use self::postgres_cdc::{
    inc_postgres_cdc_reconnect, inc_postgres_cdc_snapshot_concurrency_adjustment,
    record_postgres_cdc_durable_lsn, record_postgres_cdc_schema_evolution_observation,
    record_postgres_cdc_schema_evolution_policy, record_postgres_cdc_snapshot_concurrency,
    record_postgres_cdc_snapshot_wal_buffer_fill, record_postgres_cdc_source_connected,
    record_postgres_cdc_table_applied_lsn, record_postgres_cdc_upstream_lsn,
};

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

pub(crate) fn record_cdc_buffer_cap_utilization(
    pipeline: &str,
    limit: &str,
    used: usize,
    configured_limit: usize,
) {
    let percent = if configured_limit == 0 {
        0
    } else {
        used.saturating_mul(100) / configured_limit
    };
    CDC_BUFFER_CAP_UTILIZATION_PERCENT
        .with_label_values(&[pipeline, limit])
        .set(i64_from_usize(percent));
}

pub(crate) fn record_cdc_buffer_cap_utilization_u64(
    pipeline: &str,
    limit: &str,
    used: u64,
    configured_limit: u64,
) {
    let percent = if configured_limit == 0 {
        0
    } else {
        used.saturating_mul(100) / configured_limit
    };
    CDC_BUFFER_CAP_UTILIZATION_PERCENT
        .with_label_values(&[pipeline, limit])
        .set(i64_from_u64(percent));
}

pub(crate) fn record_cdc_buffer_integrity(
    pipeline: &str,
    missing_payload_objects: usize,
    orphan_payload_objects: usize,
    orphan_payload_bytes: usize,
) {
    CDC_BUFFER_INTEGRITY_OBJECTS
        .with_label_values(&[pipeline, "missing_payload"])
        .set(i64_from_usize(missing_payload_objects));
    CDC_BUFFER_INTEGRITY_OBJECTS
        .with_label_values(&[pipeline, "orphan_payload"])
        .set(i64_from_usize(orphan_payload_objects));
    CDC_BUFFER_ORPHAN_PAYLOAD_BYTES
        .with_label_values(&[pipeline])
        .set(i64_from_usize(orphan_payload_bytes));
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

pub(crate) fn record_cdc_buffer_cleanup(
    pipeline: &str,
    transactions: usize,
    records: usize,
    bytes: usize,
    latency_ms: u64,
) {
    CDC_BUFFER_CLEANUP_TRANSACTIONS_TOTAL
        .with_label_values(&[pipeline])
        .inc_by(u64::try_from(transactions).unwrap_or(u64::MAX));
    CDC_BUFFER_CLEANUP_RECORDS_TOTAL
        .with_label_values(&[pipeline])
        .inc_by(u64::try_from(records).unwrap_or(u64::MAX));
    CDC_BUFFER_CLEANUP_BYTES_TOTAL
        .with_label_values(&[pipeline])
        .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    CDC_BUFFER_CLEANUP_LATENCY_MS
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
    let _ = &*CDC_BUFFER_CAP_UTILIZATION_PERCENT;
    let _ = &*CDC_BUFFER_INTEGRITY_OBJECTS;
    let _ = &*CDC_BUFFER_ORPHAN_PAYLOAD_BYTES;
    let _ = &*CDC_BUFFER_OBJECT_OPS_TOTAL;
    let _ = &*CDC_BUFFER_APPENDED_RECORDS_TOTAL;
    let _ = &*CDC_BUFFER_APPENDED_BYTES_TOTAL;
    let _ = &*CDC_BUFFER_APPEND_LATENCY_MS;
    let _ = &*CDC_BUFFER_CLEANUP_TRANSACTIONS_TOTAL;
    let _ = &*CDC_BUFFER_CLEANUP_RECORDS_TOTAL;
    let _ = &*CDC_BUFFER_CLEANUP_BYTES_TOTAL;
    let _ = &*CDC_BUFFER_CLEANUP_LATENCY_MS;
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
    init_postgres_cdc_metrics();
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
