use std::sync::LazyLock;

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

pub(crate) fn init() {
    let _ = &*INGEST_QUEUE_DEPTH;
    let _ = &*INGEST_DECODE_LATENCY_MS;
    let _ = &*INGEST_TICK_LATENCY_MS;
    let _ = &*INGEST_TICK_PHASE_LATENCY_MS;
    let _ = &*INGEST_TICKS_TOTAL;
    let _ = &*LAST_COMMITTED_TICK;
    let _ = &*CHECKPOINT_AGE_SECONDS;
    let _ = &*SOURCE_OFFSET_LAG;
    let _ = &*MV_FRESHNESS_SECONDS;
    let _ = &*SINK_QUEUE_DEPTH;
    let _ = &*SINK_VERSION_LAG;
    let _ = &*SINK_FAILURES;
    let _ = &*SINK_RETRIES;
    let _ = &*RUNTIME_ERRORS;
}
