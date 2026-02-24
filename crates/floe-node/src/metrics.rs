use std::sync::LazyLock;

use prometheus::{
    Histogram, HistogramOpts, IntCounterVec, IntGauge, IntGaugeVec, register_histogram,
    register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
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

static INGEST_TICKS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_ingest_ticks_total",
        "Total number of successful ingest ticks",
        &["result"]
    )
    .expect("register floe_ingest_ticks_total")
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

pub(crate) fn inc_ingest_tick(result: &str) {
    INGEST_TICKS_TOTAL.with_label_values(&[result]).inc();
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

pub(crate) fn inc_runtime_error(component: &str) {
    RUNTIME_ERRORS.with_label_values(&[component]).inc();
}

pub(crate) fn init() {
    let _ = &*INGEST_QUEUE_DEPTH;
    let _ = &*INGEST_DECODE_LATENCY_MS;
    let _ = &*INGEST_TICK_LATENCY_MS;
    let _ = &*INGEST_TICKS_TOTAL;
    let _ = &*SINK_QUEUE_DEPTH;
    let _ = &*SINK_VERSION_LAG;
    let _ = &*SINK_FAILURES;
    let _ = &*RUNTIME_ERRORS;
}
