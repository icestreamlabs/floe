use std::sync::LazyLock;

use prometheus::{Histogram, HistogramOpts, IntGauge, register_histogram, register_int_gauge};

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

pub(crate) fn record_ingest_queue_depth(depth: usize) {
    INGEST_QUEUE_DEPTH.set(depth as i64);
}

pub(crate) fn observe_decode_latency_ms(latency_ms: u64) {
    INGEST_DECODE_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn observe_tick_latency_ms(latency_ms: u64) {
    INGEST_TICK_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn init() {
    let _ = &*INGEST_QUEUE_DEPTH;
    let _ = &*INGEST_DECODE_LATENCY_MS;
    let _ = &*INGEST_TICK_LATENCY_MS;
}
