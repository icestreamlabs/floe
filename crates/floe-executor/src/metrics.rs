use std::sync::LazyLock;

use crate::delta_consolidation::ConsolidationStats;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, register_histogram, register_histogram_vec,
    register_int_counter,
};

static SUBSCRIBE_THROUGHPUT_ROWS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_subscribe_rows_total",
        "Total number of rows emitted by SUBSCRIBE streams"
    )
    .expect("register floe_subscribe_rows_total")
});

static DELTA_BATCH_ROWS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_delta_batch_rows",
        "Number of rows emitted per delta batch",
    ))
    .expect("register floe_delta_batch_rows")
});

static DELTA_BATCH_BYTES: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_delta_batch_bytes",
        "Estimated byte size of emitted delta batches",
    ))
    .expect("register floe_delta_batch_bytes")
});

static DELTA_BATCH_FLUSHES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_delta_batch_flush_total",
        "Number of delta batch flushes emitted"
    )
    .expect("register floe_delta_batch_flush_total")
});

static DELTA_CONSOLIDATION_ROWS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_delta_consolidation_rows",
        "Rows observed during vectorized delta consolidation",
        &["phase"]
    )
    .expect("register floe_delta_consolidation_rows")
});

static DELTA_CONSOLIDATION_LATENCY_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_delta_consolidation_latency_ms",
        "Time spent consolidating vectorized delta batches in milliseconds",
    ))
    .expect("register floe_delta_consolidation_latency_ms")
});

pub(crate) fn observe_delta_batch(rows: usize, bytes: usize) {
    if rows > 0 {
        DELTA_BATCH_ROWS.observe(rows as f64);
    }
    if bytes > 0 {
        DELTA_BATCH_BYTES.observe(bytes as f64);
    }
}

pub(crate) fn inc_delta_batch_flushes() {
    DELTA_BATCH_FLUSHES.inc();
}

pub(crate) fn observe_delta_consolidation(stats: ConsolidationStats, latency_ms: u64) {
    DELTA_CONSOLIDATION_ROWS
        .with_label_values(&["input"])
        .observe(stats.input_rows as f64);
    DELTA_CONSOLIDATION_ROWS
        .with_label_values(&["grouped"])
        .observe(stats.grouped_rows as f64);
    DELTA_CONSOLIDATION_ROWS
        .with_label_values(&["output"])
        .observe(stats.output_rows as f64);
    DELTA_CONSOLIDATION_ROWS
        .with_label_values(&["zero_weight_dropped"])
        .observe(stats.zero_weight_dropped_rows as f64);
    DELTA_CONSOLIDATION_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn inc_subscribe_rows(count: usize) {
    if count > 0 {
        SUBSCRIBE_THROUGHPUT_ROWS.inc_by(count as u64);
    }
}

pub(crate) fn init() {
    let _ = &*SUBSCRIBE_THROUGHPUT_ROWS;
    let _ = &*DELTA_BATCH_ROWS;
    let _ = &*DELTA_BATCH_BYTES;
    let _ = &*DELTA_BATCH_FLUSHES;
    let _ = &*DELTA_CONSOLIDATION_ROWS;
    let _ = &*DELTA_CONSOLIDATION_LATENCY_MS;
}
