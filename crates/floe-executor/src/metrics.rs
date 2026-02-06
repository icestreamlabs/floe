use std::sync::LazyLock;

use prometheus::{Histogram, HistogramOpts, IntCounter, register_histogram, register_int_counter};

use crate::delta_consolidation::ConsolidationStats;

static MV_UPDATE_LATENCY_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_mv_update_latency_ms",
        "Time spent applying a materialized view update in milliseconds",
    ))
    .expect("register floe_mv_update_latency_ms")
});

static TAIL_THROUGHPUT_ROWS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_tail_rows_total",
        "Total number of rows emitted by tail streams"
    )
    .expect("register floe_tail_rows_total")
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

static DELTA_CONSOLIDATION_LATENCY_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_delta_consolidation_latency_ms",
        "Time spent consolidating delta batches per tick in milliseconds",
    ))
    .expect("register floe_delta_consolidation_latency_ms")
});

static DELTA_CONSOLIDATION_ROWS_IN: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_delta_consolidation_rows_in",
        "Rows entering consolidation per tick",
    ))
    .expect("register floe_delta_consolidation_rows_in")
});

static DELTA_CONSOLIDATION_ROWS_OUT: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_delta_consolidation_rows_out",
        "Rows leaving consolidation per tick",
    ))
    .expect("register floe_delta_consolidation_rows_out")
});

static DELTA_CONSOLIDATION_ZERO_DROP_RATE: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_delta_consolidation_zero_weight_drop_rate",
        "Fraction of grouped rows dropped because net weight is zero",
    ))
    .expect("register floe_delta_consolidation_zero_weight_drop_rate")
});

static DELTA_CONSOLIDATION_ZERO_DROPPED_ROWS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_delta_consolidation_zero_weight_dropped_rows_total",
        "Total number of grouped rows dropped because net weight is zero"
    )
    .expect("register floe_delta_consolidation_zero_weight_dropped_rows_total")
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

pub(crate) fn observe_mv_update_latency_ms(latency_ms: u64) {
    MV_UPDATE_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn observe_delta_consolidation(stats: ConsolidationStats, latency_ms: u64) {
    DELTA_CONSOLIDATION_LATENCY_MS.observe(latency_ms as f64);
    DELTA_CONSOLIDATION_ROWS_IN.observe(stats.input_rows as f64);
    DELTA_CONSOLIDATION_ROWS_OUT.observe(stats.output_rows as f64);
    DELTA_CONSOLIDATION_ZERO_DROPPED_ROWS.inc_by(stats.zero_weight_dropped_rows as u64);
    if stats.grouped_rows > 0 {
        let drop_rate = stats.zero_weight_dropped_rows as f64 / stats.grouped_rows as f64;
        DELTA_CONSOLIDATION_ZERO_DROP_RATE.observe(drop_rate);
    }
}

pub(crate) fn inc_tail_rows(count: usize) {
    if count > 0 {
        TAIL_THROUGHPUT_ROWS.inc_by(count as u64);
    }
}

pub(crate) fn init() {
    let _ = &*MV_UPDATE_LATENCY_MS;
    let _ = &*TAIL_THROUGHPUT_ROWS;
    let _ = &*DELTA_BATCH_ROWS;
    let _ = &*DELTA_BATCH_BYTES;
    let _ = &*DELTA_BATCH_FLUSHES;
    let _ = &*DELTA_CONSOLIDATION_LATENCY_MS;
    let _ = &*DELTA_CONSOLIDATION_ROWS_IN;
    let _ = &*DELTA_CONSOLIDATION_ROWS_OUT;
    let _ = &*DELTA_CONSOLIDATION_ZERO_DROP_RATE;
    let _ = &*DELTA_CONSOLIDATION_ZERO_DROPPED_ROWS;
}
