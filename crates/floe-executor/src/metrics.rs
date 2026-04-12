use std::sync::LazyLock;

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, register_histogram,
    register_histogram_vec, register_int_counter, register_int_counter_vec,
};

use crate::delta_consolidation::ConsolidationStats;

static MV_UPDATE_LATENCY_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_mv_update_latency_ms",
        "Time spent applying a materialized view update in milliseconds",
    ))
    .expect("register floe_mv_update_latency_ms")
});

static MV_UPDATES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_mv_updates_total",
        "Total number of materialized view updates applied"
    )
    .expect("register floe_mv_updates_total")
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

static MV_OPTIMIZATION_HOTSPOTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "floe_mv_optimization_hotspots_total",
        "Dominant materialized-view apply phase observations by path",
        &["path", "phase"]
    )
    .expect("register floe_mv_optimization_hotspots_total")
});

static MV_OPTIMIZATION_HOTSPOT_SHARE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_mv_optimization_hotspot_share",
        "Dominant materialized-view apply phase share of total latency",
        &["path", "phase"]
    )
    .expect("register floe_mv_optimization_hotspot_share")
});

static MV_OPTIMIZATION_TOTAL_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_mv_optimization_total_ms",
        "Observed materialized-view apply total latency in milliseconds by path",
        &["path"]
    )
    .expect("register floe_mv_optimization_total_ms")
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

pub(crate) fn inc_mv_updates() {
    MV_UPDATES_TOTAL.inc();
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

pub(crate) fn observe_mv_optimization_hotspot(
    path: &str,
    phase: &str,
    phase_share: f64,
    total_ms: u64,
) {
    MV_OPTIMIZATION_HOTSPOTS
        .with_label_values(&[path, phase])
        .inc();
    MV_OPTIMIZATION_HOTSPOT_SHARE
        .with_label_values(&[path, phase])
        .observe(phase_share.clamp(0.0, 1.0));
    MV_OPTIMIZATION_TOTAL_MS
        .with_label_values(&[path])
        .observe(total_ms as f64);
}

pub(crate) fn init() {
    let _ = &*MV_UPDATE_LATENCY_MS;
    let _ = &*MV_UPDATES_TOTAL;
    let _ = &*TAIL_THROUGHPUT_ROWS;
    let _ = &*DELTA_BATCH_ROWS;
    let _ = &*DELTA_BATCH_BYTES;
    let _ = &*DELTA_BATCH_FLUSHES;
    let _ = &*DELTA_CONSOLIDATION_LATENCY_MS;
    let _ = &*DELTA_CONSOLIDATION_ROWS_IN;
    let _ = &*DELTA_CONSOLIDATION_ROWS_OUT;
    let _ = &*DELTA_CONSOLIDATION_ZERO_DROP_RATE;
    let _ = &*DELTA_CONSOLIDATION_ZERO_DROPPED_ROWS;
    let _ = &*MV_OPTIMIZATION_HOTSPOTS;
    let _ = &*MV_OPTIMIZATION_HOTSPOT_SHARE;
    let _ = &*MV_OPTIMIZATION_TOTAL_MS;
}
