use std::sync::LazyLock;

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, register_histogram,
    register_histogram_vec, register_int_counter, register_int_counter_vec,
};

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

pub(crate) fn inc_subscribe_rows(count: usize) {
    if count > 0 {
        SUBSCRIBE_THROUGHPUT_ROWS.inc_by(count as u64);
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
    let _ = &*SUBSCRIBE_THROUGHPUT_ROWS;
    let _ = &*DELTA_BATCH_ROWS;
    let _ = &*DELTA_BATCH_BYTES;
    let _ = &*DELTA_BATCH_FLUSHES;
    let _ = &*MV_OPTIMIZATION_HOTSPOTS;
    let _ = &*MV_OPTIMIZATION_HOTSPOT_SHARE;
    let _ = &*MV_OPTIMIZATION_TOTAL_MS;
}
