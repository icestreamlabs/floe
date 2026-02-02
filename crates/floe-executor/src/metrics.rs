use std::sync::LazyLock;

use prometheus::{Histogram, HistogramOpts, IntCounter, register_histogram, register_int_counter};

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

pub(crate) fn observe_mv_update_latency_ms(latency_ms: u64) {
    MV_UPDATE_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn inc_tail_rows(count: usize) {
    if count > 0 {
        TAIL_THROUGHPUT_ROWS.inc_by(count as u64);
    }
}

pub(crate) fn init() {
    let _ = &*MV_UPDATE_LATENCY_MS;
    let _ = &*TAIL_THROUGHPUT_ROWS;
}
