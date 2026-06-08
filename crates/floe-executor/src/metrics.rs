use std::sync::LazyLock;

use crate::delta_consolidation::ConsolidationStats;
use prometheus::{Histogram, HistogramOpts, HistogramVec, IntCounter, core::Collector};

struct OptionalMetricValue<T> {
    metric: Option<T>,
}

trait OptionalIntCounter {
    fn inc(&self);
    fn inc_by(&self, value: u64);
}

trait OptionalHistogram {
    fn observe(&self, value: f64);
}

trait OptionalHistogramVec {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<Histogram>;
}

impl OptionalIntCounter for LazyLock<Option<IntCounter>> {
    fn inc(&self) {
        if let Some(metric) = self.as_ref() {
            metric.inc();
        }
    }

    fn inc_by(&self, value: u64) {
        if let Some(metric) = self.as_ref() {
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

fn register_metric<T>(name: &str, metric: T) -> T
where
    T: Collector + Clone + 'static,
{
    if let Err(error) = prometheus::register(Box::new(metric.clone())) {
        tracing::warn!(metric = name, %error, "failed to register Prometheus metric");
    }
    metric
}

fn int_counter(name: &str, help: &str) -> Option<IntCounter> {
    IntCounter::new(name, help)
        .map(|metric| register_metric(name, metric))
        .map_err(|error| {
            tracing::warn!(metric = name, %error, "failed to create Prometheus metric");
            error
        })
        .ok()
}

fn histogram(opts: HistogramOpts) -> Option<Histogram> {
    let name = opts.common_opts.name.clone();
    Histogram::with_opts(opts)
        .map(|metric| register_metric(&name, metric))
        .map_err(|error| {
            tracing::warn!(metric = name, %error, "failed to create Prometheus metric");
            error
        })
        .ok()
}

fn histogram_vec(name: &str, help: &str, labels: &[&str]) -> Option<HistogramVec> {
    HistogramVec::new(HistogramOpts::new(name, help), labels)
        .map(|metric| register_metric(name, metric))
        .map_err(|error| {
            tracing::warn!(metric = name, %error, "failed to create Prometheus metric");
            error
        })
        .ok()
}

static SUBSCRIBE_THROUGHPUT_ROWS: LazyLock<Option<IntCounter>> = LazyLock::new(|| {
    int_counter(
        "floe_subscribe_rows_total",
        "Total number of rows emitted by SUBSCRIBE streams",
    )
});

static DELTA_BATCH_ROWS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_delta_batch_rows",
        "Number of rows emitted per delta batch",
    ))
});

static DELTA_BATCH_BYTES: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_delta_batch_bytes",
        "Estimated byte size of emitted delta batches",
    ))
});

static DELTA_BATCH_FLUSHES: LazyLock<Option<IntCounter>> = LazyLock::new(|| {
    int_counter(
        "floe_delta_batch_flush_total",
        "Number of delta batch flushes emitted",
    )
});

static DELTA_CONSOLIDATION_ROWS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    histogram_vec(
        "floe_delta_consolidation_rows",
        "Rows observed during vectorized delta consolidation",
        &["phase"],
    )
});

static DELTA_CONSOLIDATION_LATENCY_MS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_delta_consolidation_latency_ms",
        "Time spent consolidating vectorized delta batches in milliseconds",
    ))
});

static FULL_MV_REFRESH_TICKS: LazyLock<Option<IntCounter>> = LazyLock::new(|| {
    int_counter(
        "floe_full_mv_refresh_ticks_total",
        "Number of materialized view ticks executed with full-refresh execution",
    )
});

static FULL_MV_REFRESH_ROWS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_full_mv_refresh_rows",
        "Rows scanned by full-refresh materialized view ticks",
    ))
});

static FULL_MV_REFRESH_LATENCY_MS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_full_mv_refresh_latency_ms",
        "Time spent executing full-refresh materialized view ticks in milliseconds",
    ))
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

pub(crate) fn observe_full_mv_refresh_tick(snapshot_rows: usize, latency_ms: u64) {
    FULL_MV_REFRESH_TICKS.inc();
    FULL_MV_REFRESH_ROWS.observe(snapshot_rows as f64);
    FULL_MV_REFRESH_LATENCY_MS.observe(latency_ms as f64);
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
    let _ = &*FULL_MV_REFRESH_TICKS;
    let _ = &*FULL_MV_REFRESH_ROWS;
    let _ = &*FULL_MV_REFRESH_LATENCY_MS;
}
