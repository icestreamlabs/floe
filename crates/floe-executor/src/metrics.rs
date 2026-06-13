use std::sync::LazyLock;

use prometheus::{Histogram, HistogramOpts, IntCounter, core::Collector};

trait OptionalIntCounter {
    fn inc(&self);
    fn inc_by(&self, value: u64);
}

trait OptionalHistogram {
    fn observe(&self, value: f64);
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
}
