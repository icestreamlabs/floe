use std::sync::LazyLock;

use prometheus::{Histogram, HistogramOpts, core::Collector};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogicalWorkSnapshot {
    pub input_delta_rows: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FlushWriteMetrics {
    pub(crate) write_batch_calls: u64,
    pub(crate) keys_written: u64,
}

impl FlushWriteMetrics {
    pub(crate) fn record_write_batch(&mut self, keys_written: usize) {
        self.write_batch_calls = self.write_batch_calls.saturating_add(1);
        self.keys_written = self.keys_written.saturating_add(keys_written as u64);
    }
}

trait OptionalHistogram {
    fn observe(&self, value: f64);
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

static DBSP_FLUSH_WRITE_BATCH_CALLS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_dbsp_flush_write_batch_calls",
        "Number of write_batch calls issued per DBSP flush tick",
    ))
});

static DBSP_FLUSH_KEYS_WRITTEN: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_dbsp_flush_keys_written",
        "Number of keys written per DBSP flush tick",
    ))
});

static DBSP_FOREGROUND_COMPACTION_LATENCY_MS: LazyLock<Option<Histogram>> = LazyLock::new(|| {
    histogram(HistogramOpts::new(
        "floe_dbsp_foreground_compaction_latency_ms",
        "Time spent in foreground DBSP compaction work during a flush tick in milliseconds",
    ))
});

pub(crate) fn observe_flush_write_metrics(metrics: FlushWriteMetrics) {
    DBSP_FLUSH_WRITE_BATCH_CALLS.observe(metrics.write_batch_calls as f64);
    DBSP_FLUSH_KEYS_WRITTEN.observe(metrics.keys_written as f64);
}

pub(crate) fn observe_foreground_compaction_latency_ms(latency_ms: u64) {
    DBSP_FOREGROUND_COMPACTION_LATENCY_MS.observe(latency_ms as f64);
}
