use std::sync::LazyLock;

use prometheus::{IntCounter, core::Collector};

trait OptionalIntCounter {
    fn inc_by(&self, value: u64);
}

impl OptionalIntCounter for LazyLock<Option<IntCounter>> {
    fn inc_by(&self, value: u64) {
        if let Some(metric) = self.as_ref() {
            metric.inc_by(value);
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

static SUBSCRIBE_THROUGHPUT_ROWS: LazyLock<Option<IntCounter>> = LazyLock::new(|| {
    int_counter(
        "floe_subscribe_rows_total",
        "Total number of rows emitted by SUBSCRIBE streams",
    )
});

pub(crate) fn inc_subscribe_rows(count: usize) {
    if count > 0 {
        SUBSCRIBE_THROUGHPUT_ROWS.inc_by(count as u64);
    }
}

pub(crate) fn init() {
    let _ = &*SUBSCRIBE_THROUGHPUT_ROWS;
}
