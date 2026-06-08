use std::sync::LazyLock;

use prometheus::{Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec};

pub(super) struct OptionalMetricValue<T> {
    metric: Option<T>,
}

pub(super) trait OptionalIntGauge {
    fn set(&self, value: i64);
}

pub(super) trait OptionalIntGaugeVec {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<IntGauge>;
}

pub(super) trait OptionalIntCounterVec {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<IntCounter>;
}

pub(super) trait OptionalHistogram {
    fn observe(&self, value: f64);
}

pub(super) trait OptionalHistogramVec {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<Histogram>;
}

impl OptionalIntGauge for LazyLock<Option<IntGauge>> {
    fn set(&self, value: i64) {
        if let Some(metric) = self.as_ref() {
            metric.set(value);
        }
    }
}

impl OptionalIntGauge for OptionalMetricValue<IntGauge> {
    fn set(&self, value: i64) {
        if let Some(metric) = &self.metric {
            metric.set(value);
        }
    }
}

impl OptionalIntGaugeVec for LazyLock<Option<IntGaugeVec>> {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<IntGauge> {
        OptionalMetricValue {
            metric: self
                .as_ref()
                .and_then(|metric| metric.get_metric_with_label_values(label_values).ok()),
        }
    }
}

impl OptionalIntCounterVec for LazyLock<Option<IntCounterVec>> {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<IntCounter> {
        OptionalMetricValue {
            metric: self
                .as_ref()
                .and_then(|metric| metric.get_metric_with_label_values(label_values).ok()),
        }
    }
}

impl OptionalMetricValue<IntCounter> {
    pub(super) fn inc(&self) {
        if let Some(metric) = &self.metric {
            metric.inc();
        }
    }

    pub(super) fn inc_by(&self, value: u64) {
        if let Some(metric) = &self.metric {
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
