use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use prometheus::{Histogram, HistogramOpts, HistogramVec, core::Collector};

use crate::collections::LookupMetrics;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogicalWorkSnapshot {
    pub input_delta_rows: u64,
    pub input_delta_batches: u64,
    pub output_delta_rows: u64,
    pub output_delta_batches: u64,
    pub state_lookup_keys: u64,
    pub state_lookup_rows: u64,
    pub state_scan_rows: u64,
    pub state_full_scan_count: u64,
    pub index_segments_examined: u64,
    pub index_postings_examined: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_rebuild_rows: u64,
    pub compaction_input_rows: u64,
    pub compaction_output_rows: u64,
    pub snapshot_rows: u64,
    pub persisted_rows: u64,
    pub persisted_keys: u64,
    pub left_delta_rows: u64,
    pub right_delta_rows: u64,
    pub left_changed_keys: u64,
    pub right_changed_keys: u64,
    pub left_state_rows_examined: u64,
    pub right_state_rows_examined: u64,
    pub delta_delta_rows_examined: u64,
    pub join_output_rows: u64,
    pub changed_groups: u64,
    pub group_state_rows_examined: u64,
    pub aggregate_state_rows_updated: u64,
    pub distinct_aux_rows_examined: u64,
    pub extrema_rebuild_rows: u64,
    pub changed_partitions: u64,
    pub partition_rows_examined: u64,
    pub replacement_rows: u64,
    pub changed_windows: u64,
    pub window_rows_examined: u64,
}

impl LogicalWorkSnapshot {
    pub fn from_input_delta_rows(rows: usize) -> Self {
        Self {
            input_delta_rows: rows as u64,
            input_delta_batches: (rows != 0) as u64,
            ..Self::default()
        }
    }

    pub fn add_assign(&mut self, other: Self) {
        self.input_delta_rows = self.input_delta_rows.saturating_add(other.input_delta_rows);
        self.input_delta_batches = self
            .input_delta_batches
            .saturating_add(other.input_delta_batches);
        self.output_delta_rows = self
            .output_delta_rows
            .saturating_add(other.output_delta_rows);
        self.output_delta_batches = self
            .output_delta_batches
            .saturating_add(other.output_delta_batches);
        self.state_lookup_keys = self
            .state_lookup_keys
            .saturating_add(other.state_lookup_keys);
        self.state_lookup_rows = self
            .state_lookup_rows
            .saturating_add(other.state_lookup_rows);
        self.state_scan_rows = self.state_scan_rows.saturating_add(other.state_scan_rows);
        self.state_full_scan_count = self
            .state_full_scan_count
            .saturating_add(other.state_full_scan_count);
        self.index_segments_examined = self
            .index_segments_examined
            .saturating_add(other.index_segments_examined);
        self.index_postings_examined = self
            .index_postings_examined
            .saturating_add(other.index_postings_examined);
        self.cache_hits = self.cache_hits.saturating_add(other.cache_hits);
        self.cache_misses = self.cache_misses.saturating_add(other.cache_misses);
        self.cache_rebuild_rows = self
            .cache_rebuild_rows
            .saturating_add(other.cache_rebuild_rows);
        self.compaction_input_rows = self
            .compaction_input_rows
            .saturating_add(other.compaction_input_rows);
        self.compaction_output_rows = self
            .compaction_output_rows
            .saturating_add(other.compaction_output_rows);
        self.snapshot_rows = self.snapshot_rows.saturating_add(other.snapshot_rows);
        self.persisted_rows = self.persisted_rows.saturating_add(other.persisted_rows);
        self.persisted_keys = self.persisted_keys.saturating_add(other.persisted_keys);
        self.left_delta_rows = self.left_delta_rows.saturating_add(other.left_delta_rows);
        self.right_delta_rows = self.right_delta_rows.saturating_add(other.right_delta_rows);
        self.left_changed_keys = self
            .left_changed_keys
            .saturating_add(other.left_changed_keys);
        self.right_changed_keys = self
            .right_changed_keys
            .saturating_add(other.right_changed_keys);
        self.left_state_rows_examined = self
            .left_state_rows_examined
            .saturating_add(other.left_state_rows_examined);
        self.right_state_rows_examined = self
            .right_state_rows_examined
            .saturating_add(other.right_state_rows_examined);
        self.delta_delta_rows_examined = self
            .delta_delta_rows_examined
            .saturating_add(other.delta_delta_rows_examined);
        self.join_output_rows = self.join_output_rows.saturating_add(other.join_output_rows);
        self.changed_groups = self.changed_groups.saturating_add(other.changed_groups);
        self.group_state_rows_examined = self
            .group_state_rows_examined
            .saturating_add(other.group_state_rows_examined);
        self.aggregate_state_rows_updated = self
            .aggregate_state_rows_updated
            .saturating_add(other.aggregate_state_rows_updated);
        self.distinct_aux_rows_examined = self
            .distinct_aux_rows_examined
            .saturating_add(other.distinct_aux_rows_examined);
        self.extrema_rebuild_rows = self
            .extrema_rebuild_rows
            .saturating_add(other.extrema_rebuild_rows);
        self.changed_partitions = self
            .changed_partitions
            .saturating_add(other.changed_partitions);
        self.partition_rows_examined = self
            .partition_rows_examined
            .saturating_add(other.partition_rows_examined);
        self.replacement_rows = self.replacement_rows.saturating_add(other.replacement_rows);
        self.changed_windows = self.changed_windows.saturating_add(other.changed_windows);
        self.window_rows_examined = self
            .window_rows_examined
            .saturating_add(other.window_rows_examined);
    }

    pub fn add_lookup_metrics(&mut self, metrics: LookupMetrics) {
        self.state_lookup_keys = self
            .state_lookup_keys
            .saturating_add(metrics.lookup_keys as u64);
        self.state_lookup_rows = self
            .state_lookup_rows
            .saturating_add(metrics.returned_rows as u64);
        self.index_segments_examined = self
            .index_segments_examined
            .saturating_add(metrics.index_segments_examined as u64);
        self.index_postings_examined = self
            .index_postings_examined
            .saturating_add(metrics.index_postings_examined as u64);
        self.cache_hits = self.cache_hits.saturating_add(metrics.cache_hits as u64);
        self.cache_misses = self
            .cache_misses
            .saturating_add(metrics.cache_misses as u64);
    }

    pub fn record_output_delta_rows(&mut self, rows: usize) {
        self.output_delta_rows = rows as u64;
        self.output_delta_batches = (rows != 0) as u64;
    }

    pub fn record_persisted_rows(&mut self, rows: usize) {
        self.persisted_rows = self.persisted_rows.saturating_add(rows as u64);
        self.persisted_keys = self.persisted_keys.saturating_add(rows as u64);
    }
}

#[derive(Clone, Debug, Default)]
pub struct LogicalWorkCollector {
    last_tick: LogicalWorkSnapshot,
    cumulative: LogicalWorkSnapshot,
}

impl LogicalWorkCollector {
    pub fn finish_tick(&mut self, snapshot: LogicalWorkSnapshot) {
        self.last_tick = snapshot;
        self.cumulative.add_assign(snapshot);
    }

    pub fn last_tick(&self) -> LogicalWorkSnapshot {
        self.last_tick
    }

    pub fn cumulative(&self) -> LogicalWorkSnapshot {
        self.cumulative
    }
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

struct OptionalMetricValue<T> {
    metric: Option<T>,
}

trait OptionalHistogram {
    fn observe(&self, value: f64);
}

trait OptionalHistogramVec {
    fn with_label_values(&self, label_values: &[&str]) -> OptionalMetricValue<Histogram>;
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

static DBSP_OPERATOR_PERSISTENCE_LATENCY_MS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    histogram_vec(
        "floe_dbsp_operator_persistence_latency_ms",
        "Time spent persisting DBSP operator state in milliseconds",
        &["operator", "state"],
    )
});

static DBSP_OPERATOR_PHASE_LATENCY_MS: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    histogram_vec(
        "floe_dbsp_operator_phase_latency_ms",
        "Time spent in DBSP operator phases in milliseconds",
        &["operator", "state", "phase"],
    )
});

static DBSP_OPERATOR_PERSISTENCE_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static DBSP_OPERATOR_PHASE_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static DBSP_OPERATOR_PERSISTENCE_LOG_SAMPLE_EVERY: LazyLock<u64> =
    LazyLock::new(|| env_u64("FLOE_DBSP_OPERATOR_PERSISTENCE_LOG_SAMPLE_EVERY", 128).max(1));
static DBSP_OPERATOR_PHASE_LOG_SAMPLE_EVERY: LazyLock<u64> =
    LazyLock::new(|| env_u64("FLOE_DBSP_OPERATOR_PHASE_LOG_SAMPLE_EVERY", 128).max(1));
static DBSP_OPERATOR_PERSISTENCE_LOG_MIN_MS: LazyLock<u64> = LazyLock::new(|| {
    env_u64(
        "FLOE_DBSP_OPERATOR_PERSISTENCE_LOG_MIN_MS",
        env_u64("FLOE_DBSP_OPERATOR_LOG_MIN_MS", 10),
    )
});
static DBSP_OPERATOR_PHASE_LOG_MIN_MS: LazyLock<u64> = LazyLock::new(|| {
    env_u64(
        "FLOE_DBSP_OPERATOR_PHASE_LOG_MIN_MS",
        env_u64("FLOE_DBSP_OPERATOR_LOG_MIN_MS", 10),
    )
});

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

pub(crate) fn observe_flush_write_metrics(metrics: FlushWriteMetrics) {
    DBSP_FLUSH_WRITE_BATCH_CALLS.observe(metrics.write_batch_calls as f64);
    DBSP_FLUSH_KEYS_WRITTEN.observe(metrics.keys_written as f64);
}

pub(crate) fn observe_foreground_compaction_latency_ms(latency_ms: u64) {
    DBSP_FOREGROUND_COMPACTION_LATENCY_MS.observe(latency_ms as f64);
}

pub(crate) fn observe_operator_persistence_latency_ms(
    operator: &'static str,
    state: &'static str,
    latency_ms: u64,
) {
    DBSP_OPERATOR_PERSISTENCE_LATENCY_MS
        .with_label_values(&[operator, state])
        .observe(latency_ms as f64);
    if latency_ms >= *DBSP_OPERATOR_PERSISTENCE_LOG_MIN_MS
        || DBSP_OPERATOR_PERSISTENCE_LOG_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(*DBSP_OPERATOR_PERSISTENCE_LOG_SAMPLE_EVERY)
    {
        tracing::info!(
            operator,
            state,
            latency_ms,
            "dbsp operator persistence latency"
        );
    }
}

pub(crate) fn observe_operator_phase_latency_ms(
    operator: &'static str,
    state: &'static str,
    phase: &'static str,
    latency_ms: u64,
) {
    DBSP_OPERATOR_PHASE_LATENCY_MS
        .with_label_values(&[operator, state, phase])
        .observe(latency_ms as f64);
    if latency_ms >= *DBSP_OPERATOR_PHASE_LOG_MIN_MS
        || DBSP_OPERATOR_PHASE_LOG_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(*DBSP_OPERATOR_PHASE_LOG_SAMPLE_EVERY)
    {
        tracing::info!(
            operator,
            state,
            phase,
            latency_ms,
            "dbsp operator phase latency"
        );
    }
}
