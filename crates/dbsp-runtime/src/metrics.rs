use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, register_histogram, register_histogram_vec,
};

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

static DBSP_FLUSH_WRITE_BATCH_CALLS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_dbsp_flush_write_batch_calls",
        "Number of write_batch calls issued per DBSP flush tick",
    ))
    .expect("register floe_dbsp_flush_write_batch_calls")
});

static DBSP_FLUSH_KEYS_WRITTEN: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_dbsp_flush_keys_written",
        "Number of keys written per DBSP flush tick",
    ))
    .expect("register floe_dbsp_flush_keys_written")
});

static DBSP_FOREGROUND_COMPACTION_LATENCY_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(HistogramOpts::new(
        "floe_dbsp_foreground_compaction_latency_ms",
        "Time spent in foreground DBSP compaction work during a flush tick in milliseconds",
    ))
    .expect("register floe_dbsp_foreground_compaction_latency_ms")
});

static DBSP_OPERATOR_PERSISTENCE_LATENCY_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_dbsp_operator_persistence_latency_ms",
        "Time spent persisting DBSP operator state in milliseconds",
        &["operator", "state"]
    )
    .expect("register floe_dbsp_operator_persistence_latency_ms")
});

static DBSP_OPERATOR_PHASE_LATENCY_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "floe_dbsp_operator_phase_latency_ms",
        "Time spent in DBSP operator phases in milliseconds",
        &["operator", "state", "phase"]
    )
    .expect("register floe_dbsp_operator_phase_latency_ms")
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

#[allow(dead_code)]
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
