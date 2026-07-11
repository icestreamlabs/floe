use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Default)]
struct PhaseMetric {
    calls: u64,
    elapsed: Duration,
}

static RUNTIME_PHASE_PROFILE: OnceLock<Mutex<BTreeMap<&'static str, PhaseMetric>>> =
    OnceLock::new();

fn phase_profile() -> MutexGuard<'static, BTreeMap<&'static str, PhaseMetric>> {
    RUNTIME_PHASE_PROFILE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn enabled() -> bool {
    std::env::var_os("FLOE_PROFILE_COLUMNAR_PHASES").is_some()
}

pub(crate) fn start() -> Option<Instant> {
    enabled().then(Instant::now)
}

pub(crate) fn record_since(label: &'static str, start: Option<Instant>) {
    let Some(start) = start else {
        return;
    };
    record(label, start.elapsed());
}

pub(crate) fn record(label: &'static str, elapsed: Duration) {
    if !enabled() {
        return;
    }
    let mut profile = phase_profile();
    let metric = profile.entry(label).or_default();
    metric.calls += 1;
    metric.elapsed += elapsed;
}

pub fn reset_runtime_phase_profile() {
    if !enabled() {
        return;
    }
    phase_profile().clear();
}

pub fn print_runtime_phase_profile(name: &str) {
    if !enabled() {
        return;
    }
    let profile = phase_profile();
    let mut metrics = profile.iter().collect::<Vec<_>>();
    metrics.sort_by(|(_, left), (_, right)| right.elapsed.cmp(&left.elapsed));
    for (label, metric) in metrics {
        eprintln!(
            "[dbsp-runtime-phase-profile] name={} phase={} calls={} total_ms={:.3} mean_ms={:.3}",
            name,
            label,
            metric.calls,
            duration_ms(metric.elapsed),
            duration_ms(metric.elapsed) / metric.calls.max(1) as f64,
        );
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
