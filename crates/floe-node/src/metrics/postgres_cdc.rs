use super::*;

static POSTGRES_CDC_METRIC_STATE: LazyLock<Mutex<PostgresCdcMetricState>> =
    LazyLock::new(|| Mutex::new(PostgresCdcMetricState::default()));

#[derive(Debug, Default)]
struct PostgresCdcMetricState {
    upstream_lsn_by_source: HashMap<PostgresSourceMetricKey, u64>,
    durable_lsn_by_source: HashMap<PostgresSourceMetricKey, u64>,
    table_applied_lsn: HashMap<PostgresTableMetricKey, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PostgresSourceMetricKey {
    source: String,
    slot: String,
}

impl PostgresSourceMetricKey {
    fn new(source: &str, slot: &str) -> Self {
        Self {
            source: source.to_string(),
            slot: slot.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PostgresTableMetricKey {
    source: String,
    slot: String,
    table: String,
}

impl PostgresTableMetricKey {
    fn new(source: &str, slot: &str, table: &str) -> Self {
        Self {
            source: source.to_string(),
            slot: slot.to_string(),
            table: table.to_string(),
        }
    }
}

fn metric_state_guard() -> std::sync::MutexGuard<'static, PostgresCdcMetricState> {
    match POSTGRES_CDC_METRIC_STATE.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("Postgres CDC metric state lock was poisoned; continuing");
            poisoned.into_inner()
        }
    }
}

pub(crate) fn record_postgres_cdc_upstream_lsn(source: &str, slot: &str, lsn: u64) {
    let mut state = metric_state_guard();
    let key = PostgresSourceMetricKey::new(source, slot);
    let upstream_lsn = record_max_lsn(&mut state.upstream_lsn_by_source, key.clone(), lsn);
    POSTGRES_CDC_UPSTREAM_LSN
        .with_label_values(&[source, slot])
        .set(i64_from_u64(upstream_lsn));

    if let Some(durable_lsn) = state.durable_lsn_by_source.get(&key).copied() {
        POSTGRES_CDC_SOURCE_LAG_BYTES
            .with_label_values(&[source, slot])
            .set(i64_from_u64(upstream_lsn.saturating_sub(durable_lsn)));
    }

    for (table_key, applied_lsn) in &state.table_applied_lsn {
        if table_key.source == source && table_key.slot == slot {
            POSTGRES_CDC_TABLE_LAG_BYTES
                .with_label_values(&[source, slot, table_key.table.as_str()])
                .set(i64_from_u64(upstream_lsn.saturating_sub(*applied_lsn)));
        }
    }
}

pub(crate) fn record_postgres_cdc_durable_lsn(source: &str, slot: &str, lsn: u64) {
    let mut state = metric_state_guard();
    let key = PostgresSourceMetricKey::new(source, slot);
    let durable_lsn = record_max_lsn(&mut state.durable_lsn_by_source, key.clone(), lsn);
    POSTGRES_CDC_DURABLE_LSN
        .with_label_values(&[source, slot])
        .set(i64_from_u64(durable_lsn));

    if let Some(upstream_lsn) = state.upstream_lsn_by_source.get(&key).copied() {
        POSTGRES_CDC_SOURCE_LAG_BYTES
            .with_label_values(&[source, slot])
            .set(i64_from_u64(upstream_lsn.saturating_sub(durable_lsn)));
    }
}

pub(crate) fn record_postgres_cdc_source_connected(source: &str, slot: &str, connected: bool) {
    POSTGRES_CDC_SOURCE_CONNECTED
        .with_label_values(&[source, slot])
        .set(if connected { 1 } else { 0 });
}

pub(crate) fn inc_postgres_cdc_reconnect(source: &str, slot: &str, result: &str) {
    POSTGRES_CDC_RECONNECTS_TOTAL
        .with_label_values(&[source, slot, result])
        .inc();
}

pub(crate) fn record_postgres_cdc_table_applied_lsn(
    source: &str,
    slot: &str,
    table: &str,
    lsn: u64,
) {
    let mut state = metric_state_guard();
    let key = PostgresTableMetricKey::new(source, slot, table);
    let applied_lsn = record_max_lsn(&mut state.table_applied_lsn, key, lsn);
    POSTGRES_CDC_TABLE_LAST_APPLIED_LSN
        .with_label_values(&[source, slot, table])
        .set(i64_from_u64(applied_lsn));

    let source_key = PostgresSourceMetricKey::new(source, slot);
    if let Some(upstream_lsn) = state.upstream_lsn_by_source.get(&source_key).copied() {
        POSTGRES_CDC_TABLE_LAG_BYTES
            .with_label_values(&[source, slot, table])
            .set(i64_from_u64(upstream_lsn.saturating_sub(applied_lsn)));
    }
}

pub(crate) fn record_postgres_cdc_schema_evolution_policy(source: &str, active_policy: &str) {
    for policy in [
        "fail_fast",
        "ignore_compatible",
        "apply_compatible_additions",
    ] {
        POSTGRES_CDC_SCHEMA_EVOLUTION_POLICY
            .with_label_values(&[source, policy])
            .set(i64::from(policy == active_policy));
    }
}

pub(crate) fn record_postgres_cdc_schema_evolution_observation(
    source: &str,
    table: &str,
    outcome: &str,
    policy: &str,
    observed_at_unix_ms: u64,
) {
    POSTGRES_CDC_SCHEMA_EVOLUTION_EVENTS_TOTAL
        .with_label_values(&[source, table, outcome, policy])
        .inc();
    POSTGRES_CDC_SCHEMA_EVOLUTION_LAST_OBSERVED_UNIX_MS
        .with_label_values(&[source, table, outcome, policy])
        .set(i64_from_u64(observed_at_unix_ms));
}

pub(crate) fn record_postgres_cdc_snapshot_concurrency(
    source: &str,
    slot: &str,
    target: usize,
    active: usize,
    max: usize,
) {
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_TARGET
        .with_label_values(&[source, slot])
        .set(i64_from_usize(target));
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ACTIVE
        .with_label_values(&[source, slot])
        .set(i64_from_usize(active));
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_MAX
        .with_label_values(&[source, slot])
        .set(i64_from_usize(max));
}

pub(crate) fn record_postgres_cdc_snapshot_wal_buffer_fill(
    source: &str,
    slot: &str,
    pending: usize,
    capacity: usize,
) {
    let fill_percent = if capacity == 0 {
        0
    } else {
        pending.saturating_mul(100) / capacity
    };
    POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_FILL_PERCENT
        .with_label_values(&[source, slot])
        .set(i64_from_usize(fill_percent.min(100)));
}

pub(crate) fn inc_postgres_cdc_snapshot_concurrency_adjustment(
    source: &str,
    slot: &str,
    direction: &str,
    reason: &str,
) {
    POSTGRES_CDC_SNAPSHOT_CONCURRENCY_ADJUSTMENTS_TOTAL
        .with_label_values(&[source, slot, direction, reason])
        .inc();
}

pub(super) fn init_postgres_cdc_metrics() {
    let _ = &*POSTGRES_CDC_METRIC_STATE;
}
