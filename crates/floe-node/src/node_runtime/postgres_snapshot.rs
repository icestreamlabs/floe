use super::*;

use anyhow::{Context, Result, anyhow, bail, ensure};
use floe_cdc_core::{
    CdcCheckpoint, CdcColumnarColumn, CdcColumnarRowBatch, CdcTransactionId, ChangeBatch,
    TransactionBatch,
};
use futures::{TryStreamExt, pin_mut};
use std::sync::LazyLock;
use std::time::Instant;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;

const DEFAULT_POSTGRES_SNAPSHOT_ROWS_PER_BATCH: usize = 16_384;
const DEFAULT_POSTGRES_SNAPSHOT_MAX_WORKERS: usize = 1;
const DEFAULT_POSTGRES_SNAPSHOT_INTRA_TABLE_CHUNKS: usize = 1;
const DEFAULT_POSTGRES_SNAPSHOT_ADAPTIVE_CONCURRENCY: bool = true;
const DEFAULT_POSTGRES_SNAPSHOT_MIN_WORKERS: usize = 1;
const DEFAULT_POSTGRES_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT: usize = 75;
const DEFAULT_POSTGRES_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT: usize = 25;
const DEFAULT_POSTGRES_SNAPSHOT_SLOW_SCAN_MS: u64 = 30_000;
const DEFAULT_POSTGRES_SNAPSHOT_CONTROLLER_INTERVAL_MS: u64 = 500;
static POSTGRES_SNAPSHOT_ROWS_PER_BATCH: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_ROWS_PER_BATCH)
});
static POSTGRES_SNAPSHOT_MAX_WORKERS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_MAX_WORKERS)
});
static POSTGRES_SNAPSHOT_INTRA_TABLE_CHUNKS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_INTRA_TABLE_CHUNKS)
});
static POSTGRES_SNAPSHOT_ADAPTIVE_CONCURRENCY: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_ADAPTIVE_CONCURRENCY")
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_ADAPTIVE_CONCURRENCY)
});
static POSTGRES_SNAPSHOT_MIN_WORKERS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_MIN_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_MIN_WORKERS)
});
static POSTGRES_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=100).contains(value))
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT)
});
static POSTGRES_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value <= 100)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT)
});
static POSTGRES_SNAPSHOT_SLOW_SCAN_MS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_SLOW_SCAN_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_SLOW_SCAN_MS)
});
static POSTGRES_SNAPSHOT_CONTROLLER_INTERVAL_MS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_CONTROLLER_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_CONTROLLER_INTERVAL_MS)
});
static CDC_PERF_LOGGING_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("FLOE_CDC_PERF_LOG")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
});

struct PostgresSnapshot {
    lsn: PostgresLsn,
    transaction: Option<TransactionBatch>,
    row_count: usize,
    wal_stream: Option<BufferedPostgresWalStream>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SnapshotTableChunk {
    Full,
    Int64Range {
        column: String,
        lower_inclusive: i64,
        upper_exclusive: Option<i64>,
    },
}

struct SnapshotWorkerControl {
    ready_tx: tokio::sync::oneshot::Sender<()>,
    start_rx: watch::Receiver<bool>,
    scan_limiter: Arc<SnapshotScanLimiter>,
    scan_observation_tx: Option<mpsc::UnboundedSender<SnapshotScanObservation>>,
}

struct SnapshotScanLimiter {
    source: String,
    slot: String,
    max_workers: usize,
    target_workers: AtomicUsize,
    active_workers: AtomicUsize,
    notify: tokio::sync::Notify,
}

struct SnapshotScanPermit {
    limiter: Arc<SnapshotScanLimiter>,
}

#[derive(Debug, Clone, Copy, Default)]
struct SnapshotWalBufferPressure {
    pending_events: usize,
    capacity: usize,
}

impl SnapshotWalBufferPressure {
    fn fill_percent(self) -> usize {
        if self.capacity == 0 {
            0
        } else {
            self.pending_events.saturating_mul(100) / self.capacity
        }
        .min(100)
    }
}

#[derive(Debug, Clone, Copy)]
struct SnapshotScanObservation {
    elapsed_ms: u64,
    rows: usize,
}

struct SnapshotAdaptiveConcurrencyRuntime {
    scan_limiter: Arc<SnapshotScanLimiter>,
    scan_observation_tx: Option<mpsc::UnboundedSender<SnapshotScanObservation>>,
    wal_pressure_tx: Option<watch::Sender<SnapshotWalBufferPressure>>,
    cancel: Option<CancellationToken>,
    task: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy)]
struct SnapshotAdaptiveConcurrencyConfig {
    enabled: bool,
    min_workers: usize,
    max_workers: usize,
    wal_buffer_high_watermark_percent: usize,
    wal_buffer_low_watermark_percent: usize,
    slow_scan_ms: u64,
    controller_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotConcurrencyDecision {
    target_workers: usize,
    direction: &'static str,
    reason: &'static str,
}

impl SnapshotScanLimiter {
    fn new(source: impl Into<String>, slot: impl Into<String>, max_workers: usize) -> Self {
        let source = source.into();
        let slot = slot.into();
        let max_workers = max_workers.max(1);
        let limiter = Self {
            source,
            slot,
            max_workers,
            target_workers: AtomicUsize::new(max_workers),
            active_workers: AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        };
        limiter.record_metrics();
        limiter
    }

    async fn acquire(self: &Arc<Self>) -> SnapshotScanPermit {
        loop {
            let active = self.active_workers.load(Ordering::Acquire);
            let target = self.target_workers.load(Ordering::Acquire).max(1);
            if active < target
                && self
                    .active_workers
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.record_metrics();
                return SnapshotScanPermit {
                    limiter: Arc::clone(self),
                };
            }
            self.notify.notified().await;
        }
    }

    fn set_target(&self, target_workers: usize) -> Option<(usize, usize)> {
        let target_workers = target_workers.clamp(1, self.max_workers);
        let previous = self
            .target_workers
            .swap(target_workers, Ordering::AcqRel)
            .clamp(1, self.max_workers);
        self.record_metrics();
        self.notify.notify_waiters();
        (previous != target_workers).then_some((previous, target_workers))
    }

    fn target_workers(&self) -> usize {
        self.target_workers
            .load(Ordering::Acquire)
            .clamp(1, self.max_workers)
    }

    fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Acquire)
    }

    fn record_metrics(&self) {
        crate::metrics::record_postgres_cdc_snapshot_concurrency(
            &self.source,
            &self.slot,
            self.target_workers(),
            self.active_workers(),
            self.max_workers,
        );
    }
}

impl Drop for SnapshotScanPermit {
    fn drop(&mut self) {
        self.limiter.active_workers.fetch_sub(1, Ordering::AcqRel);
        self.limiter.record_metrics();
        self.limiter.notify.notify_waiters();
    }
}

impl SnapshotAdaptiveConcurrencyRuntime {
    fn new(
        source_id: &CdcSourceId,
        slot: &str,
        max_workers: usize,
        task_count: usize,
        parent_cancel: &CancellationToken,
    ) -> Self {
        let config = snapshot_adaptive_concurrency_config(max_workers);
        let scan_limiter = Arc::new(SnapshotScanLimiter::new(
            source_id.as_str(),
            slot,
            config.max_workers,
        ));
        if !config.enabled || task_count <= 1 {
            return Self {
                scan_limiter,
                scan_observation_tx: None,
                wal_pressure_tx: None,
                cancel: None,
                task: None,
            };
        }

        let (scan_observation_tx, scan_observation_rx) = mpsc::unbounded_channel();
        let (wal_pressure_tx, wal_pressure_rx) =
            watch::channel(SnapshotWalBufferPressure::default());
        let cancel = parent_cancel.child_token();
        let task_scan_limiter = Arc::clone(&scan_limiter);
        let task_cancel = cancel.clone();
        let source = source_id.as_str().to_string();
        let slot = slot.to_string();
        let task_slot = slot.clone();
        let task = tokio::spawn(async move {
            run_snapshot_adaptive_concurrency_controller(
                source,
                task_slot,
                config,
                task_scan_limiter,
                wal_pressure_rx,
                scan_observation_rx,
                task_cancel,
            )
            .await;
        });

        tracing::info!(
            source = %source_id.as_str(),
            slot = %slot,
            min_workers = config.min_workers,
            max_workers = config.max_workers,
            wal_buffer_high_watermark_percent = config.wal_buffer_high_watermark_percent,
            wal_buffer_low_watermark_percent = config.wal_buffer_low_watermark_percent,
            slow_scan_ms = config.slow_scan_ms,
            controller_interval_ms = config.controller_interval.as_millis() as u64,
            "enabled adaptive Postgres CDC snapshot concurrency"
        );

        Self {
            scan_limiter,
            scan_observation_tx: Some(scan_observation_tx),
            wal_pressure_tx: Some(wal_pressure_tx),
            cancel: Some(cancel),
            task: Some(task),
        }
    }

    fn scan_limiter(&self) -> Arc<SnapshotScanLimiter> {
        Arc::clone(&self.scan_limiter)
    }

    fn scan_observation_tx(&self) -> Option<mpsc::UnboundedSender<SnapshotScanObservation>> {
        self.scan_observation_tx.clone()
    }

    fn wal_pressure_tx(&self) -> Option<watch::Sender<SnapshotWalBufferPressure>> {
        self.wal_pressure_tx.clone()
    }

    async fn shutdown(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }
        if let Some(task) = self.task.take()
            && let Err(err) = task.await
            && !err.is_cancelled()
        {
            tracing::warn!(
                error = %err,
                "Postgres CDC snapshot adaptive concurrency controller task failed"
            );
        }
    }
}

fn snapshot_adaptive_concurrency_config(max_workers: usize) -> SnapshotAdaptiveConcurrencyConfig {
    let max_workers = max_workers.max(1);
    let min_workers = (*POSTGRES_SNAPSHOT_MIN_WORKERS).clamp(1, max_workers);
    let high = *POSTGRES_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT;
    let mut low = *POSTGRES_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT;
    if low >= high {
        low = high.saturating_sub(1);
    }
    SnapshotAdaptiveConcurrencyConfig {
        enabled: *POSTGRES_SNAPSHOT_ADAPTIVE_CONCURRENCY && max_workers > 1,
        min_workers,
        max_workers,
        wal_buffer_high_watermark_percent: high,
        wal_buffer_low_watermark_percent: low,
        slow_scan_ms: *POSTGRES_SNAPSHOT_SLOW_SCAN_MS,
        controller_interval: Duration::from_millis(*POSTGRES_SNAPSHOT_CONTROLLER_INTERVAL_MS),
    }
}

async fn run_snapshot_adaptive_concurrency_controller(
    source: String,
    slot: String,
    config: SnapshotAdaptiveConcurrencyConfig,
    scan_limiter: Arc<SnapshotScanLimiter>,
    mut wal_pressure_rx: watch::Receiver<SnapshotWalBufferPressure>,
    mut scan_observation_rx: mpsc::UnboundedReceiver<SnapshotScanObservation>,
    cancel: CancellationToken,
) {
    let mut latest_scan_observation = None;
    let mut interval = tokio::time::interval(config.controller_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            maybe_observation = scan_observation_rx.recv() => {
                match maybe_observation {
                    Some(observation) => latest_scan_observation = Some(observation),
                    None => break,
                }
            }
            changed = wal_pressure_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = interval.tick() => {
                let current_target = scan_limiter.target_workers();
                let wal_pressure = *wal_pressure_rx.borrow();
                let scan_observation = latest_scan_observation.take();
                let scan_elapsed_ms = scan_observation.map(|observation: SnapshotScanObservation| observation.elapsed_ms);
                let scan_rows = scan_observation.map(|observation: SnapshotScanObservation| observation.rows);
                let decision = snapshot_concurrency_decision(
                    config,
                    current_target,
                    wal_pressure,
                    scan_observation,
                );
                if let Some(decision) = decision
                    && let Some((previous_target, target_workers)) =
                        scan_limiter.set_target(decision.target_workers)
                {
                    crate::metrics::inc_postgres_cdc_snapshot_concurrency_adjustment(
                        &source,
                        &slot,
                        decision.direction,
                        decision.reason,
                    );
                    tracing::info!(
                        source = %source,
                        slot = %slot,
                        previous_target,
                        target_workers,
                        active_workers = scan_limiter.active_workers(),
                        wal_buffer_fill_percent = wal_pressure.fill_percent(),
                        scan_elapsed_ms,
                        scan_rows,
                        direction = decision.direction,
                        reason = decision.reason,
                        "adjusted adaptive Postgres CDC snapshot concurrency"
                    );
                }
            }
        }
    }
}

fn snapshot_concurrency_decision(
    config: SnapshotAdaptiveConcurrencyConfig,
    current_target: usize,
    wal_pressure: SnapshotWalBufferPressure,
    scan_observation: Option<SnapshotScanObservation>,
) -> Option<SnapshotConcurrencyDecision> {
    let current_target = current_target.clamp(config.min_workers, config.max_workers);
    let wal_fill_percent = wal_pressure.fill_percent();
    if wal_pressure.capacity > 0
        && wal_fill_percent >= config.wal_buffer_high_watermark_percent
        && current_target > config.min_workers
    {
        return Some(SnapshotConcurrencyDecision {
            target_workers: current_target.saturating_sub(1).max(config.min_workers),
            direction: "decrease",
            reason: "wal_buffer_high",
        });
    }

    if let Some(scan_observation) = scan_observation
        && scan_observation.elapsed_ms >= config.slow_scan_ms
        && current_target > config.min_workers
    {
        return Some(SnapshotConcurrencyDecision {
            target_workers: current_target.saturating_sub(1).max(config.min_workers),
            direction: "decrease",
            reason: "snapshot_scan_slow",
        });
    }

    if wal_pressure.capacity > 0
        && wal_fill_percent <= config.wal_buffer_low_watermark_percent
        && current_target < config.max_workers
    {
        return Some(SnapshotConcurrencyDecision {
            target_workers: current_target.saturating_add(1).min(config.max_workers),
            direction: "increase",
            reason: "wal_buffer_low",
        });
    }

    None
}

pub(super) async fn ensure_postgres_cdc_publication_and_slot(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .context("connect Postgres control plane for CDC setup")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres CDC setup connection closed");
        }
    });

    let setup_result = ensure_postgres_cdc_publication_and_slot_with_client(
        &client,
        slot,
        publication,
        runtime_plan,
    )
    .await;
    drop(client);
    connection_task.abort();
    setup_result
}

async fn ensure_postgres_cdc_publication_and_slot_with_client(
    client: &tokio_postgres::Client,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
) -> Result<()> {
    let publication_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await
        .with_context(|| format!("check Postgres CDC publication '{publication}'"))?
        .get(0);
    if !publication_exists {
        let schemas = sorted_snapshot_schemas(&runtime_plan.schemas);
        ensure!(
            !schemas.is_empty(),
            "cannot create Postgres CDC publication '{publication}' without tables"
        );
        let mut tables = schemas
            .iter()
            .map(|schema| qualified_table_name(schema.upstream_table()))
            .collect::<Vec<_>>();
        tables.sort();
        tables.dedup();
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {} FOR TABLE {}",
                quote_pg_ident(publication),
                tables.join(", ")
            ))
            .await
            .with_context(|| format!("create Postgres CDC publication '{publication}'"))?;
        tracing::info!(
            source = %runtime_plan.source_id.as_str(),
            publication = %publication,
            tables = tables.len(),
            "created Postgres CDC publication"
        );
    }

    match postgres_replication_slot_plugin(client, slot).await? {
        Some(plugin) => ensure!(
            plugin.as_deref() == Some("pgoutput"),
            "Postgres CDC logical replication slot '{slot}' must use pgoutput, got {:?}",
            plugin
        ),
        None => {
            tracing::debug!(
                source = %runtime_plan.source_id.as_str(),
                slot = %slot,
                "Postgres CDC logical replication slot is missing; initial snapshot will create it with an exported snapshot"
            );
        }
    }

    Ok(())
}

pub(super) async fn run_initial_postgres_snapshot_if_needed(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    table_store: &CdcTableStore,
    sender: &mpsc::Sender<QueuedCdcTransaction>,
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
) -> Result<InitialPostgresSnapshot> {
    if table_store
        .load_checkpoint(&runtime_plan.source_id)
        .await
        .with_context(|| {
            format!(
                "load CDC checkpoint before Postgres snapshot for '{}'",
                runtime_plan.source_id.as_str()
            )
        })?
        .is_some()
    {
        return Ok(InitialPostgresSnapshot {
            lsn: None,
            wal_stream: None,
        });
    }

    let wal_commit_lsn_rx = commit_lsn_rx.as_ref().map(|receiver| (**receiver).clone());
    let snapshot = load_postgres_initial_snapshot(
        connection_string,
        slot,
        publication,
        runtime_plan,
        cdc_replication_debug,
        wal_commit_lsn_rx,
        cancel.clone(),
    )
    .await?;
    finish_loaded_postgres_snapshot(
        slot,
        publication,
        runtime_plan,
        table_store,
        sender,
        commit_lsn_rx,
        cancel,
        snapshot,
    )
    .await
}

async fn finish_loaded_postgres_snapshot(
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    table_store: &CdcTableStore,
    sender: &mpsc::Sender<QueuedCdcTransaction>,
    commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
    snapshot: PostgresSnapshot,
) -> Result<InitialPostgresSnapshot> {
    let lsn = snapshot.lsn;
    let row_count = snapshot.row_count;
    let mut wal_stream = snapshot.wal_stream;

    let finish_result = async {
        match snapshot.transaction {
            Some(transaction) => {
                sender
                    .send(QueuedCdcTransaction {
                        slot: slot.to_string(),
                        source_id: runtime_plan.source_id.clone(),
                        transaction,
                    })
                    .await
                    .map_err(|err| {
                        anyhow!("failed to enqueue initial Postgres CDC snapshot: {err}")
                    })?;
                wait_for_postgres_snapshot_commit(commit_lsn_rx, slot, lsn, cancel).await?;
            }
            None => {
                let checkpoint = snapshot_checkpoint(&runtime_plan.source_id, lsn)?;
                table_store.commit_checkpoint(&checkpoint).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = finish_result {
        if let Some(stream) = wal_stream.take() {
            abort_buffered_postgres_wal_stream(stream).await;
        }
        return Err(err);
    }

    if let Some(stream) = wal_stream.as_ref() {
        release_buffered_postgres_wal_feedback(stream);
    }

    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        publication = %publication,
        lsn = %lsn,
        rows = row_count,
        "completed initial Postgres CDC snapshot"
    );
    Ok(InitialPostgresSnapshot {
        lsn: Some(lsn),
        wal_stream,
    })
}

async fn load_postgres_initial_snapshot(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    wal_commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
) -> Result<PostgresSnapshot> {
    let (mut client, connection) =
        tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .context("connect Postgres control plane for initial CDC snapshot")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres initial snapshot connection closed");
        }
    });

    let snapshot = load_postgres_initial_snapshot_from_client(
        connection_string,
        slot,
        &mut client,
        publication,
        &runtime_plan.source_id,
        &runtime_plan.schemas,
        runtime_plan,
        cdc_replication_debug,
        wal_commit_lsn_rx,
        cancel,
    )
    .await;
    drop(client);
    connection_task.abort();
    snapshot
}

async fn load_postgres_initial_snapshot_from_client(
    connection_string: &str,
    slot: &str,
    client: &mut tokio_postgres::Client,
    publication: &str,
    source_id: &CdcSourceId,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    runtime_plan: &PostgresCdcRuntimePlan,
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    wal_commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
) -> Result<PostgresSnapshot> {
    let sorted_schemas = sorted_snapshot_schemas(schemas);
    let max_workers = *POSTGRES_SNAPSHOT_MAX_WORKERS;
    match postgres_replication_slot_plugin(client, slot).await? {
        Some(plugin) => {
            ensure!(
                plugin.as_deref() == Some("pgoutput"),
                "Postgres CDC logical replication slot '{slot}' must use pgoutput, got {:?}",
                plugin
            );
            bail!(
                "Postgres CDC logical replication slot '{slot}' already exists but Floe has no durable CDC checkpoint. Floe cannot derive a lock-free initial snapshot boundary from an existing slot safely; drop the slot and let Floe recreate it, or restore the matching Floe data directory/checkpoint."
            );
        }
        None => {
            return load_exported_slot_postgres_initial_snapshot_from_client(
                connection_string,
                slot,
                client,
                publication,
                source_id,
                runtime_plan,
                cdc_replication_debug,
                sorted_schemas,
                max_workers,
                wal_commit_lsn_rx,
                cancel,
            )
            .await;
        }
    }
}

async fn load_exported_slot_postgres_initial_snapshot_from_client(
    connection_string: &str,
    slot: &str,
    client: &mut tokio_postgres::Client,
    publication: &str,
    source_id: &CdcSourceId,
    runtime_plan: &PostgresCdcRuntimePlan,
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    sorted_schemas: Vec<&CdcTableSchema>,
    max_workers: usize,
    wal_commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
) -> Result<PostgresSnapshot> {
    let replication_config = replication_config_from_connection_string(
        connection_string,
        slot,
        publication,
        PostgresLsn::ZERO,
    )?;
    let exported_slot = create_pgoutput_slot_with_exported_snapshot(&replication_config)
        .await
        .with_context(|| {
            format!(
                "create Postgres CDC slot '{slot}' with an exported snapshot for lock-free initial snapshot"
            )
        })?;
    let snapshot_lsn = exported_slot.consistent_lsn();
    let exported_snapshot = exported_slot.snapshot_name().to_string();
    let transaction = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .context("begin exported-slot initial Postgres CDC snapshot transaction")?;
    bind_transaction_to_exported_snapshot(&transaction, &exported_snapshot).await?;

    validate_publication_tables(&transaction, publication, &sorted_schemas).await?;
    for schema in &sorted_schemas {
        validate_upstream_table_schema(&transaction, schema).await?;
    }

    let mut snapshot_tasks = Vec::new();
    let use_parallel_workers =
        max_workers > 1 && (sorted_schemas.len() > 1 || *POSTGRES_SNAPSHOT_INTRA_TABLE_CHUNKS > 1);
    if use_parallel_workers {
        for (table_idx, schema) in sorted_schemas.iter().enumerate() {
            let chunks = snapshot_table_chunks(&transaction, schema).await?;
            for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
                snapshot_tasks.push((table_idx, chunk_idx, (*schema).clone(), chunk));
            }
        }
    }

    let mut snapshot_transaction = Some(transaction);
    let (change_batches, row_count, task_count, wal_stream) = if use_parallel_workers {
        let task_count = snapshot_tasks.len();
        let (start_tx, start_rx) = watch::channel(false);
        let mut adaptive_concurrency = SnapshotAdaptiveConcurrencyRuntime::new(
            source_id,
            slot,
            max_workers,
            task_count,
            &cancel,
        );
        let mut worker_handles = Vec::with_capacity(task_count);
        let mut ready_receivers = Vec::with_capacity(task_count);
        for (table_idx, chunk_idx, schema, chunk) in snapshot_tasks {
            let connection_string = connection_string.to_string();
            let exported_snapshot = exported_snapshot.clone();
            let start_rx = start_rx.clone();
            let scan_limiter = adaptive_concurrency.scan_limiter();
            let scan_observation_tx = adaptive_concurrency.scan_observation_tx();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            ready_receivers.push(ready_rx);
            worker_handles.push(tokio::spawn(async move {
                snapshot_table_change_batches_from_exported_snapshot(
                    &connection_string,
                    &exported_snapshot,
                    &schema,
                    &chunk,
                    Some(SnapshotWorkerControl {
                        ready_tx,
                        start_rx,
                        scan_limiter,
                        scan_observation_tx,
                    }),
                )
                .await
                .map(|snapshot| (table_idx, chunk_idx, snapshot))
            }));
        }

        if let Err(err) = wait_for_snapshot_workers_ready(ready_receivers).await {
            adaptive_concurrency.shutdown().await;
            abort_snapshot_worker_tasks(worker_handles).await;
            return Err(err);
        }

        if let Err(err) = snapshot_transaction
            .take()
            .expect("snapshot validation transaction is present")
            .commit()
            .await
            .context("commit exported-slot initial Postgres CDC validation transaction")
        {
            adaptive_concurrency.shutdown().await;
            abort_snapshot_worker_tasks(worker_handles).await;
            return Err(err);
        }
        drop(exported_slot);

        match start_buffered_postgres_wal_stream(
            connection_string,
            slot,
            publication,
            runtime_plan,
            snapshot_lsn,
            cdc_replication_debug,
            adaptive_concurrency.wal_pressure_tx(),
            wal_commit_lsn_rx,
            cancel,
        )
        .await
        {
            Ok(stream) => {
                if let Err(err) = start_tx
                    .send(true)
                    .context("release Postgres snapshot workers after starting WAL stream")
                {
                    adaptive_concurrency.shutdown().await;
                    abort_buffered_postgres_wal_stream(stream).await;
                    abort_snapshot_worker_tasks(worker_handles).await;
                    return Err(err);
                }

                let table_snapshots = match collect_snapshot_worker_tasks(worker_handles).await {
                    Ok(snapshots) => snapshots,
                    Err(err) => {
                        adaptive_concurrency.shutdown().await;
                        abort_buffered_postgres_wal_stream(stream).await;
                        return Err(err);
                    }
                };
                adaptive_concurrency.shutdown().await;

                let mut table_snapshots = table_snapshots;
                table_snapshots.sort_by_key(|(table_idx, chunk_idx, _)| (*table_idx, *chunk_idx));

                let mut change_batches = Vec::new();
                let mut row_count = 0_usize;
                for (_, _, table_snapshot) in table_snapshots {
                    row_count = row_count.saturating_add(table_snapshot.row_count);
                    change_batches.extend(table_snapshot.change_batches);
                }
                (change_batches, row_count, task_count, stream)
            }
            Err(err) => {
                adaptive_concurrency.shutdown().await;
                abort_snapshot_worker_tasks(worker_handles).await;
                return Err(err);
            }
        }
    } else {
        drop(exported_slot);
        let stream = start_buffered_postgres_wal_stream(
            connection_string,
            slot,
            publication,
            runtime_plan,
            snapshot_lsn,
            cdc_replication_debug,
            None,
            wal_commit_lsn_rx,
            cancel,
        )
        .await?;

        let scan_result = async {
            let mut change_batches = Vec::new();
            let mut row_count = 0_usize;
            for schema in &sorted_schemas {
                let table_snapshot = snapshot_table_change_batches(
                    snapshot_transaction
                        .as_ref()
                        .expect("snapshot transaction is present"),
                    schema,
                )
                .await?;
                row_count = row_count.saturating_add(table_snapshot.row_count);
                change_batches.extend(table_snapshot.change_batches);
            }
            snapshot_transaction
                .take()
                .expect("snapshot transaction is present")
                .commit()
                .await
                .context("commit exported-slot initial Postgres CDC snapshot transaction")?;
            Ok::<_, anyhow::Error>((change_batches, row_count, sorted_schemas.len()))
        }
        .await;

        let (change_batches, row_count, task_count) = match scan_result {
            Ok(result) => result,
            Err(err) => {
                abort_buffered_postgres_wal_stream(stream).await;
                return Err(err);
            }
        };
        (change_batches, row_count, task_count, stream)
    };
    let transaction = snapshot_transaction_batch(source_id, snapshot_lsn, change_batches)?;

    tracing::info!(
        source = %source_id.as_str(),
        slot = %slot,
        snapshot = %exported_snapshot,
        lsn = %snapshot_lsn,
        tables = sorted_schemas.len(),
        tasks = task_count,
        max_workers,
        rows = row_count,
        "loaded lock-free initial Postgres CDC snapshot from exported logical-slot snapshot"
    );

    Ok(PostgresSnapshot {
        lsn: snapshot_lsn,
        transaction,
        row_count,
        wal_stream: Some(wal_stream),
    })
}

async fn start_buffered_postgres_wal_stream(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    snapshot_lsn: PostgresLsn,
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    wal_pressure_tx: Option<watch::Sender<SnapshotWalBufferPressure>>,
    commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
) -> Result<BufferedPostgresWalStream> {
    let replication_config = replication_config_from_connection_string(
        connection_string,
        slot,
        publication,
        snapshot_lsn,
    )
    .with_context(|| {
        format!("configure Postgres CDC WAL stream from snapshot LSN {snapshot_lsn}")
    })?;
    let replication = connect_postgres_replication_client_with_retry(&replication_config).await?;
    let capacity = replication_config.buffer_events();
    let slot = slot.to_string();
    let (sender, receiver) = mpsc::channel(capacity);
    let (release_feedback_tx, release_feedback_rx) = watch::channel(false);
    let task_runtime_plan = runtime_plan.clone();
    let task_slot = slot.clone();
    let task_cdc_replication_debug = Arc::clone(cdc_replication_debug);
    let task = tokio::spawn(async move {
        buffer_postgres_wal_stream(
            replication,
            task_runtime_plan,
            task_slot,
            snapshot_lsn,
            task_cdc_replication_debug,
            wal_pressure_tx,
            commit_lsn_rx,
            release_feedback_rx,
            sender,
            cancel,
        )
        .await
    });
    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        lsn = %snapshot_lsn,
        buffer_events = capacity,
        "started buffered Postgres CDC WAL stream while initial snapshot is loading"
    );
    Ok(BufferedPostgresWalStream {
        slot,
        snapshot_lsn,
        release_feedback_tx,
        receiver,
        task,
    })
}

async fn connect_postgres_replication_client_with_retry(
    config: &PostgresCdcConfig,
) -> Result<PostgresReplicationClient> {
    let started_at = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        match PostgresReplicationClient::connect(config).await {
            Ok(client) => return Ok(client),
            Err(err)
                if started_at.elapsed() < Duration::from_secs(5)
                    && format!("{err:#}").contains("active") =>
            {
                tracing::debug!(
                    slot = %config.slot(),
                    attempts,
                    error = %err,
                    "Postgres CDC WAL stream is waiting for exported snapshot slot release"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn buffer_postgres_wal_stream(
    mut replication: PostgresReplicationClient,
    runtime_plan: PostgresCdcRuntimePlan,
    slot: String,
    snapshot_lsn: PostgresLsn,
    cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    wal_pressure_tx: Option<watch::Sender<SnapshotWalBufferPressure>>,
    mut commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    mut release_feedback_rx: watch::Receiver<bool>,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    cancel: CancellationToken,
) -> Result<()> {
    let router = PostgresTableRouter::from_schemas(runtime_plan.schemas.values());
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        runtime_plan.source_id.clone(),
        router,
        runtime_plan.schemas.clone(),
        runtime_plan.schema_evolution_policy,
    );
    let mut feedback_released = false;
    let mut last_committed_tick_id = 0_u64;

    let result = async {
        loop {
            if !feedback_released && *release_feedback_rx.borrow_and_update() {
                feedback_released = true;
                replication.update_applied_lsn(snapshot_lsn);
            }
            if feedback_released {
                update_buffered_postgres_applied_lsn(
                    &mut replication,
                    commit_lsn_rx.as_mut(),
                    &slot,
                    &mut last_committed_tick_id,
                )?;
            }

            let event = tokio::select! {
                _ = cancel.cancelled() => break,
                changed = release_feedback_rx.changed(), if !feedback_released => {
                    changed.context("buffered Postgres CDC feedback release channel closed")?;
                    continue;
                }
                event = replication.recv() => event.context("receive buffered native Postgres CDC event")?,
            };
            let Some(event) = event else {
                break;
            };
            if let Some(frontier_lsn) = buffered_postgres_replication_event_frontier_lsn(&event) {
                metrics::record_postgres_cdc_upstream_lsn(
                    runtime_plan.source_id.as_str(),
                    &slot,
                    frontier_lsn.as_u64(),
                );
            }
            if matches!(event, PostgresReplicationEvent::StoppedAt { .. }) {
                break;
            }
            let transaction = match assembler.accept_event(event) {
                Ok(transaction) => {
                    let observations = assembler.drain_schema_evolution_observations();
                    if !observations.is_empty() {
                        record_postgres_schema_evolution_observations(
                            &cdc_replication_debug,
                            &runtime_plan.source_id,
                            observations,
                        )
                        .await;
                    }
                    transaction
                }
                Err(err) => {
                    let observations = assembler.drain_schema_evolution_observations();
                    if !observations.is_empty() {
                        record_postgres_schema_evolution_observations(
                            &cdc_replication_debug,
                            &runtime_plan.source_id,
                            observations,
                        )
                        .await;
                    }
                    return Err(err);
                }
            };
            let Some(transaction) = transaction else {
                continue;
            };
            let commit_lsn = PostgresLsn::from_source_position(transaction.commit_position())?;
            if commit_lsn <= snapshot_lsn {
                tracing::debug!(
                    source = %runtime_plan.source_id.as_str(),
                    slot = %slot,
                    commit_lsn = %commit_lsn,
                    snapshot_lsn = %snapshot_lsn,
                    "dropping Postgres CDC WAL transaction covered by initial snapshot"
                );
                continue;
            }
            tracing::debug!(
                source = %runtime_plan.source_id.as_str(),
                slot = %slot,
                change_batches = transaction.change_batches().len(),
                commit_position = ?transaction.commit_position(),
                "buffered native Postgres CDC transaction during initial snapshot"
            );
            record_snapshot_wal_buffer_pressure(
                &wal_pressure_tx,
                runtime_plan.source_id.as_str(),
                &slot,
                sender.max_capacity()
                    .saturating_sub(sender.capacity())
                    .saturating_add(1),
                sender.max_capacity(),
            );
            sender
                .send(QueuedCdcTransaction {
                    slot: slot.clone(),
                    source_id: runtime_plan.source_id.clone(),
                    transaction,
                })
                .await
                .map_err(|err| {
                    anyhow!("failed to enqueue buffered native Postgres CDC transaction: {err}")
                })?;
            record_snapshot_wal_buffer_pressure(
                &wal_pressure_tx,
                runtime_plan.source_id.as_str(),
                &slot,
                sender.max_capacity().saturating_sub(sender.capacity()),
                sender.max_capacity(),
            );
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    replication.stop();
    let shutdown_result = replication.shutdown().await;
    result?;
    shutdown_result
}

fn record_snapshot_wal_buffer_pressure(
    sender: &Option<watch::Sender<SnapshotWalBufferPressure>>,
    source: &str,
    slot: &str,
    pending_events: usize,
    capacity: usize,
) {
    let pending_events = pending_events.min(capacity);
    crate::metrics::record_postgres_cdc_snapshot_wal_buffer_fill(
        source,
        slot,
        pending_events,
        capacity,
    );
    if let Some(sender) = sender {
        let _ = sender.send(SnapshotWalBufferPressure {
            pending_events,
            capacity,
        });
    }
}

fn buffered_postgres_replication_event_frontier_lsn(
    event: &PostgresReplicationEvent,
) -> Option<PostgresLsn> {
    match event {
        PostgresReplicationEvent::KeepAlive { wal_end, .. } => Some(*wal_end),
        PostgresReplicationEvent::Begin { final_lsn, .. } => Some(*final_lsn),
        PostgresReplicationEvent::XLogData { wal_end, .. } => Some(*wal_end),
        PostgresReplicationEvent::Commit { end_lsn, .. } => Some(*end_lsn),
        PostgresReplicationEvent::Message { lsn, .. } => Some(*lsn),
        PostgresReplicationEvent::StoppedAt { reached } => Some(*reached),
    }
}

fn update_buffered_postgres_applied_lsn(
    replication: &mut PostgresReplicationClient,
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    last_committed_tick_id: &mut u64,
) -> Result<()> {
    let Some(receiver) = receiver else {
        return Ok(());
    };

    let mut latest_commit = None;
    while receiver.has_changed().unwrap_or(false) {
        latest_commit = Some(receiver.borrow_and_update().clone());
    }
    let Some(commit) = latest_commit else {
        return Ok(());
    };
    if commit.tick_id <= *last_committed_tick_id {
        return Ok(());
    }

    if let Some(target_lsn) = commit
        .slots
        .iter()
        .find(|entry| entry.slot == slot)
        .map(|entry| entry.lsn.as_str())
    {
        replication.update_applied_lsn(PostgresLsn::parse(target_lsn)?);
    }
    *last_committed_tick_id = commit.tick_id;
    Ok(())
}

fn release_buffered_postgres_wal_feedback(stream: &BufferedPostgresWalStream) {
    let _ = stream.release_feedback_tx.send(true);
}

async fn abort_buffered_postgres_wal_stream(stream: BufferedPostgresWalStream) {
    stream.task.abort();
    let _ = stream.task.await;
}

async fn wait_for_snapshot_workers_ready(
    ready_receivers: Vec<tokio::sync::oneshot::Receiver<()>>,
) -> Result<()> {
    for receiver in ready_receivers {
        receiver
            .await
            .context("Postgres snapshot worker exited before binding exported snapshot")?;
    }
    Ok(())
}

async fn collect_snapshot_worker_tasks(
    worker_handles: Vec<JoinHandle<Result<(usize, usize, SnapshotTableChangeBatches)>>>,
) -> Result<Vec<(usize, usize, SnapshotTableChangeBatches)>> {
    let mut snapshots = Vec::with_capacity(worker_handles.len());
    let mut first_error = None;
    for handle in worker_handles {
        match handle.await {
            Ok(Ok(snapshot)) => snapshots.push(snapshot),
            Ok(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(anyhow!("Postgres snapshot worker task failed: {err}"));
                }
            }
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(snapshots)
}

async fn abort_snapshot_worker_tasks(
    worker_handles: Vec<JoinHandle<Result<(usize, usize, SnapshotTableChangeBatches)>>>,
) {
    for handle in &worker_handles {
        handle.abort();
    }
    for handle in worker_handles {
        let _ = handle.await;
    }
}

fn snapshot_transaction_batch(
    source_id: &CdcSourceId,
    snapshot_lsn: PostgresLsn,
    change_batches: Vec<ChangeBatch>,
) -> Result<Option<TransactionBatch>> {
    if change_batches.is_empty() {
        return Ok(None);
    }
    let position = snapshot_lsn.to_source_position()?;
    Ok(Some(TransactionBatch::new(
        source_id.clone(),
        Some(snapshot_transaction_id(snapshot_lsn)?),
        Some(position.clone()),
        position,
        change_batches,
    )?))
}

fn sorted_snapshot_schemas(schemas: &HashMap<CdcTableId, CdcTableSchema>) -> Vec<&CdcTableSchema> {
    let mut sorted = schemas.values().collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        left.upstream_table()
            .schema()
            .cmp(right.upstream_table().schema())
            .then(
                left.upstream_table()
                    .table()
                    .cmp(right.upstream_table().table()),
            )
            .then(left.table_id().as_str().cmp(right.table_id().as_str()))
    });
    sorted
}

async fn postgres_replication_slot_plugin(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<Option<String>>> {
    let row = client
        .query_opt(
            "SELECT plugin
             FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("check Postgres CDC logical replication slot '{slot}'"))?;
    Ok(row.map(|row| row.get(0)))
}

async fn validate_publication_tables(
    transaction: &tokio_postgres::Transaction<'_>,
    publication: &str,
    schemas: &[&CdcTableSchema],
) -> Result<()> {
    let exists_row = transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await
        .with_context(|| format!("validate Postgres publication '{publication}'"))?;
    let publication_exists: bool = exists_row.get(0);
    ensure!(
        publication_exists,
        "Postgres CDC publication '{publication}' does not exist"
    );

    for schema in schemas {
        let upstream = schema.upstream_table();
        let row = transaction
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_publication_tables
                    WHERE pubname = $1
                      AND schemaname = $2
                      AND tablename = $3
                 )",
                &[&publication, &upstream.schema(), &upstream.table()],
            )
            .await
            .with_context(|| {
                format!(
                    "validate Postgres publication '{publication}' includes '{}.{}'",
                    upstream.schema(),
                    upstream.table()
                )
            })?;
        let included: bool = row.get(0);
        ensure!(
            included,
            "Postgres CDC publication '{publication}' does not include table '{}.{}'",
            upstream.schema(),
            upstream.table()
        );
    }

    Ok(())
}

async fn validate_upstream_table_schema(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
) -> Result<()> {
    let upstream = schema.upstream_table();
    let column_rows = transaction
        .query(
            "SELECT column_name, is_nullable, data_type, udt_name, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres table schema for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?;
    ensure!(
        !column_rows.is_empty(),
        "Postgres CDC table '{}.{}' does not exist or has no columns",
        upstream.schema(),
        upstream.table()
    );

    let mut columns = HashMap::new();
    for row in column_rows {
        let name: String = row.get("column_name");
        let is_nullable: String = row.get("is_nullable");
        let data_type: String = row.get("data_type");
        let udt_name: String = row.get("udt_name");
        let numeric_precision: Option<i32> = row.get("numeric_precision");
        let numeric_scale: Option<i32> = row.get("numeric_scale");
        columns.insert(
            name,
            (
                is_nullable == "YES",
                data_type,
                udt_name,
                numeric_precision,
                numeric_scale,
            ),
        );
    }

    for column in schema.columns() {
        let Some((nullable, data_type, udt_name, numeric_precision, numeric_scale)) =
            columns.get(column.name())
        else {
            bail!(
                "Postgres CDC table '{}.{}' is missing configured column '{}'",
                upstream.schema(),
                upstream.table(),
                column.name()
            );
        };
        ensure!(
            column.nullable() || !nullable,
            "Postgres CDC column '{}.{}' is nullable but Floe table column '{}' is NOT NULL",
            upstream.schema(),
            upstream.table(),
            column.name()
        );
        ensure!(
            postgres_type_compatible(
                column.data_type(),
                udt_name,
                data_type,
                *numeric_precision,
                *numeric_scale
            ),
            "Postgres CDC column '{}.{}' type '{}' is not compatible with Floe type {:?}",
            upstream.schema(),
            upstream.table(),
            udt_name,
            column.data_type()
        );
    }

    let primary_key = discover_primary_key(transaction, upstream).await?;
    ensure!(
        primary_key == schema.primary_key().columns(),
        "Postgres CDC table '{}.{}' primary key {:?} does not match Floe primary key {:?}",
        upstream.schema(),
        upstream.table(),
        primary_key,
        schema.primary_key().columns()
    );

    let replica_identity: String = transaction
        .query_one(
            "SELECT c.relreplident::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres replica identity for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?
        .get(0);
    ensure!(
        replica_identity != "n",
        "Postgres CDC table '{}.{}' has REPLICA IDENTITY NOTHING",
        upstream.schema(),
        upstream.table()
    );

    Ok(())
}

pub(super) async fn discover_postgres_cdc_table_schema(
    connection_string: &str,
    table_id: CdcTableId,
    upstream: UpstreamTableRef,
) -> Result<CdcTableSchema> {
    let (mut client, connection) =
        tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .context("connect Postgres control plane for CDC schema discovery")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres CDC schema discovery connection closed");
        }
    });

    let result = async {
        let transaction = client
            .transaction()
            .await
            .context("begin Postgres CDC schema discovery transaction")?;
        let schema =
            discover_postgres_cdc_table_schema_from_transaction(&transaction, table_id, upstream)
                .await?;
        transaction
            .commit()
            .await
            .context("commit Postgres CDC schema discovery transaction")?;
        Ok(schema)
    }
    .await;
    drop(client);
    connection_task.abort();
    result
}

async fn discover_postgres_cdc_table_schema_from_transaction(
    transaction: &tokio_postgres::Transaction<'_>,
    table_id: CdcTableId,
    upstream: UpstreamTableRef,
) -> Result<CdcTableSchema> {
    let rows = transaction
        .query(
            "SELECT column_name, is_nullable, data_type, udt_name, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres table schema for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?;
    ensure!(
        !rows.is_empty(),
        "Postgres CDC table '{}.{}' does not exist or has no columns",
        upstream.schema(),
        upstream.table()
    );

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get("column_name");
        let is_nullable: String = row.get("is_nullable");
        let data_type: String = row.get("data_type");
        let udt_name: String = row.get("udt_name");
        let numeric_precision: Option<i32> = row.get("numeric_precision");
        let numeric_scale: Option<i32> = row.get("numeric_scale");
        columns.push(CdcColumn::new(
            name,
            postgres_column_type(&udt_name, &data_type, numeric_precision, numeric_scale)?,
            is_nullable == "YES",
        )?);
    }

    let primary_key = discover_primary_key(transaction, &upstream).await?;
    let replica_identity: String = transaction
        .query_one(
            "SELECT c.relreplident::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres replica identity for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?
        .get(0);
    ensure!(
        replica_identity != "n",
        "Postgres CDC table '{}.{}' has REPLICA IDENTITY NOTHING",
        upstream.schema(),
        upstream.table()
    );

    CdcTableSchema::new(
        table_id,
        upstream,
        columns,
        CdcPrimaryKey::new(primary_key)?,
    )
}

async fn discover_primary_key(
    transaction: &tokio_postgres::Transaction<'_>,
    upstream: &UpstreamTableRef,
) -> Result<Vec<String>> {
    let rows = transaction
        .query(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
             WHERE n.nspname = $1
               AND c.relname = $2
               AND i.indisprimary
             ORDER BY k.ord",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres primary key for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?;
    ensure!(
        !rows.is_empty(),
        "Postgres CDC table '{}.{}' must have a primary key",
        upstream.schema(),
        upstream.table()
    );
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

fn postgres_column_type(
    udt_name: &str,
    data_type: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> Result<ColumnType> {
    let udt_name = udt_name.to_ascii_lowercase();
    let data_type = data_type.to_ascii_lowercase();
    match udt_name.as_str() {
        "int8" | "int4" | "int2" => Ok(ColumnType::Int64),
        "bool" => Ok(ColumnType::Bool),
        "text" | "varchar" | "bpchar" | "name" => Ok(ColumnType::Utf8),
        "timestamp" | "timestamptz" => Ok(ColumnType::TimestampMillis),
        "date" => Ok(ColumnType::DateDays),
        "numeric" => decimal128_type_from_precision_scale(numeric_precision, numeric_scale)
            .unwrap_or(Ok(ColumnType::Numeric)),
        _ if matches!(
            data_type.as_str(),
            "timestamp without time zone" | "timestamp with time zone"
        ) =>
        {
            Ok(ColumnType::TimestampMillis)
        }
        _ => bail!(
            "unsupported Postgres CDC column type '{}' ({}) for schema discovery",
            udt_name,
            data_type
        ),
    }
}

fn postgres_type_compatible(
    expected: &ColumnType,
    udt_name: &str,
    data_type: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> bool {
    let udt_name = udt_name.to_ascii_lowercase();
    let data_type = data_type.to_ascii_lowercase();
    match expected {
        ColumnType::Int64 => matches!(udt_name.as_str(), "int8" | "int4" | "int2"),
        ColumnType::Bool => udt_name == "bool",
        ColumnType::Utf8 => matches!(udt_name.as_str(), "text" | "varchar" | "bpchar" | "name"),
        ColumnType::TimestampMillis => {
            matches!(udt_name.as_str(), "timestamp" | "timestamptz")
                || matches!(
                    data_type.as_str(),
                    "timestamp without time zone" | "timestamp with time zone"
                )
        }
        ColumnType::DateDays => udt_name == "date" || data_type == "date",
        ColumnType::Decimal128 { precision, scale } => {
            (udt_name == "numeric" || matches!(data_type.as_str(), "numeric" | "decimal"))
                && numeric_precision == Some(i32::from(*precision))
                && numeric_scale == Some(i32::from(*scale))
        }
        ColumnType::Numeric => {
            udt_name == "numeric" || matches!(data_type.as_str(), "numeric" | "decimal")
        }
    }
}

fn decimal128_type_from_precision_scale(
    precision: Option<i32>,
    scale: Option<i32>,
) -> Option<Result<ColumnType>> {
    let (Some(precision), Some(scale)) = (precision, scale) else {
        return None;
    };
    if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
        return None;
    }
    Some(ColumnType::decimal128(
        precision as u8,
        i8::try_from(scale).expect("scale <= 38 fits i8"),
    ))
}

struct SnapshotTableChangeBatches {
    change_batches: Vec<ChangeBatch>,
    row_count: usize,
}

async fn snapshot_table_chunks(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
) -> Result<Vec<SnapshotTableChunk>> {
    let requested_chunks = *POSTGRES_SNAPSHOT_INTRA_TABLE_CHUNKS;
    if requested_chunks <= 1 {
        return Ok(vec![SnapshotTableChunk::Full]);
    }

    let Some(key_column) = single_int64_primary_key_column(schema) else {
        tracing::debug!(
            table = %schema.table_id().as_str(),
            upstream_schema = %schema.upstream_table().schema(),
            upstream_table = %schema.upstream_table().table(),
            requested_chunks,
            "Postgres CDC snapshot intra-table chunking skipped because the primary key is not a single Int64 column"
        );
        return Ok(vec![SnapshotTableChunk::Full]);
    };

    let Some((min_key, max_key)) =
        snapshot_int64_primary_key_bounds(transaction, schema, key_column.name()).await?
    else {
        return Ok(vec![SnapshotTableChunk::Full]);
    };

    Ok(int64_snapshot_range_chunks(
        key_column.name(),
        min_key,
        max_key,
        requested_chunks,
    ))
}

async fn snapshot_int64_primary_key_bounds(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
    key_column: &str,
) -> Result<Option<(i64, i64)>> {
    let quoted_key = quote_pg_ident(key_column);
    let query = format!(
        "SELECT min({quoted_key})::bigint, max({quoted_key})::bigint FROM {}",
        qualified_table_name(schema.upstream_table())
    );
    let row = transaction.query_one(&query, &[]).await.with_context(|| {
        format!(
            "discover Postgres CDC snapshot key bounds for '{}.{}'",
            schema.upstream_table().schema(),
            schema.upstream_table().table()
        )
    })?;
    let min_key: Option<i64> = row.get(0);
    let max_key: Option<i64> = row.get(1);
    Ok(min_key.zip(max_key))
}

fn single_int64_primary_key_column(schema: &CdcTableSchema) -> Option<&CdcColumn> {
    let [primary_key_column] = schema.primary_key().columns() else {
        return None;
    };
    let column_idx = schema.column_index(primary_key_column)?;
    let column = &schema.columns()[column_idx];
    (column.data_type() == &ColumnType::Int64).then_some(column)
}

fn int64_snapshot_range_chunks(
    column: &str,
    min_key: i64,
    max_key: i64,
    requested_chunks: usize,
) -> Vec<SnapshotTableChunk> {
    if requested_chunks <= 1 || min_key >= max_key {
        return vec![SnapshotTableChunk::Full];
    }

    let value_count = i128::from(max_key) - i128::from(min_key) + 1;
    let chunk_count = (requested_chunks as i128).min(value_count).max(1);
    if chunk_count <= 1 {
        return vec![SnapshotTableChunk::Full];
    }

    let width = (value_count + chunk_count - 1) / chunk_count;
    let mut chunks = Vec::with_capacity(usize::try_from(chunk_count).unwrap_or(usize::MAX));
    for idx in 0..chunk_count {
        let lower = i128::from(min_key) + idx * width;
        if lower > i128::from(max_key) {
            break;
        }
        let next = lower + width;
        let upper_exclusive = (next <= i128::from(max_key))
            .then(|| i64::try_from(next).expect("chunk upper bound remains in i64 range"));
        chunks.push(SnapshotTableChunk::Int64Range {
            column: column.to_string(),
            lower_inclusive: i64::try_from(lower).expect("chunk lower bound remains in i64 range"),
            upper_exclusive,
        });
    }
    chunks
}

async fn snapshot_table_change_batches(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
) -> Result<SnapshotTableChangeBatches> {
    let chunk = SnapshotTableChunk::Full;
    snapshot_table_change_batches_for_chunk(transaction, schema, &chunk).await
}

async fn snapshot_table_change_batches_for_chunk(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
    chunk: &SnapshotTableChunk,
) -> Result<SnapshotTableChangeBatches> {
    let query = snapshot_table_query(schema, chunk);
    let started_at = Instant::now();
    let params = std::iter::empty::<&(dyn ToSql + Sync)>();
    let stream = transaction
        .query_raw(&query, params)
        .await
        .with_context(|| {
            format!(
                "snapshot Postgres CDC table '{}.{}'",
                schema.upstream_table().schema(),
                schema.upstream_table().table()
            )
        })?;
    pin_mut!(stream);

    let mut change_batches = Vec::new();
    let mut row_count = 0_usize;
    let rows_per_batch = *POSTGRES_SNAPSHOT_ROWS_PER_BATCH;
    let mut builder = SnapshotColumnarBatchBuilder::new(schema, rows_per_batch);
    while let Some(row) = stream.try_next().await.with_context(|| {
        format!(
            "stream Postgres CDC snapshot table '{}.{}'",
            schema.upstream_table().schema(),
            schema.upstream_table().table()
        )
    })? {
        builder.append_row(schema, &row)?;
        row_count = row_count.saturating_add(1);
        if builder.len() >= rows_per_batch {
            change_batches.push(builder.finish_change_batch(schema)?);
        }
    }
    if !builder.is_empty() {
        change_batches.push(builder.finish_change_batch(schema)?);
    }
    if *CDC_PERF_LOGGING_ENABLED {
        let elapsed = started_at.elapsed();
        tracing::info!(
            table = %schema.table_id().as_str(),
            upstream_schema = %schema.upstream_table().schema(),
            upstream_table = %schema.upstream_table().table(),
            chunk = ?chunk,
            rows = row_count,
            batches = change_batches.len(),
            rows_per_batch,
            elapsed_ms = elapsed.as_millis() as u64,
            rows_per_second = (row_count as f64 / elapsed.as_secs_f64().max(0.001)) as u64,
            "postgres cdc snapshot table streamed"
        );
    }

    Ok(SnapshotTableChangeBatches {
        change_batches,
        row_count,
    })
}

async fn snapshot_table_change_batches_from_exported_snapshot(
    connection_string: &str,
    exported_snapshot: &str,
    schema: &CdcTableSchema,
    chunk: &SnapshotTableChunk,
    worker_control: Option<SnapshotWorkerControl>,
) -> Result<SnapshotTableChangeBatches> {
    let (mut client, connection) =
        tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .with_context(|| {
                format!(
                    "connect Postgres snapshot worker for '{}.{}'",
                    schema.upstream_table().schema(),
                    schema.upstream_table().table()
                )
            })?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres snapshot worker connection closed");
        }
    });

    let result = async {
        let transaction = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .context("begin Postgres snapshot worker transaction")?;
        bind_transaction_to_exported_snapshot(&transaction, exported_snapshot).await?;
        let scan_permit = if let Some(control) = worker_control {
            let SnapshotWorkerControl {
                ready_tx,
                mut start_rx,
                scan_limiter,
                scan_observation_tx,
            } = control;
            let _ = ready_tx.send(());
            wait_for_snapshot_worker_start(&mut start_rx).await?;
            Some((scan_limiter.acquire().await, scan_observation_tx))
        } else {
            None
        };
        let scan_started_at = Instant::now();
        let snapshot = snapshot_table_change_batches_for_chunk(&transaction, schema, chunk).await;
        if let (Some((_, Some(scan_observation_tx))), Ok(snapshot)) = (&scan_permit, &snapshot) {
            let _ = scan_observation_tx.send(SnapshotScanObservation {
                elapsed_ms: scan_started_at
                    .elapsed()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                rows: snapshot.row_count,
            });
        }
        transaction
            .commit()
            .await
            .context("commit Postgres snapshot worker transaction")?;
        snapshot
    }
    .await;

    drop(client);
    connection_task.abort();
    result
}

async fn wait_for_snapshot_worker_start(start_rx: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *start_rx.borrow_and_update() {
            return Ok(());
        }
        start_rx
            .changed()
            .await
            .context("Postgres snapshot worker start channel closed before WAL stream started")?;
    }
}

async fn bind_transaction_to_exported_snapshot(
    transaction: &tokio_postgres::Transaction<'_>,
    exported_snapshot: &str,
) -> Result<()> {
    transaction
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT {}",
            quote_pg_literal(exported_snapshot)
        ))
        .await
        .context("bind Postgres transaction to exported snapshot")
}

fn snapshot_table_query(schema: &CdcTableSchema, chunk: &SnapshotTableChunk) -> String {
    let select_list = schema
        .columns()
        .iter()
        .map(snapshot_select_expr)
        .collect::<Vec<_>>()
        .join(", ");
    let base = format!(
        "SELECT {select_list} FROM {}",
        qualified_table_name(schema.upstream_table())
    );
    match chunk {
        SnapshotTableChunk::Full => base,
        SnapshotTableChunk::Int64Range {
            column,
            lower_inclusive,
            upper_exclusive,
        } => {
            let quoted_column = quote_pg_ident(column);
            let upper = upper_exclusive
                .map(|upper| format!(" AND {quoted_column} < {upper}"))
                .unwrap_or_default();
            format!("{base} WHERE {quoted_column} >= {lower_inclusive}{upper}")
        }
    }
}

fn snapshot_select_expr(column: &CdcColumn) -> String {
    let quoted = quote_pg_ident(column.name());
    match column.data_type() {
        ColumnType::TimestampMillis => {
            format!("floor(extract(epoch from {quoted}) * 1000)::bigint AS {quoted}")
        }
        ColumnType::DateDays => format!("({quoted} - DATE '1970-01-01')::int AS {quoted}"),
        ColumnType::Decimal128 { .. } => format!("{quoted}::text AS {quoted}"),
        ColumnType::Numeric => format!("{quoted}::text AS {quoted}"),
        ColumnType::Int64 | ColumnType::Bool | ColumnType::Utf8 => quoted,
    }
}

struct SnapshotColumnarBatchBuilder {
    columns: Vec<SnapshotColumnBuilder>,
    len: usize,
    capacity: usize,
}

impl SnapshotColumnarBatchBuilder {
    fn new(schema: &CdcTableSchema, capacity: usize) -> Self {
        Self {
            columns: schema
                .columns()
                .iter()
                .map(|column| SnapshotColumnBuilder::new(column.data_type(), capacity))
                .collect(),
            len: 0,
            capacity,
        }
    }

    fn append_row(&mut self, schema: &CdcTableSchema, row: &tokio_postgres::Row) -> Result<()> {
        ensure!(
            row.columns().len() == schema.columns().len(),
            "Postgres CDC snapshot row has {} columns, expected {}",
            row.columns().len(),
            schema.columns().len()
        );
        for ((builder, column), idx) in self
            .columns
            .iter_mut()
            .zip(schema.columns())
            .zip(0..schema.columns().len())
        {
            builder.append(row, idx, column)?;
        }
        self.len += 1;
        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish_change_batch(&mut self, schema: &CdcTableSchema) -> Result<ChangeBatch> {
        let columns = std::mem::take(&mut self.columns)
            .into_iter()
            .map(SnapshotColumnBuilder::finish)
            .collect::<Vec<_>>();
        let rows = CdcColumnarRowBatch::new(columns)?;
        schema.validate_columnar_rows(&rows)?;
        self.columns = schema
            .columns()
            .iter()
            .map(|column| SnapshotColumnBuilder::new(column.data_type(), self.capacity))
            .collect();
        self.len = 0;
        ChangeBatch::new_snapshot_insert(schema.table_id().clone(), rows)
    }
}

enum SnapshotColumnBuilder {
    Int64(Vec<Option<i64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<String>>),
    TimestampMillis(Vec<Option<i64>>),
    DateDays(Vec<Option<i32>>),
    Decimal128 {
        precision: u8,
        scale: i8,
        values: Vec<Option<i128>>,
    },
    Numeric(Vec<Option<String>>),
}

impl SnapshotColumnBuilder {
    fn new(data_type: &ColumnType, capacity: usize) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(Vec::with_capacity(capacity)),
            ColumnType::Bool => Self::Bool(Vec::with_capacity(capacity)),
            ColumnType::Utf8 => Self::Utf8(Vec::with_capacity(capacity)),
            ColumnType::TimestampMillis => Self::TimestampMillis(Vec::with_capacity(capacity)),
            ColumnType::DateDays => Self::DateDays(Vec::with_capacity(capacity)),
            ColumnType::Decimal128 { precision, scale } => Self::Decimal128 {
                precision: *precision,
                scale: *scale,
                values: Vec::with_capacity(capacity),
            },
            ColumnType::Numeric => Self::Numeric(Vec::with_capacity(capacity)),
        }
    }

    fn append(&mut self, row: &tokio_postgres::Row, idx: usize, column: &CdcColumn) -> Result<()> {
        match (self, column.data_type()) {
            (Self::Int64(values), ColumnType::Int64) => {
                values.push(snapshot_int64_value(row, idx, row.columns()[idx].type_())?);
            }
            (Self::Bool(values), ColumnType::Bool) => {
                values.push(row.try_get::<_, Option<bool>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot bool '{}'", column.name())
                })?);
            }
            (Self::Utf8(values), ColumnType::Utf8) => {
                values.push(row.try_get::<_, Option<String>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot text '{}'", column.name())
                })?);
            }
            (Self::TimestampMillis(values), ColumnType::TimestampMillis) => {
                values.push(row.try_get::<_, Option<i64>>(idx).with_context(|| {
                    format!(
                        "decode Postgres CDC snapshot timestamp millis '{}'",
                        column.name()
                    )
                })?);
            }
            (Self::DateDays(values), ColumnType::DateDays) => {
                values.push(row.try_get::<_, Option<i32>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot date days '{}'", column.name())
                })?);
            }
            (Self::Decimal128 { scale, values, .. }, ColumnType::Decimal128 { .. }) => {
                let value = row.try_get::<_, Option<String>>(idx).with_context(|| {
                    format!(
                        "decode Postgres CDC snapshot decimal128 '{}'",
                        column.name()
                    )
                })?;
                values.push(
                    value
                        .as_deref()
                        .map(|value| parse_decimal_text_to_i128(value, *scale))
                        .transpose()?,
                );
            }
            (Self::Numeric(values), ColumnType::Numeric) => {
                values.push(row.try_get::<_, Option<String>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot numeric '{}'", column.name())
                })?);
            }
            _ => bail!(
                "Postgres CDC snapshot builder for column '{}' does not match type {:?}",
                column.name(),
                column.data_type()
            ),
        }
        Ok(())
    }

    fn finish(self) -> CdcColumnarColumn {
        match self {
            Self::Int64(values) => CdcColumnarColumn::Int64(values),
            Self::Bool(values) => CdcColumnarColumn::Bool(values),
            Self::Utf8(values) => CdcColumnarColumn::Utf8(values),
            Self::TimestampMillis(values) => CdcColumnarColumn::TimestampMillis(values),
            Self::DateDays(values) => CdcColumnarColumn::DateDays(values),
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => CdcColumnarColumn::Decimal128 {
                precision,
                scale,
                values,
            },
            Self::Numeric(values) => CdcColumnarColumn::Numeric(values),
        }
    }
}

fn parse_decimal_text_to_i128(value: &str, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).context("Decimal128 scale cannot be negative")?;
    let value = value.trim();
    ensure!(!value.is_empty(), "decimal value cannot be empty");

    let (negative, digits) = value
        .strip_prefix('-')
        .map(|rest| (true, rest))
        .unwrap_or_else(|| {
            value
                .strip_prefix('+')
                .map(|rest| (false, rest))
                .unwrap_or((false, value))
        });

    let mut parsed = 0_i128;
    let mut saw_digit = false;
    let mut saw_decimal = false;
    let mut fraction_len = 0_usize;
    let scale_usize = usize::try_from(scale).expect("u32 scale fits usize");

    for byte in digits.bytes() {
        match byte {
            b'0'..=b'9' => {
                saw_digit = true;
                if saw_decimal {
                    fraction_len = fraction_len.saturating_add(1);
                    ensure!(
                        fraction_len <= scale_usize,
                        "decimal value '{value}' has more fractional digits than scale {scale}"
                    );
                }
                parsed = parsed
                    .checked_mul(10)
                    .and_then(|acc| acc.checked_add(i128::from(byte - b'0')))
                    .with_context(|| format!("decimal value '{value}' exceeds i128 range"))?;
            }
            b'.' if !saw_decimal => {
                saw_decimal = true;
            }
            _ => bail!("invalid decimal value '{value}'"),
        }
    }

    ensure!(saw_digit, "decimal value '{value}' has no digits");
    for _ in 0..scale_usize.saturating_sub(fraction_len) {
        parsed = parsed
            .checked_mul(10)
            .with_context(|| format!("decimal value '{value}' exceeds i128 range"))?;
    }

    Ok(if negative { -parsed } else { parsed })
}

fn snapshot_int64_value(
    row: &tokio_postgres::Row,
    idx: usize,
    postgres_type: &Type,
) -> Result<Option<i64>> {
    match *postgres_type {
        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .context("decode Postgres CDC snapshot int8"),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .context("decode Postgres CDC snapshot int4")
            .map(|value| value.map(i64::from)),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .context("decode Postgres CDC snapshot int2")
            .map(|value| value.map(i64::from)),
        _ => bail!("unsupported Postgres integer snapshot type {postgres_type}"),
    }
}

fn snapshot_checkpoint(source_id: &CdcSourceId, lsn: PostgresLsn) -> Result<CdcCheckpoint> {
    Ok(CdcCheckpoint::new(
        source_id.clone(),
        lsn.to_source_position()?,
        Some(snapshot_transaction_id(lsn)?),
    ))
}

fn snapshot_transaction_id(lsn: PostgresLsn) -> Result<CdcTransactionId> {
    CdcTransactionId::new(format!("snapshot:{}", lsn.to_pg_string()))
}

async fn wait_for_postgres_snapshot_commit(
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    target_lsn: PostgresLsn,
    cancel: &CancellationToken,
) -> Result<()> {
    let Some(receiver) = receiver else {
        bail!("cannot wait for initial Postgres snapshot durability without commit receiver");
    };

    loop {
        let commit = receiver.borrow_and_update().clone();
        if postgres_commit_covers_lsn(&commit, slot, target_lsn)? {
            return Ok(());
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                bail!("cancelled while waiting for initial Postgres snapshot durability");
            }
            changed = receiver.changed() => {
                changed.context("Postgres CDC commit channel closed before initial snapshot became durable")?;
            }
        }
    }
}

fn postgres_commit_covers_lsn(
    commit: &PostgresCdcCommit,
    slot: &str,
    target_lsn: PostgresLsn,
) -> Result<bool> {
    let Some(slot_commit) = commit.slots.iter().find(|entry| entry.slot == slot) else {
        return Ok(false);
    };
    Ok(PostgresLsn::parse(&slot_commit.lsn)?.as_u64() >= target_lsn.as_u64())
}

fn qualified_table_name(upstream: &UpstreamTableRef) -> String {
    format!(
        "{}.{}",
        quote_pg_ident(upstream.schema()),
        quote_pg_ident(upstream.table())
    )
}

fn quote_pg_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use floe_cdc_core::{CdcColumn, CdcPrimaryKey, UpstreamTableRef};
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;
    use std::sync::Arc;

    #[test]
    fn parses_decimal_text_without_allocation_sensitive_edge_cases() {
        assert_eq!(parse_decimal_text_to_i128("123.45", 2).unwrap(), 12_345);
        assert_eq!(parse_decimal_text_to_i128("123", 2).unwrap(), 12_300);
        assert_eq!(parse_decimal_text_to_i128("-0.07", 2).unwrap(), -7);
        assert_eq!(parse_decimal_text_to_i128("+42.1", 3).unwrap(), 42_100);
        assert_eq!(parse_decimal_text_to_i128(" .5 ", 2).unwrap(), 50);
    }

    #[test]
    fn quotes_exported_snapshot_literal() {
        assert_eq!(
            quote_pg_literal("00000003-0000001B-1"),
            "'00000003-0000001B-1'"
        );
        assert_eq!(quote_pg_literal("snap'shot"), "'snap''shot'");
    }

    #[test]
    fn rejects_decimal_text_that_cannot_match_scale() {
        assert!(parse_decimal_text_to_i128("1.234", 2).is_err());
        assert!(parse_decimal_text_to_i128("1.2.3", 2).is_err());
        assert!(parse_decimal_text_to_i128("", 2).is_err());
        assert!(parse_decimal_text_to_i128("abc", 2).is_err());
        assert!(parse_decimal_text_to_i128("1.0", -1).is_err());
    }

    #[test]
    fn int64_primary_key_chunks_cover_range_without_overlap() {
        let chunks = int64_snapshot_range_chunks("id", 1, 10, 3);

        assert_eq!(
            chunks,
            vec![
                SnapshotTableChunk::Int64Range {
                    column: "id".to_string(),
                    lower_inclusive: 1,
                    upper_exclusive: Some(5),
                },
                SnapshotTableChunk::Int64Range {
                    column: "id".to_string(),
                    lower_inclusive: 5,
                    upper_exclusive: Some(9),
                },
                SnapshotTableChunk::Int64Range {
                    column: "id".to_string(),
                    lower_inclusive: 9,
                    upper_exclusive: None,
                },
            ]
        );
        assert_eq!(
            snapshot_table_query(&snapshot_test_schema(), &chunks[0]),
            r#"SELECT "id", "status" FROM "public"."orders" WHERE "id" >= 1 AND "id" < 5"#
        );
        assert_eq!(
            snapshot_table_query(&snapshot_test_schema(), &chunks[2]),
            r#"SELECT "id", "status" FROM "public"."orders" WHERE "id" >= 9"#
        );
    }

    #[test]
    fn snapshot_chunking_requires_single_int64_primary_key() {
        let int64_schema = snapshot_test_schema();
        assert_eq!(
            single_int64_primary_key_column(&int64_schema).map(CdcColumn::name),
            Some("id")
        );

        let text_pk_schema = CdcTableSchema::new(
            CdcTableId::new("orders_by_status").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
                CdcColumn::new("status", ColumnType::Utf8, false).expect("status"),
            ],
            CdcPrimaryKey::new(["status"]).expect("primary key"),
        )
        .expect("schema");
        assert!(single_int64_primary_key_column(&text_pk_schema).is_none());

        let composite_schema = CdcTableSchema::new(
            CdcTableId::new("orders_composite").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
                CdcColumn::new("status", ColumnType::Utf8, false).expect("status"),
            ],
            CdcPrimaryKey::new(["id", "status"]).expect("primary key"),
        )
        .expect("schema");
        assert!(single_int64_primary_key_column(&composite_schema).is_none());
    }

    #[test]
    fn adaptive_snapshot_concurrency_decision_uses_wal_and_scan_pressure() {
        let config = SnapshotAdaptiveConcurrencyConfig {
            enabled: true,
            min_workers: 1,
            max_workers: 4,
            wal_buffer_high_watermark_percent: 75,
            wal_buffer_low_watermark_percent: 25,
            slow_scan_ms: 1_000,
            controller_interval: Duration::from_millis(500),
        };

        assert_eq!(
            snapshot_concurrency_decision(
                config,
                4,
                SnapshotWalBufferPressure {
                    pending_events: 8,
                    capacity: 10,
                },
                None,
            ),
            Some(SnapshotConcurrencyDecision {
                target_workers: 3,
                direction: "decrease",
                reason: "wal_buffer_high",
            })
        );
        assert_eq!(
            snapshot_concurrency_decision(
                config,
                3,
                SnapshotWalBufferPressure {
                    pending_events: 1,
                    capacity: 10,
                },
                Some(SnapshotScanObservation {
                    elapsed_ms: 2_000,
                    rows: 10,
                }),
            ),
            Some(SnapshotConcurrencyDecision {
                target_workers: 2,
                direction: "decrease",
                reason: "snapshot_scan_slow",
            })
        );
        assert_eq!(
            snapshot_concurrency_decision(
                config,
                2,
                SnapshotWalBufferPressure {
                    pending_events: 1,
                    capacity: 10,
                },
                None,
            ),
            Some(SnapshotConcurrencyDecision {
                target_workers: 3,
                direction: "increase",
                reason: "wal_buffer_low",
            })
        );
    }

    #[tokio::test]
    async fn snapshot_scan_limiter_respects_dynamic_target() {
        let limiter = Arc::new(SnapshotScanLimiter::new("pg_test", "slot_test", 2));
        let first_permit = limiter.acquire().await;
        let second_permit = limiter.acquire().await;
        assert_eq!(limiter.active_workers(), 2);
        assert_eq!(limiter.set_target(1), Some((2, 1)));

        let acquire_waiter = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move { limiter.acquire().await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!acquire_waiter.is_finished());

        drop(first_permit);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!acquire_waiter.is_finished());

        drop(second_permit);
        let third_permit = tokio::time::timeout(Duration::from_secs(1), acquire_waiter)
            .await
            .expect("scan permit acquisition should resume")
            .expect("scan permit task should succeed");
        assert_eq!(limiter.active_workers(), 1);
        drop(third_permit);
        assert_eq!(limiter.active_workers(), 0);
    }

    #[tokio::test]
    async fn cancelled_snapshot_before_commit_leaves_no_checkpoint_for_retry() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_id = CdcTableId::new("orders").expect("table id");
        let catalog = floe_storage::SlateCatalog::in_memory()
            .await
            .expect("catalog");
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(catalog.db()));
        let table_store = CdcTableStore::new(table);
        let runtime_plan = PostgresCdcRuntimePlan {
            source_id: source_id.clone(),
            schemas: HashMap::new(),
            schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
            replication_pipelines: Vec::new(),
        };
        let lsn = PostgresLsn::from_u64(120);
        let snapshot = PostgresSnapshot {
            lsn,
            transaction: snapshot_transaction_batch(
                &source_id,
                lsn,
                vec![
                    ChangeBatch::new(
                        table_id,
                        vec![CdcChange::Insert {
                            row: floe_cdc_core::CdcRow::new([
                                Some(RowValue::Int64(1)),
                                Some(RowValue::Utf8("snapshot".to_string())),
                            ])
                            .expect("row"),
                        }],
                    )
                    .expect("snapshot change batch"),
                ],
            )
            .expect("snapshot transaction"),
            row_count: 1,
            wal_stream: None,
        };
        let (sender, mut receiver) = mpsc::channel(1);
        let (_commit_sender, mut commit_receiver) = watch::channel(PostgresCdcCommit::default());
        let cancel = CancellationToken::new();
        cancel.cancel();

        let err = finish_loaded_postgres_snapshot(
            "slot",
            "publication",
            &runtime_plan,
            &table_store,
            &sender,
            Some(&mut commit_receiver),
            &cancel,
            snapshot,
        )
        .await
        .expect_err("cancelled snapshot should not finish");

        assert!(
            format!("{err:#}").contains("cancelled while waiting for initial Postgres snapshot")
        );
        let queued = receiver.recv().await.expect("queued snapshot transaction");
        assert_eq!(queued.slot, "slot");
        assert_eq!(queued.source_id, source_id);
        assert_eq!(
            queued
                .transaction
                .transaction_id()
                .map(CdcTransactionId::as_str),
            Some("snapshot:0/78")
        );
        assert_eq!(
            table_store
                .load_checkpoint(&queued.source_id)
                .await
                .expect("load checkpoint"),
            None
        );
        assert!(
            receiver.try_recv().is_err(),
            "cancelled snapshot finalization should enqueue at most one retryable snapshot transaction"
        );
    }

    fn snapshot_test_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
                CdcColumn::new("status", ColumnType::Utf8, true).expect("status"),
            ],
            CdcPrimaryKey::new(["id"]).expect("primary key"),
        )
        .expect("schema")
    }
}
