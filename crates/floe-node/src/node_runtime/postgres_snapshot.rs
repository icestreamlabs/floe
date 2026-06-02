use super::*;

use anyhow::{Context, Result, anyhow, bail, ensure};
use floe_cdc_core::{
    CdcCheckpoint, CdcColumnarColumn, CdcColumnarRowBatch, CdcTransactionId, ChangeBatch,
    TransactionBatch,
};
use floe_config::PostgresCdcSnapshotConfig;
use futures::{TryStreamExt, pin_mut};
use std::time::Instant;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;

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
    scan_observation_tx: Option<watch::Sender<Option<SnapshotScanObservation>>>,
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
    scan_observation_tx: Option<watch::Sender<Option<SnapshotScanObservation>>>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotSinkHealth {
    Healthy,
    Backpressured,
    TargetError,
}

impl SnapshotSinkHealth {
    fn unhealthy_reason(self) -> Option<&'static str> {
        match self {
            Self::Healthy => None,
            Self::Backpressured => Some("sink_backpressure"),
            Self::TargetError => Some("sink_error"),
        }
    }
}

struct SnapshotTableChangeBatches {
    change_batches: Vec<ChangeBatch>,
    row_count: usize,
}

#[path = "postgres_snapshot/adaptive_concurrency.rs"]
mod adaptive_concurrency;
#[path = "postgres_snapshot/commit_utils.rs"]
mod commit_utils;
#[path = "postgres_snapshot/publication.rs"]
mod publication;
#[path = "postgres_snapshot/schema.rs"]
mod schema;
#[path = "postgres_snapshot/snapshot_load.rs"]
mod snapshot_load;
#[path = "postgres_snapshot/table_scan.rs"]
mod table_scan;
#[cfg(test)]
#[path = "postgres_snapshot/tests.rs"]
mod tests;

pub(super) use self::commit_utils::wait_for_postgres_cdc_commit;
pub(super) use self::publication::ensure_postgres_cdc_publication_and_slot;
pub(super) use self::schema::discover_postgres_cdc_table_schema;
pub(super) use self::snapshot_load::run_initial_postgres_snapshot_if_needed;

use self::commit_utils::*;
use self::schema::*;
use self::snapshot_load::sorted_snapshot_schemas;
use self::table_scan::*;
