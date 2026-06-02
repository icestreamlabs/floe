use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use dbsp::LogicalWorkSnapshot;
use dbsp::RowSchema;
use dbsp::StreamRetention;
use dbsp::collections::CompactionPolicy;
use dbsp::handles::ZSetHandle;
use dbsp::stream::util::DeltaZSetHandleReader;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use futures::future::BoxFuture;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, DbspView};
use crate::metrics;
use crate::mv::registry::{DbspPersistedState, MaterializedViewHandle, MaterializedViewRegistry};
use crate::outer_stream::{TransientSourceBatch, TransientSourceHandleStream};
use crate::stream_types::EncodedDeltaBatch;
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::{DbspGraphBuilder, MvFlushCoalescingConfig, OverlaySnapshotConfig};

static MV_UPDATE_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_UPDATE_LOG_SAMPLE_EVERY: u64 = 128;
static MV_OVERLAY_APPLY_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_OVERLAY_APPLY_LOG_SAMPLE_EVERY: u64 = 16;
static MV_OVERLAY_SNAPSHOT_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_OVERLAY_SNAPSHOT_LOG_SAMPLE_EVERY: u64 = 8;
static MV_OPTIMIZATION_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_OPTIMIZATION_LOG_SAMPLE_EVERY: u64 = 64;
const MV_OPTIMIZATION_LOG_MIN_TOTAL_MS: u64 = 250;

pub(super) type DeltaTransformFn =
    dyn Fn(EncodedDeltaBatch) -> BoxFuture<'static, Result<Vec<(Vec<u8>, i64)>>> + Send + Sync;

pub(crate) const TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY: usize = 1024;
const OVERLAY_SNAPSHOT_FLUSH_CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, Clone)]
pub(crate) struct TransientMaterializeBatch {
    pub version: i64,
    pub deltas: EncodedDeltaBatch,
    /// True when deltas are already coalesced by encoded row.
    pub deltas_consolidated: bool,
}

pub(crate) type TransientMaterializeReceiver = mpsc::Receiver<TransientMaterializeBatch>;
pub(crate) type TransientMaterializeSender = mpsc::Sender<TransientMaterializeBatch>;

impl From<TransientSourceBatch> for TransientMaterializeBatch {
    fn from(batch: TransientSourceBatch) -> Self {
        Self {
            version: batch.version,
            deltas: batch.deltas,
            deltas_consolidated: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FlushTrigger {
    MaxPendingDeltas,
    MaxPendingVersions,
    MaxPendingRows,
    MaxPendingBytes,
    MaxDelay,
    CatchupBoundary,
    Shutdown,
}

impl FlushTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::MaxPendingDeltas => "max_pending_deltas",
            Self::MaxPendingVersions => "max_pending_versions",
            Self::MaxPendingRows => "max_pending_rows",
            Self::MaxPendingBytes => "max_pending_bytes",
            Self::MaxDelay => "max_delay",
            Self::CatchupBoundary => "catchup_boundary",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Debug, Default)]
struct DeltaApplyStats {
    delta_rows: usize,
    delta_bytes: usize,
    load_ms: u64,
    transform_ms: u64,
    merge_ms: u64,
}

#[derive(Debug, Default)]
struct PendingMvFlush {
    pending_deltas: usize,
    pending_versions: usize,
    pending_rows: usize,
    pending_bytes: usize,
    total_load_ms: u64,
    total_transform_ms: u64,
    total_merge_ms: u64,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    first_enqueue_at: Option<Instant>,
}

impl PendingMvFlush {
    fn record(&mut self, ts: i64, apply: &DeltaApplyStats) {
        self.pending_deltas = self.pending_deltas.saturating_add(1);
        self.pending_versions = self.pending_versions.saturating_add(1);
        self.pending_rows = self.pending_rows.saturating_add(apply.delta_rows);
        self.pending_bytes = self.pending_bytes.saturating_add(apply.delta_bytes);
        self.total_load_ms = self.total_load_ms.saturating_add(apply.load_ms);
        self.total_transform_ms = self.total_transform_ms.saturating_add(apply.transform_ms);
        self.total_merge_ms = self.total_merge_ms.saturating_add(apply.merge_ms);
        if self.first_ts.is_none() {
            self.first_ts = Some(ts);
        }
        self.last_ts = Some(ts);
        if self.first_enqueue_at.is_none() {
            self.first_enqueue_at = Some(Instant::now());
        }
    }

    fn has_pending(&self) -> bool {
        self.pending_versions > 0
    }

    fn trigger(&self, cfg: MvFlushCoalescingConfig, now: Instant) -> Option<FlushTrigger> {
        if !self.has_pending() {
            return None;
        }
        if self.pending_deltas >= cfg.max_pending_deltas {
            return Some(FlushTrigger::MaxPendingDeltas);
        }
        if let Some(limit) = cfg.max_pending_versions
            && self.pending_versions >= limit
        {
            return Some(FlushTrigger::MaxPendingVersions);
        }
        if let Some(limit) = cfg.max_pending_rows
            && self.pending_rows >= limit
        {
            return Some(FlushTrigger::MaxPendingRows);
        }
        if let Some(limit) = cfg.max_pending_bytes
            && self.pending_bytes >= limit
        {
            return Some(FlushTrigger::MaxPendingBytes);
        }
        if let Some(delay_ms) = cfg.max_delay_ms
            && let Some(first_enqueue_at) = self.first_enqueue_at
            && now.duration_since(first_enqueue_at) >= Duration::from_millis(delay_ms)
        {
            return Some(FlushTrigger::MaxDelay);
        }
        None
    }

    fn delay_remaining(&self, cfg: MvFlushCoalescingConfig, now: Instant) -> Option<Duration> {
        if !self.has_pending() {
            return None;
        }
        let delay_ms = cfg.max_delay_ms?;
        let first_enqueue_at = self.first_enqueue_at?;
        let elapsed = now.duration_since(first_enqueue_at);
        let max_delay = Duration::from_millis(delay_ms);
        Some(max_delay.saturating_sub(elapsed))
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

struct FlushedBatch {
    published_ts: i64,
    handle: ZSetHandle,
    latency_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct HotspotSummary {
    phase: &'static str,
    phase_ms: u64,
    phase_share: f64,
}

fn summarize_hotspot(phases: &[(&'static str, u64)], total_ms: u64) -> Option<HotspotSummary> {
    if total_ms == 0 {
        return None;
    }
    let (phase, phase_ms) = phases.iter().max_by_key(|(_, ms)| *ms).copied()?;
    if phase_ms == 0 {
        return None;
    }
    Some(HotspotSummary {
        phase,
        phase_ms,
        phase_share: phase_ms as f64 / total_ms as f64,
    })
}

fn should_log_optimization_hotspot(total_ms: u64) -> bool {
    if total_ms >= MV_OPTIMIZATION_LOG_MIN_TOTAL_MS {
        return true;
    }
    MV_OPTIMIZATION_LOG_COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(MV_OPTIMIZATION_LOG_SAMPLE_EVERY)
}

#[derive(Debug, Default)]
struct PendingOverlaySnapshot {
    batches: usize,
    rows: usize,
    bytes: usize,
    first_version: Option<i64>,
    last_version: Option<i64>,
    first_enqueue_at: Option<Instant>,
    delta_batches: Vec<EncodedDeltaBatch>,
}

impl PendingOverlaySnapshot {
    fn record(&mut self, version: i64, deltas: EncodedDeltaBatch) {
        if deltas.is_empty() {
            return;
        }
        self.batches = self.batches.saturating_add(1);
        self.rows = self.rows.saturating_add(deltas.len());
        self.bytes = self.bytes.saturating_add(
            deltas
                .iter()
                .map(|(key, _)| key.len() + std::mem::size_of::<i64>())
                .sum::<usize>(),
        );
        if self.first_version.is_none() {
            self.first_version = Some(version);
        }
        self.last_version = Some(version);
        if self.first_enqueue_at.is_none() {
            self.first_enqueue_at = Some(Instant::now());
        }
        self.delta_batches.push(deltas);
    }

    fn has_pending(&self) -> bool {
        !self.delta_batches.is_empty()
    }

    fn should_flush(&self, config: OverlaySnapshotConfig, now: Instant) -> bool {
        if !self.has_pending() {
            return false;
        }
        if self.batches >= config.max_pending_batches || self.rows >= config.max_pending_rows {
            return true;
        }
        self.first_enqueue_at.is_some_and(|started| {
            now.duration_since(started) >= Duration::from_millis(config.max_delay_ms)
        })
    }

    fn delay_remaining(&self, config: OverlaySnapshotConfig, now: Instant) -> Option<Duration> {
        if !self.has_pending() {
            return None;
        }
        let first_enqueue_at = self.first_enqueue_at?;
        let elapsed = now.duration_since(first_enqueue_at);
        let max_delay = Duration::from_millis(config.max_delay_ms);
        Some(max_delay.saturating_sub(elapsed))
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn take_request(&mut self, reason: &'static str) -> Option<OverlaySnapshotFlushRequest> {
        if !self.has_pending() {
            return None;
        }
        let request = OverlaySnapshotFlushRequest {
            reason,
            batches: self.batches,
            rows: self.rows,
            bytes: self.bytes,
            first_version: self.first_version.unwrap_or(-1),
            last_version: self.last_version.unwrap_or(-1),
            delta_batches: std::mem::take(&mut self.delta_batches),
        };
        self.clear();
        Some(request)
    }
}

struct OverlaySnapshotFlushRequest {
    reason: &'static str,
    batches: usize,
    rows: usize,
    bytes: usize,
    first_version: i64,
    last_version: i64,
    delta_batches: Vec<EncodedDeltaBatch>,
}

fn into_owned_deltas(deltas: EncodedDeltaBatch) -> Vec<(Vec<u8>, i64)> {
    match Arc::try_unwrap(deltas) {
        Ok(deltas) => deltas,
        Err(deltas) => deltas.as_ref().clone(),
    }
}

mod core;
mod delta_overlay;
mod processing;
mod transient_overlay;
mod transient_source_overlay;

#[cfg(test)]
mod tests;
