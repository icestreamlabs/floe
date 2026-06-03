use super::*;

impl SnapshotScanLimiter {
    pub(super) fn new(
        source: impl Into<String>,
        slot: impl Into<String>,
        max_workers: usize,
    ) -> Self {
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

    pub(super) async fn acquire(self: &Arc<Self>) -> SnapshotScanPermit {
        loop {
            if let Some(permit) = self.try_acquire() {
                return permit;
            }
            self.notify.notified().await;
        }
    }

    pub(super) fn try_acquire(self: &Arc<Self>) -> Option<SnapshotScanPermit> {
        let active = self.active_workers.load(Ordering::Acquire);
        let target = self.target_workers.load(Ordering::Acquire).max(1);
        if active < target
            && self
                .active_workers
                .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.record_metrics();
            Some(SnapshotScanPermit {
                limiter: Arc::clone(self),
            })
        } else {
            None
        }
    }

    pub(super) fn set_target(&self, target_workers: usize) -> Option<(usize, usize)> {
        let target_workers = target_workers.clamp(1, self.max_workers);
        let previous = self
            .target_workers
            .swap(target_workers, Ordering::AcqRel)
            .clamp(1, self.max_workers);
        self.record_metrics();
        self.notify.notify_waiters();
        (previous != target_workers).then_some((previous, target_workers))
    }

    pub(super) fn target_workers(&self) -> usize {
        self.target_workers
            .load(Ordering::Acquire)
            .clamp(1, self.max_workers)
    }

    pub(super) fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Acquire)
    }

    pub(super) fn record_metrics(&self) {
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
    pub(super) fn new(
        source_id: &CdcSourceId,
        slot: &str,
        max_workers: usize,
        settings: PostgresCdcSnapshotConfig,
        task_count: usize,
        cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
        parent_cancel: &CancellationToken,
    ) -> Self {
        let config = snapshot_adaptive_concurrency_config(max_workers, settings);
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

        let (scan_observation_tx, scan_observation_rx) = watch::channel(None);
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
                cdc_replication_debug,
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

    pub(super) fn scan_limiter(&self) -> Arc<SnapshotScanLimiter> {
        Arc::clone(&self.scan_limiter)
    }

    pub(super) fn scan_observation_tx(
        &self,
    ) -> Option<watch::Sender<Option<SnapshotScanObservation>>> {
        self.scan_observation_tx.clone()
    }

    pub(super) fn wal_pressure_tx(&self) -> Option<watch::Sender<SnapshotWalBufferPressure>> {
        self.wal_pressure_tx.clone()
    }

    pub(super) async fn shutdown(&mut self) {
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

pub(super) fn snapshot_adaptive_concurrency_config(
    max_workers: usize,
    settings: PostgresCdcSnapshotConfig,
) -> SnapshotAdaptiveConcurrencyConfig {
    let max_workers = max_workers.max(1);
    let min_workers = settings.min_workers.clamp(1, max_workers);
    let high = settings.wal_buffer_high_watermark_percent.clamp(1, 100);
    let mut low = settings.wal_buffer_low_watermark_percent.min(100);
    if low >= high {
        low = high.saturating_sub(1);
    }
    SnapshotAdaptiveConcurrencyConfig {
        enabled: settings.adaptive_concurrency && max_workers > 1,
        min_workers,
        max_workers,
        wal_buffer_high_watermark_percent: high,
        wal_buffer_low_watermark_percent: low,
        slow_scan_ms: settings.slow_scan_ms.max(1),
        controller_interval: Duration::from_millis(settings.controller_interval_ms.max(1)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_snapshot_adaptive_concurrency_controller(
    source: String,
    slot: String,
    config: SnapshotAdaptiveConcurrencyConfig,
    scan_limiter: Arc<SnapshotScanLimiter>,
    cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    mut wal_pressure_rx: watch::Receiver<SnapshotWalBufferPressure>,
    mut scan_observation_rx: watch::Receiver<Option<SnapshotScanObservation>>,
    cancel: CancellationToken,
) {
    let mut latest_scan_observation = None;
    let mut interval = tokio::time::interval(config.controller_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            changed = scan_observation_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                latest_scan_observation = *scan_observation_rx.borrow_and_update();
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
                let sink_health = snapshot_sink_health(&cdc_replication_debug, &source).await;
                let decision = snapshot_concurrency_decision(
                    config,
                    current_target,
                    wal_pressure,
                    sink_health,
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
                        sink_health = ?sink_health,
                        direction = decision.direction,
                        reason = decision.reason,
                        "adjusted adaptive Postgres CDC snapshot concurrency"
                    );
                }
            }
        }
    }
}

pub(super) fn snapshot_concurrency_decision(
    config: SnapshotAdaptiveConcurrencyConfig,
    current_target: usize,
    wal_pressure: SnapshotWalBufferPressure,
    sink_health: SnapshotSinkHealth,
    scan_observation: Option<SnapshotScanObservation>,
) -> Option<SnapshotConcurrencyDecision> {
    let current_target = current_target.clamp(config.min_workers, config.max_workers);
    if let Some(reason) = sink_health.unhealthy_reason() {
        if current_target > config.min_workers {
            return Some(SnapshotConcurrencyDecision {
                target_workers: config.min_workers,
                direction: "decrease",
                reason,
            });
        }
        return None;
    }

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

pub(super) async fn snapshot_sink_health(
    shared: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    source: &str,
) -> SnapshotSinkHealth {
    let state = shared.read().await;
    let mut has_target_error = false;
    for pipeline in state
        .pipelines
        .iter()
        .filter(|pipeline| pipeline.source == source)
    {
        if pipeline.source_backpressure_active {
            return SnapshotSinkHealth::Backpressured;
        }
        if pipeline.last_error.is_some() {
            has_target_error = true;
        }
    }
    if has_target_error {
        SnapshotSinkHealth::TargetError
    } else {
        SnapshotSinkHealth::Healthy
    }
}
