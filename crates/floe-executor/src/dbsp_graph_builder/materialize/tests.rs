use super::*;

#[test]
fn bootstraps_authoritative_zero_only_for_zero_frontier_zero_logical_version() {
    assert!(DbspGraphBuilder::should_bootstrap_authoritative_zero(
        0, None
    ));
    assert!(DbspGraphBuilder::should_bootstrap_authoritative_zero(
        0,
        Some(0)
    ));
    assert!(!DbspGraphBuilder::should_bootstrap_authoritative_zero(
        1, None
    ));
    assert!(!DbspGraphBuilder::should_bootstrap_authoritative_zero(
        0,
        Some(1)
    ));
    assert!(!DbspGraphBuilder::should_bootstrap_authoritative_zero(
        2,
        Some(0)
    ));
}

fn delta_batch(rows: Vec<(Vec<u8>, i64)>) -> EncodedDeltaBatch {
    Arc::new(rows)
}

#[test]
fn flush_trigger_string_labels_are_stable() {
    assert_eq!(
        FlushTrigger::MaxPendingDeltas.as_str(),
        "max_pending_deltas"
    );
    assert_eq!(
        FlushTrigger::MaxPendingVersions.as_str(),
        "max_pending_versions"
    );
    assert_eq!(FlushTrigger::MaxPendingRows.as_str(), "max_pending_rows");
    assert_eq!(FlushTrigger::MaxPendingBytes.as_str(), "max_pending_bytes");
    assert_eq!(FlushTrigger::MaxDelay.as_str(), "max_delay");
    assert_eq!(FlushTrigger::CatchupBoundary.as_str(), "catchup_boundary");
    assert_eq!(FlushTrigger::Shutdown.as_str(), "shutdown");
}

#[test]
fn pending_mv_flush_tracks_stats_triggers_and_reset() {
    let mut pending = PendingMvFlush::default();
    let cfg = MvFlushCoalescingConfig {
        enabled: true,
        max_pending_deltas: 2,
        max_pending_versions: Some(8),
        max_pending_rows: Some(100),
        max_pending_bytes: Some(1000),
        max_delay_ms: Some(1_000),
        flush_on_catchup_boundary: true,
        flush_on_shutdown: true,
    };

    assert!(pending.trigger(cfg, Instant::now()).is_none());
    assert!(pending.delay_remaining(cfg, Instant::now()).is_none());

    pending.record(
        10,
        &DeltaApplyStats {
            delta_rows: 2,
            delta_bytes: 11,
            load_ms: 1,
            transform_ms: 2,
            merge_ms: 3,
        },
    );
    assert!(pending.has_pending());
    assert_eq!(pending.first_ts, Some(10));
    assert_eq!(pending.last_ts, Some(10));
    assert!(pending.delay_remaining(cfg, Instant::now()).is_some());

    pending.record(
        11,
        &DeltaApplyStats {
            delta_rows: 4,
            delta_bytes: 13,
            load_ms: 5,
            transform_ms: 7,
            merge_ms: 11,
        },
    );
    assert!(matches!(
        pending.trigger(cfg, Instant::now()),
        Some(FlushTrigger::MaxPendingDeltas)
    ));

    let mut delayed = PendingMvFlush::default();
    delayed.record(22, &DeltaApplyStats::default());
    delayed.first_enqueue_at = Some(Instant::now() - Duration::from_millis(25));
    let delay_cfg = MvFlushCoalescingConfig {
        max_pending_deltas: usize::MAX,
        max_pending_versions: None,
        max_pending_rows: None,
        max_pending_bytes: None,
        max_delay_ms: Some(5),
        ..MvFlushCoalescingConfig::default()
    };
    assert!(matches!(
        delayed.trigger(delay_cfg, Instant::now()),
        Some(FlushTrigger::MaxDelay)
    ));

    delayed.clear();
    assert!(!delayed.has_pending());
}

#[test]
fn hotspot_summary_and_logging_gate_behave_as_expected() {
    assert!(summarize_hotspot(&[], 100).is_none());
    assert!(summarize_hotspot(&[("load", 0)], 100).is_none());
    assert!(summarize_hotspot(&[("load", 10)], 0).is_none());

    let hotspot = summarize_hotspot(&[("load", 15), ("merge", 35)], 50).expect("hotspot");
    assert_eq!(hotspot.phase, "merge");
    assert_eq!(hotspot.phase_ms, 35);
    assert!((hotspot.phase_share - 0.7).abs() < f64::EPSILON);

    assert!(should_log_optimization_hotspot(
        MV_OPTIMIZATION_LOG_MIN_TOTAL_MS
    ));
    assert!(should_log_optimization_hotspot(
        MV_OPTIMIZATION_LOG_MIN_TOTAL_MS + 1
    ));
}

#[test]
fn pending_overlay_snapshot_tracks_batches_and_flush_request() {
    let mut pending = PendingOverlaySnapshot::default();
    let cfg = OverlaySnapshotConfig {
        max_pending_batches: 2,
        max_pending_rows: 10,
        max_delay_ms: 1_000,
    };

    pending.record(1, delta_batch(vec![]));
    assert!(!pending.has_pending());

    pending.record(5, delta_batch(vec![(vec![1], 1), (vec![2, 3], -1)]));
    assert!(pending.has_pending());
    assert_eq!(pending.batches, 1);
    assert_eq!(pending.rows, 2);
    assert_eq!(pending.first_version, Some(5));
    assert_eq!(pending.last_version, Some(5));
    assert!(!pending.should_flush(cfg, Instant::now()));

    pending.record(6, delta_batch(vec![(vec![9], 1)]));
    assert!(pending.should_flush(cfg, Instant::now()));

    let request = pending.take_request("test_reason").expect("flush request");
    assert_eq!(request.reason, "test_reason");
    assert_eq!(request.batches, 2);
    assert_eq!(request.rows, 3);
    assert_eq!(request.first_version, 5);
    assert_eq!(request.last_version, 6);
    assert_eq!(request.delta_batches.len(), 2);
    assert!(!pending.has_pending());
}

#[test]
fn pending_overlay_snapshot_delay_and_clear_are_consistent() {
    let mut pending = PendingOverlaySnapshot::default();
    pending.record(42, delta_batch(vec![(vec![7], 1)]));
    pending.first_enqueue_at = Some(Instant::now() - Duration::from_millis(50));

    let cfg = OverlaySnapshotConfig {
        max_pending_batches: usize::MAX,
        max_pending_rows: usize::MAX,
        max_delay_ms: 10,
    };

    assert!(pending.should_flush(cfg, Instant::now()));
    assert_eq!(
        pending.delay_remaining(cfg, Instant::now()),
        Some(Duration::from_millis(0))
    );

    pending.clear();
    assert!(!pending.has_pending());
    assert!(pending.take_request("after_clear").is_none());
}

#[test]
fn into_owned_deltas_covers_unique_and_shared_arcs() {
    let unique = delta_batch(vec![(vec![1, 2], 1)]);
    assert_eq!(into_owned_deltas(unique), vec![(vec![1, 2], 1)]);

    let shared = delta_batch(vec![(vec![9], -2)]);
    let _keep_alive = Arc::clone(&shared);
    assert_eq!(into_owned_deltas(shared), vec![(vec![9], -2)]);
}
