use super::*;

#[test]
fn buffer_limit_violation_accounts_for_incoming_payload_bytes() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: Some(100),
        max_pending_records: None,
        max_pending_transactions: None,
        max_pending_age_ms: None,
    };

    assert_eq!(
        buffer_limit_violation(0, 0, 70, None, 31, 0, limits),
        Some(ReplicationBufferLimitViolation::Bytes {
            pending_bytes: 70,
            incoming_bytes: 31,
            max_pending_bytes: 100,
        })
    );
    assert_eq!(buffer_limit_violation(0, 0, 70, None, 30, 0, limits), None);
}

#[test]
fn buffer_limit_violation_accounts_for_pending_records() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: None,
        max_pending_records: Some(10),
        max_pending_transactions: None,
        max_pending_age_ms: None,
    };

    assert_eq!(
        buffer_limit_violation(0, 8, 0, None, 0, 3, limits),
        Some(ReplicationBufferLimitViolation::Records {
            pending_records: 8,
            incoming_records: 3,
            max_pending_records: 10,
        })
    );
    assert_eq!(buffer_limit_violation(0, 8, 0, None, 0, 2, limits), None);
}

#[test]
fn buffer_limit_violation_accounts_for_pending_objects() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: None,
        max_pending_records: None,
        max_pending_transactions: Some(2),
        max_pending_age_ms: None,
    };

    assert_eq!(
        buffer_limit_violation(2, 0, 0, None, 0, 1, limits),
        Some(ReplicationBufferLimitViolation::Objects {
            pending_transactions: 2,
            incoming_transactions: 1,
            max_pending_transactions: 2,
        })
    );
    assert_eq!(buffer_limit_violation(1, 0, 0, None, 0, 1, limits), None);
}

#[test]
fn buffer_limit_violation_checks_oldest_pending_age() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: None,
        max_pending_records: None,
        max_pending_transactions: None,
        max_pending_age_ms: Some(1_000),
    };

    assert_eq!(
        buffer_limit_violation(0, 0, 0, Some(1_001), 0, 0, limits),
        Some(ReplicationBufferLimitViolation::Age {
            oldest_pending_age_ms: 1_001,
            max_pending_age_ms: 1_000,
        })
    );
    assert_eq!(
        buffer_limit_violation(0, 0, 0, Some(1_000), 0, 0, limits),
        None
    );
}

#[test]
fn estimated_buffer_payload_bytes_includes_record_framing() {
    let records = vec![
        CdcBufferRecord::new(Some(vec![1, 2, 3]), Some(vec![4])),
        CdcBufferRecord::new(None, Some(vec![5, 6])),
    ];

    assert_eq!(estimated_buffer_payload_bytes(&records), 70);
}

#[test]
fn zero_buffer_limit_override_disables_default_limit() {
    assert_eq!(effective_usize_limit(Some(0), Some(100)), None);
    assert_eq!(effective_u64_limit(Some(0), Some(100)), None);
    assert_eq!(effective_usize_limit(None, Some(100)), Some(100));
    assert_eq!(effective_u64_limit(None, Some(100)), Some(100));
}

#[test]
fn parses_arrow_ipc_compression_override() {
    assert_eq!(
        ReplicationArrowIpcCompressionConfig::parse("lz4"),
        Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
    );
    assert_eq!(
        ReplicationArrowIpcCompressionConfig::parse("lz4-frame"),
        Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
    );
    assert_eq!(ReplicationArrowIpcCompressionConfig::parse("none"), None);
    assert_eq!(ReplicationArrowIpcCompressionConfig::parse("bogus"), None);
}
