use super::*;

#[test]
fn postgres_cdc_reconnect_backoff_is_bounded_exponential() {
    let policy = PostgresCdcRuntimeReconnectPolicy {
        max_reconnects: 4,
        retry_base: Duration::from_millis(100),
        retry_max_backoff: Duration::from_millis(250),
    };

    assert_eq!(policy.backoff_for_reconnect(0), Duration::from_millis(100));
    assert_eq!(policy.backoff_for_reconnect(1), Duration::from_millis(200));
    assert_eq!(policy.backoff_for_reconnect(2), Duration::from_millis(250));
    assert_eq!(policy.backoff_for_reconnect(63), Duration::from_millis(250));
}

#[tokio::test]
async fn postgres_cdc_debug_connection_state_tracks_reconnects() {
    let shared = Arc::new(tokio::sync::RwLock::new(
        http_ingest::CdcReplicationDebugState::default(),
    ));

    record_postgres_cdc_debug_connection_state(
        &shared,
        "pg_main",
        "slot_main",
        false,
        2,
        Some("Postgres CDC stream reconnect attempts exhausted".to_string()),
    );
    let state = shared.read().await;
    let source = state
        .postgres_sources
        .iter()
        .find(|source| source.source == "pg_main")
        .expect("source state");
    assert_eq!(source.slot.as_deref(), Some("slot_main"));
    assert!(!source.connected);
    assert_eq!(source.reconnect_attempts, 2);
    assert_eq!(
        source.last_error.as_deref(),
        Some("Postgres CDC stream reconnect attempts exhausted")
    );
    drop(state);

    record_postgres_cdc_debug_connection_state(&shared, "pg_main", "slot_main", true, 2, None);
    let state = shared.read().await;
    let source = state
        .postgres_sources
        .iter()
        .find(|source| source.source == "pg_main")
        .expect("source state");
    assert!(source.connected);
    assert_eq!(source.reconnect_attempts, 2);
    assert_eq!(source.last_error, None);
}
