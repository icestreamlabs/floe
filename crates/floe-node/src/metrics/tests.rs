use prometheus::{Encoder, TextEncoder};

use super::*;

#[test]
fn postgres_cdc_metrics_record_source_and_table_lag() {
    let source = "pg_metrics_test";
    let slot = "slot_metrics_test";
    let table = "orders_metrics_test";

    record_postgres_cdc_upstream_lsn(source, slot, 150);
    record_postgres_cdc_durable_lsn(source, slot, 100);
    record_postgres_cdc_table_applied_lsn(source, slot, table, 90);
    record_postgres_cdc_schema_evolution_policy(source, "ignore_compatible");
    record_postgres_cdc_schema_evolution_observation(
        source,
        table,
        "compatible_addition",
        "ignore_compatible",
        123_456,
    );
    record_postgres_cdc_snapshot_concurrency(source, slot, 2, 1, 4);
    record_postgres_cdc_snapshot_wal_buffer_fill(source, slot, 3, 4);
    record_postgres_cdc_source_connected(source, slot, true);
    inc_postgres_cdc_reconnect(source, slot, "scheduled");
    inc_postgres_cdc_snapshot_concurrency_adjustment(source, slot, "decrease", "wal_buffer_high");

    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&prometheus::gather(), &mut buffer)
        .expect("encode metrics");
    let body = String::from_utf8(buffer).expect("metrics utf8");

    assert!(body.contains(
        "floe_postgres_cdc_source_lag_bytes{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 50"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_table_lag_bytes{slot=\"slot_metrics_test\",source=\"pg_metrics_test\",table=\"orders_metrics_test\"} 60"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_schema_evolution_policy{policy=\"ignore_compatible\",source=\"pg_metrics_test\"} 1"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_schema_evolution_policy{policy=\"fail_fast\",source=\"pg_metrics_test\"} 0"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_schema_evolution_events_total{outcome=\"compatible_addition\",policy=\"ignore_compatible\",source=\"pg_metrics_test\",table=\"orders_metrics_test\"} 1"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_schema_evolution_last_observed_unix_ms{outcome=\"compatible_addition\",policy=\"ignore_compatible\",source=\"pg_metrics_test\",table=\"orders_metrics_test\"} 123456"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_snapshot_concurrency_target{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 2"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_snapshot_concurrency_active{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 1"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_snapshot_concurrency_max{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 4"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_snapshot_wal_buffer_fill_percent{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 75"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_source_connected{slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 1"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_reconnects_total{result=\"scheduled\",slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 1"
    ));
    assert!(body.contains(
        "floe_postgres_cdc_snapshot_concurrency_adjustments_total{direction=\"decrease\",reason=\"wal_buffer_high\",slot=\"slot_metrics_test\",source=\"pg_metrics_test\"} 1"
    ));
}

#[test]
fn cdc_replication_metrics_record_replay_and_target_error_state() {
    let pipeline = "pipe_metrics_test";

    record_cdc_buffer_pending(pipeline, 2, 10, 2048, Some(100));
    record_cdc_buffer_cap_utilization(pipeline, "pending_bytes", 2048, 4096);
    record_cdc_buffer_cap_utilization_u64(pipeline, "pending_age", 100, 200);
    record_cdc_buffer_integrity(pipeline, 1, 2, 512);
    inc_cdc_buffer_object_op(pipeline, "create", 2);
    inc_cdc_buffer_object_op(pipeline, "get", 1);
    inc_cdc_buffer_object_op(pipeline, "delete", 1);
    record_cdc_buffer_append(pipeline, 10, 2048, 7);
    record_cdc_buffer_cleanup(pipeline, 1, 4, 1024, 9);
    inc_cdc_buffer_forced_flush(pipeline);
    observe_cdc_buffer_flush_latency_ms(pipeline, 3);
    inc_cdc_buffer_drain_attempt(pipeline);
    record_cdc_buffer_replay(pipeline, 4, 11);
    observe_cdc_buffer_replay_phase_latency_ms(pipeline, "target_delivery", 5);
    record_cdc_buffer_source_backpressure_active(pipeline, true);
    record_cdc_replication_replaying(pipeline, true);
    record_cdc_replication_target_error(pipeline, true);
    inc_cdc_replication_target_failure(pipeline, "kafka", "retryable");
    record_cdc_replication_target_write(pipeline, "kafka", "failure", 7, 13);
    inc_cdc_replication_dlq_replay(pipeline, "success");
    inc_cdc_replication_dlq_replay(pipeline, "failure");
    record_cdc_replication_dlq_stats(pipeline, 2, 3, 4, Some(5));

    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&prometheus::gather(), &mut buffer)
        .expect("encode metrics");
    let body = String::from_utf8(buffer).expect("metrics utf8");

    assert!(body.contains("floe_cdc_replication_replaying{pipeline=\"pipe_metrics_test\"} 1"));
    assert!(body.contains("floe_cdc_replication_target_error{pipeline=\"pipe_metrics_test\"} 1"));
    assert!(body.contains(
        "floe_cdc_replication_target_failures_total{class=\"retryable\",pipeline=\"pipe_metrics_test\",target_kind=\"kafka\"} 1"
    ));
    assert!(body.contains(
        "floe_cdc_replication_target_write_latency_ms_count{pipeline=\"pipe_metrics_test\",result=\"failure\",target_kind=\"kafka\"} 1"
    ));
    assert!(body.contains(
        "floe_cdc_replication_target_write_batch_records_sum{pipeline=\"pipe_metrics_test\",result=\"failure\",target_kind=\"kafka\"} 7"
    ));
    assert!(body.contains(
        "floe_cdc_replication_target_write_records_total{pipeline=\"pipe_metrics_test\",result=\"failure\",target_kind=\"kafka\"} 7"
    ));
    assert!(body.contains(
        "floe_cdc_replication_dlq_replays_total{pipeline=\"pipe_metrics_test\",result=\"success\"} 1"
    ));
    assert!(body.contains(
        "floe_cdc_replication_dlq_replays_total{pipeline=\"pipe_metrics_test\",result=\"failure\"} 1"
    ));
    assert!(body.contains(
        "floe_cdc_replication_dlq_entries{pipeline=\"pipe_metrics_test\",status=\"pending\"} 2"
    ));
    assert!(body.contains(
        "floe_cdc_replication_dlq_entries{pipeline=\"pipe_metrics_test\",status=\"replayed\"} 3"
    ));
    assert!(body.contains(
        "floe_cdc_replication_dlq_entries{pipeline=\"pipe_metrics_test\",status=\"discarded\"} 4"
    ));
    assert!(body.contains(
        "floe_cdc_replication_dlq_oldest_pending_age_ms{pipeline=\"pipe_metrics_test\"} 5"
    ));
    assert!(body.contains("floe_cdc_buffer_pending_objects{pipeline=\"pipe_metrics_test\"} 2"));
    assert!(body.contains(
        "floe_cdc_buffer_object_ops_total{operation=\"create\",pipeline=\"pipe_metrics_test\"} 2"
    ));
    assert!(body.contains(
        "floe_cdc_buffer_object_ops_total{operation=\"get\",pipeline=\"pipe_metrics_test\"} 1"
    ));
    assert!(body.contains(
        "floe_cdc_buffer_object_ops_total{operation=\"delete\",pipeline=\"pipe_metrics_test\"} 1"
    ));
    assert!(
        body.contains("floe_cdc_buffer_appended_records_total{pipeline=\"pipe_metrics_test\"} 10")
    );
    assert!(
        body.contains("floe_cdc_buffer_appended_bytes_total{pipeline=\"pipe_metrics_test\"} 2048")
    );
    assert!(
        body.contains("floe_cdc_buffer_append_latency_ms_count{pipeline=\"pipe_metrics_test\"} 1")
    );
    assert!(
        body.contains("floe_cdc_buffer_forced_flushes_total{pipeline=\"pipe_metrics_test\"} 1")
    );
    assert!(
        body.contains("floe_cdc_buffer_flush_latency_ms_count{pipeline=\"pipe_metrics_test\"} 1")
    );
    assert!(
        body.contains("floe_cdc_buffer_drain_attempts_total{pipeline=\"pipe_metrics_test\"} 1")
    );
    assert!(
        body.contains("floe_cdc_buffer_replayed_records_total{pipeline=\"pipe_metrics_test\"} 4")
    );
    assert!(body.contains(
        "floe_cdc_buffer_replay_latency_ms_count{phase=\"total\",pipeline=\"pipe_metrics_test\"} 1"
    ));
    assert!(body.contains(
        "floe_cdc_buffer_replay_latency_ms_count{phase=\"target_delivery\",pipeline=\"pipe_metrics_test\"} 1"
    ));
    assert!(
        body.contains(
            "floe_cdc_buffer_source_backpressure_active{pipeline=\"pipe_metrics_test\"} 1"
        )
    );
    assert!(body.contains(
        "floe_cdc_buffer_cap_utilization_percent{limit=\"pending_bytes\",pipeline=\"pipe_metrics_test\"} 50"
    ));
    assert!(body.contains(
        "floe_cdc_buffer_cap_utilization_percent{limit=\"pending_age\",pipeline=\"pipe_metrics_test\"} 50"
    ));
    assert!(body.contains(
        "floe_cdc_buffer_integrity_objects{pipeline=\"pipe_metrics_test\",state=\"missing_payload\"} 1"
    ));
    assert!(body.contains(
        "floe_cdc_buffer_integrity_objects{pipeline=\"pipe_metrics_test\",state=\"orphan_payload\"} 2"
    ));
    assert!(
        body.contains("floe_cdc_buffer_orphan_payload_bytes{pipeline=\"pipe_metrics_test\"} 512")
    );
    assert!(
        body.contains(
            "floe_cdc_buffer_cleanup_transactions_total{pipeline=\"pipe_metrics_test\"} 1"
        )
    );
    assert!(
        body.contains("floe_cdc_buffer_cleanup_records_total{pipeline=\"pipe_metrics_test\"} 4")
    );
    assert!(
        body.contains("floe_cdc_buffer_cleanup_bytes_total{pipeline=\"pipe_metrics_test\"} 1024")
    );
    assert!(
        body.contains("floe_cdc_buffer_cleanup_latency_ms_count{pipeline=\"pipe_metrics_test\"} 1")
    );

    record_cdc_replication_replaying(pipeline, false);
    record_cdc_replication_target_error(pipeline, false);
    record_cdc_buffer_source_backpressure_active(pipeline, false);
}
