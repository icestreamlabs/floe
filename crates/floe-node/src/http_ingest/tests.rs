use super::cdc_admin::*;
use super::server::*;
use super::sse_json::*;
use super::*;
use axum::body::Body;
use axum::http::{Request, header};
use serde_json::json;
use tokio::sync::mpsc;
use tower::util::ServiceExt;

#[test]
fn parse_events_accepts_source_wrapped_payload() {
    let value = json!({"source": "nexmark_bid", "data": {"auction": 1}});
    let events = parse_events(value, None).expect("parse events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source(), "nexmark_bid");
}

#[test]
fn parse_events_uses_default_source() {
    let value = json!({"auction": 1});
    let events = parse_events(value, Some("nexmark_bid")).expect("parse events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source(), "nexmark_bid");
}

#[tokio::test]
async fn http_ingest_accepts_events() {
    let (tx, mut rx) = mpsc::channel::<Vec<AppendIngestEvent>>(4);
    let state = HttpIngestState {
        sender: AppendIngestEventSender::Direct {
            sender: tx,
            pending: Default::default(),
        },
        default_source: Some("nexmark_bid".to_string()),
        cancel: CancellationToken::new(),
        health: None,
    };
    let app = Router::new()
        .route("/ingest", post(ingest))
        .with_state(state);

    let payload = json!({"auction": 1});
    let request = Request::builder()
        .method("POST")
        .uri("/ingest")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let batch = rx.recv().await.expect("batch");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].source(), "nexmark_bid");
}

#[tokio::test]
async fn http_ingest_waits_for_commit_ack() {
    let (tx, mut rx) = mpsc::channel(4);
    let state = HttpIngestState {
        sender: AppendIngestEventSender::Routed {
            connector_id: 0,
            sender: tx,
            pending: Default::default(),
        },
        default_source: Some("nexmark_bid".to_string()),
        cancel: CancellationToken::new(),
        health: None,
    };
    let app = Router::new()
        .route("/ingest", post(ingest))
        .with_state(state);

    let payload = json!({"auction": 1});
    let request = Request::builder()
        .method("POST")
        .uri("/ingest")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .expect("request");
    let response_task = tokio::spawn(async move { app.oneshot(request).await });

    let batch = rx.recv().await.expect("batch");
    assert_eq!(batch.events.len(), 1);
    assert!(!response_task.is_finished());
    batch
        .commit_ack
        .expect("commit ack")
        .record_committed()
        .await;

    let response = response_task
        .await
        .expect("response task")
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn healthz_reports_unavailable_when_executor_stops() {
    let (tx, _rx) = mpsc::channel(1);
    let state = HttpIngestState {
        sender: AppendIngestEventSender::Direct {
            sender: tx,
            pending: Default::default(),
        },
        default_source: Some("nexmark_bid".to_string()),
        cancel: CancellationToken::new(),
        health: Some(HttpIngestHealth {
            executor_running: Arc::new(AtomicBool::new(false)),
            storage_reachable: Arc::new(AtomicBool::new(true)),
            runtime_ready: Arc::new(AtomicBool::new(true)),
            watermark_debug: None,
            cdc_replication_debug: None,
        }),
    };
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state);
    let request = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let request = Request::builder()
        .method("GET")
        .uri("/readyz")
        .body(Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn debug_watermarks_returns_snapshot() {
    let (tx, _rx) = mpsc::channel(1);
    let snapshot = Arc::new(RwLock::new(WatermarkDebugState {
        global_watermark_ms: Some(42),
        policy: "min_active_sources".to_string(),
        updated_at_unix_ms: 7,
        sources: vec![WatermarkDebugSourceState {
            source: "s1".to_string(),
            watermark_ms: 42,
            idle: false,
        }],
    }));
    let state = HttpIngestState {
        sender: AppendIngestEventSender::Direct {
            sender: tx,
            pending: Default::default(),
        },
        default_source: Some("nexmark_bid".to_string()),
        cancel: CancellationToken::new(),
        health: Some(HttpIngestHealth {
            executor_running: Arc::new(AtomicBool::new(true)),
            storage_reachable: Arc::new(AtomicBool::new(true)),
            runtime_ready: Arc::new(AtomicBool::new(true)),
            watermark_debug: Some(snapshot),
            cdc_replication_debug: None,
        }),
    };
    let app = Router::new()
        .route("/debug/watermarks", get(debug_watermarks_ingest))
        .with_state(state);
    let request = Request::builder()
        .method("GET")
        .uri("/debug/watermarks")
        .body(Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn debug_cdc_replication_admin_returns_snapshot() {
    let snapshot = Arc::new(RwLock::new(CdcReplicationDebugState {
        updated_at_unix_ms: 9,
        refresh_error: None,
        postgres_sources: vec![PostgresCdcDebugSourceState {
            source: "pg_main".to_string(),
            slot: Some("slot_main".to_string()),
            schema_evolution_policy: "ignore_compatible".to_string(),
            connected: true,
            reconnect_attempts: 1,
            upstream_lsn: Some("0/16B6C80".to_string()),
            upstream_lsn_bytes: Some(23_817_344),
            durable_lsn: Some("0/16B6C50".to_string()),
            durable_lsn_bytes: Some(23_817_296),
            source_lag_bytes: Some(48),
            last_error: None,
            latest_schema_evolution: Some(PostgresCdcSchemaEvolutionDebugState {
                table: "orders".to_string(),
                upstream_table: "public.orders".to_string(),
                policy: "ignore_compatible".to_string(),
                outcome: "compatible_addition".to_string(),
                added_columns: vec!["note".to_string()],
                reason: None,
                catalog_schema_version: 1,
                observed_schema_version: 2,
                observed_at_unix_ms: 8,
            }),
        }],
        pipelines: vec![CdcReplicationDebugPipelineState {
            pipeline: "orders_pipe".to_string(),
            source: "pg_main".to_string(),
            schema_evolution_policy: "ignore_compatible".to_string(),
            error_policy: "retry_with_backoff".to_string(),
            target_kind: "kafka".to_string(),
            checkpoint_position: Some("pg/0/16B6C50".to_string()),
            checkpoint_lsn_bytes: Some(23_817_296),
            checkpoint_lag_bytes: Some(48),
            checkpoint_transaction_id: Some("pg-xid-77".to_string()),
            target_state: BTreeMap::from([(
                "target.delivery.status".to_string(),
                "pending".to_string(),
            )]),
            pending_transactions: 1,
            pending_objects: 1,
            pending_records: 2,
            pending_bytes: 3,
            oldest_pending_age_ms: Some(4),
            dlq_pending_entries: 5,
            dlq_replayed_entries: 6,
            dlq_discarded_entries: 7,
            oldest_dlq_pending_age_ms: Some(8),
            missing_payload_objects: 0,
            orphan_payload_objects: 0,
            orphan_payload_bytes: 0,
            replaying: true,
            source_backpressure_active: true,
            last_error: Some("kafka unavailable".to_string()),
        }],
    }));
    let state = HttpAdminState {
        cancel: CancellationToken::new(),
        health: HttpIngestHealth {
            executor_running: Arc::new(AtomicBool::new(true)),
            storage_reachable: Arc::new(AtomicBool::new(true)),
            runtime_ready: Arc::new(AtomicBool::new(true)),
            watermark_debug: None,
            cdc_replication_debug: Some(snapshot),
        },
        storage_db: None,
        storage_catalog: None,
        replication_runtime: None,
        materialized_views: None,
    };
    let app = Router::new()
        .route("/debug/cdc/replication", get(debug_cdc_replication_admin))
        .route("/ops/cdc/replication", get(debug_cdc_replication_admin))
        .with_state(state);
    let request = Request::builder()
        .method("GET")
        .uri("/ops/cdc/replication")
        .body(Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(body.contains("orders_pipe"));
    assert!(!body.contains("postgres://"));
    assert!(!body.contains("connection"));
}

#[tokio::test]
async fn admin_cdc_replication_dlq_lists_inspects_and_discards_entries() {
    let storage = SlateCatalog::in_memory().await.expect("storage");
    let dlq_entry = ReplicationPipelineDlqEntry::new(
        "orders_pipe",
        "entry-1",
        "pg_main",
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).expect("position"),
        Some(floe_cdc_core::CdcTransactionId::new("pg-xid-1").expect("transaction")),
        "postgres_delivery",
        "permission denied",
        1,
        Some("payloads/entry-1.bin".to_string()),
        Some("kafka_records".to_string()),
        128,
        BTreeMap::from([(
            "target.delivery.status".to_string(),
            "dead_lettered".to_string(),
        )]),
        current_unix_time_ms(),
    )
    .expect("dlq entry");
    storage
        .put_replication_pipeline_dlq_entry(dlq_entry)
        .await
        .expect("persist dlq entry");
    let second_entry = ReplicationPipelineDlqEntry::new(
        "orders_pipe",
        "entry-2",
        "pg_main",
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C60", None).expect("position"),
        Some(floe_cdc_core::CdcTransactionId::new("pg-xid-2").expect("transaction")),
        "postgres_delivery",
        "permission denied",
        1,
        Some("payloads/entry-2.bin".to_string()),
        Some("kafka_records".to_string()),
        128,
        BTreeMap::from([(
            "target.delivery.status".to_string(),
            "dead_lettered".to_string(),
        )]),
        current_unix_time_ms().saturating_add(1),
    )
    .expect("second dlq entry");
    storage
        .put_replication_pipeline_dlq_entry(second_entry)
        .await
        .expect("persist second dlq entry");
    let state = HttpAdminState {
        cancel: CancellationToken::new(),
        health: HttpIngestHealth {
            executor_running: Arc::new(AtomicBool::new(true)),
            storage_reachable: Arc::new(AtomicBool::new(true)),
            runtime_ready: Arc::new(AtomicBool::new(true)),
            watermark_debug: None,
            cdc_replication_debug: None,
        },
        storage_db: None,
        storage_catalog: Some(Arc::new(storage.clone())),
        replication_runtime: None,
        materialized_views: None,
    };
    let app = Router::new()
        .route(
            "/debug/cdc/replication/dlq",
            get(debug_cdc_replication_dlq_list_admin),
        )
        .route(
            "/ops/cdc/replication/dlq",
            get(debug_cdc_replication_dlq_list_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/retry",
            post(ops_cdc_replication_dlq_retry_batch_admin),
        )
        .route(
            "/debug/cdc/replication/dlq/:pipeline/:dlq_id",
            get(debug_cdc_replication_dlq_entry_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id",
            get(debug_cdc_replication_dlq_entry_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id/discard",
            post(ops_cdc_replication_dlq_discard_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id/retry",
            post(ops_cdc_replication_dlq_retry_admin),
        )
        .with_state(state);

    let request = Request::builder()
        .method("GET")
        .uri("/ops/cdc/replication/dlq?pipeline=orders_pipe&status=pending&offset=1&limit=1")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(value["offset"], 1);
    assert_eq!(value["limit"], 1);
    assert_eq!(value["total_matching"], 2);
    assert_eq!(value["count"], 1);
    assert_eq!(value["entries"][0]["dlq_id"], "entry-2");

    let request = Request::builder()
        .method("GET")
        .uri("/ops/cdc/replication/dlq/orders_pipe/entry-1")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let request = Request::builder()
        .method("POST")
        .uri("/ops/cdc/replication/dlq/retry?pipeline=orders_pipe&limit=0")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let request = Request::builder()
        .method("POST")
        .uri("/ops/cdc/replication/dlq/orders_pipe/entry-1/retry")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let payload = json!({
        "reason": "operator confirmed duplicate",
        "operator": "ops@example.com"
    });
    let request = Request::builder()
        .method("POST")
        .uri("/ops/cdc/replication/dlq/orders_pipe/entry-1/discard")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let discarded = storage
        .replication_pipeline_dlq_entry("orders_pipe", "entry-1")
        .await
        .expect("load entry")
        .expect("entry exists");
    assert_eq!(discarded.status(), ReplicationPipelineDlqStatus::Discarded);
    assert_eq!(
        discarded.status_reason(),
        Some("operator confirmed duplicate (operator: ops@example.com)")
    );
}

#[tokio::test]
async fn admin_cdc_replication_reconcile_validates_bounds_and_runtime() {
    let storage = SlateCatalog::in_memory().await.expect("storage");
    let state = HttpAdminState {
        cancel: CancellationToken::new(),
        health: HttpIngestHealth {
            executor_running: Arc::new(AtomicBool::new(true)),
            storage_reachable: Arc::new(AtomicBool::new(true)),
            runtime_ready: Arc::new(AtomicBool::new(true)),
            watermark_debug: None,
            cdc_replication_debug: None,
        },
        storage_db: None,
        storage_catalog: Some(Arc::new(storage)),
        replication_runtime: None,
        materialized_views: None,
    };
    let app = Router::new()
        .route(
            "/ops/cdc/replication/:pipeline/reconcile",
            post(ops_cdc_replication_reconcile_admin),
        )
        .with_state(state);

    let request = Request::builder()
        .method("POST")
        .uri("/ops/cdc/replication/orders_pipe/reconcile?max_rows=0")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let request = Request::builder()
        .method("POST")
        .uri("/ops/cdc/replication/orders_pipe/reconcile?max_rows=10")
        .body(Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn admin_mv_endpoint_selects_single_registered_mv() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.register("mv_one");
    let state = HttpAdminState {
        cancel: CancellationToken::new(),
        health: HttpIngestHealth {
            executor_running: Arc::new(AtomicBool::new(true)),
            storage_reachable: Arc::new(AtomicBool::new(true)),
            runtime_ready: Arc::new(AtomicBool::new(true)),
            watermark_debug: None,
            cdc_replication_debug: None,
        },
        storage_db: None,
        storage_catalog: None,
        replication_runtime: None,
        materialized_views: Some(registry),
    };
    let app = Router::new()
        .route("/mv", get(subscribe_sse_admin))
        .with_state(state);

    let request = Request::builder()
        .method("GET")
        .uri("/mv")
        .body(Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_mv_endpoint_requires_query_when_multiple_views_exist() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.register("mv_one");
    registry.register("mv_two");
    let state = HttpAdminState {
        cancel: CancellationToken::new(),
        health: HttpIngestHealth {
            executor_running: Arc::new(AtomicBool::new(true)),
            storage_reachable: Arc::new(AtomicBool::new(true)),
            runtime_ready: Arc::new(AtomicBool::new(true)),
            watermark_debug: None,
            cdc_replication_debug: None,
        },
        storage_db: None,
        storage_catalog: None,
        replication_runtime: None,
        materialized_views: Some(registry),
    };
    let app = Router::new()
        .route("/mv", get(subscribe_sse_admin))
        .with_state(state);

    let request = Request::builder()
        .method("GET")
        .uri("/mv")
        .body(Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("json body");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        value["error"],
        "mv query parameter is required when multiple materialized views are registered"
    );
}
