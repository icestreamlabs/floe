use super::*;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use floe_executor::mv_changelog::MvChangelogBatchKind;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;

#[test]
fn batch_policy_flushes_on_row_or_byte_threshold() {
    let policy = BatchPolicy::new(3, 10).expect("batch policy");
    assert!(!policy.should_flush(1, 5));
    assert!(policy.should_flush(3, 5));
    assert!(policy.should_flush(2, 10));
}

#[test]
fn retry_policy_backoff_is_bounded_exponential() {
    let policy = RetryPolicy::new(5, Duration::from_millis(100), Duration::from_millis(500))
        .expect("retry policy");
    assert_eq!(policy.backoff_for_failure(0), Duration::from_millis(100));
    assert_eq!(policy.backoff_for_failure(1), Duration::from_millis(200));
    assert_eq!(policy.backoff_for_failure(2), Duration::from_millis(400));
    assert_eq!(policy.backoff_for_failure(3), Duration::from_millis(500));
    assert_eq!(policy.backoff_for_failure(10), Duration::from_millis(500));
}

#[test]
fn default_kafka_transactional_id_is_stable_for_sink() {
    assert_eq!(
        kafka_backend::default_kafka_transactional_id("orders sink"),
        "floe-orders_sink"
    );
    assert_eq!(
        kafka_backend::default_kafka_transactional_id("orders sink"),
        kafka_backend::default_kafka_transactional_id("orders sink")
    );
}

#[test]
fn debezium_mv_sink_encoding_builds_kafka_key_and_envelope() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("open"), Some("closed")])),
        ],
    )
    .expect("record batch");
    let changelog = MvChangelogBatch {
        version: 42,
        version_time: Some(1_234_000),
        kind: MvChangelogBatchKind::Delta,
        batch,
        diffs: vec![1, -1],
    };
    let rows = encode_changelog_batch_as_debezium(
        &changelog,
        &schema,
        &DebeziumSinkEncoding {
            source_name: "orders_sink".to_string(),
            database_name: "floe".to_string(),
            schema_name: "public".to_string(),
            table_name: "mv_orders".to_string(),
            key_columns: vec!["id".to_string()],
        },
    )
    .expect("encode Debezium sink rows");

    assert_eq!(rows.len(), 2);
    let first_key: serde_json::Value =
        serde_json::from_str(rows[0].key.as_deref().expect("key")).expect("key JSON");
    let first_value: serde_json::Value =
        serde_json::from_str(&rows[0].payload).expect("value JSON");
    assert_eq!(first_key["payload"]["id"], 1);
    assert_eq!(first_value["payload"]["op"], "c");
    assert_eq!(first_value["payload"]["after"]["status"], "open");
    assert_eq!(first_value["payload"]["before"], serde_json::Value::Null);
    assert_eq!(first_value["payload"]["source"]["name"], "orders_sink");
    assert_eq!(first_value["payload"]["source"]["db"], "floe");
    assert_eq!(first_value["payload"]["source"]["table"], "mv_orders");
    assert_eq!(
        first_value["payload"]["source"]["position"],
        "mv/mv_orders/42"
    );
    assert_eq!(first_value["payload"]["ts_ms"], 1234);

    let second_key: serde_json::Value =
        serde_json::from_str(rows[1].key.as_deref().expect("key")).expect("key JSON");
    let second_value: serde_json::Value =
        serde_json::from_str(&rows[1].payload).expect("value JSON");
    assert_eq!(second_key["payload"]["id"], 2);
    assert_eq!(second_value["payload"]["op"], "d");
    assert_eq!(second_value["payload"]["before"]["status"], "closed");
    assert_eq!(second_value["payload"]["after"], serde_json::Value::Null);
    assert_eq!(second_value["payload"]["ts_ms"], 1234);
}

#[test]
fn debezium_mv_sink_encoding_preserves_diff_multiplicity() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("open"),
                Some("closed"),
                Some("ignored"),
            ])),
        ],
    )
    .expect("record batch");
    let changelog = MvChangelogBatch {
        version: 42,
        version_time: Some(1_234_000),
        kind: MvChangelogBatchKind::Delta,
        batch,
        diffs: vec![2, -2, 0],
    };
    let rows = encode_changelog_batch_as_debezium(
        &changelog,
        &schema,
        &DebeziumSinkEncoding {
            source_name: "orders_sink".to_string(),
            database_name: "floe".to_string(),
            schema_name: "public".to_string(),
            table_name: "mv_orders".to_string(),
            key_columns: vec!["id".to_string()],
        },
    )
    .expect("encode Debezium sink rows");

    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter().map(|row| row.row_idx).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    let operations = rows
        .iter()
        .map(|row| {
            let value: serde_json::Value = serde_json::from_str(&row.payload).expect("value JSON");
            value["payload"]["op"].as_str().unwrap().to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(operations, vec!["c", "c", "d", "d"]);
}

#[test]
fn kafka_checkpoint_selection_scans_past_unrelated_suffix_records() {
    let target_payload = serde_json::to_vec(&serde_json::json!({
        "sink": "sink_a",
        "mv_name": "mv_a",
        "last_emitted_mv_version": 17,
        "committed_at_unix_ms": 1
    }))
    .expect("target checkpoint JSON");
    let unrelated_payload = serde_json::to_vec(&serde_json::json!({
        "sink": "sink_b",
        "mv_name": "mv_b",
        "last_emitted_mv_version": 99,
        "committed_at_unix_ms": 2
    }))
    .expect("unrelated checkpoint JSON");

    let mut latest = None;
    consider_kafka_checkpoint_payload(&mut latest, 10, &target_payload, "sink_a", "mv_a");
    for offset in 11..3011 {
        consider_kafka_checkpoint_payload(
            &mut latest,
            offset,
            &unrelated_payload,
            "sink_a",
            "mv_a",
        );
    }

    let (_, cursor) = latest.expect("target checkpoint should still be found");
    assert_eq!(cursor.sink, "sink_a");
    assert_eq!(cursor.mv_name, "mv_a");
    assert_eq!(cursor.last_emitted_mv_version, 17);
    assert_eq!(kafka_checkpoint_key("sink_a", "mv_a"), "sink_a\0mv_a");
}

#[test]
fn http_idempotency_keys_include_mv_version_and_row_index() {
    let rows = vec![
        SinkRecord {
            version: 7,
            row_idx: 0,
            key: None,
            json: serde_json::json!({"k": 1}),
            payload: "{\"k\":1}".to_string(),
            byte_len: 7,
        },
        SinkRecord {
            version: 7,
            row_idx: 1,
            key: None,
            json: serde_json::json!({"k": 2}),
            payload: "{\"k\":2}".to_string(),
            byte_len: 7,
        },
    ];
    let (batch_key, keys) = build_http_idempotency_keys(&rows);
    assert_eq!(batch_key, "batch:7:0..7:1");
    assert_eq!(keys, "7:0,7:1");
}

#[derive(Clone)]
struct HttpRetryState {
    attempts: Arc<AtomicUsize>,
    keys: Arc<Mutex<Vec<String>>>,
}

#[tokio::test]
async fn http_retry_reuses_same_idempotency_key() {
    async fn collect(
        State(state): State<HttpRetryState>,
        headers: HeaderMap,
        Json(_payload): Json<serde_json::Value>,
    ) -> StatusCode {
        if let Some(value) = headers.get("idempotency-key")
            && let Ok(key) = value.to_str()
        {
            state.keys.lock().expect("lock").push(key.to_string());
        }
        let attempt = state.attempts.fetch_add(1, Ordering::Relaxed);
        if attempt == 0 {
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::OK
        }
    }

    let state = HttpRetryState {
        attempts: Arc::new(AtomicUsize::new(0)),
        keys: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/collect", post(collect))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let client = Client::new();
    let cancel = CancellationToken::new();
    let rows = vec![SinkRecord {
        version: 11,
        row_idx: 4,
        key: None,
        json: serde_json::json!({"auction": 11}),
        payload: "{\"auction\":11}".to_string(),
        byte_len: 14,
    }];
    post_http_batch_with_retry(
        "sink_http",
        &client,
        &format!("http://{addr}/collect"),
        &rows,
        RetryPolicy::new(3, Duration::from_millis(10), Duration::from_millis(50))
            .expect("retry policy"),
        &cancel,
    )
    .await
    .expect("http sink retry");
    server.abort();
    let _ = server.await;

    let keys = state.keys.lock().expect("lock");
    assert!(keys.len() >= 2);
    assert!(keys.iter().all(|key| key == "11:4"));
}

#[tokio::test]
async fn http_sink_crash_mid_batch_emits_no_request_before_flush() {
    #[derive(Clone)]
    struct RequestCount(Arc<AtomicUsize>);
    async fn count(
        State(state): State<RequestCount>,
        Json(_payload): Json<serde_json::Value>,
    ) -> StatusCode {
        state.0.fetch_add(1, Ordering::Relaxed);
        StatusCode::OK
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/collect", post(count))
        .with_state(RequestCount(Arc::clone(&counter)));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (tx, rx) = mpsc::channel(1);
    let tracker = SinkQueueTracker::new("sink_http");
    let url = format!("http://{addr}/collect");
    let worker = tokio::spawn(async move {
        let client = Client::new();
        run_http_worker(HttpWorkerConfig {
            sink_name: "sink_http",
            mv_name: "mv_bid",
            client: &client,
            url: &url,
            rx,
            tracker,
            batch_policy: BatchPolicy::new(1000, usize::MAX).expect("batch policy"),
            retry_policy: RetryPolicy::new(3, Duration::from_millis(10), Duration::from_millis(20))
                .expect("retry policy"),
            checkpoint_tx: None,
            cancel: CancellationToken::new(),
        })
        .await
    });

    tx.send(SinkEvent::Rows(vec![SinkRecord {
        version: 12,
        row_idx: 0,
        key: None,
        json: serde_json::json!({"auction": 12}),
        payload: "{\"auction\":12}".to_string(),
        byte_len: 14,
    }]))
    .await
    .expect("send row");
    tx.send(SinkEvent::Rows(vec![SinkRecord {
        version: 12,
        row_idx: 1,
        key: None,
        json: serde_json::json!({"auction": 13}),
        payload: "{\"auction\":13}".to_string(),
        byte_len: 14,
    }]))
    .await
    .expect("send buffered row after worker consumed first event");
    worker.abort();
    let _ = worker.await;
    server.abort();
    let _ = server.await;

    assert_eq!(counter.load(Ordering::Relaxed), 0);
}
