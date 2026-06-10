use super::*;
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

#[test]
fn build_batch_limits_per_connector() {
    let pending_events = core_source::PendingAppendIngestEventCounter::default();
    pending_events.record_enqueue(4);
    let mut queues = vec![
        ConnectorQueue {
            id: 0,
            name: "a".to_string(),
            pending: VecDeque::from([queued_event("s1", 1), queued_event("s1", 2)]),
        },
        ConnectorQueue {
            id: 1,
            name: "b".to_string(),
            pending: VecDeque::from([queued_event("s2", 3), queued_event("s2", 4)]),
        },
    ];

    let source_id_by_name = HashMap::from([("s1".to_string(), 0usize), ("s2".to_string(), 1usize)]);
    let selection = build_batch(BuildBatchRequest {
        queues: &mut queues,
        source_id_by_name: &source_id_by_name,
        source_count: 2,
        start_index: 0,
        max_batch: 10,
        max_per_source: 10,
        max_per_connector: 1,
        pending_events: &pending_events,
    });
    assert_eq!(selection.batch.len(), 2);
    assert_eq!(selection.batch[0].source_id, Some(0));
    assert_eq!(selection.batch[1].source_id, Some(1));
    assert_eq!(selection.per_connector_counts, vec![1, 1]);
    assert_eq!(queues[0].pending.len(), 1);
    assert_eq!(queues[1].pending.len(), 1);
    assert_eq!(pending_events.pending(), 2);
}

#[test]
fn build_batch_limits_per_source() {
    let pending_events = core_source::PendingAppendIngestEventCounter::default();
    pending_events.record_enqueue(3);
    let mut queues = vec![ConnectorQueue {
        id: 0,
        name: "a".to_string(),
        pending: VecDeque::from([
            queued_event("s1", 1),
            queued_event("s1", 2),
            queued_event("s1", 3),
        ]),
    }];

    let source_id_by_name = HashMap::from([("s1".to_string(), 0usize)]);
    let selection = build_batch(BuildBatchRequest {
        queues: &mut queues,
        source_id_by_name: &source_id_by_name,
        source_count: 1,
        start_index: 0,
        max_batch: 10,
        max_per_source: 1,
        max_per_connector: 10,
        pending_events: &pending_events,
    });
    assert_eq!(selection.batch.len(), 1);
    assert_eq!(selection.batch[0].source_id, Some(0));
    assert_eq!(queues[0].pending.len(), 2);
    assert_eq!(pending_events.pending(), 2);
}

#[test]
fn build_batch_splits_raw_kafka_batches_by_limits() {
    let pending_events = core_source::PendingAppendIngestEventCounter::default();
    pending_events.record_enqueue(3);
    let raw_batch = core_source::KafkaRawIngestBatch {
        source: "s1".to_string(),
        records: (0..3)
            .map(|offset| core_source::KafkaRawIngestRecord {
                payload: br#"{"id":1}"#.to_vec(),
                topic: Arc::<str>::from("topic"),
                partition: 0,
                offset,
                event_time_ms: None,
            })
            .collect(),
    };
    let mut queues = vec![ConnectorQueue {
        id: 0,
        name: "kafka".to_string(),
        pending: VecDeque::from([QueuedAppendIngestItem::KafkaRaw(
            QueuedKafkaRawIngestBatch {
                batch: raw_batch,
                commit_ack: None,
            },
        )]),
    }];

    let source_id_by_name = HashMap::from([("s1".to_string(), 0usize)]);
    let selection = build_batch(BuildBatchRequest {
        queues: &mut queues,
        source_id_by_name: &source_id_by_name,
        source_count: 1,
        start_index: 0,
        max_batch: 10,
        max_per_source: 2,
        max_per_connector: 10,
        pending_events: &pending_events,
    });

    assert_eq!(selection.batch.len(), 0);
    assert_eq!(selection.kafka_raw_batches.len(), 1);
    assert_eq!(selection.kafka_raw_batches[0].source_id, Some(0));
    assert_eq!(selection.kafka_raw_batches[0].batch.len(), 2);
    assert_eq!(selection.selected_rows, 2);
    assert_eq!(selection.per_connector_counts, vec![2]);
    assert_eq!(queues[0].pending_rows(), 1);
    assert_eq!(pending_events.pending(), 1);
}

#[test]
fn build_batch_splits_arrow_kafka_batches_without_metadata_ranges() {
    let pending_events = core_source::PendingAppendIngestEventCounter::default();
    pending_events.record_enqueue(3);
    let mut queues = vec![ConnectorQueue {
        id: 0,
        name: "kafka".to_string(),
        pending: VecDeque::from([QueuedAppendIngestItem::KafkaArrow(
            QueuedKafkaArrowIngestBatch {
                batch: kafka_arrow_batch("s1", 3, false),
                commit_ack: None,
            },
        )]),
    }];

    let source_id_by_name = HashMap::from([("s1".to_string(), 0usize)]);
    let selection = build_batch(BuildBatchRequest {
        queues: &mut queues,
        source_id_by_name: &source_id_by_name,
        source_count: 1,
        start_index: 0,
        max_batch: 10,
        max_per_source: 2,
        max_per_connector: 10,
        pending_events: &pending_events,
    });

    assert_eq!(selection.batch.len(), 0);
    assert_eq!(selection.kafka_arrow_batches.len(), 1);
    assert_eq!(selection.kafka_arrow_batches[0].source_id, Some(0));
    assert_eq!(selection.kafka_arrow_batches[0].batch.len(), 2);
    assert_eq!(
        selection.kafka_arrow_batches[0].batch.execution.num_rows(),
        2
    );
    assert_eq!(selection.selected_rows, 2);
    assert_eq!(queues[0].pending_rows(), 1);
    assert_eq!(pending_events.pending(), 1);
}

#[test]
fn build_batch_defers_arrow_kafka_batches_with_metadata_ranges_when_partially_filled() {
    let pending_events = core_source::PendingAppendIngestEventCounter::default();
    pending_events.record_enqueue(4);
    let mut queues = vec![ConnectorQueue {
        id: 0,
        name: "kafka".to_string(),
        pending: VecDeque::from([
            queued_event("s1", 1),
            QueuedAppendIngestItem::KafkaArrow(QueuedKafkaArrowIngestBatch {
                batch: kafka_arrow_batch("s1", 3, true),
                commit_ack: None,
            }),
        ]),
    }];

    let source_id_by_name = HashMap::from([("s1".to_string(), 0usize)]);
    let selection = build_batch(BuildBatchRequest {
        queues: &mut queues,
        source_id_by_name: &source_id_by_name,
        source_count: 1,
        start_index: 0,
        max_batch: 10,
        max_per_source: 3,
        max_per_connector: 10,
        pending_events: &pending_events,
    });

    assert_eq!(selection.batch.len(), 1);
    assert_eq!(selection.kafka_arrow_batches.len(), 0);
    assert_eq!(selection.selected_rows, 1);
    assert_eq!(queues[0].pending_rows(), 3);
    assert_eq!(pending_events.pending(), 3);
}

fn kafka_arrow_batch(
    source: &str,
    rows: usize,
    include_metadata_range: bool,
) -> core_source::KafkaArrowIngestBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let values = (0..rows)
        .map(|value| i64::try_from(value).unwrap())
        .collect::<Vec<_>>();
    let execution =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef])
            .expect("record batch");
    let records = (0..rows)
        .map(|offset| core_source::KafkaArrowIngestRecord {
            topic: Arc::<str>::from("topic"),
            partition: 0,
            offset: i64::try_from(offset).unwrap(),
            event_time_ms: None,
        })
        .collect();
    let kafka_metadata_ranges = include_metadata_range
        .then(|| core_source::KafkaArrowIngestJournalRange {
            topic: Arc::<str>::from("topic"),
            partition: 0,
            start_offset: 0,
            end_offset: i64::try_from(rows.saturating_sub(1)).unwrap(),
            row_count: u64::try_from(rows).unwrap(),
            checksum: 42,
        })
        .into_iter()
        .collect();
    core_source::KafkaArrowIngestBatch {
        source: source.to_string(),
        execution,
        query: None,
        records,
        kafka_metadata_ranges,
    }
}

#[test]
fn merge_sql_sinks_validates_mv_reference() {
    let mut sink_specs = Vec::new();
    let sql_sink_specs = vec![SinkSpec {
        name: "sink_missing".to_string(),
        config: SinkConfig::File {
            name: Some("sink_missing".to_string()),
            path: "/tmp/out.jsonl".to_string(),
            mv: "missing_mv".to_string(),
            with_snapshot: Some(false),
            as_of: None,
            append: Some(true),
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
        },
    }];
    let materialized_view_map = HashMap::new();

    let err = merge_sql_sinks(&mut sink_specs, sql_sink_specs, &materialized_view_map)
        .expect_err("expected unknown mv validation error");
    assert!(
        err.to_string()
            .contains("references unknown materialized view 'missing_mv'")
    );
}

#[test]
fn merge_sql_sinks_rejects_duplicate_names() {
    let mut sink_specs = vec![SinkSpec {
        name: "sink_dup".to_string(),
        config: SinkConfig::File {
            name: Some("sink_dup".to_string()),
            path: "/tmp/first.jsonl".to_string(),
            mv: "mv_a".to_string(),
            with_snapshot: Some(false),
            as_of: None,
            append: Some(true),
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
        },
    }];
    let sql_sink_specs = vec![SinkSpec {
        name: "sink_dup".to_string(),
        config: SinkConfig::Http {
            name: Some("sink_dup".to_string()),
            url: "http://localhost:8080".to_string(),
            mv: "mv_a".to_string(),
            with_snapshot: Some(true),
            as_of: None,
            batch_size: Some(1),
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
        },
    }];
    let mut materialized_view_map = HashMap::new();
    materialized_view_map.insert(
        "mv_a".to_string(),
        MaterializedViewDefinition::new("mv_a", "SELECT 1", false),
    );

    let err = merge_sql_sinks(&mut sink_specs, sql_sink_specs, &materialized_view_map)
        .expect_err("expected duplicate sink name error");
    assert!(err.to_string().contains("duplicate sink name 'sink_dup'"));
}

#[test]
fn validate_single_materialized_view_rejects_multiple_views() {
    let views = vec![
        MaterializedViewDefinition::new("mv_a", "SELECT 1", false),
        MaterializedViewDefinition::new("mv_b", "SELECT 2", false),
    ];

    let err = validate_single_materialized_view(&views).expect_err("expected single-MV error");

    assert!(
        err.to_string()
            .contains("at most one materialized view per process")
    );
    assert!(err.to_string().contains("mv_a, mv_b"));
}

#[test]
fn runtime_failure_records_first_error_only() {
    let state = Arc::new(StdMutex::new(None::<String>));
    record_runtime_failure(&state, "first".to_string());
    record_runtime_failure(&state, "second".to_string());
    assert_eq!(
        state.lock().expect("runtime failure lock").as_deref(),
        Some("first")
    );
}

#[test]
fn runtime_failure_recovers_poisoned_state_lock() {
    let state = Arc::new(StdMutex::new(None::<String>));
    let poisoned_state = Arc::clone(&state);
    let _ = std::thread::spawn(move || {
        let _guard = poisoned_state.lock().expect("lock runtime failure state");
        panic!("poison runtime failure state");
    })
    .join();

    record_runtime_failure(&state, "after poison".to_string());

    let guard = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(guard.as_deref(), Some("after poison"));
}

#[test]
fn collect_mv_versions_for_commit_uses_logical_overlay_versions() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let handle = registry.register("mv_overlay".to_string());
    handle.publish_logical_version(7);

    let mut last_versions = HashMap::new();
    let committed = collect_mv_versions_for_commit(&registry, &mut last_versions);
    assert_eq!(
        committed,
        vec![MaterializedViewTickVersion {
            view: "mv_overlay".to_string(),
            version: 7,
        }]
    );

    let committed_again = collect_mv_versions_for_commit(&registry, &mut last_versions);
    assert!(
        committed_again.is_empty(),
        "logical version should only commit once"
    );
}

#[tokio::test]
async fn mv_visibility_wait_returns_immediately_for_visible_versions() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let handle = registry.register("mv_visible".to_string());
    handle.publish_logical_version(4);
    let cancel = CancellationToken::new();

    let waited = wait_for_materialized_views_visible(&registry, 4, &cancel)
        .await
        .expect("visible version wait");

    assert_eq!(waited, 0);
}

#[tokio::test]
async fn mv_visibility_wait_blocks_until_target_version_is_published() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let handle = registry.register("mv_async".to_string());
    let waiting_registry = Arc::clone(&registry);
    let cancel = CancellationToken::new();
    let waiting_cancel = cancel.clone();
    let wait_task = tokio::spawn(async move {
        wait_for_materialized_views_visible(&waiting_registry, 3, &waiting_cancel).await
    });

    tokio::task::yield_now().await;
    assert!(
        !wait_task.is_finished(),
        "wait should remain pending before any version is published"
    );

    handle.publish_logical_version(2);
    tokio::task::yield_now().await;
    assert!(
        !wait_task.is_finished(),
        "wait should remain pending before target version is published"
    );

    handle.publish_logical_version(3);
    assert_eq!(wait_task.await.expect("join visibility wait").unwrap(), 1);
}

#[tokio::test]
async fn mv_visibility_wait_skips_views_with_disabled_barrier() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let handle = registry.register("mv_coalesced".to_string());
    handle.set_commit_visibility_barrier_enabled(false);
    let cancel = CancellationToken::new();

    let waited = wait_for_materialized_views_visible(&registry, 10, &cancel)
        .await
        .expect("visibility wait should skip coalesced view");

    assert_eq!(waited, 0);
}

#[test]
fn kafka_offset_commit_state_preserves_idle_topic_offsets() {
    let mut committed = HashMap::new();
    advance_kafka_offset_commit_state(
        &mut committed,
        &HashMap::from([
            ((Arc::<str>::from("auction"), 0_i32), 41_i64),
            ((Arc::<str>::from("person"), 0_i32), 17_i64),
        ]),
    );

    advance_kafka_offset_commit_state(
        &mut committed,
        &HashMap::from([((Arc::<str>::from("person"), 0_i32), 22_i64)]),
    );

    assert_eq!(
        build_kafka_offset_commit(2, &committed),
        KafkaOffsetCommit {
            tick_id: 2,
            offsets: vec![
                KafkaTopicPartitionOffset {
                    topic: "auction".to_string(),
                    partition: 0,
                    offset: 41,
                },
                KafkaTopicPartitionOffset {
                    topic: "person".to_string(),
                    partition: 0,
                    offset: 22,
                },
            ],
        }
    );
}

#[test]
fn postgres_cdc_commit_state_preserves_idle_slots() {
    let mut committed = HashMap::new();
    advance_postgres_cdc_commit_state(
        &mut committed,
        &HashMap::from([
            ("slot_a".to_string(), (16_u64, "0/10".to_string())),
            ("slot_b".to_string(), (32_u64, "0/20".to_string())),
        ]),
    );

    advance_postgres_cdc_commit_state(
        &mut committed,
        &HashMap::from([("slot_b".to_string(), (48_u64, "0/30".to_string()))]),
    );

    assert_eq!(
        build_postgres_cdc_commit(3, &committed),
        PostgresCdcCommit {
            tick_id: 3,
            slots: vec![
                PostgresSlotCommit {
                    slot: "slot_a".to_string(),
                    lsn: "0/10".to_string(),
                },
                PostgresSlotCommit {
                    slot: "slot_b".to_string(),
                    lsn: "0/30".to_string(),
                },
            ],
        }
    );
}

#[test]
fn cli_connector_creation_flags_collects_explicit_connector_inputs() {
    let mut args = default_run_args();
    args.config = Some("node.toml".to_string());
    args.input_file = Some("/tmp/events.jsonl".to_string());
    args.kafka_brokers = Some("localhost:9092".to_string());
    args.kafka_topics = vec!["nexmark_bid".to_string()];
    args.http_port = Some(8080);
    let flags = cli_connector_creation_flags(&args);
    assert_eq!(
        flags,
        vec![
            "--http-port",
            "--kafka-brokers",
            "--kafka-topics",
            "--input-file"
        ]
    );
}

#[test]
fn log_operator_hints_handles_empty_materialized_views() {
    let connectors = vec![config::ConnectorSpec {
        name: "generator".to_string(),
        config: ConnectorConfig::Generator {
            name: None,
            events_per_second: Some(10.0),
            max_events: None,
        },
    }];
    let available_sources = BTreeSet::from(["nexmark_bid".to_string()]);
    let args = default_run_args();
    log_operator_hints(&connectors, &available_sources, &[], &[], &args);
}

#[test]
fn log_startup_banner_handles_mixed_connectors() {
    let args = default_run_args();
    let connectors = vec![
        config::ConnectorSpec {
            name: "generator".to_string(),
            config: ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(10.0),
                max_events: None,
            },
        },
        config::ConnectorSpec {
            name: "http".to_string(),
            config: ConnectorConfig::Http {
                name: None,
                host: Some("127.0.0.1".to_string()),
                port: 8080,
                default_source: Some("nexmark_bid".to_string()),
            },
        },
    ];
    log_startup_banner(&args, &connectors);
}

#[test]
fn apply_runtime_config_defaults_uses_config_when_cli_values_are_defaults() {
    let mut args = default_run_args();
    let config = NodeConfig {
        runtime: config::RuntimeConfig {
            events_per_second: Some(25.0),
            max_events: Some(123),
            ingest_queue_capacity: Some(2048),
            ingest_batch_size: Some(512),
            ingest_batch_per_source: Some(128),
            ingest_batch_per_connector: Some(96),
            mv_retain_last: Some(7),
            http_host: Some("0.0.0.0".to_string()),
            kafka_group_id: Some("cfg-group".to_string()),
            kafka_poll_ms: Some(250),
            kafka_max_messages: Some(1024),
            watermark_idle_source_ms: Some(45_000),
            subscribe_channel_capacity: Some(512),
            subscribe_max_catchup_versions: Some(64),
            admin_port: Some(9090),
            pgwire_addr: Some("127.0.0.1:7777".to_string()),
            pgwire_enabled: Some(false),
            mv_flush: config::MvFlushConfig::default(),
            mv_snapshot: config::MvSnapshotConfig::default(),
        },
        storage: config::StorageConfig {
            await_durable: Some(true),
            data_dir: Some("/tmp/floe-data".to_string()),
            object_store_from_env: false,
            object_store_env_file: None,
            slatedb_name: None,
            slatedb_config: Some("/tmp/slatedb.toml".to_string()),
            slatedb_env_prefix: Some("CFG_".to_string()),
            slatedb_close_timeout_ms: Some(5_000),
            zset_compaction_max_chain_len: Some(99),
            zset_compaction_max_segments: Some(500),
            zset_compaction_backoff_ticks: Some(8),
            zset_compaction_max_concurrent_jobs: Some(4),
            zset_gc_grace_period_ms: Some(1_000),
            source_journal: None,
        },
        maintenance: config::MaintenanceConfig {
            paused: Some(true),
            inspect_namespace: vec!["ns.inspect".to_string()],
            compact_namespace: vec!["ns.compact".to_string()],
            gc_namespace: vec!["ns.gc".to_string()],
        },
        ..NodeConfig::default()
    };

    apply_runtime_config_defaults(&mut args, &config);

    assert_eq!(args.events_per_second, 25.0);
    assert_eq!(args.max_events, Some(123));
    assert_eq!(args.ingest_queue_capacity, 2048);
    assert_eq!(args.ingest_batch_size, 512);
    assert_eq!(args.ingest_batch_per_source, 128);
    assert_eq!(args.ingest_batch_per_connector, 96);
    assert_eq!(args.mv_retain_last, 7);
    assert_eq!(args.http_host, "0.0.0.0");
    assert_eq!(args.kafka_group_id, "cfg-group");
    assert_eq!(args.kafka_poll_ms, 250);
    assert_eq!(args.kafka_max_messages, 1024);
    assert_eq!(args.pgwire_addr.as_deref(), Some("127.0.0.1:7777"));
    assert!(args.disable_pgwire);
    assert_eq!(args.admin_port, Some(9090));
    assert_eq!(args.watermark_idle_source_ms, Some(45_000));
    assert_eq!(args.subscribe_channel_capacity, Some(512));
    assert_eq!(args.subscribe_max_catchup_versions, Some(64));
    assert_eq!(args.slatedb_await_durable, Some(true));
    assert_eq!(args.data_dir.as_deref(), Some("/tmp/floe-data"));
    assert_eq!(args.slatedb_config.as_deref(), Some("/tmp/slatedb.toml"));
    assert_eq!(args.slatedb_env_prefix.as_deref(), Some("CFG_"));
    assert_eq!(args.slatedb_close_timeout_ms, Some(5_000));
    assert_eq!(args.zset_compaction_max_chain_len, 99);
    assert_eq!(args.zset_compaction_max_segments, 500);
    assert_eq!(args.zset_compaction_backoff_ticks, 8);
    assert_eq!(args.zset_compaction_max_concurrent_jobs, 4);
    assert_eq!(args.zset_gc_grace_period_ms, 1_000);
    assert!(args.maintenance_paused);
    assert_eq!(
        args.maintenance_inspect_namespace,
        vec!["ns.inspect".to_string()]
    );
    assert_eq!(
        args.maintenance_compact_namespace,
        vec!["ns.compact".to_string()]
    );
    assert_eq!(args.maintenance_gc_namespace, vec!["ns.gc".to_string()]);
}
