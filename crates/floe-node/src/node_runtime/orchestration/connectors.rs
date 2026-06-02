use super::*;

pub(super) struct SpawnedConnectorTasks {
    pub(super) handles: Vec<JoinHandle<()>>,
    pub(super) queues: Vec<ConnectorQueue>,
    pub(super) kafka_commit_senders: Vec<watch::Sender<KafkaOffsetCommit>>,
    pub(super) postgres_cdc_commit_senders: Vec<watch::Sender<PostgresCdcCommit>>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_connector_tasks(
    connector_specs: Vec<floe_config::ConnectorSpec>,
    definitions: Vec<SourceDefinition>,
    postgres_cdc_runtime_plans_by_connector: &HashMap<String, PostgresCdcRuntimePlan>,
    connector_sender: mpsc::Sender<core_source::RoutedAppendIngestEventBatch>,
    pending_event_counter: core_source::PendingAppendIngestEventCounter,
    ingest_cancel: CancellationToken,
    runtime_cancel: CancellationToken,
    runtime_failure: Arc<StdMutex<Option<String>>>,
    run_args: &cli::RunArgs,
    recovered_kafka_offsets: Vec<KafkaCheckpointOffset>,
    source_journal_skipped_sources: BTreeSet<String>,
    executor_running: Arc<AtomicBool>,
    storage_reachable: Arc<AtomicBool>,
    runtime_ready: Arc<AtomicBool>,
    watermark_debug: Arc<tokio::sync::RwLock<http_ingest::WatermarkDebugState>>,
    cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    cdc_transaction_sender: mpsc::Sender<QueuedCdcTransaction>,
    cdc_table_store: CdcTableStore,
    postgres_cdc_settings: floe_config::PostgresCdcConfig,
) -> SpawnedConnectorTasks {
    let mut connector_handles: Vec<JoinHandle<()>> = Vec::new();
    let mut connector_queues: Vec<ConnectorQueue> = Vec::new();
    let mut kafka_commit_senders: Vec<watch::Sender<KafkaOffsetCommit>> = Vec::new();
    let mut postgres_cdc_commit_senders: Vec<watch::Sender<PostgresCdcCommit>> = Vec::new();
    for (connector_id, connector) in connector_specs.into_iter().enumerate() {
        let sender = core_source::routed_sender(
            connector_id,
            connector_sender.clone(),
            pending_event_counter.clone(),
        );
        let postgres_cdc_runtime_plan = postgres_cdc_runtime_plans_by_connector
            .get(&connector.name)
            .cloned();
        connector_queues.push(ConnectorQueue::new(connector_id, connector.name.clone()));
        let cancel = ingest_cancel.clone();
        let runtime_cancel = runtime_cancel.clone();
        let failure_state = Arc::clone(&runtime_failure);
        match connector.config {
            ConnectorConfig::Http {
                host,
                port,
                default_source,
                ..
            } => {
                let config = HttpIngestConfig {
                    host: host.unwrap_or_else(|| run_args.http_host.clone()),
                    port,
                    default_source,
                    health: Some(HttpIngestHealth {
                        executor_running: Arc::clone(&executor_running),
                        storage_reachable: Arc::clone(&storage_reachable),
                        runtime_ready: Arc::clone(&runtime_ready),
                        watermark_debug: Some(Arc::clone(&watermark_debug)),
                        cdc_replication_debug: Some(Arc::clone(&cdc_replication_debug)),
                    }),
                };
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    if let Err(err) =
                        http_ingest::run_http_ingest(config, sender, cancel.clone()).await
                    {
                        tracing::error!(error = %err, "HTTP ingest server failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("HTTP ingest connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::Kafka {
                brokers,
                topics,
                group_id,
                default_source,
                poll_ms,
                max_messages_per_tick,
                format,
                ..
            } => {
                let group_id = group_id.unwrap_or_else(|| run_args.kafka_group_id.clone());
                let poll_timeout = Duration::from_millis(poll_ms.unwrap_or(run_args.kafka_poll_ms));
                let max_messages_per_tick =
                    max_messages_per_tick.unwrap_or(run_args.kafka_max_messages);
                let connector_has_recovered_offsets = recovered_kafka_offsets
                    .iter()
                    .any(|offset| topics.iter().any(|topic| topic == &offset.topic));
                let should_replay_from_kafka = connector_has_recovered_offsets
                    && (default_source
                        .as_ref()
                        .is_some_and(|source| source_journal_skipped_sources.contains(source))
                        || default_source.is_none() && !source_journal_skipped_sources.is_empty());
                let resume_from_offsets = if should_replay_from_kafka {
                    recovered_kafka_offsets
                        .iter()
                        .filter(|offset| topics.iter().any(|topic| topic == &offset.topic))
                        .map(|offset| KafkaTopicPartitionOffset {
                            topic: offset.topic.clone(),
                            partition: offset.partition,
                            offset: offset.offset,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let (commit_tx, commit_rx) = watch::channel(KafkaOffsetCommit::default());
                kafka_commit_senders.push(commit_tx);
                let definitions = definitions.clone();
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = KafkaConnectorConfig {
                        brokers,
                        topics,
                        group_id,
                        default_source,
                        poll_timeout,
                        max_messages_per_tick,
                        message_format: format,
                        commit_offsets_rx: Some(commit_rx),
                        resume_from_offsets,
                    };
                    let mut connector =
                        match KafkaConnector::new(config, definitions, HashMap::new()) {
                            Ok(connector) => connector,
                            Err(err) => {
                                tracing::error!(error = %err, "Kafka connector config invalid");
                                record_runtime_failure(
                                    &failure_state,
                                    format!("Kafka connector config invalid: {err}"),
                                );
                                runtime_cancel.cancel();
                                return;
                            }
                        };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "Kafka connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("Kafka connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::File {
                path,
                default_source,
                ..
            } => {
                let definitions = definitions.clone();
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = FileConnectorConfig {
                        path: path.into(),
                        default_source,
                    };
                    let mut connector = FileConnector::new(config, definitions);
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "File connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("File connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::Generator {
                events_per_second,
                max_events,
                ..
            } => {
                let events_per_second = events_per_second.unwrap_or(run_args.events_per_second);
                let max_events = max_events.or(run_args.max_events);
                let generator_config = floe_node_core::generator::Config {
                    events_per_second,
                    max_events,
                };
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let mut connector =
                        match floe_node_core::generator::NexmarkConnector::new(generator_config) {
                            Ok(connector) => connector,
                            Err(err) => {
                                tracing::error!(error = %err, "Nexmark connector config invalid");
                                record_runtime_failure(
                                    &failure_state,
                                    format!("Nexmark connector config invalid: {err}"),
                                );
                                runtime_cancel.cancel();
                                return;
                            }
                        };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "Nexmark connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("Nexmark connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::ObjectStore {
                url,
                default_source,
                ..
            } => {
                let definitions = definitions.clone();
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = ObjectStoreConnectorConfig {
                        url,
                        default_source,
                    };
                    let mut connector = ObjectStoreConnector::new(config, definitions);
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "Object store connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("Object store connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::PostgresCdc {
                connection,
                slot,
                publication,
                auto_create_slot,
                auto_create_publication,
                ..
            } => {
                let publication = publication.unwrap_or_else(default_postgres_publication);
                let Some(runtime_plan) = postgres_cdc_runtime_plan else {
                    tracing::error!(
                        connector = %connector.name,
                        "Postgres CDC connector is not bound to a CDC table or replication pipeline"
                    );
                    record_runtime_failure(
                        &failure_state,
                        format!(
                            "Postgres CDC connector '{}' must be used by CREATE TABLE ... FROM or CREATE REPLICATION PIPELINE",
                            connector.name
                        ),
                    );
                    runtime_cancel.cancel();
                    continue;
                };
                let (commit_tx, commit_rx) = watch::channel(PostgresCdcCommit::default());
                postgres_cdc_commit_senders.push(commit_tx);
                let failure_state = Arc::clone(&failure_state);
                let config = PostgresCdcSourceConfig {
                    connection_string: connection,
                    slot,
                    publication,
                    auto_create_slot: auto_create_slot.unwrap_or(true),
                    auto_create_publication: auto_create_publication.unwrap_or(true),
                    commit_lsn_rx: Some(commit_rx),
                };
                tracing::info!(
                    connector = %connector.name,
                    source = %runtime_plan.source_id.as_str(),
                    tables = runtime_plan.schemas.len(),
                    schema_evolution_policy = ?runtime_plan.schema_evolution_policy,
                    "using native Postgres CDC table runtime"
                );
                let transaction_sender = cdc_transaction_sender.clone();
                let table_store = cdc_table_store.clone();
                let cdc_replication_debug = Arc::clone(&cdc_replication_debug);
                let snapshot_settings = postgres_cdc_settings.snapshot;
                let reconnect_settings = postgres_cdc_settings.reconnect;
                connector_handles.push(tokio::spawn(async move {
                    if let Err(err) = run_native_postgres_cdc_connector(
                        config,
                        runtime_plan,
                        snapshot_settings,
                        reconnect_settings,
                        table_store,
                        cdc_replication_debug,
                        transaction_sender,
                        cancel.clone(),
                    )
                    .await
                    {
                        if cancel.is_cancelled() {
                            tracing::debug!(
                                error = %err,
                                "native Postgres CDC connector stopped during shutdown"
                            );
                            return;
                        }
                        tracing::error!(error = %err, "native Postgres CDC connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("native Postgres CDC connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
        }
    }
    SpawnedConnectorTasks {
        handles: connector_handles,
        queues: connector_queues,
        kafka_commit_senders,
        postgres_cdc_commit_senders,
    }
}
