use super::*;

pub(super) struct SpawnedConnectorTasks {
    pub(super) handles: Vec<JoinHandle<()>>,
    pub(super) queues: Vec<ConnectorQueue>,
    pub(super) kafka_commit_senders: Vec<watch::Sender<KafkaOffsetCommit>>,
    pub(super) postgres_cdc_commit_senders: Vec<watch::Sender<PostgresCdcCommit>>,
}

pub(super) struct SpawnConnectorTasksConfig<'a> {
    pub(super) connector_specs: Vec<floe_config::ConnectorSpec>,
    pub(super) definitions: Vec<SourceDefinition>,
    pub(super) postgres_cdc_runtime_plans_by_connector: &'a HashMap<String, PostgresCdcRuntimePlan>,
    pub(super) connector_sender: mpsc::Sender<core_source::RoutedAppendIngestEventBatch>,
    pub(super) pending_event_counter: core_source::PendingAppendIngestEventCounter,
    pub(super) ingest_cancel: CancellationToken,
    pub(super) runtime_cancel: CancellationToken,
    pub(super) runtime_failure: Arc<StdMutex<Option<String>>>,
    pub(super) run_args: &'a cli::RunArgs,
    pub(super) recovered_kafka_offsets: Vec<KafkaCheckpointOffset>,
    pub(super) source_journal_skipped_sources: BTreeSet<String>,
    pub(super) required_columns_by_source_id: Arc<Vec<Option<Arc<[bool]>>>>,
    pub(super) query_batches_by_source_id: Arc<Vec<bool>>,
    pub(super) materialized_source_ids: Arc<Vec<bool>>,
    pub(super) kafka_metadata_journal_source_ids: Arc<Vec<usize>>,
    pub(super) executor_running: Arc<AtomicBool>,
    pub(super) storage_reachable: Arc<AtomicBool>,
    pub(super) runtime_ready: Arc<AtomicBool>,
    pub(super) watermark_debug: Arc<tokio::sync::RwLock<http_ingest::WatermarkDebugState>>,
    pub(super) cdc_replication_debug:
        Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    pub(super) cdc_transaction_sender: mpsc::Sender<QueuedCdcTransaction>,
    pub(super) cdc_table_store: CdcTableStore,
    pub(super) postgres_cdc_settings: floe_config::PostgresCdcConfig,
}

struct KafkaArrowDecodeRequest<'a> {
    default_source: Option<&'a str>,
    message_format: Option<&'a str>,
    definitions: &'a [SourceDefinition],
    required_columns_by_source_id: &'a [Option<Arc<[bool]>>],
    query_batches_by_source_id: &'a [bool],
    materialized_source_ids: &'a [bool],
    kafka_metadata_journal_source_ids: &'a [usize],
    max_messages_per_tick: usize,
    max_batch: usize,
    max_per_source: usize,
    max_per_connector: usize,
}

fn kafka_arrow_decode_config(
    request: KafkaArrowDecodeRequest<'_>,
) -> Option<floe_node_core::kafka_connector::KafkaArrowDecodeConfig> {
    let KafkaArrowDecodeRequest {
        default_source,
        message_format,
        definitions,
        required_columns_by_source_id,
        query_batches_by_source_id,
        materialized_source_ids,
        kafka_metadata_journal_source_ids,
        max_messages_per_tick,
        max_batch,
        max_per_source,
        max_per_connector,
    } = request;
    let source = default_source?;
    let format_is_floe_json = message_format
        .map(|format| format.eq_ignore_ascii_case("floe_json"))
        .unwrap_or(true);
    if !format_is_floe_json {
        return None;
    }
    let source_id = definitions
        .iter()
        .position(|definition| definition.name() == source)?;
    if !materialized_source_ids
        .get(source_id)
        .copied()
        .unwrap_or(false)
    {
        return None;
    }
    let max_rows_per_batch = max_messages_per_tick
        .min(max_batch)
        .min(max_per_source)
        .min(max_per_connector);
    if max_rows_per_batch == 0 {
        return None;
    }
    let batch_mode = if query_batches_by_source_id
        .get(source_id)
        .copied()
        .unwrap_or(false)
    {
        SourceArrowBatchMode::ExecutionAndQuery
    } else {
        SourceArrowBatchMode::ExecutionOnly
    };
    Some(floe_node_core::kafka_connector::KafkaArrowDecodeConfig {
        source: source.to_string(),
        definition: definitions[source_id].clone(),
        required_columns: required_columns_by_source_id
            .get(source_id)
            .cloned()
            .flatten(),
        batch_mode,
        max_rows_per_batch,
        include_metadata_journal: kafka_metadata_journal_source_ids.contains(&source_id),
    })
}

pub(super) fn spawn_connector_tasks(
    config: SpawnConnectorTasksConfig<'_>,
) -> SpawnedConnectorTasks {
    let SpawnConnectorTasksConfig {
        connector_specs,
        definitions,
        postgres_cdc_runtime_plans_by_connector,
        connector_sender,
        pending_event_counter,
        ingest_cancel,
        runtime_cancel,
        runtime_failure,
        run_args,
        recovered_kafka_offsets,
        source_journal_skipped_sources,
        required_columns_by_source_id,
        query_batches_by_source_id,
        materialized_source_ids,
        kafka_metadata_journal_source_ids,
        executor_running,
        storage_reachable,
        runtime_ready,
        watermark_debug,
        cdc_replication_debug,
        cdc_transaction_sender,
        cdc_table_store,
        postgres_cdc_settings,
    } = config;
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
                let arrow_decode = kafka_arrow_decode_config(KafkaArrowDecodeRequest {
                    default_source: default_source.as_deref(),
                    message_format: format.as_deref(),
                    definitions: &definitions,
                    required_columns_by_source_id: &required_columns_by_source_id,
                    query_batches_by_source_id: &query_batches_by_source_id,
                    materialized_source_ids: &materialized_source_ids,
                    kafka_metadata_journal_source_ids: &kafka_metadata_journal_source_ids,
                    max_messages_per_tick,
                    max_batch: run_args.ingest_batch_size,
                    max_per_source: run_args.ingest_batch_per_source,
                    max_per_connector: run_args.ingest_batch_per_connector,
                });
                let definitions = definitions.clone();
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = KafkaConnectorConfig {
                        brokers,
                        topics,
                        group_id,
                        default_source,
                        poll_timeout,
                        replay_idle_timeout: KafkaConnectorConfig::default_replay_idle_timeout(
                            poll_timeout,
                        ),
                        max_messages_per_tick,
                        message_format: format,
                        commit_offsets_rx: Some(commit_rx),
                        resume_from_offsets,
                        arrow_decode,
                    };
                    let mut connector = match KafkaConnector::new(config, definitions) {
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
                    if let Err(err) =
                        run_native_postgres_cdc_connector(NativePostgresCdcConnectorConfig {
                            config,
                            runtime_plan,
                            snapshot_settings,
                            reconnect_settings,
                            table_store,
                            cdc_replication_debug,
                            sender: transaction_sender,
                            cancel: cancel.clone(),
                        })
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
