use super::super::*;

pub(super) struct NativePostgresCdcConnectorConfig {
    pub(super) config: PostgresCdcSourceConfig,
    pub(super) runtime_plan: PostgresCdcRuntimePlan,
    pub(super) snapshot_settings: PostgresCdcSnapshotConfig,
    pub(super) reconnect_settings: PostgresCdcReconnectConfig,
    pub(super) table_store: CdcTableStore,
    pub(super) cdc_replication_debug:
        Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    pub(super) sender: mpsc::Sender<QueuedCdcTransaction>,
    pub(super) cancel: CancellationToken,
}

#[derive(Clone)]
struct PostgresCdcWalStreamContext<'a> {
    connection_string: &'a str,
    slot: &'a str,
    publication: &'a str,
    runtime_plan: &'a PostgresCdcRuntimePlan,
    table_store: &'a CdcTableStore,
    cdc_replication_debug: &'a Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    cancel: &'a CancellationToken,
}

struct PostgresCdcWalStreamReconnectConfig<'a> {
    context: PostgresCdcWalStreamContext<'a>,
    commit_lsn_rx: Option<&'a mut watch::Receiver<PostgresCdcCommit>>,
    policy: PostgresCdcRuntimeReconnectPolicy,
}

struct PostgresCdcWalStreamOnceConfig<'a> {
    context: PostgresCdcWalStreamContext<'a>,
    commit_lsn_rx: Option<&'a mut watch::Receiver<PostgresCdcCommit>>,
    reconnect_attempts: u64,
}

pub(super) async fn run_native_postgres_cdc_connector(
    connector: NativePostgresCdcConnectorConfig,
) -> anyhow::Result<()> {
    let NativePostgresCdcConnectorConfig {
        mut config,
        runtime_plan,
        snapshot_settings,
        reconnect_settings,
        table_store,
        cdc_replication_debug,
        sender,
        cancel,
    } = connector;
    config.validate()?;
    let connection_string = config.connection_string.clone();
    let slot = config.slot.clone();
    let publication = config.publication.clone();
    super::super::postgres_snapshot::ensure_postgres_cdc_publication_and_slot(
        &connection_string,
        &slot,
        &publication,
        &runtime_plan,
        config.auto_create_slot,
        config.auto_create_publication,
    )
    .await?;
    let initial_snapshot =
        super::super::postgres_snapshot::run_initial_postgres_snapshot_if_needed(
            super::super::postgres_snapshot::InitialPostgresSnapshotConfig {
                connection_string: &connection_string,
                slot: &slot,
                publication: &publication,
                runtime_plan: &runtime_plan,
                table_store: &table_store,
                sender: &sender,
                cdc_replication_debug: &cdc_replication_debug,
                settings: snapshot_settings,
                commit_lsn_rx: config.commit_lsn_rx.as_mut(),
                cancel: &cancel,
            },
        )
        .await?;
    if let Some(lsn) = initial_snapshot.lsn {
        metrics::record_postgres_cdc_upstream_lsn(
            runtime_plan.source_id.as_str(),
            &slot,
            lsn.as_u64(),
        );
        metrics::record_postgres_cdc_durable_lsn(
            runtime_plan.source_id.as_str(),
            &slot,
            lsn.as_u64(),
        );
        record_postgres_cdc_debug_lsn(
            &cdc_replication_debug,
            runtime_plan.source_id.as_str(),
            &slot,
            Some(lsn.as_u64()),
            Some(lsn.as_u64()),
        );
    }
    if let Some(wal_stream) = initial_snapshot.wal_stream {
        match forward_buffered_postgres_wal_stream(
            wal_stream,
            sender.clone(),
            config.commit_lsn_rx.as_mut(),
            &cancel,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if cancel.is_cancelled() => return Err(err),
            Err(err) if !is_reconnectable_postgres_cdc_error(&err) => return Err(err),
            Err(err) => {
                tracing::warn!(
                    source = %runtime_plan.source_id.as_str(),
                    slot = %slot,
                    error = %err,
                    "buffered Postgres CDC WAL stream failed; reconnecting from durable checkpoint"
                );
            }
        }
    }
    let reconnect_policy = PostgresCdcRuntimeReconnectPolicy::from_config(reconnect_settings);
    run_native_postgres_cdc_wal_stream_with_reconnect(PostgresCdcWalStreamReconnectConfig {
        context: PostgresCdcWalStreamContext {
            connection_string: &connection_string,
            slot: &slot,
            publication: &publication,
            runtime_plan: &runtime_plan,
            table_store: &table_store,
            cdc_replication_debug: &cdc_replication_debug,
            sender,
            cancel: &cancel,
        },
        commit_lsn_rx: config.commit_lsn_rx.as_mut(),
        policy: reconnect_policy,
    })
    .await
}

#[derive(Debug, Clone, Copy)]
pub(in crate::node_runtime) struct PostgresCdcRuntimeReconnectPolicy {
    pub(in crate::node_runtime) max_reconnects: usize,
    pub(in crate::node_runtime) retry_base: Duration,
    pub(in crate::node_runtime) retry_max_backoff: Duration,
}

impl PostgresCdcRuntimeReconnectPolicy {
    fn from_config(config: PostgresCdcReconnectConfig) -> Self {
        Self {
            max_reconnects: config.max_reconnects,
            retry_base: Duration::from_millis(config.retry_base_ms),
            retry_max_backoff: Duration::from_millis(config.retry_max_backoff_ms),
        }
    }

    pub(in crate::node_runtime) fn backoff_for_reconnect(self, reconnect_idx: usize) -> Duration {
        let base_ms = self.retry_base.as_millis() as u64;
        let max_ms = self.retry_max_backoff.as_millis() as u64;
        let factor = if reconnect_idx >= 63 {
            u64::MAX
        } else {
            1_u64 << reconnect_idx
        };
        Duration::from_millis(base_ms.saturating_mul(factor).min(max_ms))
    }
}

async fn run_native_postgres_cdc_wal_stream_with_reconnect(
    mut config: PostgresCdcWalStreamReconnectConfig<'_>,
) -> anyhow::Result<()> {
    let context = config.context;
    let policy = config.policy;
    let mut reconnects = 0usize;
    loop {
        match run_native_postgres_cdc_wal_stream_once(PostgresCdcWalStreamOnceConfig {
            context: PostgresCdcWalStreamContext {
                sender: context.sender.clone(),
                ..context.clone()
            },
            commit_lsn_rx: config.commit_lsn_rx.as_deref_mut(),
            reconnect_attempts: reconnects as u64,
        })
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if context.cancel.is_cancelled() => return Err(err),
            Err(err) if !is_reconnectable_postgres_cdc_error(&err) => return Err(err),
            Err(err) if reconnects < policy.max_reconnects => {
                let backoff = policy.backoff_for_reconnect(reconnects);
                reconnects = reconnects.saturating_add(1);
                metrics::record_postgres_cdc_source_connected(
                    context.runtime_plan.source_id.as_str(),
                    context.slot,
                    false,
                );
                metrics::inc_postgres_cdc_reconnect(
                    context.runtime_plan.source_id.as_str(),
                    context.slot,
                    "scheduled",
                );
                record_postgres_cdc_debug_connection_state(
                    context.cdc_replication_debug,
                    context.runtime_plan.source_id.as_str(),
                    context.slot,
                    false,
                    reconnects as u64,
                    Some(
                        "Postgres CDC stream disconnected; reconnecting from durable checkpoint"
                            .to_string(),
                    ),
                );
                tracing::warn!(
                    source = %context.runtime_plan.source_id.as_str(),
                    slot = %context.slot,
                    reconnects,
                    max_reconnects = policy.max_reconnects,
                    retry_delay_ms = backoff.as_millis() as u64,
                    error = %err,
                    "Postgres CDC stream failed; reconnecting from durable checkpoint"
                );
                wait_for_postgres_cdc_reconnect(backoff, context.cancel).await?;
            }
            Err(err) => {
                metrics::record_postgres_cdc_source_connected(
                    context.runtime_plan.source_id.as_str(),
                    context.slot,
                    false,
                );
                metrics::inc_postgres_cdc_reconnect(
                    context.runtime_plan.source_id.as_str(),
                    context.slot,
                    "exhausted",
                );
                record_postgres_cdc_debug_connection_state(
                    context.cdc_replication_debug,
                    context.runtime_plan.source_id.as_str(),
                    context.slot,
                    false,
                    reconnects as u64,
                    Some("Postgres CDC stream reconnect attempts exhausted".to_string()),
                );
                return Err(err).with_context(|| {
                    format!("Postgres CDC stream failed after {reconnects} reconnect attempt(s)")
                });
            }
        }
    }
}

async fn wait_for_postgres_cdc_reconnect(
    backoff: Duration,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    if backoff.is_zero() {
        return Ok(());
    }
    tokio::select! {
        _ = cancel.cancelled() => Err(anyhow!("cancelled before Postgres CDC reconnect")),
        _ = tokio::time::sleep(backoff) => Ok(()),
    }
}

async fn run_native_postgres_cdc_wal_stream_once(
    config: PostgresCdcWalStreamOnceConfig<'_>,
) -> anyhow::Result<()> {
    let PostgresCdcWalStreamOnceConfig {
        context:
            PostgresCdcWalStreamContext {
                connection_string,
                slot,
                publication,
                runtime_plan,
                table_store,
                cdc_replication_debug,
                sender,
                cancel,
            },
        mut commit_lsn_rx,
        reconnect_attempts,
    } = config;
    let start_lsn = stored_slot_start_lsn(connection_string, slot)
        .await
        .map_err(reconnectable_postgres_cdc_error)
        .with_context(|| format!("load Postgres logical slot '{slot}' start LSN"))?;
    let replication_config =
        replication_config_from_connection_string(connection_string, slot, publication, start_lsn)?;
    let replication_config =
        config_with_stored_cdc_checkpoint(replication_config, table_store, &runtime_plan.source_id)
            .await?;
    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        tables = runtime_plan.schemas.len(),
        start_lsn = ?replication_config.start_lsn(),
        "starting native Postgres CDC replication stream"
    );
    let mut replication = PostgresReplicationClient::connect(&replication_config)
        .await
        .map_err(reconnectable_postgres_cdc_error)
        .context("connect native Postgres CDC transaction stream")?;
    metrics::record_postgres_cdc_source_connected(runtime_plan.source_id.as_str(), slot, true);
    record_postgres_cdc_debug_connection_state(
        cdc_replication_debug,
        runtime_plan.source_id.as_str(),
        slot,
        true,
        reconnect_attempts,
        None,
    );
    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        "native Postgres CDC replication stream connected"
    );
    let router = PostgresTableRouter::from_schemas(runtime_plan.schemas.values());
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        runtime_plan.source_id.clone(),
        router,
        runtime_plan.schemas.clone(),
        runtime_plan.schema_evolution_policy,
    );
    let mut last_committed_tick_id = 0_u64;
    let mut last_enqueued_lsn = None;

    let result = async {
        loop {
            update_native_postgres_applied_lsn(
                &mut replication,
                commit_lsn_rx.as_deref_mut(),
                slot,
                &mut last_committed_tick_id,
            )?;

            let event = tokio::select! {
                _ = cancel.cancelled() => break,
                event = replication.recv() => event
                    .map_err(reconnectable_postgres_cdc_error)
                    .context("receive native Postgres CDC event")?,
            };
            let Some(event) = event else {
                return Err(reconnectable_postgres_cdc_error(anyhow!(
                    "native Postgres CDC replication stream ended before cancellation"
                )));
            };
            if let Some(frontier_lsn) = postgres_replication_event_frontier_lsn(&event) {
                metrics::record_postgres_cdc_upstream_lsn(
                    runtime_plan.source_id.as_str(),
                    slot,
                    frontier_lsn.as_u64(),
                );
                record_postgres_cdc_debug_lsn(
                    cdc_replication_debug,
                    runtime_plan.source_id.as_str(),
                    slot,
                    Some(frontier_lsn.as_u64()),
                    None,
                );
            }
            if let PostgresReplicationEvent::StoppedAt { reached } = event {
                return Err(reconnectable_postgres_cdc_error(anyhow!(
                    "native Postgres CDC replication stream stopped at {reached:?} before cancellation"
                )));
            }
            let transaction = match assembler.accept_event(event) {
                Ok(transaction) => {
                    let observations = assembler.drain_schema_evolution_observations();
                    if !observations.is_empty() {
                        record_postgres_schema_evolution_observations(
                            cdc_replication_debug,
                            &runtime_plan.source_id,
                            observations,
                        )
                        .await;
                    }
                    transaction
                }
                Err(err) => {
                    let observations = assembler.drain_schema_evolution_observations();
                    if !observations.is_empty() {
                        record_postgres_schema_evolution_observations(
                            cdc_replication_debug,
                            &runtime_plan.source_id,
                            observations,
                        )
                        .await;
                    }
                    return Err(err);
                }
            };
            let Some(transaction) = transaction else {
                continue;
            };
            tracing::debug!(
                source = %runtime_plan.source_id.as_str(),
                slot = %slot,
                change_batches = transaction.change_batches().len(),
                commit_position = ?transaction.commit_position(),
                "assembled native Postgres CDC transaction"
            );
            let transaction_lsn = PostgresLsn::from_source_position(transaction.commit_position())?;
            sender
                .send(QueuedCdcTransaction {
                    slot: slot.to_string(),
                    source_id: runtime_plan.source_id.clone(),
                    transaction,
                })
                .await
                .map_err(|err| {
                    anyhow!("failed to enqueue native Postgres CDC transaction: {err}")
                })?;
            last_enqueued_lsn = Some(transaction_lsn);
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    replication.stop();
    let shutdown_result = replication.shutdown().await;
    if let Err(err) = result {
        wait_for_enqueued_postgres_cdc_lsn(commit_lsn_rx, slot, last_enqueued_lsn, cancel).await?;
        return Err(err);
    }
    shutdown_result
}

async fn forward_buffered_postgres_wal_stream(
    mut stream: BufferedPostgresWalStream,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(
        slot = %stream.slot,
        snapshot_lsn = %stream.snapshot_lsn,
        "forwarding buffered Postgres CDC WAL stream after durable initial snapshot"
    );

    let mut last_enqueued_lsn = None;
    loop {
        let transaction = tokio::select! {
            _ = cancel.cancelled() => break,
            transaction = stream.receiver.recv() => transaction,
        };
        let Some(transaction) = transaction else {
            break;
        };
        let transaction_lsn =
            PostgresLsn::from_source_position(transaction.transaction.commit_position())?;
        if let Err(err) = sender.send(transaction).await {
            stream.task.abort();
            let _ = stream.task.await;
            return Err(anyhow!(
                "failed to enqueue buffered native Postgres CDC transaction: {err}"
            ));
        }
        last_enqueued_lsn = Some(transaction_lsn);
    }

    let stream_result = match stream.task.await {
        Ok(result) => result,
        Err(err) if err.is_cancelled() && cancel.is_cancelled() => Ok(()),
        Err(err) => Err(anyhow!("buffered Postgres CDC WAL task failed: {err}")),
    };
    if let Err(err) = stream_result {
        wait_for_enqueued_postgres_cdc_lsn(commit_lsn_rx, &stream.slot, last_enqueued_lsn, cancel)
            .await?;
        return Err(err);
    }
    Ok(())
}

async fn wait_for_enqueued_postgres_cdc_lsn(
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    target_lsn: Option<PostgresLsn>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let Some(target_lsn) = target_lsn else {
        return Ok(());
    };
    super::super::postgres_snapshot::wait_for_postgres_cdc_commit(
        receiver,
        slot,
        target_lsn,
        cancel,
        "queued Postgres CDC transactions to become durable before reconnect",
    )
    .await
}

fn postgres_replication_event_frontier_lsn(
    event: &PostgresReplicationEvent,
) -> Option<PostgresLsn> {
    match event {
        PostgresReplicationEvent::KeepAlive { wal_end, .. } => Some(*wal_end),
        PostgresReplicationEvent::Begin { final_lsn, .. } => Some(*final_lsn),
        PostgresReplicationEvent::XLogData { wal_end, .. } => Some(*wal_end),
        PostgresReplicationEvent::Commit { end_lsn, .. } => Some(*end_lsn),
        PostgresReplicationEvent::Message { lsn, .. } => Some(*lsn),
        PostgresReplicationEvent::StoppedAt { reached } => Some(*reached),
    }
}

fn update_native_postgres_applied_lsn(
    replication: &mut PostgresReplicationClient,
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    last_committed_tick_id: &mut u64,
) -> anyhow::Result<()> {
    let Some(receiver) = receiver else {
        return Ok(());
    };

    let mut latest_commit = None;
    while receiver.has_changed().unwrap_or(false) {
        latest_commit = Some(receiver.borrow_and_update().clone());
    }
    let Some(commit) = latest_commit else {
        return Ok(());
    };
    if commit.tick_id <= *last_committed_tick_id {
        return Ok(());
    }

    if let Some(target_lsn) = commit
        .slots
        .iter()
        .find(|entry| entry.slot == slot)
        .map(|entry| entry.lsn.as_str())
    {
        replication.update_applied_lsn(PostgresLsn::parse(target_lsn)?);
    }
    *last_committed_tick_id = commit.tick_id;
    Ok(())
}
