use super::*;

const POSTGRES_SNAPSHOT_SLOT_HANDOFF_RETRY_WINDOW: Duration = Duration::from_secs(5);
const POSTGRES_SNAPSHOT_SLOT_HANDOFF_RETRY_DELAY: Duration = Duration::from_millis(50);

pub(in crate::node_runtime) struct InitialPostgresSnapshotConfig<'a> {
    pub(in crate::node_runtime) connection_string: &'a str,
    pub(in crate::node_runtime) slot: &'a str,
    pub(in crate::node_runtime) publication: &'a str,
    pub(in crate::node_runtime) runtime_plan: &'a PostgresCdcRuntimePlan,
    pub(in crate::node_runtime) table_store: &'a CdcTableStore,
    pub(in crate::node_runtime) sender: &'a mpsc::Sender<QueuedCdcTransaction>,
    pub(in crate::node_runtime) cdc_replication_debug:
        &'a Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    pub(in crate::node_runtime) settings: PostgresCdcSnapshotConfig,
    pub(in crate::node_runtime) commit_lsn_rx: Option<&'a mut watch::Receiver<PostgresCdcCommit>>,
    pub(in crate::node_runtime) cancel: &'a CancellationToken,
}

pub(super) struct FinishLoadedPostgresSnapshotConfig<'a> {
    pub(super) slot: &'a str,
    pub(super) publication: &'a str,
    pub(super) runtime_plan: &'a PostgresCdcRuntimePlan,
    pub(super) table_store: &'a CdcTableStore,
    pub(super) sender: &'a mpsc::Sender<QueuedCdcTransaction>,
    pub(super) commit_lsn_rx: Option<&'a mut watch::Receiver<PostgresCdcCommit>>,
    pub(super) cancel: &'a CancellationToken,
    pub(super) snapshot: PostgresSnapshot,
}

pub(super) struct LoadPostgresInitialSnapshotConfig<'a> {
    connection_string: &'a str,
    slot: &'a str,
    publication: &'a str,
    runtime_plan: &'a PostgresCdcRuntimePlan,
    cdc_replication_debug: &'a Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    settings: PostgresCdcSnapshotConfig,
    wal_commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
}

pub(super) struct LoadPostgresInitialSnapshotFromClientConfig<'a> {
    connection_string: &'a str,
    slot: &'a str,
    client: &'a mut tokio_postgres::Client,
    publication: &'a str,
    source_id: &'a CdcSourceId,
    schemas: &'a HashMap<CdcTableId, CdcTableSchema>,
    runtime_plan: &'a PostgresCdcRuntimePlan,
    cdc_replication_debug: &'a Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    settings: PostgresCdcSnapshotConfig,
    wal_commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
}

pub(super) struct LoadExportedSlotPostgresInitialSnapshotFromClientConfig<'a> {
    connection_string: &'a str,
    slot: &'a str,
    client: &'a mut tokio_postgres::Client,
    publication: &'a str,
    source_id: &'a CdcSourceId,
    runtime_plan: &'a PostgresCdcRuntimePlan,
    cdc_replication_debug: &'a Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    sorted_schemas: Vec<&'a CdcTableSchema>,
    max_workers: usize,
    settings: PostgresCdcSnapshotConfig,
    wal_commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
}

pub(super) struct StartBufferedPostgresWalStreamConfig<'a> {
    connection_string: &'a str,
    slot: &'a str,
    publication: &'a str,
    runtime_plan: &'a PostgresCdcRuntimePlan,
    snapshot_lsn: PostgresLsn,
    cdc_replication_debug: &'a Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    wal_pressure_tx: Option<watch::Sender<SnapshotWalBufferPressure>>,
    commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    cancel: CancellationToken,
}

pub(super) struct BufferPostgresWalStreamConfig {
    replication: PostgresReplicationClient,
    runtime_plan: PostgresCdcRuntimePlan,
    slot: String,
    snapshot_lsn: PostgresLsn,
    cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    wal_pressure_tx: Option<watch::Sender<SnapshotWalBufferPressure>>,
    commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
    release_feedback_rx: watch::Receiver<bool>,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    cancel: CancellationToken,
}

pub(in crate::node_runtime) async fn run_initial_postgres_snapshot_if_needed(
    mut config: InitialPostgresSnapshotConfig<'_>,
) -> Result<InitialPostgresSnapshot> {
    let InitialPostgresSnapshotConfig {
        connection_string,
        slot,
        publication,
        runtime_plan,
        table_store,
        sender,
        cdc_replication_debug,
        settings,
        ref mut commit_lsn_rx,
        cancel,
    } = config;
    if table_store
        .load_checkpoint(&runtime_plan.source_id)
        .await
        .with_context(|| {
            format!(
                "load CDC checkpoint before Postgres snapshot for '{}'",
                runtime_plan.source_id.as_str()
            )
        })?
        .is_some()
    {
        return Ok(InitialPostgresSnapshot {
            lsn: None,
            wal_stream: None,
        });
    }

    let wal_commit_lsn_rx = commit_lsn_rx.as_ref().map(|receiver| (**receiver).clone());
    let snapshot = load_postgres_initial_snapshot(LoadPostgresInitialSnapshotConfig {
        connection_string,
        slot,
        publication,
        runtime_plan,
        cdc_replication_debug,
        settings,
        wal_commit_lsn_rx,
        cancel: cancel.clone(),
    })
    .await?;
    finish_loaded_postgres_snapshot(FinishLoadedPostgresSnapshotConfig {
        slot,
        publication,
        runtime_plan,
        table_store,
        sender,
        commit_lsn_rx: commit_lsn_rx.as_deref_mut(),
        cancel,
        snapshot,
    })
    .await
}

pub(super) async fn finish_loaded_postgres_snapshot(
    config: FinishLoadedPostgresSnapshotConfig<'_>,
) -> Result<InitialPostgresSnapshot> {
    let FinishLoadedPostgresSnapshotConfig {
        slot,
        publication,
        runtime_plan,
        table_store,
        sender,
        commit_lsn_rx,
        cancel,
        snapshot,
    } = config;
    let lsn = snapshot.lsn;
    let row_count = snapshot.row_count;
    let mut wal_stream = snapshot.wal_stream;

    let finish_result = async {
        match snapshot.transaction {
            Some(transaction) => {
                sender
                    .send(QueuedCdcTransaction {
                        slot: slot.to_string(),
                        source_id: runtime_plan.source_id.clone(),
                        transaction,
                    })
                    .await
                    .map_err(|err| {
                        anyhow!("failed to enqueue initial Postgres CDC snapshot: {err}")
                    })?;
                wait_for_postgres_snapshot_commit(commit_lsn_rx, slot, lsn, cancel).await?;
            }
            None => {
                let checkpoint = snapshot_checkpoint(&runtime_plan.source_id, lsn)?;
                table_store.commit_checkpoint(&checkpoint).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = finish_result {
        if let Some(stream) = wal_stream.take() {
            abort_buffered_postgres_wal_stream(stream).await;
        }
        return Err(err);
    }

    if let Some(stream) = wal_stream.as_ref() {
        release_buffered_postgres_wal_feedback(stream);
    }

    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        publication = %publication,
        lsn = %lsn,
        rows = row_count,
        "completed initial Postgres CDC snapshot"
    );
    Ok(InitialPostgresSnapshot {
        lsn: Some(lsn),
        wal_stream,
    })
}

pub(super) async fn load_postgres_initial_snapshot(
    config: LoadPostgresInitialSnapshotConfig<'_>,
) -> Result<PostgresSnapshot> {
    let LoadPostgresInitialSnapshotConfig {
        connection_string,
        slot,
        publication,
        runtime_plan,
        cdc_replication_debug,
        settings,
        wal_commit_lsn_rx,
        cancel,
    } = config;
    let (mut client, connection) =
        tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .context("connect Postgres control plane for initial CDC snapshot")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres initial snapshot connection closed");
        }
    });

    let snapshot =
        load_postgres_initial_snapshot_from_client(LoadPostgresInitialSnapshotFromClientConfig {
            connection_string,
            slot,
            client: &mut client,
            publication,
            source_id: &runtime_plan.source_id,
            schemas: &runtime_plan.schemas,
            runtime_plan,
            cdc_replication_debug,
            settings,
            wal_commit_lsn_rx,
            cancel,
        })
        .await;
    drop(client);
    connection_task.abort();
    snapshot
}

pub(super) async fn load_postgres_initial_snapshot_from_client(
    config: LoadPostgresInitialSnapshotFromClientConfig<'_>,
) -> Result<PostgresSnapshot> {
    let LoadPostgresInitialSnapshotFromClientConfig {
        connection_string,
        slot,
        client,
        publication,
        source_id,
        schemas,
        runtime_plan,
        cdc_replication_debug,
        settings,
        wal_commit_lsn_rx,
        cancel,
    } = config;
    let sorted_schemas = sorted_snapshot_schemas(schemas);
    let max_workers = settings.max_workers.max(1);
    match postgres_replication_slot_plugin(client, slot).await? {
        Some(plugin) => {
            ensure!(
                plugin.as_deref() == Some("pgoutput"),
                "Postgres CDC logical replication slot '{slot}' must use pgoutput, got {:?}",
                plugin
            );
            bail!(
                "Postgres CDC logical replication slot '{slot}' already exists but Floe has no durable CDC checkpoint. Floe cannot derive a lock-free initial snapshot boundary from an existing slot safely; drop the slot and let Floe recreate it, or restore the matching Floe data directory/checkpoint."
            );
        }
        None => {
            return load_exported_slot_postgres_initial_snapshot_from_client(
                LoadExportedSlotPostgresInitialSnapshotFromClientConfig {
                    connection_string,
                    slot,
                    client,
                    publication,
                    source_id,
                    runtime_plan,
                    cdc_replication_debug,
                    sorted_schemas,
                    max_workers,
                    settings,
                    wal_commit_lsn_rx,
                    cancel,
                },
            )
            .await;
        }
    }
}

pub(super) async fn load_exported_slot_postgres_initial_snapshot_from_client(
    config: LoadExportedSlotPostgresInitialSnapshotFromClientConfig<'_>,
) -> Result<PostgresSnapshot> {
    let LoadExportedSlotPostgresInitialSnapshotFromClientConfig {
        connection_string,
        slot,
        client,
        publication,
        source_id,
        runtime_plan,
        cdc_replication_debug,
        sorted_schemas,
        max_workers,
        settings,
        wal_commit_lsn_rx,
        cancel,
    } = config;
    let replication_config = replication_config_from_connection_string(
        connection_string,
        slot,
        publication,
        PostgresLsn::ZERO,
    )?;
    let exported_slot = create_pgoutput_slot_with_exported_snapshot(&replication_config)
        .await
        .with_context(|| {
            format!(
                "create Postgres CDC slot '{slot}' with an exported snapshot for lock-free initial snapshot"
            )
        })?;
    let snapshot_lsn = exported_slot.consistent_lsn();
    let exported_snapshot = exported_slot.snapshot_name().to_string();
    let transaction = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .context("begin exported-slot initial Postgres CDC snapshot transaction")?;
    bind_transaction_to_exported_snapshot(&transaction, &exported_snapshot).await?;

    validate_publication_tables(&transaction, publication, &sorted_schemas).await?;
    for schema in &sorted_schemas {
        validate_upstream_table_schema(&transaction, schema).await?;
    }

    let mut snapshot_tasks = Vec::new();
    let use_parallel_workers =
        max_workers > 1 && (sorted_schemas.len() > 1 || settings.intra_table_chunks > 1);
    if use_parallel_workers {
        for (table_idx, schema) in sorted_schemas.iter().enumerate() {
            let chunks = snapshot_table_chunks(&transaction, schema, settings).await?;
            for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
                snapshot_tasks.push((table_idx, chunk_idx, (*schema).clone(), chunk));
            }
        }
    }

    let mut snapshot_transaction = Some(transaction);
    let (change_batches, row_count, task_count, wal_stream) = if use_parallel_workers {
        let task_count = snapshot_tasks.len();
        let (start_tx, start_rx) = watch::channel(false);
        let mut adaptive_concurrency = SnapshotAdaptiveConcurrencyRuntime::new(
            source_id,
            slot,
            max_workers,
            settings,
            task_count,
            Arc::clone(cdc_replication_debug),
            &cancel,
        );
        let mut worker_handles = Vec::with_capacity(task_count);
        let mut ready_receivers = Vec::with_capacity(task_count);
        for (table_idx, chunk_idx, schema, chunk) in snapshot_tasks {
            let connection_string = connection_string.to_string();
            let exported_snapshot = exported_snapshot.clone();
            let start_rx = start_rx.clone();
            let scan_limiter = adaptive_concurrency.scan_limiter();
            let scan_observation_tx = adaptive_concurrency.scan_observation_tx();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            ready_receivers.push(ready_rx);
            worker_handles.push(tokio::spawn(async move {
                snapshot_table_change_batches_from_exported_snapshot(
                    &connection_string,
                    &exported_snapshot,
                    &schema,
                    &chunk,
                    settings,
                    Some(SnapshotWorkerControl {
                        ready_tx,
                        start_rx,
                        scan_limiter,
                        scan_observation_tx,
                    }),
                )
                .await
                .map(|snapshot| (table_idx, chunk_idx, snapshot))
            }));
        }

        if let Err(err) = wait_for_snapshot_workers_ready(ready_receivers).await {
            adaptive_concurrency.shutdown().await;
            abort_snapshot_worker_tasks(worker_handles).await;
            return Err(err);
        }

        let Some(transaction) = snapshot_transaction.take() else {
            adaptive_concurrency.shutdown().await;
            abort_snapshot_worker_tasks(worker_handles).await;
            return Err(anyhow::anyhow!(
                "snapshot validation transaction is not present"
            ));
        };
        if let Err(err) = transaction
            .commit()
            .await
            .context("commit exported-slot initial Postgres CDC validation transaction")
        {
            adaptive_concurrency.shutdown().await;
            abort_snapshot_worker_tasks(worker_handles).await;
            return Err(err);
        }
        drop(exported_slot);

        match start_buffered_postgres_wal_stream(StartBufferedPostgresWalStreamConfig {
            connection_string,
            slot,
            publication,
            runtime_plan,
            snapshot_lsn,
            cdc_replication_debug,
            wal_pressure_tx: adaptive_concurrency.wal_pressure_tx(),
            commit_lsn_rx: wal_commit_lsn_rx,
            cancel,
        })
        .await
        {
            Ok(stream) => {
                if let Err(err) = start_tx
                    .send(true)
                    .context("release Postgres snapshot workers after starting WAL stream")
                {
                    adaptive_concurrency.shutdown().await;
                    abort_buffered_postgres_wal_stream(stream).await;
                    abort_snapshot_worker_tasks(worker_handles).await;
                    return Err(err);
                }

                let table_snapshots = match collect_snapshot_worker_tasks(worker_handles).await {
                    Ok(snapshots) => snapshots,
                    Err(err) => {
                        adaptive_concurrency.shutdown().await;
                        abort_buffered_postgres_wal_stream(stream).await;
                        return Err(err);
                    }
                };
                adaptive_concurrency.shutdown().await;

                let mut table_snapshots = table_snapshots;
                table_snapshots.sort_by_key(|(table_idx, chunk_idx, _)| (*table_idx, *chunk_idx));

                let mut change_batches = Vec::new();
                let mut row_count = 0_usize;
                for (_, _, table_snapshot) in table_snapshots {
                    row_count = row_count.saturating_add(table_snapshot.row_count);
                    change_batches.extend(table_snapshot.change_batches);
                }
                (change_batches, row_count, task_count, stream)
            }
            Err(err) => {
                adaptive_concurrency.shutdown().await;
                abort_snapshot_worker_tasks(worker_handles).await;
                return Err(err);
            }
        }
    } else {
        drop(exported_slot);
        let stream = start_buffered_postgres_wal_stream(StartBufferedPostgresWalStreamConfig {
            connection_string,
            slot,
            publication,
            runtime_plan,
            snapshot_lsn,
            cdc_replication_debug,
            wal_pressure_tx: None,
            commit_lsn_rx: wal_commit_lsn_rx,
            cancel,
        })
        .await?;

        let scan_result = async {
            let mut change_batches = Vec::new();
            let mut row_count = 0_usize;
            for schema in &sorted_schemas {
                let transaction = snapshot_transaction
                    .as_ref()
                    .context("snapshot transaction is not present")?;
                let table_snapshot =
                    snapshot_table_change_batches(transaction, schema, settings).await?;
                row_count = row_count.saturating_add(table_snapshot.row_count);
                change_batches.extend(table_snapshot.change_batches);
            }
            snapshot_transaction
                .take()
                .context("snapshot transaction is not present")?
                .commit()
                .await
                .context("commit exported-slot initial Postgres CDC snapshot transaction")?;
            Ok::<_, anyhow::Error>((change_batches, row_count, sorted_schemas.len()))
        }
        .await;

        let (change_batches, row_count, task_count) = match scan_result {
            Ok(result) => result,
            Err(err) => {
                abort_buffered_postgres_wal_stream(stream).await;
                return Err(err);
            }
        };
        (change_batches, row_count, task_count, stream)
    };
    let transaction = snapshot_transaction_batch(source_id, snapshot_lsn, change_batches)?;

    tracing::info!(
        source = %source_id.as_str(),
        slot = %slot,
        snapshot = %exported_snapshot,
        lsn = %snapshot_lsn,
        tables = sorted_schemas.len(),
        tasks = task_count,
        max_workers,
        rows = row_count,
        "loaded lock-free initial Postgres CDC snapshot from exported logical-slot snapshot"
    );

    Ok(PostgresSnapshot {
        lsn: snapshot_lsn,
        transaction,
        row_count,
        wal_stream: Some(wal_stream),
    })
}

pub(super) async fn start_buffered_postgres_wal_stream(
    config: StartBufferedPostgresWalStreamConfig<'_>,
) -> Result<BufferedPostgresWalStream> {
    let StartBufferedPostgresWalStreamConfig {
        connection_string,
        slot,
        publication,
        runtime_plan,
        snapshot_lsn,
        cdc_replication_debug,
        wal_pressure_tx,
        commit_lsn_rx,
        cancel,
    } = config;
    let replication_config = replication_config_from_connection_string(
        connection_string,
        slot,
        publication,
        snapshot_lsn,
    )
    .with_context(|| {
        format!("configure Postgres CDC WAL stream from snapshot LSN {snapshot_lsn}")
    })?;
    let replication = connect_postgres_replication_client_with_retry(&replication_config).await?;
    let capacity = replication_config.buffer_events();
    let slot = slot.to_string();
    let (sender, receiver) = mpsc::channel(capacity);
    let (release_feedback_tx, release_feedback_rx) = watch::channel(false);
    let task_runtime_plan = runtime_plan.clone();
    let task_slot = slot.clone();
    let task_cdc_replication_debug = Arc::clone(cdc_replication_debug);
    let task = tokio::spawn(async move {
        buffer_postgres_wal_stream(BufferPostgresWalStreamConfig {
            replication,
            runtime_plan: task_runtime_plan,
            slot: task_slot,
            snapshot_lsn,
            cdc_replication_debug: task_cdc_replication_debug,
            wal_pressure_tx,
            commit_lsn_rx,
            release_feedback_rx,
            sender,
            cancel,
        })
        .await
    });
    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        lsn = %snapshot_lsn,
        buffer_events = capacity,
        "started buffered Postgres CDC WAL stream while initial snapshot is loading"
    );
    Ok(BufferedPostgresWalStream {
        slot,
        snapshot_lsn,
        release_feedback_tx,
        receiver,
        task,
    })
}

pub(super) async fn connect_postgres_replication_client_with_retry(
    config: &PostgresCdcConfig,
) -> Result<PostgresReplicationClient> {
    let started_at = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        match PostgresReplicationClient::connect(config).await {
            Ok(client) => return Ok(client),
            Err(err)
                if started_at.elapsed() < POSTGRES_SNAPSHOT_SLOT_HANDOFF_RETRY_WINDOW
                    && format!("{err:#}").contains("active") =>
            {
                tracing::debug!(
                    slot = %config.slot(),
                    attempts,
                    error = %err,
                    "Postgres CDC WAL stream is waiting for exported snapshot slot release"
                );
                tokio::time::sleep(POSTGRES_SNAPSHOT_SLOT_HANDOFF_RETRY_DELAY).await;
            }
            Err(err) => return Err(err),
        }
    }
}

pub(super) async fn buffer_postgres_wal_stream(
    config: BufferPostgresWalStreamConfig,
) -> Result<()> {
    let BufferPostgresWalStreamConfig {
        mut replication,
        runtime_plan,
        slot,
        snapshot_lsn,
        cdc_replication_debug,
        wal_pressure_tx,
        mut commit_lsn_rx,
        mut release_feedback_rx,
        sender,
        cancel,
    } = config;
    let router = PostgresTableRouter::from_schemas(runtime_plan.schemas.values());
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        runtime_plan.source_id.clone(),
        router,
        runtime_plan.schemas.clone(),
        runtime_plan.schema_evolution_policy,
    );
    let mut feedback_released = false;
    let mut last_committed_tick_id = 0_u64;

    let result = async {
        loop {
            if !feedback_released && *release_feedback_rx.borrow_and_update() {
                feedback_released = true;
                replication.update_applied_lsn(snapshot_lsn);
            }
            if feedback_released {
                update_buffered_postgres_applied_lsn(
                    &mut replication,
                    commit_lsn_rx.as_mut(),
                    &slot,
                    &mut last_committed_tick_id,
                )?;
            }

            let event = tokio::select! {
                _ = cancel.cancelled() => break,
                changed = release_feedback_rx.changed(), if !feedback_released => {
                    changed.context("buffered Postgres CDC feedback release channel closed")?;
                    continue;
                }
                event = replication.recv() => event
                    .map_err(reconnectable_postgres_cdc_error)
                    .context("receive buffered native Postgres CDC event")?,
            };
            let Some(event) = event else {
                break;
            };
            if let Some(frontier_lsn) = buffered_postgres_replication_event_frontier_lsn(&event) {
                metrics::record_postgres_cdc_upstream_lsn(
                    runtime_plan.source_id.as_str(),
                    &slot,
                    frontier_lsn.as_u64(),
                );
                record_postgres_cdc_debug_lsn(
                    &cdc_replication_debug,
                    runtime_plan.source_id.as_str(),
                    &slot,
                    Some(frontier_lsn.as_u64()),
                    None,
                );
            }
            if matches!(event, PostgresReplicationEvent::StoppedAt { .. }) {
                break;
            }
            let transaction = match assembler.accept_event(event) {
                Ok(transaction) => {
                    let observations = assembler.drain_schema_evolution_observations();
                    if !observations.is_empty() {
                        record_postgres_schema_evolution_observations(
                            &cdc_replication_debug,
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
                            &cdc_replication_debug,
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
            let commit_lsn = PostgresLsn::from_source_position(transaction.commit_position())?;
            if commit_lsn <= snapshot_lsn {
                tracing::debug!(
                    source = %runtime_plan.source_id.as_str(),
                    slot = %slot,
                    commit_lsn = %commit_lsn,
                    snapshot_lsn = %snapshot_lsn,
                    "dropping Postgres CDC WAL transaction covered by initial snapshot"
                );
                continue;
            }
            tracing::debug!(
                source = %runtime_plan.source_id.as_str(),
                slot = %slot,
                change_batches = transaction.change_batches().len(),
                commit_position = ?transaction.commit_position(),
                "buffered native Postgres CDC transaction during initial snapshot"
            );
            record_snapshot_wal_buffer_pressure(
                &wal_pressure_tx,
                runtime_plan.source_id.as_str(),
                &slot,
                sender
                    .max_capacity()
                    .saturating_sub(sender.capacity())
                    .saturating_add(1),
                sender.max_capacity(),
            );
            sender
                .send(QueuedCdcTransaction {
                    slot: slot.clone(),
                    source_id: runtime_plan.source_id.clone(),
                    transaction,
                })
                .await
                .map_err(|err| {
                    anyhow!("failed to enqueue buffered native Postgres CDC transaction: {err}")
                })?;
            record_snapshot_wal_buffer_pressure(
                &wal_pressure_tx,
                runtime_plan.source_id.as_str(),
                &slot,
                sender.max_capacity().saturating_sub(sender.capacity()),
                sender.max_capacity(),
            );
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    replication.stop();
    let shutdown_result = replication.shutdown().await;
    result?;
    shutdown_result
}

pub(super) fn record_snapshot_wal_buffer_pressure(
    sender: &Option<watch::Sender<SnapshotWalBufferPressure>>,
    source: &str,
    slot: &str,
    pending_events: usize,
    capacity: usize,
) {
    let pending_events = pending_events.min(capacity);
    crate::metrics::record_postgres_cdc_snapshot_wal_buffer_fill(
        source,
        slot,
        pending_events,
        capacity,
    );
    if let Some(sender) = sender {
        let _ = sender.send(SnapshotWalBufferPressure {
            pending_events,
            capacity,
        });
    }
}

pub(super) fn buffered_postgres_replication_event_frontier_lsn(
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

pub(super) fn update_buffered_postgres_applied_lsn(
    replication: &mut PostgresReplicationClient,
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    last_committed_tick_id: &mut u64,
) -> Result<()> {
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

pub(super) fn release_buffered_postgres_wal_feedback(stream: &BufferedPostgresWalStream) {
    let _ = stream.release_feedback_tx.send(true);
}

pub(super) async fn abort_buffered_postgres_wal_stream(stream: BufferedPostgresWalStream) {
    stream.task.abort();
    let _ = stream.task.await;
}

pub(super) async fn wait_for_snapshot_workers_ready(
    ready_receivers: Vec<tokio::sync::oneshot::Receiver<()>>,
) -> Result<()> {
    for receiver in ready_receivers {
        receiver
            .await
            .context("Postgres snapshot worker exited before binding exported snapshot")?;
    }
    Ok(())
}

pub(super) async fn collect_snapshot_worker_tasks(
    worker_handles: Vec<JoinHandle<Result<(usize, usize, SnapshotTableChangeBatches)>>>,
) -> Result<Vec<(usize, usize, SnapshotTableChangeBatches)>> {
    let mut snapshots = Vec::with_capacity(worker_handles.len());
    let mut first_error = None;
    for handle in worker_handles {
        match handle.await {
            Ok(Ok(snapshot)) => snapshots.push(snapshot),
            Ok(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(anyhow!("Postgres snapshot worker task failed: {err}"));
                }
            }
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(snapshots)
}

pub(super) async fn abort_snapshot_worker_tasks(
    worker_handles: Vec<JoinHandle<Result<(usize, usize, SnapshotTableChangeBatches)>>>,
) {
    for handle in &worker_handles {
        handle.abort();
    }
    for handle in worker_handles {
        let _ = handle.await;
    }
}

pub(super) fn snapshot_transaction_batch(
    source_id: &CdcSourceId,
    snapshot_lsn: PostgresLsn,
    change_batches: Vec<ChangeBatch>,
) -> Result<Option<TransactionBatch>> {
    if change_batches.is_empty() {
        return Ok(None);
    }
    let position = snapshot_lsn.to_source_position()?;
    Ok(Some(TransactionBatch::new(
        source_id.clone(),
        Some(snapshot_transaction_id(snapshot_lsn)?),
        Some(position.clone()),
        position,
        change_batches,
    )?))
}

pub(super) fn sorted_snapshot_schemas(
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
) -> Vec<&CdcTableSchema> {
    let mut sorted = schemas.values().collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        left.upstream_table()
            .schema()
            .cmp(right.upstream_table().schema())
            .then(
                left.upstream_table()
                    .table()
                    .cmp(right.upstream_table().table()),
            )
            .then(left.table_id().as_str().cmp(right.table_id().as_str()))
    });
    sorted
}
