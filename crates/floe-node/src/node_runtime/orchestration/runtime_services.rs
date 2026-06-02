use super::*;

pub(super) struct RuntimeServices {
    pub(super) postgres_cdc_runtime_plans_by_connector: HashMap<String, PostgresCdcRuntimePlan>,
    pub(super) replication_pipeline_runtime: Arc<ReplicationPipelineRuntime>,
    pub(super) admin_handle: JoinHandle<()>,
    pub(super) cdc_replication_debug_handle: JoinHandle<()>,
}

pub(super) struct RuntimeServicesConfig<'a> {
    pub(super) connector_specs: &'a [floe_config::ConnectorSpec],
    pub(super) source_registry: &'a SourceRegistry,
    pub(super) source_backed_tables: &'a HashMap<String, SourceBackedTableDefinition>,
    pub(super) replication_pipelines: &'a HashMap<String, CatalogReplicationPipelineDefinition>,
    pub(super) node_config: Option<&'a NodeConfig>,
    pub(super) storage: Arc<floe_storage::SlateCatalog>,
    pub(super) service_cancel: CancellationToken,
    pub(super) runtime_cancel: CancellationToken,
    pub(super) runtime_failure: Arc<StdMutex<Option<String>>>,
    pub(super) run_args: &'a cli::RunArgs,
    pub(super) executor_running: Arc<AtomicBool>,
    pub(super) storage_reachable: Arc<AtomicBool>,
    pub(super) runtime_ready: Arc<AtomicBool>,
    pub(super) watermark_debug: Arc<tokio::sync::RwLock<http_ingest::WatermarkDebugState>>,
    pub(super) cdc_replication_debug:
        Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    pub(super) mv_registry: Arc<MaterializedViewRegistry>,
}

pub(super) async fn start_runtime_services(
    config: RuntimeServicesConfig<'_>,
) -> anyhow::Result<RuntimeServices> {
    let mut postgres_cdc_runtime_plans_by_connector = HashMap::new();
    for connector in config.connector_specs {
        let ConnectorConfig::PostgresCdc {
            connection,
            schema_evolution_policy,
            include_tables,
            ..
        } = &connector.config
        else {
            continue;
        };
        let schema_evolution_policy = schema_evolution_policy
            .as_ref()
            .copied()
            .unwrap_or(CatalogPostgresCdcSchemaEvolutionPolicy::FailFast);
        if let Some(plan) = postgres_cdc_runtime_plan(
            &connector.name,
            connection,
            postgres_schema_evolution_policy_from_catalog(schema_evolution_policy),
            include_tables.as_deref(),
            config.source_registry,
            config.source_backed_tables,
            config.replication_pipelines,
        )
        .await
        .with_context(|| {
            format!(
                "build native Postgres CDC runtime plan for connector '{}'",
                connector.name
            )
        })? {
            metrics::record_postgres_cdc_schema_evolution_policy(
                plan.source_id.as_str(),
                plan.schema_evolution_policy.as_str(),
            );
            postgres_cdc_runtime_plans_by_connector.insert(connector.name.clone(), plan);
        }
    }
    initialize_postgres_cdc_debug_sources(
        &config.cdc_replication_debug,
        postgres_cdc_runtime_plans_by_connector.values(),
    )
    .await;
    let replication_settings = config
        .node_config
        .map(|cfg| cfg.replication.clone())
        .unwrap_or_default();
    let replication_pipeline_runtime = Arc::new(ReplicationPipelineRuntime::new(
        postgres_cdc_runtime_plans_by_connector
            .values()
            .flat_map(|plan| plan.replication_pipelines.iter().cloned()),
        replication_settings,
    )?);
    replication_pipeline_runtime
        .refresh_debug_state(&config.storage, &config.cdc_replication_debug)
        .await
        .context("refresh initial CDC replication debug state")?;
    let replayed_replication_records = replication_pipeline_runtime
        .replay_buffered(&config.storage)
        .await
        .context("replay buffered replication pipeline records")?;
    replication_pipeline_runtime
        .refresh_debug_state(&config.storage, &config.cdc_replication_debug)
        .await
        .context("refresh CDC replication debug state after replay")?;
    if replayed_replication_records > 0 {
        tracing::info!(
            records = replayed_replication_records,
            "replayed buffered replication pipeline records during startup"
        );
    }

    let cdc_replication_debug_handle = spawn_cdc_replication_debug_refresh(
        config.service_cancel.clone(),
        Arc::clone(&config.cdc_replication_debug),
        config.storage.clone(),
        Arc::clone(&replication_pipeline_runtime),
    );
    let admin_handle = spawn_admin_server(AdminServerConfig {
        run_args: config.run_args,
        storage: config.storage,
        runtime_cancel: config.runtime_cancel,
        runtime_failure: config.runtime_failure,
        service_cancel: config.service_cancel,
        executor_running: config.executor_running,
        storage_reachable: config.storage_reachable,
        runtime_ready: config.runtime_ready,
        watermark_debug: config.watermark_debug,
        cdc_replication_debug: config.cdc_replication_debug,
        replication_pipeline_runtime: Arc::clone(&replication_pipeline_runtime),
        mv_registry: config.mv_registry,
    });

    Ok(RuntimeServices {
        postgres_cdc_runtime_plans_by_connector,
        replication_pipeline_runtime,
        admin_handle,
        cdc_replication_debug_handle,
    })
}

fn spawn_cdc_replication_debug_refresh(
    service_cancel: CancellationToken,
    cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    storage: Arc<floe_storage::SlateCatalog>,
    replication_pipeline_runtime: Arc<ReplicationPipelineRuntime>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut refresh_interval = tokio::time::interval(Duration::from_secs(1));
        refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = service_cancel.cancelled() => break,
                _ = refresh_interval.tick() => {
                    if let Err(err) = replication_pipeline_runtime
                        .refresh_debug_state(&storage, &cdc_replication_debug)
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            "failed to refresh CDC replication debug state"
                        );
                    }
                }
            }
        }
    })
}

struct AdminServerConfig<'a> {
    run_args: &'a cli::RunArgs,
    storage: Arc<floe_storage::SlateCatalog>,
    runtime_cancel: CancellationToken,
    runtime_failure: Arc<StdMutex<Option<String>>>,
    service_cancel: CancellationToken,
    executor_running: Arc<AtomicBool>,
    storage_reachable: Arc<AtomicBool>,
    runtime_ready: Arc<AtomicBool>,
    watermark_debug: Arc<tokio::sync::RwLock<http_ingest::WatermarkDebugState>>,
    cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    replication_pipeline_runtime: Arc<ReplicationPipelineRuntime>,
    mv_registry: Arc<MaterializedViewRegistry>,
}

fn spawn_admin_server(config: AdminServerConfig<'_>) -> JoinHandle<()> {
    let admin_health = HttpIngestHealth {
        executor_running: config.executor_running,
        storage_reachable: config.storage_reachable,
        runtime_ready: config.runtime_ready,
        watermark_debug: Some(config.watermark_debug),
        cdc_replication_debug: Some(config.cdc_replication_debug),
    };
    let admin_config = HttpAdminConfig {
        host: config.run_args.http_host.clone(),
        port: config.run_args.admin_port.unwrap_or(DEFAULT_ADMIN_PORT),
        health: admin_health,
        storage_db: Some(config.storage.db()),
        storage_catalog: Some(config.storage),
        replication_runtime: Some(config.replication_pipeline_runtime),
        materialized_views: Some(config.mv_registry),
    };
    tokio::spawn(async move {
        if let Err(err) =
            http_ingest::run_admin_server(admin_config, config.service_cancel.clone()).await
        {
            tracing::error!(error = %err, "admin HTTP server failed");
            record_runtime_failure(
                &config.runtime_failure,
                format!("admin HTTP server failed: {err}"),
            );
            config.runtime_cancel.cancel();
        }
    })
}
