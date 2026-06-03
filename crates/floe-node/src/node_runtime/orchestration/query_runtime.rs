use super::*;

pub(super) struct StartRuntimeFrontendServicesConfig<'a> {
    pub(super) storage: Arc<floe_storage::SlateCatalog>,
    pub(super) vectorized_source_table_providers:
        Vec<(String, Arc<dyn datafusion::catalog::TableProvider>)>,
    pub(super) planned_materialized_views: &'a [PlannedMaterializedView],
    pub(super) mv_registry: &'a Arc<MaterializedViewRegistry>,
    pub(super) sink_specs: Vec<SinkSpec>,
    pub(super) sink_resume_cursors: HashMap<String, SinkCursor>,
    pub(super) sink_checkpoint_tx: mpsc::Sender<SinkCursor>,
    pub(super) sink_cancel: CancellationToken,
    pub(super) runtime_cancel: CancellationToken,
    pub(super) ingest_cancel: CancellationToken,
    pub(super) shutdown_signal: CancellationToken,
    pub(super) service_cancel: CancellationToken,
    pub(super) runtime_failure: Arc<StdMutex<Option<String>>>,
    pub(super) pgwire_addr: Option<String>,
    pub(super) pgwire_enabled: bool,
    pub(super) subscribe_execution_config: SubscribeExecutionConfig,
}

pub(super) struct RuntimeFrontendServices {
    pub(super) query: FloeQueryContext,
    pub(super) sink_handles: Vec<JoinHandle<()>>,
    pub(super) signal_handle: JoinHandle<()>,
    pub(super) server_handle: JoinHandle<anyhow::Result<()>>,
}

pub(super) async fn start_runtime_frontend_services(
    config: StartRuntimeFrontendServicesConfig<'_>,
) -> anyhow::Result<RuntimeFrontendServices> {
    let StartRuntimeFrontendServicesConfig {
        storage,
        vectorized_source_table_providers,
        planned_materialized_views,
        mv_registry,
        sink_specs,
        sink_resume_cursors,
        sink_checkpoint_tx,
        sink_cancel,
        runtime_cancel,
        ingest_cancel,
        shutdown_signal,
        service_cancel,
        runtime_failure,
        pgwire_addr,
        pgwire_enabled,
        subscribe_execution_config,
    } = config;
    let query = prepare_query_context(
        storage,
        vectorized_source_table_providers,
        planned_materialized_views,
        mv_registry,
    )
    .await?;
    let sink_handles = sinks::spawn_sinks(
        sink_specs,
        Arc::clone(mv_registry),
        sink_resume_cursors,
        Some(sink_checkpoint_tx),
        sink_cancel,
        runtime_cancel.clone(),
        Arc::clone(&runtime_failure),
    );
    let signal_handle =
        spawn_signal_handler(runtime_cancel.clone(), ingest_cancel, shutdown_signal);
    let server_handle = spawn_pgwire_server(
        query.clone(),
        Arc::clone(mv_registry),
        service_cancel,
        runtime_cancel,
        runtime_failure,
        pgwire_enabled,
        pgwire_addr.unwrap_or_else(|| DEFAULT_PGWIRE_ADDR.to_string()),
        server::ServerRuntimeConfig {
            subscribe: subscribe_execution_config,
        },
    );
    Ok(RuntimeFrontendServices {
        query,
        sink_handles,
        signal_handle,
        server_handle,
    })
}

async fn prepare_query_context(
    storage: Arc<floe_storage::SlateCatalog>,
    vectorized_source_table_providers: Vec<(String, Arc<dyn datafusion::catalog::TableProvider>)>,
    planned_materialized_views: &[PlannedMaterializedView],
    mv_registry: &Arc<MaterializedViewRegistry>,
) -> anyhow::Result<FloeQueryContext> {
    let query = FloeQueryContext::new(storage);
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;
    let session = query.session();
    for (name, provider) in vectorized_source_table_providers {
        let _ = session.deregister_table(&name);
        session
            .register_table(&name, provider)
            .with_context(|| format!("register vectorized source table {name}"))?;
    }
    register_materialized_view_tables(&query, planned_materialized_views, mv_registry)
        .await
        .context("register materialized view tables")?;
    Ok(query)
}
