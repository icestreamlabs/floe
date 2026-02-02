use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use anyhow::Result;
use dbsp::StreamRetention;
use floe_executor::{
    BuildInputs, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError,
    MaterializedViewRegistry, OuterStreamRegistry, ValidatedPlan, load_or_register_mv,
    validate_dbsp_plan,
};
use floe_node::executor::{available_sources_from_registry, build_dataflows};
use floe_node::generator;
use floe_node::planner::plan_materialized_views;
use floe_node::source::SourceRegistry;
use floe_sql_parser::parse_materialized_view;
use floe_storage::SlateCatalog;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) struct MvTestHarness {
    pub(crate) catalog: Arc<SlateCatalog>,
    pub(crate) db: Arc<slatedb::Db>,
    pub(crate) mv_registry: Arc<MaterializedViewRegistry>,
    pub(crate) outer: OuterStreamRegistry,
    pub(crate) ingestion_bridge: DbspBridge,
    pub(crate) view_name: String,
}

impl MvTestHarness {
    pub(crate) async fn new(view_name: &str, view_sql: &str) -> Result<Self> {
        let catalog = Arc::new(SlateCatalog::in_memory().await?);
        Self::new_with_catalog(catalog, view_name, view_sql).await
    }

    pub(crate) async fn new_with_catalog(
        catalog: Arc<SlateCatalog>,
        view_name: &str,
        view_sql: &str,
    ) -> Result<Self> {
        let db = catalog.db();

        let mut registry = SourceRegistry::new();
        registry.extend(generator::definitions()?);
        let available_sources = available_sources_from_registry(&registry);

        let definition = parse_materialized_view(view_sql)?;
        let planned = plan_materialized_views(&registry, &[definition]).await?;
        assert_eq!(
            planned.len(),
            1,
            "expected a single planned materialized view"
        );
        let circuit_plans = build_dataflows(&planned, &available_sources)?;
        assert_eq!(
            circuit_plans.len(),
            1,
            "expected a single circuit plan for the view"
        );

        let ValidatedPlan {
            required_sources, ..
        } = validate_dbsp_plan(&circuit_plans[0], &available_sources, view_name)?;

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db)).await?;
        let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let outer =
            OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
                .await?;
        let source_refs: Vec<&str> = required_sources.iter().map(String::as_str).collect();
        let handle_streams = gather_handle_streams(&outer, &source_refs);
        let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
        graph_builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan: &circuit_plans[0],
                cancel: CancellationToken::new(),
                task_events: task_tx.clone(),
                mv_registry: Arc::clone(&mv_registry),
                outer_handle_streams: &handle_streams,
                mv_retention: StreamRetention::KeepLast { keep_last: 1 },
                watermark: Arc::new(AtomicI64::new(-1)),
            })
            .await?;

        Ok(Self {
            catalog,
            db,
            mv_registry,
            outer,
            ingestion_bridge,
            view_name: view_name.to_string(),
        })
    }

    pub(crate) async fn session_with_view(
        &self,
    ) -> Result<(datafusion::execution::context::SessionContext, DbspBridge)> {
        let query = FloeQueryContext::new(Arc::clone(&self.catalog));
        let session = query.session();
        let mut bridge = DbspBridge::new(Arc::clone(&self.db)).await?;
        load_or_register_mv(
            &session,
            Arc::clone(&self.mv_registry),
            &mut bridge,
            &self.view_name,
        )
        .await?;
        Ok((session, bridge))
    }
}

fn gather_handle_streams(
    outer: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, dbsp::DeltaHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = outer.delta_handle_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}
