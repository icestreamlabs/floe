use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use dbsp::circuit::{CircuitPlan, CircuitPlanner, PlannerConfig, PlannerError};
use dbsp::circuit::tables::{
    nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table,
    nexmark_bid_table, nexmark_person_alias_table, nexmark_person_table,
};
use dbsp::storage::{KeyValueTable, SlateTable};
use floe_storage::SlateCatalog;

use crate::dbsp_table_environment::DbspTableEnvironment;
use crate::materialized_view::MaterializedViewRegistry;
use crate::plan_to_pipeline::PipelineFromCircuit;

fn planner_with_nexmark() -> PlannerConfig {
    let mut cfg = PlannerConfig::new();
    cfg.register_table(nexmark_person_table());
    cfg.register_table(nexmark_person_alias_table());
    cfg.register_table(nexmark_auction_table());
    cfg.register_table(nexmark_auction_alias_table());
    cfg.register_table(nexmark_bid_table());
    cfg.register_table(nexmark_bid_alias_table());
    cfg
}

fn register_descriptor(ctx: &SessionContext, descriptor: &'static dbsp::TableDescriptor) -> Result<()> {
    let schema = descriptor.schema().to_arrow_schema();
    let provider = MemTable::try_new(schema, vec![])?;
    ctx.register_table(descriptor.name, Arc::new(provider))?;
    Ok(())
}

fn create_session_for_nexmark() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    register_descriptor(&ctx, nexmark_person_table())?;
    register_descriptor(&ctx, nexmark_person_alias_table())?;
    register_descriptor(&ctx, nexmark_auction_table())?;
    register_descriptor(&ctx, nexmark_auction_alias_table())?;
    register_descriptor(&ctx, nexmark_bid_table())?;
    register_descriptor(&ctx, nexmark_bid_alias_table())?;
    Ok(ctx)
}

fn parse_create_view(sql: &str) -> Result<(String, String)> {
    let lower = sql.to_lowercase();
    let needle = "create materialized view";
    let pos = lower
        .find(needle)
        .context("expected 'CREATE MATERIALIZED VIEW' in SQL")?;
    let rest = sql[pos + needle.len()..].trim_start();
    let mut parts = rest.splitn(2, "as");
    let name = parts
        .next()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .context("missing view name")?;
    let select_sql = parts
        .next()
        .map(|s| s.trim())
        .context("missing SELECT clause after AS")?;
    Ok((name.to_string(), select_sql.to_string()))
}

async fn logical_plan(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    Ok(plan)
}

pub async fn create_materialized_view(
    sql: &str,
    registry: Arc<MaterializedViewRegistry>,
    storage: Arc<SlateCatalog>,
) -> Result<()> {
    let (view_name, select_sql) = parse_create_view(sql)?;

    let ctx = create_session_for_nexmark()?;
    let plan = logical_plan(&ctx, &select_sql).await?;
    let planner = CircuitPlanner::new(planner_with_nexmark());
    let circuit: CircuitPlan = planner.plan(&plan).map_err(|e: PlannerError| anyhow!(e.to_string()))?;

    let db = floe_storage::catalog::catalog_db(&storage);
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    let env = DbspTableEnvironment::with_table(table.clone())
        .await
        .context("build DBSP table environment")?;

    let pipeline_builder = PipelineFromCircuit {
        plan: &circuit,
        tables: &env,
        registry: registry.clone(),
    };
    let mut pipeline = pipeline_builder
        .build_mv_pipeline(&view_name)
        .await
        .context("build materialized view pipeline")?;

    tokio::spawn(async move {
        loop {
            if let Err(err) = pipeline.step_once().await {
                eprintln!("pipeline step failed: {err}");
                break;
            }
        }
    });

    Ok(())
}
