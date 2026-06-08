use std::collections::BTreeSet;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use floe_executor::dbsp_plan::{DbspPlanBuilder, nexmark_config, validate_dbsp_plan};
use floe_node_core::generator;
use floe_node_core::nexmark_queries::canonical_nexmark_queries;
use floe_node_core::planner::plan_materialized_views;
use floe_node_core::source::SourceRegistry;
use floe_sql_parser::parse_materialized_view;

#[tokio::main]
async fn main() -> Result<()> {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions()?);

    let available_sources = [
        "nexmark_person",
        "person",
        "nexmark_auction",
        "auction",
        "nexmark_bid",
        "bid",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();

    let planner = DbspPlanBuilder::new(nexmark_config()?);

    println!("query,latency_ms,throughput_qps,memory_delta_kb");
    for query in canonical_nexmark_queries() {
        let before_rss = read_rss_kb().unwrap_or(0);
        let start = Instant::now();

        let definition = parse_materialized_view(&format!(
            "CREATE MATERIALIZED VIEW {} AS {}",
            query.id, query.sql
        ))
        .with_context(|| format!("parse {}", query.id))?;
        let logical = plan_materialized_views(&registry, &[definition])
            .await
            .with_context(|| format!("logical planning {}", query.id))?;
        let plan = planner
            .build(logical[0].logical_plan())
            .with_context(|| format!("circuit planning {}", query.id))?;
        validate_dbsp_plan(&plan, &available_sources, query.id)
            .with_context(|| format!("runtime validation {}", query.id))?;

        let elapsed = start.elapsed();
        let after_rss = read_rss_kb().unwrap_or(before_rss);
        let latency_ms = elapsed.as_secs_f64() * 1_000.0;
        let throughput_qps = if latency_ms > 0.0 {
            1_000.0 / latency_ms
        } else {
            0.0
        };
        let memory_delta_kb = after_rss as i64 - before_rss as i64;

        println!(
            "{},{:.3},{:.3},{}",
            query.id, latency_ms, throughput_qps, memory_delta_kb
        );
    }

    Ok(())
}

fn read_rss_kb() -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status").context("read /proc/self/status")?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| anyhow!("VmRSS line missing value"))?
                .parse::<u64>()
                .context("parse VmRSS")?;
            return Ok(kb);
        }
    }
    Err(anyhow!("VmRSS not found in /proc/self/status"))
}
