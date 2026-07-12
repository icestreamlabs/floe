use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::{Optimizer, OptimizerContext};

use crate::planner::PlannedMaterializedView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamCompactionConfig {
    pub max_chain_len: usize,
    pub max_segments: usize,
    pub scheduler_backoff_ticks: u64,
    pub scheduler_max_concurrent_jobs: usize,
}

impl Default for StreamCompactionConfig {
    fn default() -> Self {
        Self {
            max_chain_len: 32,
            max_segments: 256,
            scheduler_backoff_ticks: 1,
            scheduler_max_concurrent_jobs: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamGcConfig {
    pub grace_period_ms: u64,
}

impl Default for StreamGcConfig {
    fn default() -> Self {
        Self {
            grace_period_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanSourceRequirements {
    pub source_name: String,
    pub required_columns: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataflowAnalysis {
    pub required_sources: BTreeSet<String>,
    pub source_requirements: Vec<PlanSourceRequirements>,
}

pub fn available_sources_from_registry(
    registry: &crate::source::SourceRegistry,
) -> BTreeSet<String> {
    registry
        .definitions()
        .iter()
        .flat_map(|definition| {
            let mut names = vec![definition.name().to_string()];
            if let Some(alias) = definition.property("query_alias") {
                names.push(alias.to_string());
            }
            names
        })
        .collect()
}

pub fn analyze_dataflows(
    views: &[PlannedMaterializedView],
    sources: &crate::source::SourceRegistry,
) -> Result<Vec<DataflowAnalysis>> {
    views
        .iter()
        .map(|view| analyze_logical_plan(view.logical_plan(), sources))
        .collect()
}

pub fn analyze_logical_plan(
    plan: &LogicalPlan,
    sources: &crate::source::SourceRegistry,
) -> Result<DataflowAnalysis> {
    let mut source_names = HashMap::new();
    for definition in sources.definitions() {
        source_names.insert(definition.name().to_string(), definition.name().to_string());
        if let Some(alias) = definition.property("query_alias") {
            source_names.insert(alias.to_string(), definition.name().to_string());
        }
    }

    let mut required_columns = BTreeMap::<String, BTreeSet<usize>>::new();
    let requirement_plan = Optimizer::with_rules(vec![Arc::new(OptimizeProjections::new())])
        .optimize(
            plan.clone(),
            &OptimizerContext::new().with_skip_failing_rules(false),
            |_, _| {},
        )?;
    collect_scan_requirements(
        &requirement_plan,
        sources,
        &source_names,
        &mut required_columns,
    )?;
    let required_sources = required_columns.keys().cloned().collect();
    let source_requirements = required_columns
        .into_iter()
        .map(|(source_name, required_columns)| PlanSourceRequirements {
            source_name,
            required_columns: required_columns.into_iter().collect(),
        })
        .collect();
    Ok(DataflowAnalysis {
        required_sources,
        source_requirements,
    })
}

fn collect_scan_requirements(
    plan: &LogicalPlan,
    sources: &crate::source::SourceRegistry,
    source_names: &HashMap<String, String>,
    required_columns: &mut BTreeMap<String, BTreeSet<usize>>,
) -> Result<()> {
    if let LogicalPlan::TableScan(scan) = plan {
        let scan_name = scan.table_name.table();
        let source_name = source_names
            .get(scan_name)
            .ok_or_else(|| anyhow!("logical plan referenced unknown source '{scan_name}'"))?;
        let definition = sources
            .get(source_name)
            .ok_or_else(|| anyhow!("source registry lost definition '{source_name}'"))?;
        let columns = required_columns.entry(source_name.clone()).or_default();
        if let Some(projection) = &scan.projection {
            columns.extend(projection.iter().copied());
        } else {
            columns.extend(0..definition.columns().len());
        }
    }
    for input in plan.inputs() {
        collect_scan_requirements(input, sources, source_names, required_columns)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition, SourceRegistry};
    use floe_sql_parser::parse_materialized_view;

    use super::*;
    use crate::planner::plan_materialized_views;

    #[tokio::test]
    async fn dataflow_analysis_uses_table_scan_projection_and_resolves_alias() {
        let mut bid = SourceDefinition::new(
            "nexmark_bid",
            vec![
                SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
                SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            ],
        )
        .expect("bid source");
        bid.set_property("query_alias", "bid");
        let mut sources = SourceRegistry::new();
        sources.register(bid);
        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv AS SELECT auction, price FROM bid WHERE bidder = 42",
        )
        .expect("parse materialized view");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan materialized view");
        let analyses = analyze_dataflows(&planned, &sources).expect("analyze dataflow");
        assert_eq!(
            analyses[0].required_sources,
            BTreeSet::from(["nexmark_bid".into()])
        );
        assert_eq!(
            analyses[0].source_requirements[0].required_columns,
            vec![0, 1, 2]
        );
    }

    #[tokio::test]
    async fn dataflow_analysis_does_not_require_unused_source_columns() {
        let mut bid = SourceDefinition::new(
            "nexmark_bid",
            vec![
                SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
                SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            ],
        )
        .expect("bid source");
        bid.set_property("query_alias", "bid");
        let mut sources = SourceRegistry::new();
        sources.register(bid);
        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv AS SELECT auction FROM bid WHERE price > 42",
        )
        .expect("parse materialized view");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan materialized view");
        let analyses = analyze_dataflows(&planned, &sources).expect("analyze dataflow");
        assert_eq!(
            analyses[0].source_requirements[0].required_columns,
            vec![0, 2]
        );
    }
}
