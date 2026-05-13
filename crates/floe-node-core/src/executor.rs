use std::collections::BTreeSet;

use anyhow::{Context, Result};
use floe_core::source::SourceDataType;
use floe_executor::DbspPlanBuilder;
use floe_executor::dbsp_plan::{
    CircuitPlan, DbspScalarType, Field, PlannerConfig, TableDescriptor, nexmark_config,
    validate_dbsp_plan,
};

use crate::planner::PlannedMaterializedView;
use crate::planner::to_camel_case;
use crate::source::SourceDefinition;

const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";

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
pub fn available_sources_from_registry(
    registry: &crate::source::SourceRegistry,
) -> BTreeSet<String> {
    registry
        .definitions()
        .iter()
        .flat_map(|definition| {
            let mut names = Vec::with_capacity(2);
            names.push(definition.name().to_string());
            if let Some(alias) = definition.name().strip_prefix("nexmark_") {
                names.push(alias.to_string());
            }
            names
        })
        .collect()
}

pub fn build_dataflows(
    views: &[PlannedMaterializedView],
    available_sources: &BTreeSet<String>,
    sources: &crate::source::SourceRegistry,
) -> Result<Vec<CircuitPlan>> {
    let planner = DbspPlanBuilder::new(planner_config_from_sources(sources)?);
    views
        .iter()
        .map(|planned| {
            let plan = planner
                .build(planned.logical_plan())
                .with_context(|| format!("build DBSP plan for {}", planned.definition().name()))?;
            validate_dbsp_plan(&plan, available_sources, planned.definition().name())
                .context("validating query plan")?;
            Ok(plan)
        })
        .collect()
}

pub fn planner_config_from_sources(
    registry: &crate::source::SourceRegistry,
) -> Result<PlannerConfig> {
    let mut config = nexmark_config();
    for definition in registry.definitions() {
        if definition.property(SOURCE_PRIMARY_KEY_PROPERTY).is_none() {
            continue;
        }
        config.register_owned_table(table_descriptor_from_source(
            definition,
            definition.name().to_string(),
            false,
        )?);
        if let Some(short_name) = definition.name().strip_prefix("nexmark_") {
            config.register_owned_table(table_descriptor_from_source(
                definition,
                short_name.to_string(),
                true,
            )?);
        }
    }
    Ok(config)
}

fn table_descriptor_from_source(
    definition: &SourceDefinition,
    name: String,
    camel_case_columns: bool,
) -> Result<TableDescriptor> {
    let fields = definition
        .columns()
        .iter()
        .map(|column| {
            let name = if camel_case_columns {
                to_camel_case(column.name())
            } else {
                column.name().to_string()
            };
            Field::new(
                name,
                source_type_to_dbsp(column.data_type()),
                column.nullable(),
            )
        })
        .collect::<Vec<_>>();
    let primary_key = definition
        .property(SOURCE_PRIMARY_KEY_PROPERTY)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(|column| {
                    if camel_case_columns {
                        to_camel_case(column)
                    } else {
                        column.to_string()
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    TableDescriptor::try_new_dynamic(name, fields, &primary_key)
}

fn source_type_to_dbsp(data_type: &SourceDataType) -> DbspScalarType {
    match data_type {
        SourceDataType::Int64 => DbspScalarType::Int64,
        SourceDataType::Bool => DbspScalarType::Bool,
        SourceDataType::Utf8 => DbspScalarType::Utf8,
        SourceDataType::TimestampMillis => DbspScalarType::TimestampMillis,
        SourceDataType::DateDays => DbspScalarType::DateDays,
        SourceDataType::Numeric => DbspScalarType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
    use floe_sql_parser::parse_materialized_view;

    use super::*;
    use crate::generator;
    use crate::planner::plan_materialized_views;
    use crate::source::SourceRegistry;
    use floe_executor::dbsp_plan::DbspNodeKind;

    #[tokio::test]
    async fn plans_projection_materialized_view() {
        let mut sources = SourceRegistry::new();
        sources.extend(generator::definitions().expect("generator definitions"));
        let available_sources = available_sources_from_registry(&sources);

        let definition =
            parse_materialized_view("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM person")
                .expect("parse mv");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan mv");

        let plans =
            build_dataflows(&planned, &available_sources, &sources).expect("build dbsp plan");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        let root = plan.node(plan.root).expect("root node exists");
        match &root.kind {
            DbspNodeKind::Project(project) => {
                assert_eq!(project.expressions().len(), 2);
            }
            other => panic!("expected project root node, found {other:?}"),
        }
        assert!(
            plan.nodes()
                .iter()
                .any(|node| matches!(node.kind, DbspNodeKind::Source(_))),
            "expected plan to contain a source node"
        );
    }

    #[tokio::test]
    async fn plans_filter_materialized_view() {
        let mut sources = SourceRegistry::new();
        sources.extend(generator::definitions().expect("generator definitions"));
        let available_sources = available_sources_from_registry(&sources);

        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv AS SELECT * FROM bid WHERE bidder = 42",
        )
        .expect("parse mv");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan mv");

        let plans =
            build_dataflows(&planned, &available_sources, &sources).expect("build dbsp plan");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert!(
            plan.nodes()
                .iter()
                .any(|node| matches!(node.kind, DbspNodeKind::Select(_))),
            "expected plan to contain a select node"
        );
    }

    #[tokio::test]
    async fn plans_simple_join_materialized_view() {
        let mut sources = SourceRegistry::new();
        sources.extend(generator::definitions().expect("generator definitions"));
        let available_sources = available_sources_from_registry(&sources);

        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv AS SELECT b.auction, b.bidder, a.seller \
             FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id",
        )
        .expect("parse mv");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan mv");

        let plans =
            build_dataflows(&planned, &available_sources, &sources).expect("build dbsp plan");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert!(
            plan.nodes()
                .iter()
                .any(|node| matches!(node.kind, DbspNodeKind::Join(_))),
            "expected plan to contain a join node"
        );
    }

    #[tokio::test]
    async fn plans_join_over_dynamic_source_registry_schemas() {
        let mut sources = SourceRegistry::new();
        let mut bid = SourceDefinition::new(
            "nexmark_bid",
            vec![
                SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
                SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            ],
        )
        .expect("bid source");
        bid.set_property("primary_key", "auction");
        let mut auction = SourceDefinition::new(
            "nexmark_auction",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            ],
        )
        .expect("auction source");
        auction.set_property("primary_key", "id");
        sources.register(bid);
        sources.register(auction);
        let available_sources = available_sources_from_registry(&sources);

        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv AS SELECT b.auction, b.bidder, a.seller \
             FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id",
        )
        .expect("parse mv");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan mv");

        let plans =
            build_dataflows(&planned, &available_sources, &sources).expect("build dbsp plan");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert!(
            plan.nodes()
                .iter()
                .any(|node| matches!(node.kind, DbspNodeKind::Join(_))),
            "expected plan to contain a join node"
        );
    }
}
