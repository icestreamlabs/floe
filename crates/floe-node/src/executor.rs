use anyhow::Result;
use floe_executor::dbsp_plan::CircuitPlan;
use floe_executor::{DbspPlanBuilder, SourceRegistry, nexmark_config};

use crate::planner::PlannedMaterializedView;
use crate::source;

#[allow(dead_code)]
pub fn build_executor_sources(sources: &source::SourceRegistry) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry.extend(sources.definitions().iter().cloned());
    registry
}

pub fn build_dataflows(views: &[PlannedMaterializedView]) -> Result<Vec<CircuitPlan>> {
    let planner = DbspPlanBuilder::new(nexmark_config());
    views
        .iter()
        .map(|planned| {
            planner
                .build(planned.logical_plan())
                .map_err(|err| anyhow::anyhow!(err.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
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

        let definition =
            parse_materialized_view("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM person")
                .expect("parse mv");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan mv");

        let plans = build_dataflows(&planned).expect("build dbsp plan");
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

        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv AS SELECT * FROM bid WHERE bidder = 42",
        )
        .expect("parse mv");
        let planned = plan_materialized_views(&sources, &[definition])
            .await
            .expect("plan mv");

        let plans = build_dataflows(&planned).expect("build dbsp plan");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert!(
            plan.nodes()
                .iter()
                .any(|node| matches!(node.kind, DbspNodeKind::Select(_))),
            "expected plan to contain a select node"
        );
    }
}
