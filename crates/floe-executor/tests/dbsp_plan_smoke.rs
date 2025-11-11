use anyhow::Result;
use arrow_schema::SchemaRef;
use datafusion::common::Column;
use datafusion::logical_expr::{col, lit, table_scan, JoinType};

use floe_executor::dbsp_plan::{
    CircuitNode, CircuitPlan, DbspNodeKind, DbspPlanBuilder, PlannerError, TableDescriptor,
    nexmark_auction_table, nexmark_bid_table, nexmark_config, nexmark_person_table,
};

#[test]
fn plans_scan_then_project() -> Result<()> {
    let logical_plan = table_scan(Some(nexmark_person_table().name), &schema_for(nexmark_person_table()), None)?
        .project(vec![col("id"), col("name")])?
        .build()?;

    let plan = planner().build(&logical_plan)?;
    let root = root_node(&plan);
    match &root.kind {
        DbspNodeKind::Project(project) => {
            assert_eq!(project.expressions().len(), 2);
            assert_eq!(project.output_schema().len(), 2);
        }
        other => panic!("expected project root, found {other:?}"),
    }

    Ok(())
}

#[test]
fn plans_scan_then_filter() -> Result<()> {
    let bid_table = nexmark_bid_table();
    let predicate = col("bidder")
        .eq(lit(42i64))
        .and(col("price").gt(lit(10i64)));
    let logical_plan = table_scan(Some(bid_table.name), &schema_for(bid_table), None)?
        .filter(predicate)?
        .build()?;

    let plan = planner().build(&logical_plan)?;
    let root = root_node(&plan);
    match &root.kind {
        DbspNodeKind::Select(_) => {
            assert_eq!(root.output_schema.len(), bid_table.schema().len());
        }
        other => panic!("expected select root, found {other:?}"),
    }

    Ok(())
}

#[test]
fn plans_join_with_single_key() -> Result<()> {
    let right_plan =
        table_scan(Some(nexmark_person_table().name), &schema_for(nexmark_person_table()), None)?
            .build()?;
    let logical_plan = table_scan(Some(nexmark_auction_table().name), &schema_for(nexmark_auction_table()), None)?
        .join(
            right_plan,
            JoinType::Inner,
            (
                vec![Column::from_name("seller")],
                vec![Column::from_name("id")],
            ),
            None,
        )?
        .build()?;

    let plan = planner().build(&logical_plan)?;
    let root = root_node(&plan);
    match &root.kind {
        DbspNodeKind::Join(join) => {
            assert_eq!(join.keys.len(), 1);
            let left_width = join.left_schema.len();
            let right_width = join.right_schema.len();
            assert!(
                join.output_schema.len() > left_width.max(right_width),
                "join output schema should include columns from both inputs"
            );
        }
        other => panic!("expected join root, found {other:?}"),
    }

    Ok(())
}

#[test]
fn missing_table_raises_planner_error() {
    let schema = schema_for(nexmark_person_table());
    let logical_plan = table_scan(Some("unregistered_table"), &schema, None)
        .expect("table scan")
        .build()
        .expect("plan build");

    let err = planner().build(&logical_plan).unwrap_err();
    match err {
        PlannerError::TableNotFound(name) => {
            assert_eq!(name, "unregistered_table");
        }
        other => panic!("expected table not found error, got {other:?}"),
    }
}

fn planner() -> DbspPlanBuilder {
    DbspPlanBuilder::new(nexmark_config())
}

fn schema_for(table: &'static TableDescriptor) -> SchemaRef {
    table.schema().to_arrow_schema()
}

fn root_node(plan: &CircuitPlan) -> &CircuitNode {
    plan.node(plan.root).expect("plan root node")
}
