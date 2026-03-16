use anyhow::Result;
use arrow_schema::SchemaRef;
use datafusion::common::Column;
use datafusion::logical_expr::{JoinType, col, lit, table_scan};

use floe_executor::dbsp_plan::{
    CircuitNode, CircuitPlan, DbspNodeKind, DbspPlanBuilder, PlannerError, TableDescriptor,
    nexmark_auction_table, nexmark_bid_table, nexmark_config, nexmark_person_table,
};
use floe_executor::plan_source_requirements;

#[test]
fn plans_scan_then_project() -> Result<()> {
    let logical_plan = table_scan(
        Some(nexmark_person_table().name),
        &schema_for(nexmark_person_table()),
        None,
    )?
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
    let right_plan = table_scan(
        Some(nexmark_person_table().name),
        &schema_for(nexmark_person_table()),
        None,
    )?
    .build()?;
    let logical_plan = table_scan(
        Some(nexmark_auction_table().name),
        &schema_for(nexmark_auction_table()),
        None,
    )?
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
fn source_requirement_analysis_tracks_filter_projection_inputs() -> Result<()> {
    let logical_plan = table_scan(
        Some(nexmark_bid_table().name),
        &schema_for(nexmark_bid_table()),
        None,
    )?
    .filter(col("auction").lt_eq(lit(5000i64)))?
    .project(vec![col("auction"), col("bidder"), col("price")])?
    .build()?;

    let plan = planner().build(&logical_plan)?;
    let requirements = plan_source_requirements(&plan)?
        .expect("filter/projection plan should support source requirement analysis");
    assert_eq!(requirements.len(), 1);
    assert_eq!(requirements[0].source_name, "nexmark_bid");
    assert_eq!(requirements[0].required_columns, vec![0, 1, 2]);

    Ok(())
}

#[test]
fn source_requirement_analysis_tracks_join_inputs() -> Result<()> {
    let auction_scan = table_scan(
        Some(nexmark_auction_table().name),
        &schema_for(nexmark_auction_table()),
        None,
    )?
    .filter(col("category").eq(lit(10i64)))?
    .build()?;
    let logical_plan = table_scan(
        Some(nexmark_bid_table().name),
        &schema_for(nexmark_bid_table()),
        None,
    )?
    .join(
        auction_scan,
        JoinType::Inner,
        (
            vec![Column::from_name("auction")],
            vec![Column::from_name("id")],
        ),
        None,
    )?
    .project(vec![
        col("auction"),
        col("bidder"),
        col("price"),
        col("seller"),
    ])?
    .build()?;

    let plan = planner().build(&logical_plan)?;
    let requirements = plan_source_requirements(&plan)?
        .expect("join plan should support source requirement analysis");
    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements[0].source_name, "nexmark_auction");
    assert_eq!(requirements[0].required_columns, vec![0, 5, 6]);
    assert_eq!(requirements[1].source_name, "nexmark_bid");
    assert_eq!(requirements[1].required_columns, vec![0, 1, 2]);

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
