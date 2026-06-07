use anyhow::Result;
use arrow_schema::SchemaRef;
use datafusion::common::Column;
use datafusion::logical_expr::{JoinType, LogicalPlanBuilder, col, lit, table_scan};

use floe_executor::dbsp_plan::{
    CircuitNode, CircuitPlan, DbspNodeKind, DbspPlanBuilder, PlannerError, RowSchema,
    TableDescriptor, nexmark_auction_table, nexmark_bid_alias_table, nexmark_bid_table,
    nexmark_config, nexmark_person_table,
};
use floe_executor::plan_source_requirements;

#[test]
fn plans_scan_then_project() -> Result<()> {
    let logical_plan = table_scan(
        Some(nexmark_person_table().name()),
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
    let logical_plan = table_scan(Some(bid_table.name()), &schema_for(bid_table), None)?
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
        Some(nexmark_person_table().name()),
        &schema_for(nexmark_person_table()),
        None,
    )?
    .build()?;
    let logical_plan = table_scan(
        Some(nexmark_auction_table().name()),
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
        Some(nexmark_bid_table().name()),
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
        Some(nexmark_auction_table().name()),
        &schema_for(nexmark_auction_table()),
        None,
    )?
    .filter(col("category").eq(lit(10i64)))?
    .build()?;
    let logical_plan = table_scan(
        Some(nexmark_bid_table().name()),
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
fn source_requirement_analysis_tracks_right_anti_join_output_side() -> Result<()> {
    let auction_scan = table_scan(
        Some(nexmark_auction_table().name()),
        &schema_for(nexmark_auction_table()),
        None,
    )?
    .build()?;
    let logical_plan = table_scan(
        Some(nexmark_person_table().name()),
        &schema_for(nexmark_person_table()),
        None,
    )?
    .join(
        auction_scan,
        JoinType::RightAnti,
        (
            vec![Column::from_name("id")],
            vec![Column::from_name("seller")],
        ),
        None,
    )?
    .project(vec![col("id")])?
    .build()?;

    let plan = planner().build(&logical_plan)?;
    let requirements = plan_source_requirements(&plan)?
        .expect("right anti join plan should support source requirement analysis");
    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements[0].source_name, "nexmark_auction");
    assert_eq!(requirements[0].required_columns, vec![0, 5]);
    assert_eq!(requirements[1].source_name, "nexmark_person");
    assert_eq!(requirements[1].required_columns, vec![0]);

    Ok(())
}

#[test]
fn source_requirement_analysis_tracks_topn_inputs() -> Result<()> {
    let bid_table = nexmark_bid_table();
    let logical_plan = table_scan(Some(bid_table.name()), &schema_for(bid_table), None)?
        .sort(vec![col("price").sort(false, true)])?
        .limit(0, Some(5))?
        .project(vec![col("auction")])?
        .build()?;

    let plan = planner().build(&logical_plan)?;
    let requirements = plan_source_requirements(&plan)?
        .expect("topn plan should support source requirement analysis");
    assert_eq!(requirements.len(), 1);
    assert_eq!(requirements[0].source_name, "nexmark_bid");
    assert_eq!(requirements[0].required_columns, vec![0, 2]);

    Ok(())
}

#[test]
fn source_requirement_analysis_tracks_distinct_and_alias_sources() -> Result<()> {
    let bid_alias = nexmark_bid_alias_table();
    let logical_plan = table_scan(Some(bid_alias.name()), &schema_for(bid_alias), None)?
        .project(vec![col("auction"), col("price")])?
        .distinct()?
        .project(vec![col("auction")])?
        .build()?;

    let plan = planner().build(&logical_plan)?;
    let requirements = plan_source_requirements(&plan)?
        .expect("distinct plan should support source requirement analysis");
    assert_eq!(requirements.len(), 1);
    assert_eq!(requirements[0].source_name, "nexmark_bid");
    assert_eq!(requirements[0].required_columns, vec![0, 2]);

    Ok(())
}

#[test]
fn source_requirement_analysis_tracks_union_inputs() -> Result<()> {
    let bid_table = nexmark_bid_table();
    let bid_alias = nexmark_bid_alias_table();
    let left = table_scan(Some(bid_table.name()), &schema_for(bid_table), None)?
        .project(vec![col("auction"), col("price")])?
        .build()?;
    let right = table_scan(Some(bid_alias.name()), &schema_for(bid_alias), None)?
        .project(vec![col("auction"), col("price")])?
        .build()?;
    let logical_plan = LogicalPlanBuilder::from(left)
        .union(right)?
        .project(vec![col("auction")])?
        .build()?;

    let plan = planner().build(&logical_plan)?;
    let requirements = plan_source_requirements(&plan)?
        .expect("union plan should support source requirement analysis");
    assert_eq!(requirements.len(), 1);
    assert_eq!(requirements[0].source_name, "nexmark_bid");
    assert_eq!(requirements[0].required_columns, vec![0]);

    Ok(())
}

#[test]
fn join_filter_pushdown_prunes_join_inputs() -> Result<()> {
    let auction_scan = table_scan(
        Some(nexmark_auction_table().name()),
        &schema_for(nexmark_auction_table()),
        None,
    )?
    .build()?;
    let logical_plan = table_scan(
        Some(nexmark_bid_table().name()),
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
    .filter(col("category").eq(lit(10i64)))?
    .project(vec![
        col("auction"),
        col("bidder"),
        col("price"),
        col("seller"),
    ])?
    .build()?;

    let plan = planner().build(&logical_plan)?;
    let root = root_node(&plan);
    let join = match &input_node(&plan, root, 0).kind {
        DbspNodeKind::Join(join) => join,
        other => panic!("expected join below projection root, found {other:?}"),
    };
    assert_eq!(
        field_names(join.left_schema.as_ref()),
        vec!["auction", "bidder", "price"]
    );
    assert_eq!(
        field_names(join.right_schema.as_ref()),
        vec!["id", "seller"]
    );

    let left = plan
        .node(input_node(&plan, root, 0).inputs[0])
        .expect("left join input");
    match &left.kind {
        DbspNodeKind::Project(project) => {
            assert_eq!(
                field_names(project.output_schema().as_ref()),
                vec!["auction", "bidder", "price"]
            );
        }
        other => panic!("expected left join input project, found {other:?}"),
    }

    let right = plan
        .node(input_node(&plan, root, 0).inputs[1])
        .expect("right join input");
    let right_select = match &input_node(&plan, right, 0).kind {
        DbspNodeKind::Select(_) => input_node(&plan, right, 0),
        other => {
            panic!("expected pushed right-side filter below final projection, found {other:?}")
        }
    };
    match &right.kind {
        DbspNodeKind::Project(project) => {
            assert_eq!(
                field_names(project.output_schema().as_ref()),
                vec!["id", "seller"]
            );
        }
        other => panic!("expected projected right join input, found {other:?}"),
    };
    match &right_select.kind {
        DbspNodeKind::Select(_) => {
            let pushed_filter_input = input_node(&plan, right_select, 0);
            match &pushed_filter_input.kind {
                DbspNodeKind::Source(_) => {}
                other => {
                    panic!("expected pushed filter to read directly from source, found {other:?}")
                }
            }
        }
        other => panic!("expected select for pushed right filter, found {other:?}"),
    }

    let requirements = plan_source_requirements(&plan)?
        .expect("optimized join plan should support source requirement analysis");
    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements[0].source_name, "nexmark_auction");
    assert_eq!(requirements[0].required_columns, vec![0, 5, 6]);
    assert_eq!(requirements[1].source_name, "nexmark_bid");
    assert_eq!(requirements[1].required_columns, vec![0, 1, 2]);

    Ok(())
}

#[test]
fn mixed_join_filter_stays_above_join() -> Result<()> {
    let auction_scan = table_scan(
        Some(nexmark_auction_table().name()),
        &schema_for(nexmark_auction_table()),
        None,
    )?
    .build()?;
    let logical_plan = table_scan(
        Some(nexmark_bid_table().name()),
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
    .filter(col("price").gt(col("reserve")))?
    .project(vec![col("auction"), col("price"), col("reserve")])?
    .build()?;

    let plan = planner().build(&logical_plan)?;
    let root = root_node(&plan);
    let select = input_node(&plan, root, 0);
    let join = match &input_node(&plan, select, 0).kind {
        DbspNodeKind::Join(join) => join,
        other => panic!("expected join below remaining select, found {other:?}"),
    };
    assert_eq!(
        field_names(join.left_schema.as_ref()),
        vec!["auction", "price"]
    );
    assert_eq!(
        field_names(join.right_schema.as_ref()),
        vec!["id", "reserve"]
    );

    match &select.kind {
        DbspNodeKind::Select(_) => {}
        other => panic!("expected mixed predicate to remain above join, found {other:?}"),
    }

    let requirements = plan_source_requirements(&plan)?
        .expect("mixed join filter plan should support source requirement analysis");
    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements[0].source_name, "nexmark_auction");
    assert_eq!(requirements[0].required_columns, vec![0, 4]);
    assert_eq!(requirements[1].source_name, "nexmark_bid");
    assert_eq!(requirements[1].required_columns, vec![0, 2]);

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

#[test]
fn recursive_plans_are_rejected_until_guarded_feedback_is_supported() -> Result<()> {
    let schema = schema_for(nexmark_person_table());
    let static_term = table_scan(Some(nexmark_person_table().name()), &schema, None)?
        .project(vec![col("id")])?
        .build()?;
    let recursive_term = static_term.clone();
    let recursive_plan = LogicalPlanBuilder::from(static_term)
        .to_recursive_query("recursive_ids".to_string(), recursive_term, false)?
        .build()?;

    let err = planner().build(&recursive_plan).unwrap_err();
    match err {
        PlannerError::UnsupportedPlan(desc) => {
            assert!(
                desc.contains("RecursiveQuery") || desc.contains("not supported"),
                "expected recursive query rejection, got {desc}"
            );
        }
        other => panic!("expected unsupported recursive query error, got {other:?}"),
    }
    Ok(())
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

fn input_node<'a>(plan: &'a CircuitPlan, node: &'a CircuitNode, index: usize) -> &'a CircuitNode {
    let input_id = *node.inputs.get(index).expect("input index");
    plan.node(input_id).expect("input node")
}

fn field_names(schema: &RowSchema) -> Vec<&str> {
    schema
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect()
}
