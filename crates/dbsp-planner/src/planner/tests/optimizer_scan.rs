use super::*;

#[test]
fn count_star_maps_to_untyped_count() {
    #[allow(deprecated)]
    let wildcard = Expr::Wildcard {
        qualifier: None,
        options: Box::<WildcardOptions>::default(),
    };

    let expr = count(wildcard);
    let (function, arg, filter, distinct, alias) =
        map_aggregate_expr(&expr).expect("map aggregate");

    assert!(matches!(function, DbspAggregateFunction::Count));
    assert!(arg.is_none());
    assert!(filter.is_none());
    assert!(!distinct);
    assert_eq!(alias.as_deref(), Some("count(*)"));
}

#[test]
fn plans_projection_over_scan() {
    let table = nexmark_person_table();
    let plan = LogicalPlanBuilder::scan(table.name(), table_source(table), None)
        .unwrap()
        .project(vec![
            col(qualified(table, "id")),
            col(qualified(table, "name")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::Project(project) => {
            assert_eq!(project.output_schema().len(), 2);
        }
        other => panic!("expected project node, found {other:?}"),
    }
}

#[test]
fn plans_scan_pushdown_filter_and_projection() {
    let table = nexmark_person_table();
    let plan = LogicalPlanBuilder::scan_with_filters(
        table.name(),
        table_source(table),
        Some(vec![0, 1]),
        vec![col(qualified(table, "id")).gt(lit(5_i64))],
    )
    .unwrap()
    .build()
    .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");

    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::Project(project) => {
            assert_eq!(project.output_schema().len(), 2);
            let select_id = *root.inputs.first().expect("project input");
            let select = circuit_plan.node(select_id).expect("select node");
            assert!(matches!(select.kind, DbspNodeKind::Select(_)));
            let source_id = *select.inputs.first().expect("select input");
            let source = circuit_plan.node(source_id).expect("source node");
            assert!(matches!(source.kind, DbspNodeKind::Source(_)));
        }
        other => panic!("expected Project node, found {other:?}"),
    }
}

#[tokio::test]
async fn pushes_filter_through_subquery_projection_alias() {
    let plan = sql_plan("SELECT p FROM (SELECT price AS p, auction FROM bid) q WHERE p > 10").await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();

    let project = match &root.kind {
        DbspNodeKind::Project(project) => project,
        other => panic!("expected Project root, found {other:?}"),
    };
    assert_eq!(project.output_schema().field(0).unwrap().name, "p");

    let predicate = select_predicate_in_unary_chain(&circuit_plan, root.id)
        .expect("pushed Select below projection alias");
    assert!(
        predicate.contains("price"),
        "predicate should be rewritten onto the base column, got {predicate}",
    );
}

#[test]
fn merges_consecutive_projection_nodes() {
    let bid = nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .project(vec![
            col(qualified(bid, "price")).alias("p"),
            col(qualified(bid, "auction")).alias("a"),
        ])
        .unwrap()
        .project(vec![col("p").alias("price_alias")])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let project_count = circuit_plan
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, DbspNodeKind::Project(_)))
        .count();
    assert_eq!(project_count, 1);
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    let project = match &root.kind {
        DbspNodeKind::Project(project) => project,
        other => panic!("expected Project root, found {other:?}"),
    };
    assert_eq!(
        project.output_schema().field(0).unwrap().name,
        "price_alias"
    );
}

#[test]
fn optimizer_diagnostics_report_named_stages_and_rules() {
    let bid = nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .project(vec![
            col(qualified(bid, "price")).alias("p"),
            col(qualified(bid, "auction")).alias("a"),
        ])
        .unwrap()
        .project(vec![col("p").alias("price_alias")])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let (_plan, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");

    assert!(diagnostics.total_applications() > 0);
    assert_eq!(diagnostics.rule_application_count("MergeProjections"), 1);
    assert!(!diagnostics.max_passes_reached());
    assert!(
        diagnostics
            .stages()
            .iter()
            .any(|stage| stage.name() == "Normalize")
    );
}

#[test]
fn can_disable_optimizer_rule_by_name() {
    let bid = nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .project(vec![
            col(qualified(bid, "price")).alias("p"),
            col(qualified(bid, "auction")).alias("a"),
        ])
        .unwrap()
        .project(vec![col("p").alias("price_alias")])
        .unwrap()
        .build()
        .unwrap();

    let planner =
        CircuitPlanner::new(planner_config().with_disabled_optimizer_rule("MergeProjections"));
    let (_plan, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");
    assert_eq!(diagnostics.rule_application_count("MergeProjections"), 0);

    let circuit_plan = planner.plan(&plan).expect("plan");
    let project_count = circuit_plan
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, DbspNodeKind::Project(_)))
        .count();
    assert_eq!(project_count, 2);
}

#[test]
fn optimizer_diagnostics_include_disabled_rules_and_stage_counts() {
    let bid = nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .project(vec![
            col(qualified(bid, "price")).alias("p"),
            col(qualified(bid, "auction")).alias("a"),
        ])
        .unwrap()
        .project(vec![col("p").alias("price_alias")])
        .unwrap()
        .build()
        .unwrap();

    let planner =
        CircuitPlanner::new(planner_config().with_disabled_optimizer_rule("MergeProjections"));
    let (_plan, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");

    assert_eq!(diagnostics.rule_application_count("MergeProjections"), 0);
    assert!(diagnostics.disabled_rules().contains(&"MergeProjections"));
    assert_eq!(diagnostics.stage_application_count("Normalize"), 0);
    let normalize = diagnostics
        .stages()
        .iter()
        .find(|stage| stage.name() == "Normalize")
        .expect("normalize stage diagnostics");
    assert!(normalize.disabled_rules().contains(&"MergeProjections"));
}

#[test]
fn pushes_filter_and_projection_into_union_inputs() {
    let bid = nexmark_bid_table();
    let left = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .union(right)
        .unwrap()
        .filter(col("price").gt(lit(100_i64)))
        .unwrap()
        .project(vec![col("auction")])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();

    let union = match &root.kind {
        DbspNodeKind::Union(union) => union,
        other => panic!("expected Union root, found {other:?}"),
    };
    assert_eq!(union.output_schema().len(), 1);
    assert_eq!(root.inputs.len(), 2);

    for input_id in &root.inputs {
        let project = circuit_plan.node(*input_id).expect("union input");
        let select_id = match &project.kind {
            DbspNodeKind::Project(_) => *project.inputs.first().expect("project input"),
            other => panic!("expected Project below Union, found {other:?}"),
        };
        let select = circuit_plan.node(select_id).expect("select input");
        assert!(matches!(select.kind, DbspNodeKind::Select(_)));
    }
}

#[test]
fn skips_union_filter_pushdown_when_duplication_input_gate_exceeded() {
    let bid = nexmark_bid_table();
    let left = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .union(right)
        .unwrap()
        .filter(col("price").gt(lit(100_i64)))
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config().with_optimizer_max_duplicated_inputs(1));
    let (optimized, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");

    assert_eq!(
        diagnostics.rule_application_count("FilterUnionTranspose"),
        0
    );
    match optimized {
        datafusion::logical_expr::LogicalPlan::Filter(filter) => {
            assert!(matches!(
                filter.input.as_ref(),
                datafusion::logical_expr::LogicalPlan::Union(_)
            ));
        }
        other => panic!("expected Filter over Union after gated pushdown, found {other:?}"),
    }
}

#[test]
fn skips_union_projection_pushdown_when_expression_duplication_gate_exceeded() {
    let bid = nexmark_bid_table();
    let left = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .union(right)
        .unwrap()
        .project(vec![col("auction")])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config().with_optimizer_max_duplicated_expr_nodes(1));
    let (optimized, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");

    assert_eq!(
        diagnostics.rule_application_count("ProjectUnionTranspose"),
        0
    );
    match optimized {
        datafusion::logical_expr::LogicalPlan::Projection(projection) => {
            assert!(matches!(
                projection.input.as_ref(),
                datafusion::logical_expr::LogicalPlan::Union(_)
            ));
        }
        other => panic!("expected Projection over Union after gated pushdown, found {other:?}"),
    }
}

#[test]
fn flattens_nested_union_nodes() {
    let bid = nexmark_bid_table();
    let first = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let second = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let third = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(first)
        .union(second)
        .unwrap()
        .union(third)
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::Union(_) => assert_eq!(root.inputs.len(), 3),
        other => panic!("expected flattened Union root, found {other:?}"),
    }
}
