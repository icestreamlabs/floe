use super::*;

#[test]
fn plans_distinct() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .project(vec![col(qualified(bid, "auction"))])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    assert!(matches!(root.kind, DbspNodeKind::Distinct(_)));
}

#[test]
fn plans_multi_column_distinct() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .project(vec![
            col(qualified(bid, "auction")),
            col(qualified(bid, "bidder")),
        ])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    assert!(matches!(root.kind, DbspNodeKind::Distinct(_)));
}

#[test]
fn plans_aggregate_over_distinct_subquery() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let distinct = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .project(vec![
            col(qualified(bid, "auction")),
            col(qualified(bid, "bidder")),
        ])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();
    let plan = LogicalPlanBuilder::from(distinct)
        .aggregate(Vec::<Expr>::new(), vec![count(col("auction"))])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root");
    assert!(matches!(root.kind, DbspNodeKind::Aggregate(_)));
    let input = *root.inputs.first().expect("aggregate input");
    let distinct_node = circuit_plan.node(input).expect("distinct input");
    assert!(matches!(distinct_node.kind, DbspNodeKind::Distinct(_)));
}

#[test]
fn prunes_unused_aggregate_calls_under_projection() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .aggregate(
            vec![col(qualified(bid, "auction"))],
            vec![
                sum(col(qualified(bid, "price"))).alias("total_price"),
                count(col(qualified(bid, "price"))).alias("bid_count"),
                avg(col(qualified(bid, "price"))).alias("avg_price"),
            ],
        )
        .unwrap()
        .project(vec![col("auction"), col("total_price")])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let (_plan, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");
    assert_eq!(
        diagnostics.rule_application_count("ProjectAggregatePrune"),
        1
    );

    let circuit_plan = planner.plan(&plan).expect("plan");
    let aggregate = circuit_plan
        .nodes
        .iter()
        .find_map(|node| match &node.kind {
            DbspNodeKind::Aggregate(aggregate) => Some(aggregate),
            _ => None,
        })
        .expect("aggregate node");
    assert_eq!(aggregate.aggregates().len(), 1);
    assert_eq!(aggregate.output_schema().len(), 2);
}

#[test]
fn prunes_unused_aggregate_calls_through_aggregate_filter() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .aggregate(
            vec![col(qualified(bid, "auction"))],
            vec![
                sum(col(qualified(bid, "price"))).alias("total_price"),
                count(col(qualified(bid, "price"))).alias("bid_count"),
                avg(col(qualified(bid, "price"))).alias("avg_price"),
            ],
        )
        .unwrap()
        .filter(col("bid_count").gt(lit(1_i64)))
        .unwrap()
        .project(vec![col("auction"), col("total_price")])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let (_plan, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");
    assert_eq!(
        diagnostics.rule_application_count("ProjectFilterAggregatePrune"),
        1
    );

    let circuit_plan = planner.plan(&plan).expect("plan");
    let aggregate = circuit_plan
        .nodes
        .iter()
        .find_map(|node| match &node.kind {
            DbspNodeKind::Aggregate(aggregate) => Some(aggregate),
            _ => None,
        })
        .expect("aggregate node");
    assert_eq!(aggregate.aggregates().len(), 2);
    assert_eq!(aggregate.output_schema().len(), 3);
}

#[test]
fn pushes_only_group_key_filters_below_aggregate() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .aggregate(
            vec![col(qualified(bid, "auction"))],
            vec![count(col(qualified(bid, "price"))).alias("bid_count")],
        )
        .unwrap()
        .filter(
            col("auction")
                .gt(lit(10_i64))
                .and(col("bid_count").gt(lit(1_i64))),
        )
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let (_plan, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");
    assert_eq!(
        diagnostics.rule_application_count("FilterAggregateTranspose"),
        1
    );

    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root");
    assert!(matches!(root.kind, DbspNodeKind::Select(_)));

    let aggregate_id = *root.inputs.first().expect("root select input");
    let aggregate = circuit_plan.node(aggregate_id).expect("aggregate");
    assert!(matches!(aggregate.kind, DbspNodeKind::Aggregate(_)));

    let pushed_select_id = *aggregate.inputs.first().expect("aggregate input");
    let pushed_select = circuit_plan.node(pushed_select_id).expect("pushed select");
    match &pushed_select.kind {
        DbspNodeKind::Select(select) => {
            let predicate = format!("{:?}", select.predicate().expression().expr());
            assert!(
                predicate.contains("auction"),
                "expected group-key predicate below aggregate, got {predicate}",
            );
            assert!(
                !predicate.contains("bid_count"),
                "aggregate-result predicate should remain above aggregate, got {predicate}",
            );
        }
        other => panic!("expected pushed Select below Aggregate, found {other:?}"),
    }
}

#[test]
fn plans_union_distinct_as_union_plus_distinct() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let left = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .project(vec![col(qualified(bid, "auction"))])
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .project(vec![col(qualified(bid, "auction"))])
        .unwrap()
        .build()
        .unwrap();
    let plan = LogicalPlanBuilder::from(left)
        .union_distinct(right)
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root node");
    let union_id = *root.inputs.first().expect("distinct input");
    assert!(matches!(root.kind, DbspNodeKind::Distinct(_)));

    let union = circuit_plan.node(union_id).expect("union node");
    match &union.kind {
        DbspNodeKind::Union(_) => assert_eq!(union.inputs.len(), 2),
        other => panic!("expected Union under Distinct, found {other:?}"),
    }
}
