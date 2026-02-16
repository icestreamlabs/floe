use std::sync::Arc;

use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::logical_expr::expr::WildcardOptions;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlanBuilder, TableSource, col, lit};

use dbsp_circuit::circuit::plan::{DbspAggregateFunction, DbspNodeKind};
use dbsp_circuit::circuit::tables::TableDescriptor;

use super::expr::map_aggregate_expr;
use super::{CircuitPlanner, PlannerConfig};

fn planner_config() -> PlannerConfig {
    let mut config = PlannerConfig::new();
    config.register_table(dbsp_circuit::circuit::tables::nexmark_person_table());
    config.register_table(dbsp_circuit::circuit::tables::nexmark_auction_table());
    config.register_table(dbsp_circuit::circuit::tables::nexmark_bid_table());
    config
}

fn table_source(table: &'static TableDescriptor) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(table.schema().to_arrow_schema()))
}

fn qualified(table: &'static TableDescriptor, column: &str) -> String {
    format!("{}.{}", table.name, column)
}

#[test]
fn count_star_maps_to_untyped_count() {
    #[allow(deprecated)]
    let wildcard = Expr::Wildcard {
        qualifier: None,
        options: Box::<WildcardOptions>::default(),
    };

    let expr = count(wildcard);
    let (function, arg, alias) = map_aggregate_expr(&expr).expect("map aggregate");

    assert!(matches!(function, DbspAggregateFunction::Count));
    assert!(arg.is_none());
    assert!(alias.is_none());
}

#[test]
fn plans_projection_over_scan() {
    let table = dbsp_circuit::circuit::tables::nexmark_person_table();
    let plan = LogicalPlanBuilder::scan(table.name, table_source(table), None)
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
    let table = dbsp_circuit::circuit::tables::nexmark_person_table();
    let plan = LogicalPlanBuilder::scan_with_filters(
        table.name,
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

#[test]
fn plans_inner_join() {
    let person = dbsp_circuit::circuit::tables::nexmark_person_table();
    let auction = dbsp_circuit::circuit::tables::nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name, table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name, table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (
                vec![qualified(person, "id")],
                vec![qualified(auction, "seller")],
            ),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::Join(join) => {
            assert_eq!(join.keys.len(), 1);
        }
        other => panic!("expected join node, found {other:?}"),
    }
}

#[test]
fn plans_multi_column_join() {
    let person = dbsp_circuit::circuit::tables::nexmark_person_table();
    let auction = dbsp_circuit::circuit::tables::nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name, table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name, table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (
                vec![qualified(person, "id"), qualified(person, "date_time")],
                vec![qualified(auction, "seller"), qualified(auction, "expires")],
            ),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::Join(join) => {
            assert_eq!(join.keys.len(), 2);
        }
        other => panic!("expected join node, found {other:?}"),
    }
}

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

#[test]
fn plans_aggregate_and_topn() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();

    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .aggregate(
            vec![col(qualified(bid, "bidder"))],
            vec![
                sum(col(qualified(bid, "price"))).alias("total_price"),
                count(col(qualified(bid, "price"))).alias("bid_count"),
                avg(col(qualified(bid, "price"))).alias("avg_price"),
            ],
        )
        .unwrap()
        .sort(vec![col("total_price").sort(true, true)])
        .unwrap()
        .limit(0, Some(5))
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::TopN(topn) => {
            assert_eq!(topn.output_schema().len(), 4);
        }
        other => panic!("expected TopN node, found {other:?}"),
    }
}
