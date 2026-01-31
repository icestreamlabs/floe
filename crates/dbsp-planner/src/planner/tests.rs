use std::sync::Arc;

use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::logical_expr::expr::WildcardOptions;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlanBuilder, TableSource, col};

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
