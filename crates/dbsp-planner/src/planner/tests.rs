use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::logical_expr::expr::WildcardOptions;
use datafusion::logical_expr::expr_fn::SimpleScalarUDF;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{
    ColumnarValue, Expr, JoinType, LogicalPlanBuilder, ScalarFunctionImplementation, ScalarUDF,
    Signature, TableSource, TypeSignature, Volatility, col, lit,
};
use datafusion::prelude::SessionContext;

use dbsp_circuit::circuit::plan::{DbspAggregateFunction, DbspJoinType, DbspNodeKind};
use dbsp_circuit::circuit::tables::TableDescriptor;

use super::expr::map_aggregate_expr;
use super::{CircuitPlanner, PlannerConfig};

fn planner_config() -> PlannerConfig {
    let mut config = PlannerConfig::new();
    config.register_table(dbsp_circuit::circuit::tables::nexmark_person_table());
    config.register_table(dbsp_circuit::circuit::tables::nexmark_person_alias_table());
    config.register_table(dbsp_circuit::circuit::tables::nexmark_auction_table());
    config.register_table(dbsp_circuit::circuit::tables::nexmark_auction_alias_table());
    config.register_table(dbsp_circuit::circuit::tables::nexmark_bid_table());
    config.register_table(dbsp_circuit::circuit::tables::nexmark_bid_alias_table());
    config
}

fn table_source(table: &'static TableDescriptor) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(table.schema().to_arrow_schema()))
}

fn udf_batch_len(args: &[ColumnarValue]) -> usize {
    args.iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1)
}

fn null_ts_value(len: usize) -> ColumnarValue {
    let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![None; len]));
    ColumnarValue::Array(array)
}

async fn sql_plan(sql: &str) -> datafusion::logical_expr::LogicalPlan {
    let ctx = SessionContext::new();
    for table in [
        dbsp_circuit::circuit::tables::nexmark_person_table(),
        dbsp_circuit::circuit::tables::nexmark_person_alias_table(),
        dbsp_circuit::circuit::tables::nexmark_auction_table(),
        dbsp_circuit::circuit::tables::nexmark_auction_alias_table(),
        dbsp_circuit::circuit::tables::nexmark_bid_table(),
        dbsp_circuit::circuit::tables::nexmark_bid_alias_table(),
    ] {
        let provider: Arc<dyn TableProvider> =
            Arc::new(EmptyTable::new(table.schema().to_arrow_schema()));
        ctx.register_table(table.name, provider)
            .expect("register nexmark table");
    }
    let passthrough_ts: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(udf_batch_len(args))))
        },
    );
    let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
    let tumble_sig = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64]),
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64, DataType::Int64]),
        ],
        Volatility::Immutable,
    );
    let hop_sig = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64, DataType::Int64]),
            TypeSignature::Exact(vec![
                ts.clone(),
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
            ]),
        ],
        Volatility::Immutable,
    );
    let session_sig = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64]),
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64, DataType::Int64]),
        ],
        Volatility::Immutable,
    );
    ctx.register_udf(ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "tumble",
        tumble_sig,
        ts.clone(),
        Arc::clone(&passthrough_ts),
    )));
    ctx.register_udf(ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "hop",
        hop_sig,
        ts.clone(),
        Arc::clone(&passthrough_ts),
    )));
    ctx.register_udf(ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "session",
        session_sig,
        ts,
        passthrough_ts,
    )));

    ctx.state()
        .create_logical_plan(sql)
        .await
        .expect("build SQL logical plan")
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
fn plans_left_outer_join_with_nullable_right_columns() {
    let person = dbsp_circuit::circuit::tables::nexmark_person_table();
    let auction = dbsp_circuit::circuit::tables::nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(auction.name, table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(person.name, table_source(person), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Left,
            (
                vec![qualified(auction, "seller")],
                vec![qualified(person, "id")],
            ),
            None,
        )
        .unwrap()
        .project(vec![
            col(qualified(auction, "id")),
            col(qualified(person, "name")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root");
    let join_id = *root.inputs.first().expect("project input");
    let join_node = circuit_plan.node(join_id).expect("join node");
    match &join_node.kind {
        DbspNodeKind::Join(join) => {
            assert!(matches!(join.join_type, DbspJoinType::LeftOuter));
            let right_start = join.left_schema.len();
            let right_name_field = join
                .output_schema
                .field(right_start + 1)
                .expect("right-side name field");
            assert!(right_name_field.nullable);
        }
        other => panic!("expected join node, found {other:?}"),
    }
}

#[test]
fn plans_right_outer_join_with_nullable_left_columns() {
    let person = dbsp_circuit::circuit::tables::nexmark_person_table();
    let auction = dbsp_circuit::circuit::tables::nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(auction.name, table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(person.name, table_source(person), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Right,
            (
                vec![qualified(auction, "seller")],
                vec![qualified(person, "id")],
            ),
            None,
        )
        .unwrap()
        .project(vec![
            col(qualified(auction, "id")),
            col(qualified(person, "name")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root");
    let join_id = *root.inputs.first().expect("project input");
    let join_node = circuit_plan.node(join_id).expect("join node");
    match &join_node.kind {
        DbspNodeKind::Join(join) => {
            assert!(matches!(join.join_type, DbspJoinType::RightOuter));
            let left_id_field = join.output_schema.field(0).expect("left-side id field");
            assert!(left_id_field.nullable);
        }
        other => panic!("expected join node, found {other:?}"),
    }
}

#[test]
fn plans_full_outer_join_with_nullable_both_sides() {
    let person = dbsp_circuit::circuit::tables::nexmark_person_table();
    let auction = dbsp_circuit::circuit::tables::nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(auction.name, table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(person.name, table_source(person), None)
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Full,
            (
                vec![qualified(auction, "seller")],
                vec![qualified(person, "id")],
            ),
            None,
        )
        .unwrap()
        .project(vec![
            col(qualified(auction, "id")),
            col(qualified(person, "name")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root");
    let join_id = *root.inputs.first().expect("project input");
    let join_node = circuit_plan.node(join_id).expect("join node");
    match &join_node.kind {
        DbspNodeKind::Join(join) => {
            assert!(matches!(join.join_type, DbspJoinType::FullOuter));
            let left_id_field = join.output_schema.field(0).expect("left-side id field");
            let right_start = join.left_schema.len();
            let right_name_field = join
                .output_schema
                .field(right_start + 1)
                .expect("right-side name field");
            assert!(left_id_field.nullable);
            assert!(right_name_field.nullable);
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

#[tokio::test]
async fn lowers_row_number_filter_to_partitioned_topn() {
    let sql = "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY \"dateTime\" DESC) AS rank_number FROM bid) ranked \
        WHERE rank_number <= 1";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");

    let topn_nodes = circuit_plan
        .nodes
        .iter()
        .filter_map(|node| match &node.kind {
            DbspNodeKind::TopN(topn) => Some(topn),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(topn_nodes.len(), 1, "expected exactly one TopN node");
    assert_eq!(topn_nodes[0].limit(), 1);
    assert_eq!(topn_nodes[0].partition_by().len(), 2);
}

#[tokio::test]
async fn preserves_subquery_projection_aliases_after_row_number_lowering() {
    let sql = "SELECT auction, bidder, price, \"bidTime\" \
        FROM (SELECT b.auction, b.bidder, b.price, b.\"dateTime\" AS \"bidTime\", \
              ROW_NUMBER() OVER (PARTITION BY b.auction ORDER BY b.price DESC, b.\"dateTime\" ASC) AS rownum \
              FROM bid b) ranked \
        WHERE rownum <= 1";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root node");
    let root_fields = root
        .output_schema
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(root_fields, vec!["auction", "bidder", "price", "bidTime"]);

    let topn = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::TopN(topn) => Some(topn),
        _ => None,
    });
    let topn = topn.expect("expected lowered TopN node");
    assert_eq!(topn.limit(), 1);
    assert_eq!(topn.partition_by().len(), 1);
    assert_eq!(topn.order_by().len(), 2);
    assert!(
        !topn.order_by()[0].ascending(),
        "first ORDER BY key should preserve DESC"
    );
    assert!(
        topn.order_by()[1].ascending(),
        "second ORDER BY key should preserve ASC"
    );
}

#[tokio::test]
async fn preserves_q9_row_number_ordering_after_lowering() {
    let sql = "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" \
        FROM (SELECT a.id, a.item_name AS \"itemName\", a.description, a.initial_bid AS \"initialBid\", a.reserve, a.date_time AS \"dateTime\", a.expires, a.seller, a.category, a.extra, \
              b.auction, b.bidder, b.price, b.date_time AS \"bidTime\", b.extra AS \"bidExtra\", \
              ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.date_time ASC) AS rownum \
              FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
              WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked \
        WHERE rownum <= 1";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let topn = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::TopN(topn) => Some(topn),
        _ => None,
    });
    let topn = topn.expect("expected lowered TopN node");
    assert_eq!(topn.limit(), 1);
    assert_eq!(topn.partition_by().len(), 1);
    assert_eq!(topn.order_by().len(), 2);
    assert!(
        !topn.order_by()[0].ascending(),
        "q9 primary ORDER BY key should preserve DESC"
    );
    assert!(
        topn.order_by()[1].ascending(),
        "q9 secondary ORDER BY key should preserve ASC"
    );
}

#[tokio::test]
async fn plans_hop_grouping_as_window_aggregate() {
    let sql =
        "SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let window = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::WindowAggregate(window) => Some(window),
        _ => None,
    });
    let window = window.expect("expected WindowAggregate node");
    assert_eq!(window.aggregate.group_keys().len(), 1);
    assert_eq!(window.window.allowed_lateness_ms, i64::MAX);
    match &window.window.policy {
        dbsp_circuit::circuit::plan::DbspWindowPolicy::Hopping { size_ms, slide_ms } => {
            assert_eq!(*size_ms, 10_000);
            assert_eq!(*slide_ms, 2_000);
        }
        other => panic!("expected hopping window, got {other:?}"),
    }
}

#[tokio::test]
async fn plans_hop_grouping_with_allowed_lateness() {
    let sql = "SELECT auction, COUNT(*) AS num \
        FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000, 1500)";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let window = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::WindowAggregate(window) => Some(window),
        _ => None,
    });
    let window = window.expect("expected WindowAggregate node");
    assert_eq!(window.window.allowed_lateness_ms, 1_500);
}

#[tokio::test]
async fn plans_tumble_grouping_as_window_aggregate() {
    let sql = "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE(\"dateTime\", 10000)";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let window = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::WindowAggregate(window) => Some(window),
        _ => None,
    });
    let window = window.expect("expected WindowAggregate node");
    assert_eq!(window.aggregate.group_keys().len(), 1);
    assert_eq!(window.window.allowed_lateness_ms, i64::MAX);
    match &window.window.policy {
        dbsp_circuit::circuit::plan::DbspWindowPolicy::Tumbling { size_ms } => {
            assert_eq!(*size_ms, 10_000);
        }
        other => panic!("expected tumbling window, got {other:?}"),
    }
}

#[tokio::test]
async fn plans_tumble_grouping_with_allowed_lateness() {
    let sql = "SELECT bidder, COUNT(*) AS bid_count \
        FROM bid GROUP BY bidder, TUMBLE(\"dateTime\", 10000, 750)";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let window = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::WindowAggregate(window) => Some(window),
        _ => None,
    });
    let window = window.expect("expected WindowAggregate node");
    assert_eq!(window.window.allowed_lateness_ms, 750);
}

#[tokio::test]
async fn plans_session_grouping_as_window_aggregate() {
    let sql = "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, SESSION(\"dateTime\", 5000)";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let window = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::WindowAggregate(window) => Some(window),
        _ => None,
    });
    let window = window.expect("expected WindowAggregate node");
    assert_eq!(window.window.allowed_lateness_ms, i64::MAX);
    match &window.window.policy {
        dbsp_circuit::circuit::plan::DbspWindowPolicy::Session { gap_ms } => {
            assert_eq!(*gap_ms, 5_000);
        }
        other => panic!("expected session window, got {other:?}"),
    }
}

#[tokio::test]
async fn plans_session_grouping_with_allowed_lateness() {
    let sql = "SELECT bidder, COUNT(*) AS bid_count \
        FROM bid GROUP BY bidder, SESSION(\"dateTime\", 5000, 1200)";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let window = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::WindowAggregate(window) => Some(window),
        _ => None,
    });
    let window = window.expect("expected WindowAggregate node");
    assert_eq!(window.window.allowed_lateness_ms, 1_200);
}

#[tokio::test]
async fn plans_filtered_and_distinct_count_aggregates() {
    let sql = "SELECT \
        COUNT(*) FILTER (WHERE price > 100) AS filtered_rows, \
        COUNT(DISTINCT bidder) FILTER (WHERE price > 100) AS filtered_distinct_bidders \
        FROM bid";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let aggregate = circuit_plan.nodes.iter().find_map(|node| match &node.kind {
        DbspNodeKind::Aggregate(aggregate) => Some(aggregate),
        _ => None,
    });
    let aggregate = aggregate.expect("expected Aggregate node");
    assert_eq!(aggregate.aggregates().len(), 2);

    let filtered = &aggregate.aggregates()[0];
    assert!(matches!(filtered.function(), DbspAggregateFunction::Count));
    assert!(filtered.filter().is_some());
    assert!(!filtered.distinct());

    let filtered_distinct = &aggregate.aggregates()[1];
    assert!(matches!(
        filtered_distinct.function(),
        DbspAggregateFunction::Count
    ));
    assert!(filtered_distinct.filter().is_some());
    assert!(filtered_distinct.distinct());
}
