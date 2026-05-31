use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::logical_expr::expr::WildcardOptions;
use datafusion::logical_expr::expr_fn::SimpleScalarUDF;
use datafusion::logical_expr::logical_plan::Sort as LogicalSort;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{
    ColumnarValue, Expr, JoinType, LogicalPlanBuilder, ScalarFunctionImplementation, ScalarUDF,
    Signature, TableSource, TypeSignature, Volatility, col, lit,
};
use datafusion::prelude::SessionContext;

use dbsp_circuit::circuit::plan::{
    DbspAggregateFunction, DbspJoinType, DbspNodeKind, DbspWindowPolicy,
};
use dbsp_circuit::circuit::schema::Field;
use dbsp_circuit::circuit::tables::TableDescriptor;
use dbsp_circuit::circuit::types::DbspScalarType;

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

fn table_source_owned(table: &TableDescriptor) -> Arc<dyn TableSource> {
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

fn select_predicate_in_unary_chain(
    circuit_plan: &super::CircuitPlan,
    mut node_id: usize,
) -> Option<String> {
    loop {
        let node = circuit_plan.node(node_id)?;
        if let DbspNodeKind::Select(select) = &node.kind {
            return Some(format!("{:?}", select.predicate().expression().expr()));
        }
        if node.inputs.len() != 1 {
            return None;
        }
        node_id = node.inputs[0];
    }
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let left = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let left = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let left = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let first = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let second = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let third = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
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
fn plans_half_open_range_join_without_equi_keys() {
    let windows = TableDescriptor::try_new_dynamic(
        "range_windows",
        vec![
            Field::new("window_id", DbspScalarType::Int64, false),
            Field::new("start_ts", DbspScalarType::TimestampMillis, false),
            Field::new("end_ts", DbspScalarType::TimestampMillis, false),
        ],
        &[String::from("window_id")],
    )
    .expect("windows descriptor");
    let events = TableDescriptor::try_new_dynamic(
        "range_events",
        vec![
            Field::new("event_id", DbspScalarType::Int64, false),
            Field::new("event_ts", DbspScalarType::TimestampMillis, false),
        ],
        &[String::from("event_id")],
    )
    .expect("events descriptor");

    let left = LogicalPlanBuilder::scan(windows.name, table_source_owned(&windows), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(events.name, table_source_owned(&events), None)
        .unwrap()
        .build()
        .unwrap();
    let filter = col("event_ts")
        .gt_eq(col("start_ts"))
        .and(col("event_ts").lt(col("end_ts")));

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (
                Vec::<datafusion::common::Column>::new(),
                Vec::<datafusion::common::Column>::new(),
            ),
            Some(filter),
        )
        .unwrap()
        .build()
        .unwrap();

    let mut config = planner_config();
    config.register_owned_table(windows);
    config.register_owned_table(events);
    let planner = CircuitPlanner::new(config);
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::Join(join) => {
            assert!(join.keys.is_empty());
            assert!(join.range.is_some());
            assert!(join.residual.is_none());
        }
        other => panic!("expected range join node, found {other:?}"),
    }
}

#[test]
fn infers_join_key_predicates_for_opposite_input() {
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
        .filter(col(qualified(person, "id")).gt(lit(10_i64)))
        .unwrap()
        .project(vec![
            col(qualified(person, "name")),
            col(qualified(auction, "item_name")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let join_node = circuit_plan
        .nodes
        .iter()
        .find(|node| matches!(node.kind, DbspNodeKind::Join(_)))
        .expect("join node");
    assert_eq!(join_node.inputs.len(), 2);

    assert!(
        select_predicate_in_unary_chain(&circuit_plan, join_node.inputs[0]).is_some(),
        "left-side predicate should be pushed below the join",
    );

    let right_predicate = select_predicate_in_unary_chain(&circuit_plan, join_node.inputs[1])
        .expect("inferred Select on right join input");
    assert!(
        right_predicate.contains("seller"),
        "right-side inferred predicate should target seller, got {right_predicate}",
    );
}

#[test]
fn infers_join_key_predicates_for_ambiguous_equivalence_class() {
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
                vec![qualified(person, "id"), qualified(person, "id")],
                vec![qualified(auction, "seller"), qualified(auction, "category")],
            ),
            None,
        )
        .unwrap()
        .filter(col(qualified(person, "id")).gt(lit(10_i64)))
        .unwrap()
        .project(vec![col(qualified(person, "name"))])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let predicates = circuit_plan
        .nodes
        .iter()
        .filter_map(|node| match &node.kind {
            DbspNodeKind::Select(select) => {
                Some(format!("{:?}", select.predicate().expression().expr()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let predicate = predicates
        .iter()
        .find(|predicate| predicate.contains("seller") && predicate.contains("category"))
        .unwrap_or_else(|| {
            panic!("expected inferred seller/category predicate, got {predicates:?}")
        });
    assert!(
        predicate.contains("seller"),
        "expected inferred seller predicate, got {predicate}",
    );
    assert!(
        predicate.contains("category"),
        "expected inferred category predicate, got {predicate}",
    );
}

#[tokio::test]
async fn prunes_join_expression_key_redundant_with_direct_key() {
    let plan = sql_plan(
        "SELECT b.auction, a.seller \
         FROM bid b JOIN auction a \
         ON b.auction = a.id AND b.auction % 10000 = a.id % 10000",
    )
    .await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let join_nodes = circuit_plan
        .nodes
        .iter()
        .filter_map(|node| match &node.kind {
            DbspNodeKind::Join(join) => Some(join),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(join_nodes.len(), 1, "expected exactly one join");

    let join = join_nodes[0];
    assert_eq!(join.keys.len(), 1);
    assert!(matches!(
        join.keys[0].left_expression().expr(),
        Expr::Column(column) if column.name == "auction"
    ));
    assert!(matches!(
        join.keys[0].right_expression().expr(),
        Expr::Column(column) if column.name == "id"
    ));
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
fn plans_left_semi_join_with_left_schema_output() {
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
            JoinType::LeftSemi,
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
    let root = circuit_plan.node(circuit_plan.root).expect("root");
    match &root.kind {
        DbspNodeKind::Join(join) => {
            assert!(matches!(join.join_type, DbspJoinType::LeftSemi));
            assert_eq!(join.output_schema.len(), join.left_schema.len());
            assert!(join.output_schema.field_index("name").is_some());
            assert!(join.output_schema.field_index("item_name").is_none());
        }
        other => panic!("expected join node, found {other:?}"),
    }
}

#[test]
fn plans_right_anti_join_with_right_schema_output() {
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
            JoinType::RightAnti,
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
    let root = circuit_plan.node(circuit_plan.root).expect("root");
    match &root.kind {
        DbspNodeKind::Join(join) => {
            assert!(matches!(join.join_type, DbspJoinType::RightAnti));
            assert_eq!(join.output_schema.len(), join.right_schema.len());
            assert!(join.output_schema.field_index("item_name").is_some());
            assert!(join.output_schema.field_index("name").is_none());
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

#[test]
fn normalizes_limit_sort_to_sort_fetch_for_topn() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();

    let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .sort(vec![col(qualified(bid, "price")).sort(false, true)])
        .unwrap()
        .limit(0, Some(5))
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let (optimized, diagnostics) = planner
        .optimize_logical_plan_with_diagnostics(&plan)
        .expect("optimize plan");
    assert_eq!(
        diagnostics.rule_application_count("LimitSortToSortFetch"),
        1
    );

    match optimized {
        datafusion::logical_expr::LogicalPlan::Sort(sort) => assert_eq!(sort.fetch, Some(5)),
        other => panic!("expected normalized Sort(fetch), found {other:?}"),
    }
}

#[test]
fn lowers_sort_fetch_to_topn() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();
    let input = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let plan = datafusion::logical_expr::LogicalPlan::Sort(LogicalSort {
        expr: vec![col(qualified(bid, "price")).sort(false, true)],
        input: Arc::new(input),
        fetch: Some(5),
    });

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::TopN(topn) => {
            assert_eq!(topn.limit(), 5);
            assert_eq!(topn.order_by().len(), 1);
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
async fn q9_post_projection_keeps_distinct_bid_alias_sources() {
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

    let root = circuit_plan.node(circuit_plan.root).expect("root");
    let project = match &root.kind {
        DbspNodeKind::Project(project) => project,
        other => panic!("expected root project node, got {other:?}"),
    };

    let mut date_time_expr = None;
    let mut bid_time_expr = None;
    let mut extra_expr = None;
    let mut bid_extra_expr = None;
    for projection in project.expressions() {
        match projection.alias() {
            "dateTime" => date_time_expr = Some(format!("{:?}", projection.expression().expr())),
            "bidTime" => bid_time_expr = Some(format!("{:?}", projection.expression().expr())),
            "extra" => extra_expr = Some(format!("{:?}", projection.expression().expr())),
            "bidExtra" => bid_extra_expr = Some(format!("{:?}", projection.expression().expr())),
            _ => {}
        }
    }

    let date_time_expr = date_time_expr.expect("missing dateTime projection");
    let bid_time_expr = bid_time_expr.expect("missing bidTime projection");
    let extra_expr = extra_expr.expect("missing extra projection");
    let bid_extra_expr = bid_extra_expr.expect("missing bidExtra projection");

    assert_ne!(
        date_time_expr, bid_time_expr,
        "dateTime and bidTime should come from distinct join columns"
    );
    assert_ne!(
        extra_expr, bid_extra_expr,
        "extra and bidExtra should come from distinct join columns"
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
    match &window.window.policy {
        DbspWindowPolicy::Session { gap_ms } => assert_eq!(*gap_ms, 5_000),
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
    match &window.window.policy {
        DbspWindowPolicy::Session { gap_ms } => assert_eq!(*gap_ms, 5_000),
        other => panic!("expected session window, got {other:?}"),
    }
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
