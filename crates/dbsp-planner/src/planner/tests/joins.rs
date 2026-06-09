use super::*;

#[test]
fn plans_inner_join() {
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
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

    let left = LogicalPlanBuilder::scan(windows.name(), table_source_owned(&windows), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(events.name(), table_source_owned(&events), None)
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
fn plans_asof_join_without_equi_keys() {
    let auction = nexmark_auction_table();
    let bid = nexmark_bid_table();

    let left = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .build()
        .unwrap();
    let filter = col("price").lt_eq(col("reserve"));

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

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).unwrap();
    match &root.kind {
        DbspNodeKind::Join(join) => {
            assert!(join.keys.is_empty());
            assert!(join.range.is_none());
            assert!(join.asof.is_some());
        }
        other => panic!("expected ASOF join node, found {other:?}"),
    }
}

#[tokio::test]
async fn preplans_sql_asof_join_as_left_asof_node() {
    let plan = sql_plan(
        "SELECT a.id, b.price \
         FROM auction a ASOF JOIN bid b \
         MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") \
         ON a.id = b.auction",
    )
    .await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan ASOF SQL");
    let join_nodes = circuit_plan
        .nodes
        .iter()
        .filter_map(|node| match &node.kind {
            DbspNodeKind::Join(join) => Some(join),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(join_nodes.len(), 1, "expected exactly one ASOF join");
    let join = join_nodes[0];
    assert!(matches!(join.join_type, DbspJoinType::LeftOuter));
    assert_eq!(join.keys.len(), 1);
    assert!(join.asof.is_some());
    assert!(join.range.is_none());
    assert!(
        join.output_schema.fields()[join.left_schema.len()].nullable,
        "ASOF SQL should expose nullable RHS columns"
    );
}

#[test]
fn infers_join_key_predicates_for_opposite_input() {
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
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
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
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
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
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
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
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
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
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
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
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
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
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

#[tokio::test]
async fn plans_projected_right_semi_join_output_columns() {
    let plan = sql_plan(
        "SELECT key, value \
        FROM (SELECT a.id AS key, a.seller AS value \
            FROM bid b RIGHT SEMI JOIN auction a ON b.auction = a.id) s",
    )
    .await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root");

    assert_eq!(
        root.output_schema
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["key", "value"]
    );
    let join = circuit_plan
        .nodes
        .iter()
        .find_map(|node| match &node.kind {
            DbspNodeKind::Join(join) => Some(join),
            _ => None,
        })
        .expect("right semi join");
    assert!(matches!(join.join_type, DbspJoinType::RightSemi));
}

#[tokio::test]
async fn plans_projected_right_anti_join_output_columns() {
    let plan = sql_plan(
        "SELECT key, value \
        FROM (SELECT a.id AS key, a.seller AS value \
            FROM bid b RIGHT ANTI JOIN auction a ON b.auction = a.id) s",
    )
    .await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let root = circuit_plan.node(circuit_plan.root).expect("root");

    assert_eq!(
        root.output_schema
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["key", "value"]
    );
    let join = circuit_plan
        .nodes
        .iter()
        .find_map(|node| match &node.kind {
            DbspNodeKind::Join(join) => Some(join),
            _ => None,
        })
        .expect("right anti join");
    assert!(matches!(join.join_type, DbspJoinType::RightAnti));
}

#[test]
fn plans_multi_column_join() {
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();

    let left = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
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

#[tokio::test]
async fn plans_three_way_join_as_binary_join_composition() {
    let person = nexmark_person_table();
    let auction = nexmark_auction_table();
    let bid = nexmark_bid_table();
    let auction_plan = LogicalPlanBuilder::scan(auction.name(), table_source(auction), None)
        .unwrap()
        .project(vec![
            col(qualified(auction, "id")).alias("auction_id"),
            col(qualified(auction, "seller")),
        ])
        .unwrap()
        .build()
        .unwrap();
    let bid_plan = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
        .unwrap()
        .project(vec![
            col(qualified(bid, "auction")).alias("bid_auction"),
            col(qualified(bid, "price")),
        ])
        .unwrap()
        .build()
        .unwrap();
    let plan = LogicalPlanBuilder::scan(person.name(), table_source(person), None)
        .unwrap()
        .project(vec![col(qualified(person, "id")).alias("person_id")])
        .unwrap()
        .join(
            auction_plan,
            JoinType::Inner,
            (
                vec![Column::from_name("person_id")],
                vec![Column::from_name("seller")],
            ),
            None,
        )
        .unwrap()
        .join(
            bid_plan,
            JoinType::Inner,
            (
                vec![Column::from_name("auction_id")],
                vec![Column::from_name("bid_auction")],
            ),
            None,
        )
        .unwrap()
        .project(vec![col("person_id"), col("price")])
        .unwrap()
        .build()
        .unwrap();

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");
    let join_count = circuit_plan
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, DbspNodeKind::Join(_)))
        .count();
    assert_eq!(join_count, 2);
}

#[tokio::test]
async fn plans_scalar_subquery_filter_as_cross_join() {
    let sql = "SELECT auction, price \
        FROM bid \
        WHERE price > (SELECT MIN(\"initialBid\") FROM auction)";
    let plan = sql_plan(sql).await;

    let planner = CircuitPlanner::new(planner_config());
    let circuit_plan = planner.plan(&plan).expect("plan");

    let join = circuit_plan
        .nodes
        .iter()
        .find_map(|node| match &node.kind {
            DbspNodeKind::Join(join) => Some(join),
            _ => None,
        })
        .expect("expected scalar subquery rewrite to produce a join");
    assert!(matches!(join.join_type, DbspJoinType::LeftOuter));
    assert!(join.keys.is_empty());
    assert!(join.range.is_none());
    assert!(join.asof.is_none());
    assert!(
        circuit_plan
            .nodes
            .iter()
            .any(|node| matches!(node.kind, DbspNodeKind::Select(_))),
        "expected rewritten scalar predicate to be planned as a select"
    );
}
