use super::*;

#[test]
fn plans_aggregate_and_topn() {
    let bid = dbsp_circuit::circuit::tables::nexmark_bid_table();

    let plan = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
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

    let plan = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
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
    let input = LogicalPlanBuilder::scan(bid.name(), table_source(bid), None)
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
