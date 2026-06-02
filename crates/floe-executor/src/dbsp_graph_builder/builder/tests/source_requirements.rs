use super::*;

#[test]
fn benchmark_join_shape_still_matches_transient_join_root() {
    let logical = benchmark_join_logical_plan();
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    let persistence_policy = PersistencePolicy::for_plan(&plan);
    let transient_opt = try_build_transient_segment_optimization(
        &plan,
        plan.root,
        &HashMap::new(),
        "benchmark_result",
        true,
        &persistence_policy,
    )
    .expect("transient optimization result");

    assert!(
        transient_opt.is_some(),
        "expected transient optimization for benchmark query plan: {plan:#?}"
    );
    let transient_sources = source_batch_journal_root_sources(&plan)
        .expect("source batch journal root sources")
        .expect("source batch journal root sources");
    assert_eq!(
        transient_sources,
        BTreeSet::from(["nexmark_auction".to_string(), "nexmark_bid".to_string()])
    );
    let transient_opt = transient_opt.expect("transient opt");
    let join_node = plan
        .node(transient_opt.durable_input_idx)
        .expect("durable input node");
    assert!(
        matches!(join_node.kind, DbspNodeKind::Join(_)),
        "expected durable input to be a join node: {plan:#?}"
    );
    let join = match &join_node.kind {
        DbspNodeKind::Join(join) => join,
        other => panic!("expected join node, got {other:?}"),
    };
    let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");
    assert!(
        try_build_transient_source_root_materialization(&plan, left_idx)
            .expect("left transient input shape")
            .is_some(),
        "expected left benchmark join input to be transient-eligible: {plan:#?}"
    );
    assert!(
        try_build_transient_source_root_materialization(&plan, right_idx)
            .expect("right transient input shape")
            .is_some(),
        "expected right benchmark join input to be transient-eligible: {plan:#?}"
    );
    assert!(
        try_build_direct_join_output_projection(join, &transient_opt.steps).is_some(),
        "expected benchmark join root to expose a direct output projection: {plan:#?}"
    );
}

#[test]
fn nested_source_projection_root_stays_source_batch_journal_eligible() {
    let source_table = nexmark_bid_table();
    let source_schema = source_table.schema().clone();
    let first_items = source_schema
        .fields()
        .iter()
        .map(|field| ProjectItem {
            expr: col(field.name.as_str()),
            alias: Some(field.name.clone()),
        })
        .collect::<Vec<_>>();
    let first_project =
        DbspProjectNode::try_new(Arc::clone(&source_schema), first_items).expect("project");
    let first_schema = first_project.output_schema().clone();
    let second_items = first_schema
        .fields()
        .iter()
        .map(|field| ProjectItem {
            expr: col(field.name.as_str()),
            alias: Some(field.name.clone()),
        })
        .collect::<Vec<_>>();
    let second_project =
        DbspProjectNode::try_new(Arc::clone(&first_schema), second_items).expect("project");
    let second_schema = second_project.output_schema().clone();
    let plan = CircuitPlan {
        root: 2,
        nodes: vec![
            CircuitNode {
                id: 0,
                kind: DbspNodeKind::Source(DbspSourceNode {
                    table: Arc::new(source_table.clone()),
                }),
                inputs: vec![],
                output_schema: source_schema,
            },
            CircuitNode {
                id: 1,
                kind: DbspNodeKind::Project(first_project),
                inputs: vec![0],
                output_schema: first_schema,
            },
            CircuitNode {
                id: 2,
                kind: DbspNodeKind::Project(second_project),
                inputs: vec![1],
                output_schema: second_schema,
            },
        ],
    };

    let transient_sources = source_batch_journal_root_sources(&plan)
        .expect("source batch journal root sources")
        .expect("source batch journal root sources");
    assert_eq!(
        transient_sources,
        BTreeSet::from(["nexmark_bid".to_string()])
    );
    assert!(
        try_build_transient_source_root_materialization(&plan, plan.root)
            .expect("transient source root materialization")
            .is_some(),
        "expected nested source projections to remain transient-eligible: {plan:#?}"
    );
}

#[tokio::test]
async fn q4_join_aggregate_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT category, AVG(max) \
             FROM (SELECT MAX(b.price) AS max, a.category \
                   FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
                   WHERE b.date_time BETWEEN a.date_time AND a.expires \
                   GROUP BY a.id, a.category) per_auction \
             GROUP BY category",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let transient_sources = source_batch_journal_root_sources(&plan)
        .expect("source batch journal root sources")
        .expect("source batch journal root sources");
    assert_eq!(
        transient_sources,
        BTreeSet::from(["nexmark_auction".to_string(), "nexmark_bid".to_string()])
    );
}

#[tokio::test]
async fn q4_plan_source_requirements_prune_unused_source_columns() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT category, AVG(max) \
             FROM (SELECT MAX(b.price) AS max, a.category \
                   FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
                   WHERE b.date_time BETWEEN a.date_time AND a.expires \
                   GROUP BY a.id, a.category) per_auction \
             GROUP BY category",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    assert_eq!(
        requirements,
        vec![
            PlanSourceRequirements {
                source_name: "nexmark_auction".to_string(),
                required_columns: vec![0, 6, 7, 8],
            },
            PlanSourceRequirements {
                source_name: "nexmark_bid".to_string(),
                required_columns: vec![0, 2],
            },
        ]
    );
}

#[tokio::test]
async fn q16_plan_source_requirements_prune_unused_source_columns() {
    let logical = sql_plan_with_auction_and_bid(
            "SELECT channel, DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day, \
                    MAX(DATE_FORMAT(date_time, 'HH:mm')) AS minute, \
                    COUNT(*) AS total_bids, \
                    COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, \
                    COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, \
                    COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, \
                    COUNT(DISTINCT bidder) AS total_bidders, \
                    COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, \
                    COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, \
                    COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, \
                    COUNT(DISTINCT auction) AS total_auctions, \
                    COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, \
                    COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, \
                    COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions \
             FROM nexmark_bid \
             GROUP BY channel, DATE_FORMAT(date_time, 'yyyy-MM-dd')",
        )
        .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    assert_eq!(
        requirements,
        vec![PlanSourceRequirements {
            source_name: "nexmark_bid".to_string(),
            required_columns: vec![0, 1, 2, 3, 5],
        }]
    );
}

#[tokio::test]
async fn q5_plan_source_requirements_prune_unused_source_columns() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT auction, COUNT(*) AS num \
             FROM nexmark_bid \
             GROUP BY auction, HOP(date_time, 2000, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    assert_eq!(
        requirements,
        vec![PlanSourceRequirements {
            source_name: "nexmark_bid".to_string(),
            required_columns: vec![0, 5],
        }]
    );
}

#[tokio::test]
async fn q7_plan_source_requirements_prune_unused_source_columns() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT MAX(price) AS maxprice \
             FROM nexmark_bid \
             GROUP BY TUMBLE(date_time, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    assert_eq!(
        requirements,
        vec![PlanSourceRequirements {
            source_name: "nexmark_bid".to_string(),
            required_columns: vec![2, 5],
        }]
    );
}

#[tokio::test]
async fn q12_plan_source_requirements_prune_unused_source_columns() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT bidder, COUNT(*) AS bid_count \
             FROM nexmark_bid \
             GROUP BY bidder, TUMBLE(date_time, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    assert_eq!(
        requirements,
        vec![PlanSourceRequirements {
            source_name: "nexmark_bid".to_string(),
            required_columns: vec![1, 5],
        }]
    );
}

#[tokio::test]
async fn session_window_plan_source_requirements_prune_unused_source_columns() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT bidder, COUNT(*) AS bid_count \
             FROM nexmark_bid \
             GROUP BY bidder, SESSION(date_time, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    assert_eq!(
        requirements,
        vec![PlanSourceRequirements {
            source_name: "nexmark_bid".to_string(),
            required_columns: vec![1, 5],
        }]
    );
}

#[tokio::test]
async fn q12_window_count_star_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT bidder, COUNT(*) AS bid_count \
             FROM nexmark_bid \
             GROUP BY bidder, TUMBLE(date_time, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let transient_sources = source_batch_journal_root_sources(&plan)
        .expect("source batch journal root sources")
        .expect("source batch journal root sources");
    assert_eq!(
        transient_sources,
        BTreeSet::from(["nexmark_bid".to_string()])
    );
}

#[tokio::test]
async fn session_window_count_star_shape_uses_normal_runtime_path() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT bidder, COUNT(*) AS bid_count \
             FROM nexmark_bid \
             GROUP BY bidder, SESSION(date_time, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    assert!(
        source_batch_journal_root_sources(&plan)
            .expect("source batch journal root sources")
            .is_none()
    );
    assert!(
        try_build_transient_source_window_count_star_root_shape(&plan, plan.root)
            .expect("build session window count-star transient shape")
            .is_none()
    );
}

#[tokio::test]
async fn q5_window_count_star_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT auction, COUNT(*) AS num \
             FROM nexmark_bid \
             GROUP BY auction, HOP(date_time, 2000, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let transient_sources = source_batch_journal_root_sources(&plan)
        .expect("source batch journal root sources")
        .expect("source batch journal root sources");
    assert_eq!(
        transient_sources,
        BTreeSet::from(["nexmark_bid".to_string()])
    );
}

#[tokio::test]
async fn q5_window_count_star_shape_projects_group_key_and_count_directly() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT auction, COUNT(*) AS num \
             FROM nexmark_bid \
             GROUP BY auction, HOP(date_time, 2000, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let shape = try_build_transient_source_window_count_star_root_shape(&plan, plan.root)
        .expect("build q5 transient shape")
        .expect("q5 transient shape");
    assert!(shape.transform.is_none());
    assert!(matches!(
        shape.output_projection,
        Some(TransientWindowCountOutputProjection::GroupKeyAndCount)
    ));
}

#[tokio::test]
async fn q7_window_incremental_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT MAX(price) AS maxprice \
             FROM nexmark_bid \
             GROUP BY TUMBLE(date_time, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    let transient_sources = source_batch_journal_root_sources(&plan)
        .expect("source batch journal root sources")
        .expect("source batch journal root sources");
    assert_eq!(
        transient_sources,
        BTreeSet::from(["nexmark_bid".to_string()])
    );
}

#[tokio::test]
async fn optimized_q5_window_aggregate_elides_redundant_scan_projection() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT auction, COUNT(*) AS num \
             FROM nexmark_bid \
             GROUP BY auction, HOP(date_time, 2000, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    validate_dbsp_plan(
        &plan,
        &std::collections::BTreeSet::from(["nexmark_bid".to_string()]),
        "benchmark_result",
    )
    .expect("validated circuit plan");

    let root = plan.node(plan.root).expect("root node");
    let &window_idx = root.inputs.first().expect("window aggregate input");
    let window = plan.node(window_idx).expect("window aggregate node");
    assert!(
        matches!(window.kind, DbspNodeKind::WindowAggregate(_)),
        "expected root input to be window aggregate, found {:?}",
        window.kind
    );
    let &window_input_idx = window.inputs.first().expect("window source input");
    let window_input = plan.node(window_input_idx).expect("window input node");
    match &window_input.kind {
        DbspNodeKind::Source(_) => {}
        DbspNodeKind::Project(project) => {
            let &project_input_idx = window_input.inputs.first().expect("project source input");
            let project_input = plan
                .node(project_input_idx)
                .expect("project source input node");
            assert!(
                matches!(project_input.kind, DbspNodeKind::Source(_)),
                "expected optimized q5 window aggregate projection input to be source, found {:?}",
                project_input.kind
            );
            let projected_fields = project
                .output_schema()
                .fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                projected_fields,
                vec!["auction", "date_time"],
                "optimized q5 window aggregate projection should only keep required columns"
            );
        }
        other => panic!(
            "expected optimized q5 window aggregate input to be source or source projection, found {other:?}"
        ),
    }
}

#[tokio::test]
async fn optimized_q7_window_aggregate_elides_redundant_scan_projection() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT MAX(price) AS maxprice \
             FROM nexmark_bid \
             GROUP BY TUMBLE(date_time, 10000)",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    validate_dbsp_plan(
        &plan,
        &std::collections::BTreeSet::from(["nexmark_bid".to_string()]),
        "benchmark_result",
    )
    .expect("validated circuit plan");

    let root = plan.node(plan.root).expect("root node");
    let &window_idx = root.inputs.first().expect("window aggregate input");
    let window = plan.node(window_idx).expect("window aggregate node");
    assert!(
        matches!(window.kind, DbspNodeKind::WindowAggregate(_)),
        "expected root input to be window aggregate, found {:?}",
        window.kind
    );
    let &window_input_idx = window.inputs.first().expect("window source input");
    let window_input = plan.node(window_input_idx).expect("window input node");
    match &window_input.kind {
        DbspNodeKind::Source(_) => {}
        DbspNodeKind::Project(project) => {
            let &project_input_idx = window_input.inputs.first().expect("project source input");
            let project_input = plan
                .node(project_input_idx)
                .expect("project source input node");
            assert!(
                matches!(project_input.kind, DbspNodeKind::Source(_)),
                "expected optimized q7 window aggregate projection input to be source, found {:?}",
                project_input.kind
            );
            let projected_fields = project
                .output_schema()
                .fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                projected_fields,
                vec!["price", "date_time"],
                "optimized q7 window aggregate projection should only keep required columns"
            );
        }
        other => panic!(
            "expected optimized q7 window aggregate input to be source or source projection, found {other:?}"
        ),
    }
}
