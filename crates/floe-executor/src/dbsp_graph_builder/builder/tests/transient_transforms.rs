use super::*;

#[test]
fn optimized_benchmark_join_has_consistent_project_input_schemas() {
    let logical = benchmark_join_logical_plan();
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");

    for node in &plan.nodes {
        let DbspNodeKind::Project(project) = &node.kind else {
            continue;
        };
        let input_idx = *node.inputs.first().expect("project input");
        let input_node = plan
            .node(input_idx)
            .unwrap_or_else(|| panic!("missing project input node {input_idx}"));
        assert_eq!(
            project.input_schema().to_arrow_schema(),
            input_node.output_schema.to_arrow_schema(),
            "project node {} input schema drifted from upstream node {} output schema",
            node.id,
            input_idx
        );
    }
}

#[test]
fn transient_filter_map_transform_accepts_rows_when_project_schema_is_stale() {
    let full_schema = nexmark_bid_table().schema().clone();
    let select = DbspSelectNode::try_new(Arc::clone(&full_schema), col("auction").gt(lit(0i64)))
        .expect("select");

    let narrow_items = ["auction", "bidder", "price"]
        .iter()
        .map(|name| ProjectItem {
            expr: col(*name),
            alias: Some((*name).to_string()),
        })
        .collect::<Vec<_>>();
    let narrow = DbspProjectNode::try_new(Arc::clone(&full_schema), narrow_items)
        .expect("narrow source projection");
    let narrow_schema = narrow.output_schema().clone();

    let stale_items = narrow_schema
        .fields()
        .iter()
        .map(|field| ProjectItem {
            expr: col(field.name.as_str()),
            alias: Some(field.name.clone()),
        })
        .collect::<Vec<_>>();
    let stale_project =
        DbspProjectNode::try_new(Arc::clone(&narrow_schema), stale_items).expect("project");

    let transform =
        build_filter_map_transform(&select, &stale_project).expect("filter_map transform");

    let decoder = SourceRowDecoder::new(nexmark_bid_source_definition());
    let encoded = encode_event(&decoder, bid_event_payload(9, 101, 1000), "nexmark_bid");
    let transformed = futures::executor::block_on(transform(Arc::new(vec![(encoded, 1)])))
        .expect("transform rows");
    assert_eq!(transformed.len(), 1);

    let mut decoded = Vec::new();
    crate::encoding::decode_all_encoded_row_scalars_into(&transformed[0].0, &mut decoded)
        .expect("decode transformed row");
    assert_eq!(
        decoded.len(),
        3,
        "expected projected output width to remain narrow"
    );
}

#[tokio::test]
async fn optimized_q14_collapses_common_expr_projection_chain() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT auction, bidder, price * 908 / 1000 AS price, \
                    CASE WHEN HOUR(date_time) >= 8 AND HOUR(date_time) <= 18 THEN 'dayTime' \
                         WHEN HOUR(date_time) <= 6 OR HOUR(date_time) >= 20 THEN 'nightTime' \
                         ELSE 'otherTime' END AS bid_time_type, \
                    date_time, extra, COUNT_CHAR(extra, 'c') AS c_counts \
             FROM nexmark_bid \
             WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000",
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

    let project = plan.node(plan.root).expect("root node");
    assert!(
        matches!(project.kind, DbspNodeKind::Project(_)),
        "expected q14 root to be final project, found {:?}",
        project.kind
    );

    let mut project_layers_before_select = 0usize;
    let mut select_idx = *project.inputs.first().expect("project input");
    loop {
        let node = plan.node(select_idx).expect("plan node");
        match &node.kind {
            DbspNodeKind::Project(_) => {
                project_layers_before_select += 1;
                select_idx = *node.inputs.first().expect("project child");
            }
            DbspNodeKind::Select(_) => break,
            other => panic!(
                "expected optimized q14 root path to reach a select, found {:?}",
                other
            ),
        }
    }
    assert!(
        project_layers_before_select <= 2,
        "expected q14 common-expression normalization to bound the projection chain before the select, found {project_layers_before_select} layers"
    );

    let select = plan.node(select_idx).expect("select node");
    assert!(
        matches!(select.kind, DbspNodeKind::Select(_)),
        "expected optimized q14 root path to reach a select, found {:?}",
        select.kind
    );
}

#[tokio::test]
async fn optimized_q20_preserves_right_side_duplicate_columns() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT b.auction, b.bidder, b.price, b.channel, b.url, \
                    b.date_time AS \"dateTime\", b.extra, \
                    a.item_name AS \"itemName\", a.description, \
                    a.initial_bid AS \"initialBid\", a.reserve, \
                    a.date_time AS auction_time, a.expires, a.seller, \
                    a.category, a.extra AS auction_extra \
             FROM nexmark_bid AS b \
             JOIN nexmark_auction AS a ON b.auction = a.id \
             WHERE a.category = 10",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    validate_dbsp_plan(
        &plan,
        &std::collections::BTreeSet::from([
            "nexmark_auction".to_string(),
            "nexmark_bid".to_string(),
        ]),
        "benchmark_result",
    )
    .expect("validated circuit plan");

    let project_node = plan.node(plan.root).expect("root node");
    let DbspNodeKind::Project(project) = &project_node.kind else {
        panic!(
            "expected q20 root to be project, found {:?}",
            project_node.kind
        );
    };

    let auction_time = project
        .expressions()
        .iter()
        .find(|expr| expr.alias() == "auction_time")
        .expect("auction_time expression");
    assert_eq!(
        auction_time.expression().expr(),
        &Expr::Column(Column::from_name("date_time_1"))
    );

    let auction_extra = project
        .expressions()
        .iter()
        .find(|expr| expr.alias() == "auction_extra")
        .expect("auction_extra expression");
    assert_eq!(
        auction_extra.expression().expr(),
        &Expr::Column(Column::from_name("extra_1"))
    );

    let &join_idx = project_node.inputs.first().expect("join input");
    let join = plan.node(join_idx).expect("join node");
    assert!(
        matches!(join.kind, DbspNodeKind::Join(_)),
        "expected q20 root project to read directly from join, found {:?}",
        join.kind
    );
    let DbspNodeKind::Join(join_node) = &join.kind else {
        unreachable!();
    };
    let (left_idx, right_idx) = join_inputs(join).expect("join inputs");
    assert!(
        join_input_unique_on_direct_source_primary_key(
            &plan,
            right_idx,
            join_node.keys.iter().map(|key| key.right_expression()),
            join_node.right_schema.as_ref(),
        )
        .expect("right uniqueness analysis"),
        "q20 auction side should be unique on the join key"
    );
    assert!(
        !join_input_unique_on_direct_source_primary_key(
            &plan,
            left_idx,
            join_node.keys.iter().map(|key| key.left_expression()),
            join_node.left_schema.as_ref(),
        )
        .expect("left uniqueness analysis"),
        "q20 bid side should not be unique on auction"
    );
}

#[tokio::test]
async fn q20_filtered_unique_auction_side_emits_closed_join_keys() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT b.auction, b.bidder, b.price, b.channel, b.url, \
                    b.date_time AS \"dateTime\", b.extra, \
                    a.item_name AS \"itemName\", a.description, \
                    a.initial_bid AS \"initialBid\", a.reserve, \
                    a.date_time AS auction_time, a.expires, a.seller, \
                    a.category, a.extra AS auction_extra \
             FROM nexmark_bid AS b \
             JOIN nexmark_auction AS a ON b.auction = a.id \
             WHERE a.category = 10",
    )
    .await;
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    let project_node = plan.node(plan.root).expect("root node");
    let &join_idx = project_node.inputs.first().expect("join input");
    let join_node = plan.node(join_idx).expect("join node");
    let DbspNodeKind::Join(join) = &join_node.kind else {
        panic!("expected q20 join node");
    };
    let (_, right_idx) = join_inputs(join_node).expect("join inputs");
    let right_key_columns = join_input_direct_source_primary_key_columns(
        &plan,
        right_idx,
        join.keys.iter().map(|key| key.right_expression()),
        join.right_schema.as_ref(),
    )
    .expect("right key columns")
    .expect("q20 right side primary key columns");
    let closed_key_transform = try_build_transient_join_closed_key_transform(
        &plan,
        right_idx,
        Some(Arc::clone(&right_key_columns)),
    )
    .expect("closed-key transform")
    .expect("filtered right side should produce closed-key transform");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    let auction_definition = nexmark_auction_source_definition();
    let auction_mask = required_mask(&requirements, &auction_definition, "nexmark_auction");
    let auction_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        auction_definition,
        Some(Arc::clone(&auction_mask)),
    );
    let matching = encode_event(
        &auction_decoder,
        auction_event_payload(1, 100, 10),
        "nexmark_auction",
    );
    let nonmatching = encode_event(
        &auction_decoder,
        auction_event_payload(2, 200, 5),
        "nexmark_auction",
    );
    let closed_keys = futures::executor::block_on(closed_key_transform(Arc::new(vec![
        (matching, 1),
        (nonmatching.clone(), 1),
    ])))
    .expect("closed keys");
    let expected_key = extract_encoded_row_columns(&nonmatching, right_key_columns.as_ref(), true)
        .expect("extract nonmatching auction key")
        .expect("nonmatching auction key");
    assert_eq!(closed_keys, vec![(expected_key, 1)]);
}

#[tokio::test]
async fn q16_transient_aggregate_precompute_accepts_pruned_bid_rows() {
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
    let shape = try_build_transient_source_aggregate_root_shape(&plan, plan.root)
        .expect("transient aggregate root shape")
        .expect("transient aggregate root shape");
    let (precompute_evaluator, aggregate_input_schema, expression_columns) =
        build_transient_aggregate_precompute(&shape.aggregate)
            .expect("build transient aggregate precompute");
    let precompute_evaluator = precompute_evaluator.expect("precompute evaluator");

    let field_names = aggregate_input_schema
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    assert!(field_names.contains(&"auction"));
    assert!(field_names.contains(&"bidder"));
    assert!(field_names.contains(&"channel"));
    assert!(!field_names.contains(&"url"));
    assert!(!field_names.contains(&"extra"));

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    let bid_definition = nexmark_bid_source_definition();
    let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
    let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        bid_definition,
        Some(Arc::clone(&bid_mask)),
    );
    let encoded = encode_event(&bid_decoder, bid_event_payload(7, 42, 9_999), "nexmark_bid");
    let source_deltas = (shape.source_root.transform)(Arc::new(vec![(encoded, 1)]))
        .await
        .expect("source transform");
    let precomputed = precompute_evaluator
        .transform_delta_arrow("benchmark_result", Arc::new(source_deltas))
        .await
        .expect("precompute q16 pruned bid row");
    assert_eq!(precomputed.len(), 1);

    let row_evaluator = build_incremental_aggregate_batch_row_evaluator(
        Arc::clone(&aggregate_input_schema),
        shape.aggregate.group_keys().to_vec(),
        shape.aggregate.aggregates().to_vec(),
        Arc::clone(&expression_columns),
        "benchmark_result".to_string(),
        "transient_aggregate",
    );
    let rows = row_evaluator(&precomputed);
    let row = &rows.first().expect("incremental aggregate row").1;
    assert_eq!(row.slots.len(), shape.aggregate.aggregates().len());
}

#[tokio::test]
async fn q16_transient_incremental_aggregate_emits_utf8_group_keys() {
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
    let shape = try_build_transient_source_aggregate_root_shape(&plan, plan.root)
        .expect("transient aggregate root shape")
        .expect("transient aggregate root shape");
    let (precompute_evaluator, aggregate_input_schema, expression_columns) =
        build_transient_aggregate_precompute(&shape.aggregate)
            .expect("build transient aggregate precompute");
    let precompute_evaluator = precompute_evaluator.expect("precompute evaluator");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    let bid_definition = nexmark_bid_source_definition();
    let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
    let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        bid_definition,
        Some(Arc::clone(&bid_mask)),
    );

    let encoded_one = encode_event(
        &bid_decoder,
        bid_event_payload_with_channel_and_ts(7, 42, 9_999, "web", 1_700_000_036_211),
        "nexmark_bid",
    );
    let encoded_two = encode_event(
        &bid_decoder,
        bid_event_payload_with_channel_and_ts(8, 99, 15_000, "web", 1_700_000_096_211),
        "nexmark_bid",
    );

    let source_deltas =
        (shape.source_root.transform)(Arc::new(vec![(encoded_one, 1), (encoded_two, 1)]))
            .await
            .expect("source transform");
    let precomputed = precompute_evaluator
        .transform_delta_arrow("benchmark_result", Arc::new(source_deltas))
        .await
        .expect("precompute q16 rows");

    let row_evaluator = build_incremental_aggregate_batch_row_evaluator(
        Arc::clone(&aggregate_input_schema),
        shape.aggregate.group_keys().to_vec(),
        shape.aggregate.aggregates().to_vec(),
        Arc::clone(&expression_columns),
        "benchmark_result".to_string(),
        "transient_aggregate",
    );
    let aggregate = dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, Vec<u8>>::new_batch(
        row_evaluator,
        build_incremental_aggregate_slot_kinds(shape.aggregate.aggregates())
            .expect("incremental aggregate slot kinds"),
    )
    .await
    .expect("create transient incremental aggregate");

    let output = aggregate
        .apply_deltas(precomputed)
        .await
        .expect("apply q16 transient aggregate deltas");

    assert_eq!(
        output.len(),
        1,
        "expected q16 rows to group into one output row"
    );
    let ((row, values), diff) = &output[0];
    assert_eq!(*diff, 1);
    assert_eq!(
        crate::encoding::extract_encoded_row_scalars(row, &[0, 1]).expect("decode q16 group key"),
        vec![
            Some(crate::encoding::EncodedRowScalar::Utf8("web".to_string())),
            Some(crate::encoding::EncodedRowScalar::Utf8(
                "2023-11-14".to_string()
            )),
        ]
    );
    assert_eq!(
        values.first(),
        Some(&dbsp::AggregateValue::Utf8("22:14".to_string()))
    );
}
