use super::*;

#[tokio::test]
async fn q6_join_topn_aggregate_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid(
            "SELECT seller, AVG(price) AS moving_avg_price \
             FROM (SELECT a.seller, b.price, b.date_time, \
                          ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum \
                   FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
                   WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked \
             WHERE rownum <= 1 \
             GROUP BY seller",
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
async fn q6_alias_join_topn_aggregate_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid_aliases(
            "SELECT seller, AVG(price) AS moving_avg_price \
             FROM (SELECT a.seller, b.price, b.\"dateTime\", \
                          ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum \
                   FROM auction a JOIN bid b ON a.id = b.auction \
                   WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
             WHERE rownum <= 1 \
             GROUP BY seller",
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
async fn q9_alias_join_topn_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid_aliases(
            "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" \
             FROM (SELECT a.id, a.\"itemName\", a.description, a.\"initialBid\", a.reserve, a.\"dateTime\", a.expires, a.seller, a.category, a.extra, \
                          b.auction, b.bidder, b.price, b.\"dateTime\" AS \"bidTime\", b.extra AS \"bidExtra\", \
                          ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.\"dateTime\" ASC) AS rownum \
                   FROM auction a JOIN bid b ON a.id = b.auction \
                   WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
             WHERE rownum <= 1",
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
async fn q19_source_topn_shape_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid(
            "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
             FROM (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra, \
                          ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, date_time ASC, bidder ASC, channel ASC, url ASC, extra ASC) AS rank_number \
                   FROM nexmark_bid) ranked \
             WHERE rank_number <= 10",
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
async fn q13_join_shape_left_input_is_source_batch_journal_eligible() {
    let logical = sql_plan_with_auction_and_bid(
        "SELECT b.auction, b.bidder, b.price, b.date_time AS \"dateTime\", a.seller AS value \
             FROM (SELECT *, PROCTIME() AS p_time FROM nexmark_bid) b \
             JOIN nexmark_auction AS a ON b.auction = a.id \
             WHERE b.auction % 10000 = a.id % 10000",
    )
    .await;
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
    .expect("transient optimization result")
    .expect("transient optimization");
    let join_node = plan
        .node(transient_opt.durable_input_idx)
        .expect("durable input node");
    let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");

    assert!(
        try_build_transient_source_root_materialization(&plan, left_idx)
            .expect("left transient input shape")
            .is_some(),
        "expected left q13 join input to be transient-eligible: {plan:#?}"
    );
    assert!(
        try_build_transient_source_root_materialization(&plan, right_idx)
            .expect("right transient input shape")
            .is_some(),
        "expected right q13 join input to be transient-eligible: {plan:#?}"
    );
}
