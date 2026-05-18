use super::transient_topn::{
    TransientDirectTop1Config, TransientDirectTop1PartitionKey, TransientDirectTop1PartitionLayout,
    TransientDirectTop1Processor, TransientTop1Processor, TransientTopNKeyLayout,
    TransientTopNProcessor,
};
use super::*;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use chrono::Utc;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Column;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::logical_expr::expr_fn::create_udf;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, Signature, TypeSignature, Volatility,
};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, col, lit, table_scan};
use datafusion::prelude::SessionContext;
use dbsp::DbspJoin;
use dbsp::DbspPredicate;
use dbsp::join::TransientJoinInputBatch;
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::StreamCursor;
use dbsp::stream::util::materialize_zset_handle;
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition, SourceEvent};
use object_store::memory::InMemory;
use serde_json::{Value, json};
use slatedb::Db;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::GraphTaskError;
use crate::dbsp_bridge::DbspBridge;
use crate::dbsp_plan::{
    CircuitNode, CircuitPlan, DbspNodeKind, DbspPlanBuilder, DbspProjectNode, DbspSelectNode,
    DbspSourceNode, ProjectItem, nexmark_auction_alias_table, nexmark_auction_table,
    nexmark_bid_alias_table, nexmark_bid_table, nexmark_config, validate_dbsp_plan,
};
use crate::materialized_view::MaterializedViewRegistry;
use crate::outer_stream::OuterStreamRegistry;
use crate::source_decoder::SourceRowDecoder;

#[tokio::test]
async fn persistent_transient_input_state_roundtrips_coalesced_rows() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("persistent-transient-input-state", store)
            .await
            .expect("open db"),
    );
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));

    let mut state =
        PersistentTransientInputState::load(Some(Arc::clone(&table)), "graph-a", "topn")
            .await
            .expect("load empty state");
    state
        .apply_deltas(&[
            (b"row-1".to_vec(), 2),
            (b"row-2".to_vec(), 1),
            (b"row-1".to_vec(), -1),
        ])
        .await
        .expect("apply deltas");

    let reloaded = PersistentTransientInputState::load(Some(table), "graph-a", "topn")
        .await
        .expect("reload state");
    let mut rows = reloaded.snapshot_deltas();
    rows.sort();
    assert_eq!(rows, vec![(b"row-1".to_vec(), 1), (b"row-2".to_vec(), 1)]);
}

#[tokio::test]
async fn persistent_transient_top1_output_deltas_store_only_winners() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("persistent-transient-top1-compact-state", store)
            .await
            .expect("open db"),
    );
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    let decoder = SourceRowDecoder::new(nexmark_bid_source_definition());
    let row_30 = encode_event(&decoder, bid_event_payload(1, 101, 30), "nexmark_bid");
    let row_20 = encode_event(&decoder, bid_event_payload(1, 102, 20), "nexmark_bid");
    let row_40 = encode_event(&decoder, bid_event_payload(1, 103, 40), "nexmark_bid");
    let topn = test_topn_node(1, 0);
    let key_layout = test_topn_key_layout();
    let mut processor = TransientTop1Processor::new("graph-top1", &topn, &key_layout);
    let output_deltas = processor
        .apply_deltas(vec![(row_30, 1), (row_20.clone(), 1), (row_40, 1)])
        .expect("apply top1 rows");

    let mut state =
        PersistentTransientInputState::load(Some(Arc::clone(&table)), "graph-top1", "source_topn")
            .await
            .expect("load empty state");
    state
        .apply_deltas(&output_deltas)
        .await
        .expect("persist compact output deltas");

    let row_10 = encode_event(&decoder, bid_event_payload(1, 104, 10), "nexmark_bid");
    let output_deltas = processor
        .apply_deltas(vec![(row_10.clone(), 1)])
        .expect("apply replacement winner");
    state
        .apply_deltas(&output_deltas)
        .await
        .expect("persist replacement output deltas");

    let reloaded = PersistentTransientInputState::load(Some(table), "graph-top1", "source_topn")
        .await
        .expect("reload compact state");
    assert_eq!(reloaded.snapshot_deltas(), vec![(row_10, 1)]);
}

#[test]
fn append_only_direct_top1_retains_only_partition_winner() {
    let decoder = SourceRowDecoder::new(nexmark_bid_source_definition());
    let row_30 = encode_event(&decoder, bid_event_payload(1, 101, 30), "nexmark_bid");
    let row_20 = encode_event(&decoder, bid_event_payload(1, 102, 20), "nexmark_bid");
    let row_40 = encode_event(&decoder, bid_event_payload(1, 103, 40), "nexmark_bid");
    let mut processor = TransientDirectTop1Processor::new(
        "test-graph",
        TransientDirectTop1Config {
            partition_layout: TransientDirectTop1PartitionLayout::One(0),
            order_idx: 2,
            ascending: true,
        },
        true,
    );

    assert_eq!(
        processor.apply_deltas(vec![(row_30.clone(), 1)]).unwrap(),
        vec![(row_30.clone(), 1)]
    );

    let mut output = processor.apply_deltas(vec![(row_20.clone(), 1)]).unwrap();
    output.sort();
    let mut expected = vec![(row_20.clone(), 1), (row_30.clone(), -1)];
    expected.sort();
    assert_eq!(output, expected);

    assert_eq!(
        processor.apply_deltas(vec![(row_40.clone(), 1)]).unwrap(),
        Vec::<(Vec<u8>, i64)>::new()
    );

    let partition = processor
        .partitions
        .get(&TransientDirectTop1PartitionKey::One(1))
        .expect("partition state");
    assert_eq!(partition.live_rows.len(), 1);
    assert!(partition.live_rows.contains_key(&row_20));
    assert_eq!(processor.snapshot_deltas(), vec![(row_20.clone(), 1)]);

    assert_eq!(
        processor.apply_deltas(vec![(row_20.clone(), 1)]).unwrap(),
        Vec::<(Vec<u8>, i64)>::new()
    );
    let partition = processor
        .partitions
        .get(&TransientDirectTop1PartitionKey::One(1))
        .expect("partition state");
    assert_eq!(partition.live_rows.len(), 1);
    assert_eq!(processor.snapshot_deltas(), vec![(row_20, 2)]);
}

#[test]
fn generic_top1_snapshot_retains_only_partition_winner() {
    let decoder = SourceRowDecoder::new(nexmark_bid_source_definition());
    let row_30 = encode_event(&decoder, bid_event_payload(1, 101, 30), "nexmark_bid");
    let row_20 = encode_event(&decoder, bid_event_payload(1, 102, 20), "nexmark_bid");
    let row_40 = encode_event(&decoder, bid_event_payload(1, 103, 40), "nexmark_bid");
    let key_layout = test_topn_key_layout();
    let topn = test_topn_node(1, 0);
    let mut processor = TransientTop1Processor::new("test-graph", &topn, &key_layout);

    processor
        .apply_deltas(vec![(row_30.clone(), 1), (row_20.clone(), 1), (row_40, 1)])
        .expect("apply top1 rows");

    assert_eq!(processor.snapshot_deltas(), vec![(row_20, 1)]);
}

#[test]
fn generic_topn_snapshot_retains_offset_plus_limit_rows() {
    let decoder = SourceRowDecoder::new(nexmark_bid_source_definition());
    let row_10 = encode_event(&decoder, bid_event_payload(1, 101, 10), "nexmark_bid");
    let row_20 = encode_event(&decoder, bid_event_payload(1, 102, 20), "nexmark_bid");
    let row_30 = encode_event(&decoder, bid_event_payload(1, 103, 30), "nexmark_bid");
    let row_40 = encode_event(&decoder, bid_event_payload(1, 104, 40), "nexmark_bid");
    let row_05 = encode_event(&decoder, bid_event_payload(1, 105, 5), "nexmark_bid");
    let key_layout = test_topn_key_layout();
    let topn = test_topn_node(2, 1);
    let mut processor = TransientTopNProcessor::new("test-graph", &topn, &key_layout, true);

    processor
        .apply_deltas(vec![
            (row_10.clone(), 1),
            (row_20.clone(), 1),
            (row_30.clone(), 1),
            (row_40, 1),
        ])
        .expect("apply topn rows");

    let mut snapshot = processor.snapshot_deltas();
    snapshot.sort();
    let mut expected_snapshot = vec![(row_10.clone(), 1), (row_20.clone(), 1), (row_30, 1)];
    expected_snapshot.sort();
    assert_eq!(snapshot, expected_snapshot);

    let mut restored = TransientTopNProcessor::new("test-graph", &topn, &key_layout, true);
    restored
        .apply_deltas(snapshot)
        .expect("restore compact topn snapshot");

    let original_output = processor
        .apply_deltas(vec![(row_05.clone(), 1)])
        .expect("apply follow-up to original")
        .into_iter()
        .collect::<HashSet<_>>();
    let restored_output = restored
        .apply_deltas(vec![(row_05, 1)])
        .expect("apply follow-up to restored")
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(restored_output, original_output);
}

#[test]
fn transient_count_aggregate_state_snapshot_roundtrips() {
    let snapshot = dbsp::TransientCountAggregateSnapshot {
        grouped: vec![dbsp::TransientCountAggregateGroupedState {
            key: b"group-a".to_vec(),
            total_rows: 3,
            counts: vec![3, 2],
        }],
        distinct: vec![dbsp::TransientCountAggregateDistinctWeight {
            group_key: b"group-a".to_vec(),
            slot: 1,
            value: b"distinct-a".to_vec(),
            weight: 2,
        }],
    };

    let encoded = encode_transient_count_aggregate_snapshot(snapshot.clone())
        .expect("encode count aggregate snapshot");
    let decoded = decode_transient_count_aggregate_snapshot(encoded)
        .expect("decode count aggregate snapshot");
    assert_eq!(decoded, snapshot);
}

#[test]
fn transient_incremental_aggregate_state_snapshot_roundtrips() {
    let snapshot = dbsp::TransientIncrementalAggregateSnapshot {
        grouped: vec![dbsp::TransientIncrementalAggregateGroupedState {
            key: b"group-a".to_vec(),
            total_rows: 3,
            slots: vec![
                dbsp::IncrementalAggregateSlotState::Count { count: 3 },
                dbsp::IncrementalAggregateSlotState::Sum {
                    sum: 120,
                    non_null_count: 2,
                },
                dbsp::IncrementalAggregateSlotState::Min {
                    current: Some(dbsp::AggregateValue::Int64(10)),
                },
                dbsp::IncrementalAggregateSlotState::Max { current: None },
            ],
        }],
        distinct: vec![dbsp::TransientIncrementalAggregateDistinctWeight {
            group_key: b"group-a".to_vec(),
            slot: 1,
            value: dbsp::AggregateValue::Int64(42),
            weight: 1,
        }],
        input: vec![dbsp::TransientIncrementalAggregateInputWeight {
            group_key: b"group-a".to_vec(),
            value: b"input-row".to_vec(),
            weight: 2,
        }],
    };

    let encoded = encode_transient_incremental_aggregate_snapshot(snapshot.clone())
        .expect("encode incremental aggregate snapshot");
    let decoded = decode_transient_incremental_aggregate_snapshot(encoded)
        .expect("decode incremental aggregate snapshot");
    assert_eq!(decoded, snapshot);
}

#[test]
fn transient_window_incremental_aggregate_state_snapshot_roundtrips() {
    let snapshot = dbsp::TransientIncrementalAggregateSnapshot {
        grouped: vec![dbsp::TransientIncrementalAggregateGroupedState {
            key: b"window-group-a".to_vec(),
            total_rows: 2,
            slots: vec![dbsp::IncrementalAggregateSlotState::Max {
                current: Some(dbsp::AggregateValue::TimestampMillis(1_700_000_000_000)),
            }],
        }],
        distinct: vec![dbsp::TransientIncrementalAggregateDistinctWeight {
            group_key: b"window-group-a".to_vec(),
            slot: 0,
            value: dbsp::AggregateValue::Utf8("bidder-a".to_string()),
            weight: 1,
        }],
        input: vec![dbsp::TransientIncrementalAggregateInputWeight {
            group_key: b"window-group-a".to_vec(),
            value: (b"window-group-a".to_vec(), b"input-row".to_vec()),
            weight: 2,
        }],
    };

    let encoded = encode_transient_window_incremental_aggregate_snapshot(snapshot.clone())
        .expect("encode window incremental aggregate snapshot");
    let decoded = decode_transient_window_incremental_aggregate_snapshot(encoded)
        .expect("decode window incremental aggregate snapshot");
    assert_eq!(decoded, snapshot);
}

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
    let transformed = transform(&vec![(encoded, 1)]).expect("transform rows");
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
    let closed_keys =
        closed_key_transform(&vec![(matching, 1), (nonmatching.clone(), 1)]).expect("closed keys");
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
    let source_deltas = (shape.source_root.transform)(&[(encoded, 1)]).expect("source transform");
    let precomputed = precompute_evaluator
        .transform_delta("benchmark_result", &source_deltas)
        .expect("precompute q16 pruned bid row");
    assert_eq!(precomputed.len(), 1);

    let row_evaluator = build_incremental_aggregate_row_evaluator(
        Arc::clone(&aggregate_input_schema),
        shape.aggregate.group_keys().to_vec(),
        shape.aggregate.aggregates().to_vec(),
        Arc::clone(&expression_columns),
        "benchmark_result".to_string(),
        "transient_aggregate",
    );
    let row = row_evaluator(&precomputed[0].0).expect("incremental aggregate row");
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

    let source_deltas = (shape.source_root.transform)(&[(encoded_one, 1), (encoded_two, 1)])
        .expect("source transform");
    let precomputed = precompute_evaluator
        .transform_delta("benchmark_result", &source_deltas)
        .expect("precompute q16 rows");

    let row_evaluator = build_incremental_aggregate_row_evaluator(
        Arc::clone(&aggregate_input_schema),
        shape.aggregate.group_keys().to_vec(),
        shape.aggregate.aggregates().to_vec(),
        Arc::clone(&expression_columns),
        "benchmark_result".to_string(),
        "transient_aggregate",
    );
    let aggregate = dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, Vec<u8>>::new(
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

#[tokio::test]
async fn benchmark_join_child_transforms_match_pruned_source_handle_outputs() {
    let db = test_db("benchmark-join-child-transform-equivalence").await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let view_name = "benchmark_result";
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let logical = benchmark_join_logical_plan();
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    let persistence_policy = PersistencePolicy::for_plan(&plan);
    let root_transient = try_build_transient_segment_optimization(
        &plan,
        plan.root,
        &HashMap::new(),
        view_name,
        true,
        &persistence_policy,
    )
    .expect("root transient opt")
    .expect("root transient opt");
    let join_node = plan
        .node(root_transient.durable_input_idx)
        .expect("join node");
    let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");

    let left_transient = try_build_transient_segment_optimization(
        &plan,
        left_idx,
        &HashMap::new(),
        "left_child",
        false,
        &persistence_policy,
    )
    .expect("left transient opt")
    .expect("left transient opt");
    let right_transient = try_build_transient_segment_optimization(
        &plan,
        right_idx,
        &HashMap::new(),
        "right_child",
        false,
        &persistence_policy,
    )
    .expect("right transient opt")
    .expect("right transient opt");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    let bid_definition = nexmark_bid_source_definition();
    let auction_definition = nexmark_auction_source_definition();
    let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
    let auction_mask = required_mask(&requirements, &auction_definition, "nexmark_auction");

    let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        bid_definition,
        Some(Arc::clone(&bid_mask)),
    );
    let auction_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        auction_definition,
        Some(Arc::clone(&auction_mask)),
    );

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;
    let mut registry = OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
        .await
        .expect("outer streams");

    let handle_streams = required_sources
        .iter()
        .filter_map(|source| {
            registry
                .delta_handle_stream(source)
                .map(|stream| (source.clone(), stream))
        })
        .collect::<HashMap<_, _>>();

    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    builder.watermark = Arc::new(AtomicI64::new(-1));
    builder.ns.set_graph_id(view_name);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut mv_latest = HashMap::new();
    let mut built = HashMap::new();
    let cancel = CancellationToken::new();
    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let left_stream = builder
        .compile_node(
            &plan,
            left_idx,
            &handle_streams,
            &cancel,
            &task_tx,
            &mut built,
            &mv_registry,
            &mut mv_latest,
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
            &persistence_policy,
        )
        .await
        .expect("compile left child");
    let right_stream = builder
        .compile_node(
            &plan,
            right_idx,
            &handle_streams,
            &cancel,
            &task_tx,
            &mut built,
            &mv_registry,
            &mut mv_latest,
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
            &persistence_policy,
        )
        .await
        .expect("compile right child");

    let mut left_cursor = StreamCursor::new(left_stream.stream());
    let mut right_cursor = StreamCursor::new(right_stream.stream());
    let _ = left_cursor.snapshot().await.expect("left initial snapshot");
    let _ = right_cursor
        .snapshot()
        .await
        .expect("right initial snapshot");

    let auction_batch = vec![
        (
            encode_event(
                &auction_decoder,
                auction_event_payload(1, 100, 10),
                "nexmark_auction",
            ),
            1,
        ),
        (
            encode_event(
                &auction_decoder,
                auction_event_payload(2, 200, 5),
                "nexmark_auction",
            ),
            1,
        ),
    ];
    {
        let writer = registry
            .writer_mut("nexmark_auction")
            .expect("auction writer");
        for (encoded, diff) in &auction_batch {
            writer
                .append_encoded(encoded.clone(), *diff)
                .expect("append encoded auction");
        }
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction batch");
    assert_tick_matches_transform(
        &table,
        &mut left_cursor,
        Vec::new(),
        &left_transient.transform,
        "left tick 1",
    )
    .await;
    assert_tick_matches_transform(
        &table,
        &mut right_cursor,
        auction_batch,
        &right_transient.transform,
        "right tick 1",
    )
    .await;

    for tick in 0..64usize {
        let bid_batch = vec![
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(1, 1_000 + tick as i64, 10 + tick as i64),
                    "nexmark_bid",
                ),
                1,
            ),
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(2, 2_000 + tick as i64, 20 + tick as i64),
                    "nexmark_bid",
                ),
                1,
            ),
        ];
        {
            let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
            for (encoded, diff) in &bid_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded bid");
            }
        }
        registry
            .tick_all_with_version(i64::try_from(tick + 2).expect("tick version"))
            .await
            .expect("tick bid batch");
        assert_tick_matches_transform(
            &table,
            &mut left_cursor,
            bid_batch,
            &left_transient.transform,
            "left bid tick",
        )
        .await;
        assert_tick_matches_transform(
            &table,
            &mut right_cursor,
            Vec::new(),
            &right_transient.transform,
            "right bid tick",
        )
        .await;
    }
}

#[tokio::test]
async fn benchmark_large_bid_batch_transform_matches_pruned_source_handle_output() {
    let db = test_db("benchmark-large-bid-transform-equivalence").await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let view_name = "benchmark_result";
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let logical = benchmark_join_logical_plan();
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    let persistence_policy = PersistencePolicy::for_plan(&plan);
    let root_transient = try_build_transient_segment_optimization(
        &plan,
        plan.root,
        &HashMap::new(),
        view_name,
        true,
        &persistence_policy,
    )
    .expect("root transient opt")
    .expect("root transient opt");
    let join_node = plan
        .node(root_transient.durable_input_idx)
        .expect("join node");
    let (left_idx, _right_idx) = join_inputs(join_node).expect("join inputs");

    let left_transient = try_build_transient_segment_optimization(
        &plan,
        left_idx,
        &HashMap::new(),
        "left_child",
        false,
        &persistence_policy,
    )
    .expect("left transient opt")
    .expect("left transient opt");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    let bid_definition = nexmark_bid_source_definition();
    let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
    let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        bid_definition,
        Some(Arc::clone(&bid_mask)),
    );

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;
    let mut registry = OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
        .await
        .expect("outer streams");

    let handle_streams = required_sources
        .iter()
        .filter_map(|source| {
            registry
                .delta_handle_stream(source)
                .map(|stream| (source.clone(), stream))
        })
        .collect::<HashMap<_, _>>();

    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    builder.watermark = Arc::new(AtomicI64::new(-1));
    builder.ns.set_graph_id(view_name);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut mv_latest = HashMap::new();
    let mut built = HashMap::new();
    let cancel = CancellationToken::new();
    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let left_stream = builder
        .compile_node(
            &plan,
            left_idx,
            &handle_streams,
            &cancel,
            &task_tx,
            &mut built,
            &mv_registry,
            &mut mv_latest,
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
            &persistence_policy,
        )
        .await
        .expect("compile left child");
    let mut left_cursor = StreamCursor::new(left_stream.stream());
    let _ = left_cursor.snapshot().await.expect("left initial snapshot");

    let full_batch = (0..16_384usize)
        .map(|offset| {
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(
                        i64::try_from((offset % 10_000) + 1).expect("auction id"),
                        1_000_000 + i64::try_from(offset).expect("bidder"),
                        10_000 + i64::try_from(offset).expect("price"),
                    ),
                    "nexmark_bid",
                ),
                1,
            )
        })
        .collect::<Vec<_>>();
    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        for (encoded, diff) in &full_batch {
            writer
                .append_encoded(encoded.clone(), *diff)
                .expect("append encoded bid full batch");
        }
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick full bid batch");
    assert_tick_matches_transform(
        &table,
        &mut left_cursor,
        full_batch,
        &left_transient.transform,
        "left full 16k batch",
    )
    .await;

    let partial_batch = (0..576usize)
        .map(|offset| {
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(
                        i64::try_from((offset % 10_000) + 1).expect("auction id"),
                        2_000_000 + i64::try_from(offset).expect("bidder"),
                        20_000 + i64::try_from(offset).expect("price"),
                    ),
                    "nexmark_bid",
                ),
                1,
            )
        })
        .collect::<Vec<_>>();
    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        for (encoded, diff) in &partial_batch {
            writer
                .append_encoded(encoded.clone(), *diff)
                .expect("append encoded bid partial batch");
        }
    }
    registry
        .tick_all_with_version(2)
        .await
        .expect("tick partial bid batch");
    assert_tick_matches_transform(
        &table,
        &mut left_cursor,
        partial_batch,
        &left_transient.transform,
        "left partial 576 batch",
    )
    .await;
}

#[tokio::test]
async fn benchmark_transient_join_inputs_match_canonical_join_output() {
    let db = test_db("benchmark-join-transient-input-equivalence").await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let view_name = "benchmark_result";
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let logical = benchmark_join_logical_plan();
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    let persistence_policy = PersistencePolicy::for_plan(&plan);
    let root_transient = try_build_transient_segment_optimization(
        &plan,
        plan.root,
        &HashMap::new(),
        view_name,
        true,
        &persistence_policy,
    )
    .expect("root transient opt")
    .expect("root transient opt");
    let join_node = plan
        .node(root_transient.durable_input_idx)
        .expect("join node");
    let join = match &join_node.kind {
        DbspNodeKind::Join(join) => join.clone(),
        other => panic!("expected join node, got {other:?}"),
    };
    let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");

    let left_transient = try_build_transient_segment_optimization(
        &plan,
        left_idx,
        &HashMap::new(),
        "left_child",
        false,
        &persistence_policy,
    )
    .expect("left transient opt")
    .expect("left transient opt");
    let right_transient = try_build_transient_segment_optimization(
        &plan,
        right_idx,
        &HashMap::new(),
        "right_child",
        false,
        &persistence_policy,
    )
    .expect("right transient opt")
    .expect("right transient opt");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    let bid_definition = nexmark_bid_source_definition();
    let auction_definition = nexmark_auction_source_definition();
    let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
    let auction_mask = required_mask(&requirements, &auction_definition, "nexmark_auction");

    let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        bid_definition,
        Some(Arc::clone(&bid_mask)),
    );
    let auction_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        auction_definition,
        Some(Arc::clone(&auction_mask)),
    );

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;
    let mut registry = OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
        .await
        .expect("outer streams");

    let handle_streams = required_sources
        .iter()
        .filter_map(|source| {
            registry
                .delta_handle_stream(source)
                .map(|stream| (source.clone(), stream))
        })
        .collect::<HashMap<_, _>>();

    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    builder.watermark = Arc::new(AtomicI64::new(-1));
    builder.ns.set_graph_id(view_name);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut mv_latest = HashMap::new();
    let mut built = HashMap::new();
    let cancel = CancellationToken::new();
    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let left_stream = builder
        .compile_node(
            &plan,
            left_idx,
            &handle_streams,
            &cancel,
            &task_tx,
            &mut built,
            &mv_registry,
            &mut mv_latest,
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
            &persistence_policy,
        )
        .await
        .expect("compile left child");
    let right_stream = builder
        .compile_node(
            &plan,
            right_idx,
            &handle_streams,
            &cancel,
            &task_tx,
            &mut built,
            &mv_registry,
            &mut mv_latest,
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
            &persistence_policy,
        )
        .await
        .expect("compile right child");

    let left_schema = Arc::clone(&join.left_schema);
    let right_schema = Arc::clone(&join.right_schema);
    let output_schema = Arc::clone(&join.output_schema);
    let left_key_columns = Arc::new(
        join.keys
            .iter()
            .map(|key| {
                projection_direct_column_index_expression(
                    key.left_expression().expr(),
                    left_schema.as_ref(),
                )
            })
            .collect::<Option<Vec<_>>>()
            .expect("benchmark join left keys should be direct"),
    );
    let right_key_columns = Arc::new(
        join.keys
            .iter()
            .map(|key| {
                projection_direct_column_index_expression(
                    key.right_expression().expr(),
                    right_schema.as_ref(),
                )
            })
            .collect::<Option<Vec<_>>>()
            .expect("benchmark join right keys should be direct"),
    );
    let residual_evaluator = join.residual.as_ref().map(|expr| {
        let predicate = DbspPredicate::try_new(expr.expr().clone(), Arc::clone(&output_schema))
            .expect("build benchmark join residual predicate");
        Arc::new(
            VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&output_schema))
                .expect("build benchmark join residual evaluator"),
        )
    });
    let left_key = {
        let left_key_columns = Arc::clone(&left_key_columns);
        move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            extract_encoded_row_columns(left_bytes, left_key_columns.as_ref(), true)
                .ok()
                .flatten()
        }
    };
    let right_key = {
        let right_key_columns = Arc::clone(&right_key_columns);
        move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            extract_encoded_row_columns(right_bytes, right_key_columns.as_ref(), true)
                .ok()
                .flatten()
        }
    };
    let predicate = |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true };
    let projector = |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
        crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
    };

    let canonical_join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
        &left_stream,
        &right_stream,
        left_key.clone(),
        right_key.clone(),
        predicate.clone(),
        projector,
        None,
    )
    .await
    .expect("canonical join");
    let mut canonical_cursor = StreamCursor::new(canonical_join.stream().stream());
    let _ = canonical_cursor
        .snapshot()
        .await
        .expect("initial canonical join snapshot");

    let (observer_tx, mut observer_rx) =
        mpsc::unbounded_channel::<(i64, Arc<Vec<(Vec<u8>, i64)>>)>();
    let observer = Arc::new(move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| {
        let _ = observer_tx.send((version, deltas));
    });
    let (left_transient_tx, left_transient_rx) =
        mpsc::unbounded_channel::<TransientJoinInputBatch<Vec<u8>, Vec<u8>>>();
    let (right_transient_tx, right_transient_rx) =
        mpsc::unbounded_channel::<TransientJoinInputBatch<Vec<u8>, Vec<u8>>>();
    DbspJoin::spawn_transient_with_inputs::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
        &left_stream,
        &right_stream,
        Some(left_transient_rx),
        Some(right_transient_rx),
        false,
        None,
        left_key,
        right_key,
        predicate,
        |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
        },
        observer,
        None,
    )
    .await
    .expect("transient join with inputs");

    let auction_batch = vec![
        (
            encode_event(
                &auction_decoder,
                auction_event_payload(1, 100, 10),
                "nexmark_auction",
            ),
            1,
        ),
        (
            encode_event(
                &auction_decoder,
                auction_event_payload(2, 200, 5),
                "nexmark_auction",
            ),
            1,
        ),
    ];
    let transformed_right_tick1 =
        (right_transient.transform)(&auction_batch).expect("transform auction batch");
    right_transient_tx
        .send(TransientJoinInputBatch {
            ts: 1,
            deltas: Arc::new(transformed_right_tick1),
            closed_keys: Arc::new(Vec::new()),
        })
        .expect("send auction transient batch");
    {
        let writer = registry
            .writer_mut("nexmark_auction")
            .expect("auction writer");
        for (encoded, diff) in &auction_batch {
            writer
                .append_encoded(encoded.clone(), *diff)
                .expect("append encoded auction");
        }
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction batch");
    let (ts, canonical_handle) = timeout(Duration::from_secs(1), canonical_cursor.next())
        .await
        .expect("wait canonical join build tick")
        .expect("canonical join build tick");
    assert_eq!(ts, 1);
    let build_tick_delta = materialize_zset_handle::<Vec<u8>>(
        Arc::clone(&table),
        &mut HashMap::new(),
        &canonical_handle,
    )
    .await
    .expect("materialize canonical build tick");
    let build_tick_delta = if let Some(evaluator) = residual_evaluator.as_ref() {
        consolidate_encoded_deltas(
            evaluator
                .transform_delta(
                    "benchmark_join_build_tick_residual",
                    &build_tick_delta.into_iter().collect::<Vec<_>>(),
                )
                .expect("apply benchmark join build tick residual filter"),
        )
    } else {
        build_tick_delta
    };
    assert!(
        build_tick_delta.is_empty(),
        "auction build tick should emit an explicit empty canonical join handle"
    );
    assert!(
        timeout(Duration::from_millis(100), observer_rx.recv())
            .await
            .is_err(),
        "auction build tick should not emit transient join output"
    );

    let mut cache = HashMap::new();
    let mut expected_transient_version = 1_i64;
    for tick in 0..64usize {
        let ts = i64::try_from(tick + 2).expect("tick version");
        let bid_batch = vec![
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(1, 1_000 + tick as i64, 10 + tick as i64),
                    "nexmark_bid",
                ),
                1,
            ),
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(2, 2_000 + tick as i64, 20 + tick as i64),
                    "nexmark_bid",
                ),
                1,
            ),
        ];
        let transformed_left = (left_transient.transform)(&bid_batch).expect("transform bid batch");
        if tick != 16 {
            left_transient_tx
                .send(TransientJoinInputBatch {
                    ts,
                    deltas: Arc::new(transformed_left),
                    closed_keys: Arc::new(Vec::new()),
                })
                .expect("send bid transient batch");
        }
        right_transient_tx
            .send(TransientJoinInputBatch {
                ts,
                deltas: Arc::new(Vec::new()),
                closed_keys: Arc::new(Vec::new()),
            })
            .expect("send empty right transient batch");
        {
            let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
            for (encoded, diff) in &bid_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded bid");
            }
        }
        registry
            .tick_all_with_version(ts)
            .await
            .expect("tick bid batch");

        let (_, canonical_handle) = timeout(Duration::from_secs(1), canonical_cursor.next())
            .await
            .expect("wait canonical join output")
            .expect("canonical join output");
        let actual =
            materialize_zset_handle::<Vec<u8>>(Arc::clone(&table), &mut cache, &canonical_handle)
                .await
                .expect("materialize canonical join delta");
        let actual = if let Some(evaluator) = residual_evaluator.as_ref() {
            consolidate_encoded_deltas(
                evaluator
                    .transform_delta(
                        "benchmark_join_tick_residual",
                        &actual.into_iter().collect::<Vec<_>>(),
                    )
                    .expect("apply benchmark join residual filter"),
            )
        } else {
            actual
        };

        let recv_timeout = if actual.is_empty() {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(1)
        };
        let transient_raw = match timeout(recv_timeout, observer_rx.recv()).await {
            Ok(Some((version, transient_batch))) => {
                assert_eq!(
                    version, expected_transient_version,
                    "unexpected transient join output version at bid tick {tick}"
                );
                expected_transient_version = expected_transient_version.saturating_add(1);
                transient_batch.as_ref().clone()
            }
            Ok(None) | Err(_) => Vec::new(),
        };
        let transient_raw = if let Some(evaluator) = residual_evaluator.as_ref() {
            evaluator
                .transform_delta("benchmark_join_tick_residual", &transient_raw)
                .expect("apply benchmark transient join residual filter")
        } else {
            transient_raw
        };
        let expected = consolidate_encoded_deltas(transient_raw);
        assert_eq!(actual, expected, "join output mismatch at bid tick {tick}");
    }
}

#[tokio::test]
async fn benchmark_transient_source_task_join_inputs_match_canonical_join_output() {
    let db = test_db("benchmark-join-source-task-input-equivalence").await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let view_name = "benchmark_result";
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let logical = benchmark_join_logical_plan();
    let planner = DbspPlanBuilder::new(nexmark_config());
    let plan = planner.build(&logical).expect("circuit plan");
    let persistence_policy = PersistencePolicy::for_plan(&plan);
    let root_transient = try_build_transient_segment_optimization(
        &plan,
        plan.root,
        &HashMap::new(),
        view_name,
        true,
        &persistence_policy,
    )
    .expect("root transient opt")
    .expect("root transient opt");
    let join_node = plan
        .node(root_transient.durable_input_idx)
        .expect("join node");
    let join = match &join_node.kind {
        DbspNodeKind::Join(join) => join.clone(),
        other => panic!("expected join node, got {other:?}"),
    };
    let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");

    let requirements = plan_source_requirements(&plan)
        .expect("source requirements")
        .expect("source requirements");
    let bid_definition = nexmark_bid_source_definition();
    let auction_definition = nexmark_auction_source_definition();
    let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
    let auction_mask = required_mask(&requirements, &auction_definition, "nexmark_auction");

    let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        bid_definition,
        Some(Arc::clone(&bid_mask)),
    );
    let auction_decoder = SourceRowDecoder::new_with_encoded_required_columns(
        auction_definition,
        Some(Arc::clone(&auction_mask)),
    );

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;
    let mut registry = OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
        .await
        .expect("outer streams");

    let handle_streams = required_sources
        .iter()
        .filter_map(|source| {
            registry
                .delta_handle_stream(source)
                .map(|stream| (source.clone(), stream))
        })
        .collect::<HashMap<_, _>>();
    let transient_streams = required_sources
        .iter()
        .filter_map(|source| {
            registry
                .transient_stream(source)
                .map(|stream| (source.clone(), stream))
        })
        .collect::<HashMap<_, _>>();

    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    builder.watermark = Arc::new(AtomicI64::new(-1));
    builder.ns.set_graph_id(view_name);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut mv_latest = HashMap::new();
    let mut built = HashMap::new();
    let cancel = CancellationToken::new();
    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let left_stream = builder
        .compile_node(
            &plan,
            left_idx,
            &handle_streams,
            &cancel,
            &task_tx,
            &mut built,
            &mv_registry,
            &mut mv_latest,
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
            &persistence_policy,
        )
        .await
        .expect("compile left child");
    let right_stream = builder
        .compile_node(
            &plan,
            right_idx,
            &handle_streams,
            &cancel,
            &task_tx,
            &mut built,
            &mv_registry,
            &mut mv_latest,
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
            &persistence_policy,
        )
        .await
        .expect("compile right child");

    let left_transient = try_build_transient_join_input_optimization(
        builder.graph_id(),
        &plan,
        left_idx,
        &transient_streams,
        None,
        &cancel,
    )
    .expect("left transient input opt")
    .expect("left transient input opt");
    let right_transient = try_build_transient_join_input_optimization(
        builder.graph_id(),
        &plan,
        right_idx,
        &transient_streams,
        None,
        &cancel,
    )
    .expect("right transient input opt")
    .expect("right transient input opt");

    let left_schema = Arc::clone(&join.left_schema);
    let right_schema = Arc::clone(&join.right_schema);
    let output_schema = Arc::clone(&join.output_schema);
    let left_key_columns = Arc::new(
        join.keys
            .iter()
            .map(|key| {
                projection_direct_column_index_expression(
                    key.left_expression().expr(),
                    left_schema.as_ref(),
                )
            })
            .collect::<Option<Vec<_>>>()
            .expect("benchmark join left keys should be direct"),
    );
    let right_key_columns = Arc::new(
        join.keys
            .iter()
            .map(|key| {
                projection_direct_column_index_expression(
                    key.right_expression().expr(),
                    right_schema.as_ref(),
                )
            })
            .collect::<Option<Vec<_>>>()
            .expect("benchmark join right keys should be direct"),
    );
    let residual_evaluator = join.residual.as_ref().map(|expr| {
        let predicate = DbspPredicate::try_new(expr.expr().clone(), Arc::clone(&output_schema))
            .expect("build benchmark join residual predicate");
        Arc::new(
            VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&output_schema))
                .expect("build benchmark join residual evaluator"),
        )
    });
    let left_key = {
        let left_key_columns = Arc::clone(&left_key_columns);
        move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            extract_encoded_row_columns(left_bytes, left_key_columns.as_ref(), true)
                .ok()
                .flatten()
        }
    };
    let right_key = {
        let right_key_columns = Arc::clone(&right_key_columns);
        move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            extract_encoded_row_columns(right_bytes, right_key_columns.as_ref(), true)
                .ok()
                .flatten()
        }
    };
    let predicate = |_left_bytes: &Vec<u8>, _right_bytes: &Vec<u8>| -> bool { true };

    let canonical_join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
        &left_stream,
        &right_stream,
        left_key.clone(),
        right_key.clone(),
        predicate.clone(),
        |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
        },
        None,
    )
    .await
    .expect("canonical join");
    let mut canonical_cursor = StreamCursor::new(canonical_join.stream().stream());
    let _ = canonical_cursor
        .snapshot()
        .await
        .expect("initial canonical join snapshot");

    let (observer_tx, mut observer_rx) =
        mpsc::unbounded_channel::<(i64, Arc<Vec<(Vec<u8>, i64)>>)>();
    let observer = Arc::new(move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| {
        let _ = observer_tx.send((version, deltas));
    });
    DbspJoin::spawn_transient_with_inputs::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
        &left_stream,
        &right_stream,
        Some(left_transient.receiver),
        Some(right_transient.receiver),
        true,
        None,
        left_key,
        right_key,
        predicate,
        |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
        },
        observer,
        None,
    )
    .await
    .expect("transient join with source tasks");

    let auction_batch = vec![
        (
            encode_event(
                &auction_decoder,
                auction_event_payload(1, 100, 10),
                "nexmark_auction",
            ),
            1,
        ),
        (
            encode_event(
                &auction_decoder,
                auction_event_payload(2, 200, 5),
                "nexmark_auction",
            ),
            1,
        ),
    ];
    {
        let writer = registry
            .writer_mut("nexmark_auction")
            .expect("auction writer");
        for (encoded, diff) in &auction_batch {
            writer
                .append_encoded(encoded.clone(), *diff)
                .expect("append encoded auction");
        }
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction batch");
    let (ts, canonical_handle) = timeout(Duration::from_secs(1), canonical_cursor.next())
        .await
        .expect("wait canonical join build tick")
        .expect("canonical join build tick");
    assert_eq!(ts, 1);
    let build_tick_delta = materialize_zset_handle::<Vec<u8>>(
        Arc::clone(&table),
        &mut HashMap::new(),
        &canonical_handle,
    )
    .await
    .expect("materialize canonical build tick");
    let build_tick_delta = if let Some(evaluator) = residual_evaluator.as_ref() {
        consolidate_encoded_deltas(
            evaluator
                .transform_delta(
                    "benchmark_join_source_task_build_tick_residual",
                    &build_tick_delta.into_iter().collect::<Vec<_>>(),
                )
                .expect("apply benchmark source-task join build tick residual filter"),
        )
    } else {
        build_tick_delta
    };
    assert!(
        build_tick_delta.is_empty(),
        "auction build tick should emit an explicit empty canonical join handle"
    );
    assert!(
        timeout(Duration::from_millis(100), observer_rx.recv())
            .await
            .is_err(),
        "auction build tick should not emit transient join output"
    );

    let mut cache = HashMap::new();
    for tick in 0..64usize {
        let ts = i64::try_from(tick + 2).expect("tick version");
        let bid_batch = vec![
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(1, 1_000 + tick as i64, 10 + tick as i64),
                    "nexmark_bid",
                ),
                1,
            ),
            (
                encode_event(
                    &bid_decoder,
                    bid_event_payload(2, 2_000 + tick as i64, 20 + tick as i64),
                    "nexmark_bid",
                ),
                1,
            ),
        ];
        {
            let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
            for (encoded, diff) in &bid_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded bid");
            }
        }
        registry
            .tick_all_with_version(ts)
            .await
            .expect("tick bid batch");

        let (_, canonical_handle) = timeout(Duration::from_secs(1), canonical_cursor.next())
            .await
            .expect("wait canonical join output")
            .expect("canonical join output");
        let actual =
            materialize_zset_handle::<Vec<u8>>(Arc::clone(&table), &mut cache, &canonical_handle)
                .await
                .expect("materialize canonical join delta");
        let actual = if let Some(evaluator) = residual_evaluator.as_ref() {
            consolidate_encoded_deltas(
                evaluator
                    .transform_delta(
                        "benchmark_join_source_task_tick_residual",
                        &actual.into_iter().collect::<Vec<_>>(),
                    )
                    .expect("apply benchmark source-task join residual filter"),
            )
        } else {
            actual
        };

        let recv_timeout = if actual.is_empty() {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(1)
        };
        let transient_raw = match timeout(recv_timeout, observer_rx.recv()).await {
            Ok(Some((version, transient_batch))) => {
                assert_eq!(
                    version, ts,
                    "unexpected transient join output version at bid tick {tick}"
                );
                transient_batch.as_ref().clone()
            }
            Ok(None) | Err(_) => Vec::new(),
        };
        let transient_raw = if let Some(evaluator) = residual_evaluator.as_ref() {
            evaluator
                .transform_delta("benchmark_join_source_task_tick_residual", &transient_raw)
                .expect("apply benchmark source-task transient join residual filter")
        } else {
            transient_raw
        };
        let expected = consolidate_encoded_deltas(transient_raw);
        assert_eq!(actual, expected, "join output mismatch at bid tick {tick}");
    }
}

fn benchmark_join_logical_plan() -> LogicalPlan {
    let bid = nexmark_bid_table();
    let auction = nexmark_auction_table();
    let bid_schema = bid.schema().to_arrow_schema();
    let auction_schema = auction.schema().to_arrow_schema();
    table_scan(Some("nexmark_bid"), &bid_schema, None)
        .expect("bid scan")
        .join(
            table_scan(Some("nexmark_auction"), &auction_schema, None)
                .expect("auction scan")
                .build()
                .expect("auction logical"),
            JoinType::Inner,
            (
                vec![Column::from_name("auction")],
                vec![Column::from_name("id")],
            ),
            None,
        )
        .expect("join")
        .filter(col("category").eq(lit(10i64)))
        .expect("filter")
        .project(vec![
            col("auction"),
            col("bidder"),
            col("price").alias("projected_price"),
            col("seller"),
        ])
        .expect("project")
        .build()
        .expect("logical plan")
}

async fn sql_plan_with_auction_and_bid(sql: &str) -> LogicalPlan {
    let ctx = SessionContext::new();
    let bid_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_bid_table().schema().to_arrow_schema(),
    ));
    let auction_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_auction_table().schema().to_arrow_schema(),
    ));
    ctx.register_table("nexmark_bid", bid_provider)
        .expect("register nexmark_bid");
    ctx.register_table("nexmark_auction", auction_provider)
        .expect("register nexmark_auction");
    register_planner_test_udfs(&ctx);
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .expect("build logical plan");
    let optimized = ctx.state().optimize(&plan).expect("optimize logical plan");
    if logical_plan_uses_only_dbsp_supported_types(&optimized) {
        optimized
    } else {
        plan
    }
}

async fn sql_plan_with_auction_and_bid_aliases(sql: &str) -> LogicalPlan {
    let ctx = SessionContext::new();
    let bid_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_bid_table().schema().to_arrow_schema(),
    ));
    let auction_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_auction_table().schema().to_arrow_schema(),
    ));
    let bid_alias_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_bid_alias_table().schema().to_arrow_schema(),
    ));
    let auction_alias_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_auction_alias_table().schema().to_arrow_schema(),
    ));
    ctx.register_table("nexmark_bid", bid_provider)
        .expect("register nexmark_bid");
    ctx.register_table("nexmark_auction", auction_provider)
        .expect("register nexmark_auction");
    ctx.register_table("bid", bid_alias_provider)
        .expect("register bid alias");
    ctx.register_table("auction", auction_alias_provider)
        .expect("register auction alias");
    register_planner_test_udfs(&ctx);
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .expect("build logical plan");
    let optimized = ctx.state().optimize(&plan).expect("optimize logical plan");
    if logical_plan_uses_only_dbsp_supported_types(&optimized) {
        optimized
    } else {
        plan
    }
}

fn logical_plan_uses_only_dbsp_supported_types(plan: &LogicalPlan) -> bool {
    logical_plan_node_supported(plan)
        && plan
            .inputs()
            .into_iter()
            .all(logical_plan_uses_only_dbsp_supported_types)
}

fn logical_plan_node_supported(plan: &LogicalPlan) -> bool {
    plan.schema()
        .fields()
        .iter()
        .all(|field| dbsp_supported_arrow_type(field.data_type()))
}

fn dbsp_supported_arrow_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::Utf8
            | DataType::Boolean
            | DataType::Timestamp(TimeUnit::Millisecond, None)
    )
}

fn register_planner_test_udfs(ctx: &SessionContext) {
    let proctime: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::TimestampMillisecondArray::from(vec![None::<i64>; len]),
            )))
        },
    );
    let passthrough_ts: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args.first().cloned().unwrap_or_else(|| {
                ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::TimestampMillisecondArray::from(vec![None::<i64>; 1]),
                ))
            }))
        },
    );
    let date_format_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(
                        datafusion::arrow::array::TimestampMillisecondArray::from(vec![
                            None::<i64>;
                            len
                        ]),
                    ))
                })
                .into_array(len)?;
            let fmt = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(
                        vec![None::<&str>; len],
                    )))
                })
                .into_array(len)?;
            let (Some(ts), Some(fmt)) = (
                ts.as_any()
                    .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>(),
                fmt.as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>(),
            ) else {
                return Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(vec![None::<&str>; len]),
                )));
            };

            let values = (0..len)
                .map(|row_idx| {
                    if ts.is_null(row_idx) || fmt.is_null(row_idx) {
                        return None;
                    }
                    let dt = chrono::DateTime::<Utc>::from_timestamp_millis(ts.value(row_idx))?;
                    let pattern = fmt
                        .value(row_idx)
                        .replace("yyyy", "%Y")
                        .replace("MM", "%m")
                        .replace("dd", "%d")
                        .replace("HH", "%H")
                        .replace("mm", "%M")
                        .replace("ss", "%S");
                    Some(dt.format(&pattern).to_string())
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::StringArray::from(values),
            )))
        },
    );
    let hour_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(
                        datafusion::arrow::array::TimestampMillisecondArray::from(vec![
                            None::<i64>;
                            len
                        ]),
                    ))
                })
                .into_array(len)?;
            let Some(ts) = ts
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
            else {
                return Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::Int64Array::from(vec![None::<i64>; len]),
                )));
            };

            let values = (0..len)
                .map(|row_idx| {
                    (!ts.is_null(row_idx))
                        .then(|| ts.value(row_idx).div_euclid(3_600_000).rem_euclid(24))
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::Int64Array::from(values),
            )))
        },
    );
    let count_char_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(
                        vec![None::<&str>; len],
                    )))
                })
                .into_array(len)?;
            let needle = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(
                        vec![None::<&str>; len],
                    )))
                })
                .into_array(len)?;
            let (Some(text), Some(needle)) = (
                text.as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>(),
                needle
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>(),
            ) else {
                return Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::Int64Array::from(vec![None::<i64>; len]),
                )));
            };

            let values = (0..len)
                .map(|row_idx| {
                    if text.is_null(row_idx) || needle.is_null(row_idx) {
                        return None;
                    }
                    let haystack = text.value(row_idx);
                    let token = needle.value(row_idx);
                    Some(if token.is_empty() {
                        0
                    } else {
                        i64::try_from(haystack.matches(token).count()).unwrap_or(i64::MAX)
                    })
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::Int64Array::from(values),
            )))
        },
    );
    ctx.register_udf(create_udf(
        "proctime",
        vec![],
        DataType::Timestamp(TimeUnit::Millisecond, None),
        Volatility::Volatile,
        proctime,
    ));
    ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
        datafusion::logical_expr::expr_fn::SimpleScalarUDF::new_with_signature(
            "tumble",
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::clone(&passthrough_ts),
        ),
    ));
    ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
        datafusion::logical_expr::expr_fn::SimpleScalarUDF::new_with_signature(
            "hop",
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::clone(&passthrough_ts),
        ),
    ));
    ctx.register_udf(create_udf(
        "date_format",
        vec![
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Utf8,
        ],
        DataType::Utf8,
        Volatility::Immutable,
        date_format_udf,
    ));
    ctx.register_udf(create_udf(
        "hour",
        vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        hour_udf,
    ));
    ctx.register_udf(create_udf(
        "count_char",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        count_char_udf,
    ));
}

async fn assert_tick_matches_transform(
    table: &Arc<dyn KeyValueTable>,
    cursor: &mut StreamCursor<dbsp::handles::ZSetHandle>,
    source_batch: Vec<(Vec<u8>, i64)>,
    transform: &Arc<DeltaTransformFn>,
    label: &str,
) {
    let (_, handle) = cursor.next().await.expect("next child handle");
    let mut cache = HashMap::new();
    let actual = materialize_zset_handle::<Vec<u8>>(Arc::clone(table), &mut cache, &handle)
        .await
        .expect("materialize child handle");
    let expected = consolidate_encoded_deltas(transform(&source_batch).expect("transform"));
    assert_eq!(actual, expected, "{label}");
}

fn consolidate_encoded_deltas(deltas: Vec<(Vec<u8>, i64)>) -> HashMap<Vec<u8>, i64> {
    let mut map = HashMap::new();
    for (row, diff) in deltas {
        let next = map.get(&row).copied().unwrap_or(0i64).saturating_add(diff);
        if next == 0 {
            map.remove(&row);
        } else {
            map.insert(row, next);
        }
    }
    map
}

fn required_mask(
    requirements: &[PlanSourceRequirements],
    definition: &SourceDefinition,
    source_name: &str,
) -> Arc<[bool]> {
    let requirement = requirements
        .iter()
        .find(|requirement| requirement.source_name == source_name)
        .unwrap_or_else(|| panic!("missing source requirement for {source_name}"));
    let mut mask = vec![false; definition.columns().len()];
    for column_idx in &requirement.required_columns {
        mask[*column_idx] = true;
    }
    Arc::from(mask)
}

fn encode_event(decoder: &SourceRowDecoder, payload: Value, source: &str) -> Vec<u8> {
    let event = SourceEvent::new(source, payload);
    decoder
        .encode_row_key(&event)
        .expect("encode source event")
        .0
}

fn test_topn_key_layout() -> TransientTopNKeyLayout {
    TransientTopNKeyLayout {
        partition_columns: Arc::new(vec![0]),
        order_columns: Arc::new(vec![2]),
        order_types: Arc::new(vec![DbspScalarType::Int64]),
        precompute_evaluator: None,
    }
}

fn test_topn_node(limit: usize, offset: usize) -> DbspTopNNode {
    let input_schema = nexmark_bid_table().schema().clone();
    let order_by = vec![
        dbsp::OrderExpr::try_new(col("price"), Arc::clone(&input_schema), true, true)
            .expect("order expr"),
    ];
    DbspTopNNode::try_new(input_schema, vec![col("auction")], order_by, limit, offset)
        .expect("topn node")
}

fn bid_event_payload(auction: i64, bidder: i64, price: i64) -> Value {
    bid_event_payload_with_channel_and_ts(auction, bidder, price, "channel", 1_700_000_000_000i64)
}

fn bid_event_payload_with_channel_and_ts(
    auction: i64,
    bidder: i64,
    price: i64,
    channel: &str,
    date_time: i64,
) -> Value {
    json!({
        "auction": auction,
        "bidder": bidder,
        "price": price,
        "channel": channel,
        "url": "https://example.invalid/bid",
        "date_time": date_time,
        "extra": "extra"
    })
}

fn auction_event_payload(id: i64, seller: i64, category: i64) -> Value {
    json!({
        "id": id,
        "item_name": "item",
        "description": "description",
        "initial_bid": 1i64,
        "reserve": 2i64,
        "seller": seller,
        "category": category,
        "expires": 1_700_000_000_000i64,
        "date_time": 1_700_000_000_000i64,
        "extra": "extra"
    })
}

fn nexmark_bid_source_definition() -> SourceDefinition {
    SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new("auction", SourceDataType::Int64),
            SourceColumn::new("bidder", SourceDataType::Int64),
            SourceColumn::new("price", SourceDataType::Int64),
            SourceColumn::new("channel", SourceDataType::Utf8),
            SourceColumn::new("url", SourceDataType::Utf8),
            SourceColumn::new("date_time", SourceDataType::TimestampMillis),
            SourceColumn::new("extra", SourceDataType::Utf8),
        ],
    )
    .expect("bid definition")
}

fn nexmark_auction_source_definition() -> SourceDefinition {
    SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("item_name", SourceDataType::Utf8),
            SourceColumn::new("description", SourceDataType::Utf8),
            SourceColumn::new("initial_bid", SourceDataType::Int64),
            SourceColumn::new("reserve", SourceDataType::Int64),
            SourceColumn::new("seller", SourceDataType::Int64),
            SourceColumn::new("category", SourceDataType::Int64),
            SourceColumn::new("expires", SourceDataType::TimestampMillis),
            SourceColumn::new("date_time", SourceDataType::TimestampMillis),
            SourceColumn::new("extra", SourceDataType::Utf8),
        ],
    )
    .expect("auction definition")
}

async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}
