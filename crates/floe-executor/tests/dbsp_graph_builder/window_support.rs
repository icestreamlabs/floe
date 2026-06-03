use super::*;

pub(super) fn hopping_window_count_plan() -> dbsp::CircuitPlan {
    let bid = nexmark_bid_table();
    let input_schema = bid.schema().clone();
    let aggregate = dbsp::DbspAggregateNode::try_new(
        input_schema.clone(),
        vec![(col("auction"), None)],
        vec![(
            dbsp::DbspAggregateFunction::Count,
            None,
            None,
            false,
            Some("num".to_string()),
        )],
    )
    .expect("build hopping aggregate");

    let window = dbsp::DbspWindowSpec::try_new(
        dbsp::DbspWindowPolicy::Hopping {
            size_ms: 10_000,
            slide_ms: 2_000,
        },
        col("date_time"),
        input_schema.clone(),
        0,
    )
    .expect("build hopping window");

    build_window_plan(bid, input_schema, aggregate, window)
}

pub(super) fn tumbling_window_max_plan() -> dbsp::CircuitPlan {
    let bid = nexmark_bid_table();
    let input_schema = bid.schema().clone();
    let aggregate = dbsp::DbspAggregateNode::try_new(
        input_schema.clone(),
        vec![],
        vec![(
            dbsp::DbspAggregateFunction::Max,
            Some(col("price")),
            None,
            false,
            Some("maxprice".to_string()),
        )],
    )
    .expect("build tumbling max aggregate");

    let window = dbsp::DbspWindowSpec::try_new(
        dbsp::DbspWindowPolicy::Tumbling { size_ms: 10_000 },
        col("date_time"),
        input_schema.clone(),
        0,
    )
    .expect("build tumbling window");

    build_window_plan(bid, input_schema, aggregate, window)
}

pub(super) fn tumbling_window_avg_plan() -> dbsp::CircuitPlan {
    let bid = nexmark_bid_table();
    let input_schema = bid.schema().clone();
    let aggregate = dbsp::DbspAggregateNode::try_new(
        input_schema.clone(),
        vec![],
        vec![(
            dbsp::DbspAggregateFunction::Avg,
            Some(col("price")),
            None,
            false,
            Some("avgprice".to_string()),
        )],
    )
    .expect("build tumbling avg aggregate");

    let window = dbsp::DbspWindowSpec::try_new(
        dbsp::DbspWindowPolicy::Tumbling { size_ms: 10_000 },
        col("date_time"),
        input_schema.clone(),
        0,
    )
    .expect("build tumbling window");

    build_window_plan(bid, input_schema, aggregate, window)
}

pub(super) fn tumbling_window_count_by_bidder_plan() -> dbsp::CircuitPlan {
    let bid = nexmark_bid_table();
    let input_schema = bid.schema().clone();
    let aggregate = dbsp::DbspAggregateNode::try_new(
        input_schema.clone(),
        vec![(col("bidder"), None)],
        vec![(
            dbsp::DbspAggregateFunction::Count,
            None,
            None,
            false,
            Some("bid_count".to_string()),
        )],
    )
    .expect("build tumbling bidder aggregate");

    let window = dbsp::DbspWindowSpec::try_new(
        dbsp::DbspWindowPolicy::Tumbling { size_ms: 10_000 },
        col("date_time"),
        input_schema.clone(),
        0,
    )
    .expect("build tumbling bidder window");

    build_window_plan(bid, input_schema, aggregate, window)
}

pub(super) fn build_window_plan(
    table: &'static dbsp::TableDescriptor,
    input_schema: Arc<dbsp::RowSchema>,
    aggregate: dbsp::DbspAggregateNode,
    window: dbsp::DbspWindowSpec,
) -> dbsp::CircuitPlan {
    let mut fields = Vec::new();
    fields.push(dbsp::Field::new(
        "window_start",
        dbsp::DbspScalarType::TimestampMillis,
        false,
    ));
    fields.push(dbsp::Field::new(
        "window_end",
        dbsp::DbspScalarType::TimestampMillis,
        false,
    ));
    fields.extend(aggregate.output_schema().fields().iter().cloned());
    let output_schema = dbsp::RowSchema::try_new(fields).expect("build window output schema");

    let source = dbsp::CircuitNode {
        id: 0,
        kind: dbsp::DbspNodeKind::Source(dbsp::DbspSourceNode {
            table: Arc::new(table.clone()),
        }),
        inputs: vec![],
        output_schema: input_schema,
    };
    let window_node = dbsp::CircuitNode {
        id: 1,
        kind: dbsp::DbspNodeKind::WindowAggregate(dbsp::DbspWindowAggregateNode {
            aggregate,
            window,
        }),
        inputs: vec![0],
        output_schema,
    };

    dbsp::CircuitPlan {
        root: 1,
        nodes: vec![source, window_node],
    }
}

pub(super) async fn build_window_plan_rows(
    db_name: &str,
    view_name: &str,
    plan: &dbsp::CircuitPlan,
    bids: &[(i64, i64, i64, i64)],
) -> Vec<TestRow> {
    build_window_plan_rows_with_durable_source(db_name, view_name, plan, bids, false).await
}

pub(super) async fn build_window_plan_rows_with_durable_source(
    db_name: &str,
    view_name: &str,
    plan: &dbsp::CircuitPlan,
    bids: &[(i64, i64, i64, i64)],
    durable_source: bool,
) -> Vec<TestRow> {
    assert_eq!(
        source_batch_journal_root_sources(plan)
            .expect("source journal root source analysis")
            .expect("source journal root source set"),
        BTreeSet::from(["nexmark_bid".to_string()])
    );

    let db = test_db(db_name).await;
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let available_sources = ["nexmark_bid"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");
    registry.set_durable_enabled("nexmark_bid", durable_source);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(view_name, root_arrow_schema(plan));

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        for (auction, bidder, price, date_time_ms) in bids {
            writer
                .append_encoded(
                    encoded_bid_row_with_ts(*auction, *bidder, *price, *date_time_ms),
                    1,
                )
                .expect("append bid");
        }
        writer.flush().await.expect("flush bids");
    }

    wait_for_visible_row_count(&mv_registry, view_name, 1).await;
    materialized_rows(&mv_registry, view_name).await
}
