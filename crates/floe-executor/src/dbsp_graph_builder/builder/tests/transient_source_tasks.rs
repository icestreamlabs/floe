use super::*;

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
    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(crate::task_events::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
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
        &task_tx,
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
        &task_tx,
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

    let canonical_join =
        DbspJoin::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left_stream,
            &right_stream,
            None,
            {
                let left_key = left_key.clone();
                move |deltas: &[(Vec<u8>, i64)]| {
                    deltas
                        .iter()
                        .filter_map(|(row, weight)| {
                            left_key(row).map(|key| (key, row.clone(), *weight))
                        })
                        .collect()
                }
            },
            {
                let right_key = right_key.clone();
                move |deltas: &[(Vec<u8>, i64)]| {
                    deltas
                        .iter()
                        .filter_map(|(row, weight)| {
                            right_key(row).map(|key| (key, row.clone(), *weight))
                        })
                        .collect()
                }
            },
            predicate,
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

    let (observer_tx, mut observer_rx) = mpsc::channel::<(i64, Arc<Vec<(Vec<u8>, i64)>>)>(1024);
    let observer = Arc::new(move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| {
        let _ = observer_tx.try_send((version, deltas));
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
                .transform_delta_arrow(
                    "benchmark_join_source_task_build_tick_residual",
                    Arc::new(build_tick_delta.into_iter().collect::<Vec<_>>()),
                )
                .await
                .expect("apply benchmark source-task join build tick residual filter"),
        )
    } else {
        build_tick_delta
    };
    assert!(
        build_tick_delta.is_empty(),
        "auction build tick should emit an explicit empty canonical join handle"
    );
    let (build_version, build_transient_batch) =
        timeout(Duration::from_secs(1), observer_rx.recv())
            .await
            .expect("wait transient join build tick")
            .expect("transient join build tick");
    assert_eq!(build_version, 1);
    assert!(
        build_transient_batch.is_empty(),
        "auction build tick should emit an explicit empty transient join batch"
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
                    .transform_delta_arrow(
                        "benchmark_join_source_task_tick_residual",
                        Arc::new(actual.into_iter().collect::<Vec<_>>()),
                    )
                    .await
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
                .transform_delta_arrow(
                    "benchmark_join_source_task_tick_residual",
                    Arc::new(transient_raw),
                )
                .await
                .expect("apply benchmark source-task transient join residual filter")
        } else {
            transient_raw
        };
        let expected = consolidate_encoded_deltas(transient_raw);
        assert_eq!(actual, expected, "join output mismatch at bid tick {tick}");
    }
}
