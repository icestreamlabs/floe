use super::*;

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
    let topn = test_topn_node(1, 0);
    let mut processor = TransientDirectTop1Processor::new(
        "test-graph",
        &topn,
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
fn direct_partition_topn_positive_fast_path_keeps_full_state_for_retractions() {
    let decoder = SourceRowDecoder::new(nexmark_bid_source_definition());
    let row_05 = encode_event(&decoder, bid_event_payload(1, 105, 5), "nexmark_bid");
    let row_10 = encode_event(&decoder, bid_event_payload(1, 101, 10), "nexmark_bid");
    let row_20 = encode_event(&decoder, bid_event_payload(1, 102, 20), "nexmark_bid");
    let row_30 = encode_event(&decoder, bid_event_payload(1, 103, 30), "nexmark_bid");
    let key_layout = test_topn_key_layout();
    let topn = test_topn_node(2, 0);
    let mut processor = TransientDirectPartitionTopNProcessor::new(
        "test-graph",
        TransientDirectPartitionTopNConfig { partition_idx: 0 },
        &topn,
        &key_layout,
    );

    let initial_output = processor
        .apply_deltas(vec![(row_10.clone(), 1), (row_20.clone(), 1)])
        .expect("apply initial topn rows")
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(
        initial_output,
        HashMap::from([(row_10.clone(), 1), (row_20.clone(), 1)])
    );

    let worse_output = processor
        .apply_deltas(vec![(row_30.clone(), 1)])
        .expect("apply worse row");
    assert!(worse_output.is_empty());

    let better_output = processor
        .apply_deltas(vec![(row_05.clone(), 1)])
        .expect("apply better row")
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(
        better_output,
        HashMap::from([(row_05.clone(), 1), (row_20.clone(), -1)])
    );

    let retraction_output = processor
        .apply_deltas(vec![(row_10.clone(), -1)])
        .expect("apply retraction")
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(
        retraction_output,
        HashMap::from([(row_10, -1), (row_20, 1)])
    );
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
                dbsp::IncrementalAggregateSlotState::DecimalSum {
                    sum: 12345,
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
fn transient_window_count_eviction_schedule_requires_finite_lateness() {
    let key = TransientWindowCountKey {
        start: 0,
        end: 10_000,
        key: Arc::<[u8]>::from([1_u8, 2, 3]),
    };
    let mut counts = AHashMap::new();
    let mut eviction_schedule = std::collections::BTreeMap::new();
    let mut updates = TransientWindowCountUpdates::new(None);

    apply_transient_window_count_delta(
        &mut counts,
        &mut eviction_schedule,
        &mut updates,
        key.clone(),
        1,
        false,
    );
    assert!(eviction_schedule.is_empty());

    let mut counts = AHashMap::new();
    let mut eviction_schedule = std::collections::BTreeMap::new();
    let mut updates = TransientWindowCountUpdates::new(None);
    apply_transient_window_count_delta(
        &mut counts,
        &mut eviction_schedule,
        &mut updates,
        key,
        1,
        true,
    );
    assert_eq!(eviction_schedule.get(&10_000).map(Vec::len), Some(1));
}
