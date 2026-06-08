use super::*;
use crate::storage::SlateTable;
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::materialize_zset_handle;
use object_store::memory::InMemory;
use slatedb::Db;
use std::sync::atomic::{AtomicU64, Ordering};

type LeftRow = (i64, i64, i64);
type RightRow = (i64, i64);
type OutRow = (i64, i64);

static TEST_NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_test_suffix() -> u64 {
    TEST_NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed)
}

async fn build_db(suffix: u64) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(
        Db::open(format!("range_join_op_{suffix}"), store)
            .await
            .expect("open SlateDB"),
    )
}

async fn stage_version<T>(
    dict: Arc<Dictionary<T>>,
    table: Arc<SlateTable>,
    ns: &str,
    deltas: &[(T, i64)],
) -> ZSetHandle
where
    T: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    if deltas.is_empty() {
        return ZSetHandle {
            ns: ns.to_string(),
            version: 0,
        };
    }

    let mut zset = VersionedZSet::new(dict, table, ns.to_string())
        .await
        .expect("versioned zset");
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let values = deltas.iter().map(|(value, _)| value);
    let ids = zset
        .dictionary()
        .intern_many_values_unique(values)
        .await
        .expect("intern values");
    for ((_, weight), id) in deltas.iter().zip(ids.into_iter()) {
        buckets
            .entry(bucket_for(id))
            .or_default()
            .push((id, *weight));
    }
    let segments = buckets
        .into_iter()
        .map(|(bucket, deltas)| SegmentRecord {
            id: 0,
            bucket,
            deltas,
        })
        .collect();
    let version = zset
        .create_version_with_base(segments, None)
        .await
        .expect("create version");
    let handle = zset.handle_for_version(version);
    publish_transient_zset_batch(&handle, Arc::new(deltas.to_vec()));
    handle
}

async fn build_op(
    suffix: u64,
) -> (
    RangeJoinOp<LeftRow, RightRow, OutRow, i64>,
    Arc<Dictionary<LeftRow>>,
    Arc<Dictionary<RightRow>>,
    Arc<SlateTable>,
) {
    let db = build_db(suffix).await;
    let table = Arc::new(SlateTable::new(db));
    let left_dict = Arc::new(
        Dictionary::<LeftRow>::with_table(
            table.clone(),
            format!("range_left_stream_{suffix}"),
            None,
        )
        .await
        .expect("left dict"),
    );
    let right_dict = Arc::new(
        Dictionary::<RightRow>::with_table(
            table.clone(),
            format!("range_right_stream_{suffix}"),
            None,
        )
        .await
        .expect("right dict"),
    );
    let out_dict = Arc::new(
        Dictionary::<OutRow>::with_table(table.clone(), format!("range_output_{suffix}"), None)
            .await
            .expect("output dict"),
    );

    let left_state = RelationState::empty(table.clone(), format!("range_left_state_{suffix}"))
        .await
        .expect("left state");
    let right_state = RelationState::empty(table.clone(), format!("range_right_state_{suffix}"))
        .await
        .expect("right state");
    let output = VersionedZSet::new(out_dict, table.clone(), format!("range_output_{suffix}"))
        .await
        .expect("output zset");
    let right_index =
        IndexedBatchZSet::with_range_index(table.clone(), format!("range_right_index_{suffix}"));
    let left_range: BatchLeftRangeExtractor<LeftRow, i64> = Arc::new(|deltas| {
        deltas
            .iter()
            .map(|(row @ (_, lower, upper), weight)| (*lower, *upper, *row, *weight))
            .collect()
    });
    let right_key: BatchRightKeyExtractor<RightRow, i64> = Arc::new(|deltas| {
        deltas
            .iter()
            .map(|(row @ (key, _), weight)| (*key, *row, *weight))
            .collect()
    });
    let predicate: RangeJoinPredicate<LeftRow, RightRow> = Arc::new(|_, _| true);
    let projector: RangeJoinProjector<LeftRow, RightRow, OutRow> =
        Arc::new(|left, right| (left.0, right.1));

    let op = RangeJoinOp::new_batch(RangeJoinBatchConfig {
        left_state,
        right_state,
        right_index,
        left_range,
        right_key,
        predicate,
        projector,
        table: table.clone(),
        output,
        integrated: None,
    });

    (op, left_dict, right_dict, table)
}

#[tokio::test]
async fn range_join_emits_all_three_delta_terms() {
    let suffix = next_test_suffix();
    let (mut op, left_dict, right_dict, table) = build_op(suffix).await;
    let mut cache = HashMap::new();

    let left_t1 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_left_stream_t1",
        &[((1, 10, 20), 1)],
    )
    .await;
    let right_t1 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_right_stream_t1",
        &[((15, 100), 1)],
    )
    .await;
    let out_t1 = op
        .on_step(1, &[left_t1, right_t1])
        .await
        .expect("range join t1")
        .expect("output t1");
    let materialized_t1 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t1)
        .await
        .expect("materialize t1");
    assert_eq!(materialized_t1, HashMap::from([((1, 100), 1)]));

    let left_t2 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_left_stream_t2",
        &[],
    )
    .await;
    let right_t2 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_right_stream_t2",
        &[((12, 101), 1)],
    )
    .await;
    let out_t2 = op
        .on_step(2, &[left_t2, right_t2])
        .await
        .expect("range join t2")
        .expect("output t2");
    let materialized_t2 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t2)
        .await
        .expect("materialize t2");
    assert_eq!(materialized_t2, HashMap::from([((1, 101), 1)]));

    let left_t3 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_left_stream_t3",
        &[((2, 10, 13), 1)],
    )
    .await;
    let right_t3 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_right_stream_t3",
        &[],
    )
    .await;
    let out_t3 = op
        .on_step(3, &[left_t3, right_t3])
        .await
        .expect("range join t3")
        .expect("output t3");
    let materialized_t3 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t3)
        .await
        .expect("materialize t3");
    assert_eq!(materialized_t3, HashMap::from([((2, 101), 1)]));
    assert_eq!(op.last_logical_work().output_delta_rows, 1);
}

#[tokio::test]
async fn range_join_retracts_right_delta_against_existing_left_ranges() {
    let suffix = next_test_suffix();
    let (mut op, left_dict, right_dict, table) = build_op(suffix).await;
    let mut cache = HashMap::new();

    let left_t1 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_retract_left_stream_t1",
        &[((1, 10, 20), 1), ((2, 10, 13), 1)],
    )
    .await;
    let right_t1 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_retract_right_stream_t1",
        &[((12, 101), 1)],
    )
    .await;
    op.on_step(1, &[left_t1, right_t1])
        .await
        .expect("range join t1");

    let left_t2 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_retract_left_stream_t2",
        &[],
    )
    .await;
    let right_t2 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_retract_right_stream_t2",
        &[((12, 101), -1)],
    )
    .await;
    let out_t2 = op
        .on_step(2, &[left_t2, right_t2])
        .await
        .expect("range join t2")
        .expect("output t2");
    let materialized_t2 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t2)
        .await
        .expect("materialize t2");
    assert_eq!(
        materialized_t2,
        HashMap::from([((1, 101), -1), ((2, 101), -1)])
    );
    assert_eq!(op.last_logical_work().output_delta_rows, 2);
}

#[tokio::test]
async fn range_join_right_delta_uses_left_interval_index() {
    let suffix = next_test_suffix();
    let (mut op, left_dict, right_dict, table) = build_op(suffix).await;
    let mut cache = HashMap::new();

    let left_rows = (0..100)
        .map(|id| ((id, id * 10, id * 10 + 5), 1))
        .collect::<Vec<_>>();
    let left_t1 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_index_left_stream_t1",
        &left_rows,
    )
    .await;
    let right_t1 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_index_right_stream_t1",
        &[],
    )
    .await;
    op.on_step(1, &[left_t1, right_t1])
        .await
        .expect("seed left ranges");

    let left_t2 = stage_version(left_dict, table.clone(), "range_index_left_stream_t2", &[]).await;
    let right_t2 = stage_version(
        right_dict,
        table.clone(),
        "range_index_right_stream_t2",
        &[((502, 900), 1)],
    )
    .await;
    let out_t2 = op
        .on_step(2, &[left_t2, right_t2])
        .await
        .expect("probe right delta")
        .expect("output t2");
    let materialized_t2 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t2)
        .await
        .expect("materialize t2");
    assert_eq!(materialized_t2, HashMap::from([((50, 900), 1)]));
    assert_eq!(
        op.last_logical_work().left_state_rows_examined,
        1,
        "right-delta probing should visit matching left intervals, not the whole left cache",
    );
}

#[tokio::test]
async fn range_join_interval_overlay_masks_stale_index_entries() {
    let suffix = next_test_suffix();
    let (mut op, left_dict, right_dict, table) = build_op(suffix).await;
    let mut cache = HashMap::new();

    let left_t1 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_overlay_left_stream_t1",
        &[((1, 10, 20), 1)],
    )
    .await;
    let right_t1 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_overlay_right_stream_t1",
        &[],
    )
    .await;
    op.on_step(1, &[left_t1, right_t1])
        .await
        .expect("seed left range");

    let left_t2 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_overlay_left_stream_t2",
        &[((1, 10, 20), -1), ((1, 30, 40), 1)],
    )
    .await;
    let right_t2 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_overlay_right_stream_t2",
        &[],
    )
    .await;
    op.on_step(2, &[left_t2, right_t2])
        .await
        .expect("move left range");

    let left_t3 = stage_version(
        left_dict.clone(),
        table.clone(),
        "range_overlay_left_stream_t3",
        &[],
    )
    .await;
    let right_t3 = stage_version(
        right_dict.clone(),
        table.clone(),
        "range_overlay_right_stream_t3",
        &[((15, 100), 1)],
    )
    .await;
    let out_t3 = op
        .on_step(3, &[left_t3, right_t3])
        .await
        .expect("probe stale interval")
        .expect("empty output handle t3");
    let materialized_t3 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t3)
        .await
        .expect("materialize t3");
    assert!(
        materialized_t3.is_empty(),
        "right probe must not join against an interval removed after the index was built"
    );

    let left_t4 = stage_version(
        left_dict,
        table.clone(),
        "range_overlay_left_stream_t4",
        &[],
    )
    .await;
    let right_t4 = stage_version(
        right_dict,
        table.clone(),
        "range_overlay_right_stream_t4",
        &[((35, 101), 1)],
    )
    .await;
    let out_t4 = op
        .on_step(4, &[left_t4, right_t4])
        .await
        .expect("probe overlay interval")
        .expect("output t4");
    let materialized_t4 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t4)
        .await
        .expect("materialize t4");
    assert_eq!(materialized_t4, HashMap::from([((1, 101), 1)]));
}
