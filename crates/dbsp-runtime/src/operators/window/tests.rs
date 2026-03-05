use super::*;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::storage::dictionary::Dictionary;
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{compute_delta, materialize_zset_handle};
use object_store::memory::InMemory;
use slatedb::Db;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicI64;

type Row = i64;

async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("window_agg", store).await.expect("open SlateDB"))
}

async fn stage_version<K>(
    dict: Arc<Dictionary<K>>,
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
    deltas: &[(K, i64)],
) -> ZSetHandle
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        let id = dict_batch
            .intern(key)
            .await
            .expect("intern key for window test");
        buckets
            .entry(bucket_for(id))
            .or_default()
            .push((id, *delta));
    }
    drop(dict_batch);

    let mut segments = Vec::new();
    for (bucket, mut bucket_deltas) in buckets {
        bucket_deltas.retain(|(_, delta)| *delta != 0);
        if bucket_deltas.is_empty() {
            continue;
        }
        bucket_deltas.sort_by_key(|(id, _)| *id);
        segments.push(SegmentRecord {
            id: 0,
            bucket,
            deltas: bucket_deltas,
        });
    }

    let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
        .await
        .expect("build versioned");
    let version = versioned
        .create_version_with_base(segments, None)
        .await
        .expect("create version");
    versioned.handle_for_version(version)
}

#[tokio::test]
async fn window_aggregate_groups_by_window() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

    let input_dict = Arc::new(
        Dictionary::<Row>::with_table(table.clone(), "window_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<(WindowKey<i64>, i64)>::with_table(table.clone(), "window_output", None)
            .await
            .expect("output dict"),
    );

    let state = RelationState::empty(table.clone(), "window_state".to_string())
        .await
        .expect("window state");
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "window_output".to_string(),
    )
    .await
    .expect("output zset");

    let index = IndexedBatchZSet::new(table.clone(), "window_index");
    let key_extractor = Arc::new(|row: &Row| Some(*row % 2));
    let time_extractor = Arc::new(|row: &Row| Some(*row));
    let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
        Arc::new(|_key, values| {
            let mut count = 0i64;
            let mut has_rows = false;
            for (_row, weight) in values {
                if *weight == 0 {
                    continue;
                }
                has_rows = true;
                count += *weight;
            }
            if has_rows { Some(count) } else { None }
        });
    let watermark = Arc::new(AtomicI64::new(-1));

    let mut op = WindowAggregateOp::new(
        state,
        index,
        table.clone(),
        key_extractor,
        time_extractor,
        aggregator,
        output,
        2,
        2,
        0,
        watermark,
    )
    .expect("window aggregate op");

    let deltas: Vec<Vec<(Row, i64)>> = vec![
        vec![(1, 1), (2, 1)],
        vec![(3, 1)],
        vec![(4, 1), (1, -1)],
        vec![],
    ];

    let mut window_counts: HashMap<WindowKey<i64>, i64> = HashMap::new();
    let mut prev_output: HashMap<(WindowKey<i64>, i64), i64> = HashMap::new();

    let mut cache_out = HashMap::new();
    cache_out.insert("window_output".to_string(), output_dict.clone());

    for (step, delta) in deltas.iter().enumerate() {
        for (row, weight) in delta {
            for (start, end) in op.windows_for(*row) {
                let key = WindowKey {
                    start,
                    end,
                    key: row % 2,
                };
                let entry = window_counts.entry(key.clone()).or_insert(0);
                *entry += *weight;
                if *entry == 0 {
                    window_counts.remove(&key);
                }
            }
        }
        let mut aggregated = HashMap::new();
        for (key, count) in &window_counts {
            aggregated.insert((key.clone(), *count), 1);
        }

        let expected_delta: HashMap<(WindowKey<i64>, i64), i64> =
            compute_delta(&prev_output, &aggregated)
                .into_iter()
                .collect();

        let handle = if delta.is_empty() {
            ZSetHandle {
                ns: "window_input".to_string(),
                version: 0,
            }
        } else {
            stage_version(input_dict.clone(), table.clone(), "window_input", delta).await
        };

        let out_handle = op
            .on_step(step as i64, &[handle])
            .await
            .expect("window step");

        if expected_delta.is_empty() {
            assert!(out_handle.is_none(), "expected empty output at step {step}");
        } else {
            let out_handle = out_handle.expect("output handle");
            let materialized = materialize_zset_handle::<(WindowKey<i64>, i64)>(
                table.clone(),
                &mut cache_out,
                &out_handle,
            )
            .await
            .expect("materialize output");
            assert_eq!(materialized, expected_delta, "step {step}");
        }

        prev_output = aggregated;
    }
}

#[tokio::test]
async fn window_aggregate_respects_watermark_allowed_lateness_cutoff() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

    let input_dict = Arc::new(
        Dictionary::<Row>::with_table(table.clone(), "window_late_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<(WindowKey<i64>, i64)>::with_table(table.clone(), "window_late_output", None)
            .await
            .expect("output dict"),
    );

    let state = RelationState::empty(table.clone(), "window_late_state".to_string())
        .await
        .expect("window state");
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "window_late_output".to_string(),
    )
    .await
    .expect("output zset");

    let index = IndexedBatchZSet::new(table.clone(), "window_late_index");
    let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
    let time_extractor = Arc::new(|row: &Row| Some(*row));
    let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
        Arc::new(|_key, values| {
            let mut count = 0i64;
            for (_row, weight) in values {
                count += *weight;
            }
            (count != 0).then_some(count)
        });
    let watermark = Arc::new(AtomicI64::new(5_000));

    let mut op = WindowAggregateOp::new(
        state,
        index,
        table.clone(),
        key_extractor,
        time_extractor,
        aggregator,
        output,
        1_000,
        1_000,
        500,
        watermark,
    )
    .expect("window aggregate op");

    let handle = stage_version(
        input_dict,
        table.clone(),
        "window_late_input",
        &[(4_499, 1), (4_500, 1), (5_200, 1)],
    )
    .await;
    let out = op
        .on_step(1, &[handle])
        .await
        .expect("window step")
        .expect("non-empty output");

    let mut cache = HashMap::new();
    cache.insert("window_late_output".to_string(), output_dict);
    let materialized =
        materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out)
            .await
            .expect("materialize output");

    // 4499 is dropped (< watermark - allowed_lateness = 4500).
    assert_eq!(materialized.len(), 2);
    assert_eq!(
        materialized.get(&(
            WindowKey {
                start: 4_000,
                end: 5_000,
                key: 0
            },
            1
        )),
        Some(&1)
    );
    assert_eq!(
        materialized.get(&(
            WindowKey {
                start: 5_000,
                end: 6_000,
                key: 0
            },
            1
        )),
        Some(&1)
    );
}

#[tokio::test]
async fn window_aggregate_accepts_out_of_order_events_within_lateness() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

    let input_dict = Arc::new(
        Dictionary::<Row>::with_table(table.clone(), "window_ooo_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<(WindowKey<i64>, i64)>::with_table(table.clone(), "window_ooo_output", None)
            .await
            .expect("output dict"),
    );

    let state = RelationState::empty(table.clone(), "window_ooo_state".to_string())
        .await
        .expect("window state");
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "window_ooo_output".to_string(),
    )
    .await
    .expect("output zset");

    let index = IndexedBatchZSet::new(table.clone(), "window_ooo_index");
    let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
    let time_extractor = Arc::new(|row: &Row| Some(*row));
    let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
        Arc::new(|_key, values| {
            let mut count = 0i64;
            for (_row, weight) in values {
                count += *weight;
            }
            (count != 0).then_some(count)
        });
    let watermark = Arc::new(AtomicI64::new(5_000));

    let mut op = WindowAggregateOp::new(
        state,
        index,
        table.clone(),
        key_extractor,
        time_extractor,
        aggregator,
        output,
        1_000,
        1_000,
        500,
        watermark,
    )
    .expect("window aggregate op");

    // 5_200 arrives before 4_600; both are >= watermark - allowed_lateness (4_500).
    let handle = stage_version(
        input_dict,
        table.clone(),
        "window_ooo_input",
        &[(5_200, 1), (4_600, 1)],
    )
    .await;
    let out = op
        .on_step(1, &[handle])
        .await
        .expect("window step")
        .expect("non-empty output");

    let mut cache = HashMap::new();
    cache.insert("window_ooo_output".to_string(), output_dict);
    let materialized =
        materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out)
            .await
            .expect("materialize output");

    assert_eq!(materialized.len(), 2);
    assert_eq!(
        materialized.get(&(
            WindowKey {
                start: 4_000,
                end: 5_000,
                key: 0
            },
            1
        )),
        Some(&1)
    );
    assert_eq!(
        materialized.get(&(
            WindowKey {
                start: 5_000,
                end: 6_000,
                key: 0
            },
            1
        )),
        Some(&1)
    );
}

#[tokio::test]
async fn window_aggregate_ignores_too_late_retractions_after_window_close() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

    let input_dict = Arc::new(
        Dictionary::<Row>::with_table(table.clone(), "window_retract_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<(WindowKey<i64>, i64)>::with_table(
            table.clone(),
            "window_retract_output",
            None,
        )
        .await
        .expect("output dict"),
    );

    let state = RelationState::empty(table.clone(), "window_retract_state".to_string())
        .await
        .expect("window state");
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "window_retract_output".to_string(),
    )
    .await
    .expect("output zset");

    let index = IndexedBatchZSet::new(table.clone(), "window_retract_index");
    let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
    let time_extractor = Arc::new(|row: &Row| Some(*row));
    let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
        Arc::new(|_key, values| {
            let mut count = 0i64;
            for (_row, weight) in values {
                count += *weight;
            }
            (count != 0).then_some(count)
        });
    let watermark = Arc::new(AtomicI64::new(-1));

    let mut op = WindowAggregateOp::new(
        state,
        index,
        table.clone(),
        key_extractor,
        time_extractor,
        aggregator,
        output,
        1_000,
        1_000,
        0,
        Arc::clone(&watermark),
    )
    .expect("window aggregate op");

    let first = stage_version(
        input_dict.clone(),
        table.clone(),
        "window_retract_input",
        &[(1_000, 1)],
    )
    .await;
    let out1 = op
        .on_step(1, &[first])
        .await
        .expect("window step")
        .expect("non-empty output");
    let mut cache = HashMap::new();
    cache.insert("window_retract_output".to_string(), output_dict.clone());
    let out1_materialized =
        materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize output");
    assert_eq!(out1_materialized.len(), 1);

    // Advance watermark so event timestamp 1000 is now too late.
    watermark.store(3_000, Ordering::Relaxed);
    let retract = stage_version(
        input_dict,
        table.clone(),
        "window_retract_input",
        &[(1_000, -1)],
    )
    .await;
    let out2 = op
        .on_step(2, &[retract])
        .await
        .expect("window step")
        .expect("eviction output");
    let out2_materialized =
        materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize output");
    assert_eq!(
        out2_materialized.get(&(
            WindowKey {
                start: 1_000,
                end: 2_000,
                key: 0
            },
            1
        )),
        Some(&-1)
    );
}

#[tokio::test]
async fn window_aggregate_evicts_expired_windows_on_watermark_advance() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

    let input_dict = Arc::new(
        Dictionary::<Row>::with_table(table.clone(), "window_evict_input", None)
            .await
            .expect("input dict"),
    );
    let output_dict = Arc::new(
        Dictionary::<(WindowKey<i64>, i64)>::with_table(table.clone(), "window_evict_output", None)
            .await
            .expect("output dict"),
    );

    let state = RelationState::empty(table.clone(), "window_evict_state".to_string())
        .await
        .expect("window state");
    let output = VersionedZSet::new(
        output_dict.clone(),
        table.clone(),
        "window_evict_output".to_string(),
    )
    .await
    .expect("output zset");
    let index = IndexedBatchZSet::new(table.clone(), "window_evict_index");
    let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
    let time_extractor = Arc::new(|row: &Row| Some(*row));
    let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
        Arc::new(|_key, values| {
            let mut count = 0_i64;
            for (_row, weight) in values {
                count += *weight;
            }
            (count != 0).then_some(count)
        });
    let watermark = Arc::new(AtomicI64::new(-1));

    let mut op = WindowAggregateOp::new(
        state,
        index,
        table.clone(),
        key_extractor,
        time_extractor,
        aggregator,
        output,
        1_000,
        1_000,
        0,
        Arc::clone(&watermark),
    )
    .expect("window aggregate op");

    let first = stage_version(
        input_dict.clone(),
        table.clone(),
        "window_evict_input",
        &[(1_000, 1)],
    )
    .await;
    let _ = op
        .on_step(1, &[first])
        .await
        .expect("window step")
        .expect("non-empty output");

    watermark.store(3_000, Ordering::Relaxed);
    let empty_handle = ZSetHandle {
        ns: "window_evict_input".to_string(),
        version: 0,
    };
    let out = op
        .on_step(2, &[empty_handle])
        .await
        .expect("window step")
        .expect("eviction output");

    let mut cache = HashMap::new();
    cache.insert("window_evict_output".to_string(), output_dict);
    let materialized =
        materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out)
            .await
            .expect("materialize output");

    assert_eq!(
        materialized.get(&(
            WindowKey {
                start: 1_000,
                end: 2_000,
                key: 0
            },
            1
        )),
        Some(&-1)
    );
}
