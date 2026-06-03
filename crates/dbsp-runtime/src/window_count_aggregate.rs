use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow, ensure};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::VersionedZSet;
use crate::collections::{DEFAULT_HOT_KEY_COMPACTION_THRESHOLD, IndexedBatchZSet};
use crate::handles::ZSetHandle;
use crate::operators::count_aggregate::{
    CountAggregateOp, CountAggregateRow, CountAggregateSlotKind, GroupedCountState,
};
use crate::operators::window::{
    WINDOW_DROPPED_TOO_LATE_TOTAL, WINDOW_STATE_ENTRIES, WINDOW_STATE_LIMIT,
    WINDOW_STATE_LIMIT_EXCEEDED_TOTAL, WindowKey,
};
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::runtime::{HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, delta_zset_handle, publish_scheduled_value,
    push_value_in_place,
};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct WindowCountInput<K, V> {
    pub window_key: WindowKey<K>,
    pub value: V,
}

type BatchWindowExtractor<V, K> = Arc<dyn Fn(&[(V, i64)]) -> Vec<(V, i64, K, i64)> + Send + Sync>;
type BatchWindowRowEvaluator<K, V, D> = Arc<
    dyn Fn(&[(WindowCountInput<K, V>, i64)]) -> Vec<(CountAggregateRow<WindowKey<K>, D>, i64)>
        + Send
        + Sync,
>;

pub struct DbspWindowCountAggregate {
    stream: DeltaHandleStream,
}

struct WindowCountAggregateOp<K, V, D>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    D: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    window_extractor: BatchWindowExtractor<V, K>,
    count_op: CountAggregateOp<WindowKey<K>, WindowCountInput<K, V>, D>,
    watermark: Arc<AtomicI64>,
    window_size: i64,
    window_slide: i64,
    allowed_lateness_ms: i64,
}

impl<K, V, D> WindowCountAggregateOp<K, V, D>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    D: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    fn for_each_window<F>(&self, ts: i64, mut visit: F)
    where
        F: FnMut(i64, i64),
    {
        if self.window_size == self.window_slide {
            let start = ts.div_euclid(self.window_slide) * self.window_slide;
            visit(start, start + self.window_size);
            return;
        }

        let latest_start = ts.div_euclid(self.window_slide) * self.window_slide;
        let count = (self.window_size / self.window_slide).max(1);
        let first_start = latest_start - (count - 1) * self.window_slide;
        for i in 0..count {
            let start = first_start + i * self.window_slide;
            visit(start, start + self.window_size);
        }
    }

    fn watermark_cutoff(&self) -> Option<i64> {
        let watermark = self.watermark.load(Ordering::Relaxed);
        if watermark < 0 {
            return None;
        }
        Some(watermark.saturating_sub(self.allowed_lateness_ms.max(0)))
    }

    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64> {
        let mut merged = HashMap::new();
        for (row, weight) in deltas {
            match merged.entry(row) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let next = *entry.get() + weight;
                    if next == 0 {
                        entry.remove();
                    } else {
                        *entry.get_mut() = next;
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if weight != 0 {
                        entry.insert(weight);
                    }
                }
            }
        }
        merged
    }

    async fn on_step(&mut self, inputs: &[ZSetHandle]) -> Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("window count aggregate requires one input delta handle")?;
        let delta_values =
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for window count aggregate")?;
        let coalesced = self.coalesce_deltas(delta_values);
        let cutoff = self.watermark_cutoff();

        let mut expanded = Vec::new();
        let mut dropped_too_late = 0_u64;
        let delta_rows = coalesced.into_iter().collect::<Vec<_>>();
        for (row, weight, key, event_ts) in (self.window_extractor)(&delta_rows) {
            if weight == 0 {
                continue;
            }
            if event_ts < 0 {
                continue;
            }
            if let Some(cutoff) = cutoff
                && event_ts < cutoff
            {
                dropped_too_late = dropped_too_late.saturating_add(weight.unsigned_abs());
                continue;
            }
            self.for_each_window(event_ts, |window_start, window_end| {
                expanded.push((
                    WindowCountInput {
                        window_key: WindowKey {
                            start: window_start,
                            end: window_end,
                            key: key.clone(),
                        },
                        value: row.clone(),
                    },
                    weight,
                ));
            });
        }
        if dropped_too_late > 0 {
            WINDOW_DROPPED_TOO_LATE_TOTAL.inc_by(dropped_too_late);
        }

        let mut output_deltas = self.count_op.apply_delta_values(&expanded).await?;
        if let Some(cutoff) = cutoff {
            merge_output_deltas(
                &mut output_deltas,
                self.count_op
                    .evict_keys_where(|key| key.end <= cutoff)
                    .await
                    .context("evict expired window count aggregate keys")?,
            );
        }

        let state_entries = self.count_op.state_entry_count().await?;
        WINDOW_STATE_ENTRIES.set(i64::try_from(state_entries).unwrap_or(i64::MAX));
        if let Some(limit) = *WINDOW_STATE_LIMIT
            && state_entries > limit
        {
            WINDOW_STATE_LIMIT_EXCEEDED_TOTAL.inc();
            tracing::warn!(
                current_entries = state_entries,
                limit,
                "window aggregate state exceeds configured limit"
            );
        }

        if output_deltas.is_empty() {
            return Ok(Some(self.count_op.empty_output_handle()));
        }

        let output_handle = self
            .count_op
            .persist_output_deltas(&output_deltas)
            .await
            .context("persist window count aggregate output delta")?;
        Ok(Some(output_handle))
    }
}

impl DbspWindowCountAggregate {
    #[allow(clippy::too_many_arguments)]
    pub async fn new_batch<K, V, D, FWindow, FRow>(
        input: &DeltaHandleStream,
        window_extractor: FWindow,
        row_evaluator: FRow,
        slot_kinds: Vec<CountAggregateSlotKind>,
        window_size: i64,
        window_slide: i64,
        allowed_lateness_ms: i64,
        watermark: Arc<AtomicI64>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        V: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        D: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        FWindow: Fn(&[(V, i64)]) -> Vec<(V, i64, K, i64)> + Send + Sync + 'static,
        FRow: Fn(&[(WindowCountInput<K, V>, i64)]) -> Vec<(CountAggregateRow<WindowKey<K>, D>, i64)>
            + Send
            + Sync
            + 'static,
    {
        Self::new_batch_with_state_namespace(
            input,
            None,
            window_extractor,
            row_evaluator,
            slot_kinds,
            window_size,
            window_slide,
            allowed_lateness_ms,
            watermark,
            error_handler,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_batch_with_state_namespace<K, V, D, FWindow, FRow>(
        input: &DeltaHandleStream,
        state_namespace: Option<String>,
        window_extractor: FWindow,
        row_evaluator: FRow,
        slot_kinds: Vec<CountAggregateSlotKind>,
        window_size: i64,
        window_slide: i64,
        allowed_lateness_ms: i64,
        watermark: Arc<AtomicI64>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        V: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        D: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        FWindow: Fn(&[(V, i64)]) -> Vec<(V, i64, K, i64)> + Send + Sync + 'static,
        FRow: Fn(&[(WindowCountInput<K, V>, i64)]) -> Vec<(CountAggregateRow<WindowKey<K>, D>, i64)>
            + Send
            + Sync
            + 'static,
    {
        ensure!(window_size > 0, "window size must be positive");
        ensure!(window_slide > 0, "window slide must be positive");
        ensure!(
            window_size % window_slide == 0,
            "window size must be a multiple of slide"
        );
        ensure!(
            allowed_lateness_ms >= 0,
            "allowed lateness must be non-negative"
        );

        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let aggregate_id = NEXT_WINDOW_COUNT_AGGREGATE_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("window_count_aggregate_output_{aggregate_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let state = match state_namespace {
            Some(namespace) => {
                RelationState::<(WindowKey<K>, GroupedCountState)>::empty(table.clone(), namespace)
                    .await?
            }
            None => {
                RelationState::<(WindowKey<K>, GroupedCountState)>::empty_uncheckpointed(
                    table.clone(),
                    format!("window_count_aggregate_state_{aggregate_id}"),
                )
                .await?
            }
        };
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<K>, Vec<i64>)>::with_table(
                table.clone(),
                output_ns.clone(),
                None,
            )
            .await
            .context("create output dictionary for window count aggregate")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for window count aggregate")?;
        let distinct_index = slot_kinds
            .iter()
            .any(|kind| matches!(kind, CountAggregateSlotKind::Distinct))
            .then(|| {
                IndexedBatchZSet::with_hot_key_compaction_threshold(
                    table.clone(),
                    format!("window_count_aggregate_distinct_{aggregate_id}"),
                    DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
                )
            });

        let count_op = CountAggregateOp::new_batch(
            state,
            table.clone(),
            Arc::new(row_evaluator) as BatchWindowRowEvaluator<K, V, D>,
            output,
            slot_kinds,
            distinct_index,
        );
        let window_op = Arc::new(AsyncMutex::new(WindowCountAggregateOp {
            table: table.clone(),
            dict_cache: HashMap::new(),
            window_extractor: Arc::new(window_extractor),
            count_op,
            watermark,
            window_size,
            window_slide,
            allowed_lateness_ms,
        }));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for handle in history {
            let out_handle = {
                let mut op_guard = window_op.lock().await;
                op_guard.on_step(std::slice::from_ref(&handle)).await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "window_count_aggregate_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        {
            let mut op_guard = window_op.lock().await;
            op_guard.count_op.enable_live_output_replayable();
        }

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let op = Arc::clone(&window_op);
            let writer = Arc::clone(&writer);
            let empty_handle = empty_handle.clone();
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow!(
                        "window count aggregate runtime expected 1 handle, got {}",
                        handles_vec.len()
                    ));
                }
                if ts <= horizon {
                    let mut writer_guard = writer.lock().await;
                    publish_scheduled_value(&mut writer_guard, ts).await?;
                    return Ok(());
                }
                let mut op_guard = op.lock().await;
                let out_handle = op_guard
                    .on_step(&handles_vec)
                    .await?
                    .unwrap_or_else(|| empty_handle.clone());
                let mut writer_guard = writer.lock().await;
                push_value_in_place(&mut writer_guard, out_handle);
                writer_guard.flush().await?;
                Ok(())
            })
        });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "window_count_aggregate", err);
                    break;
                }
            }
        });

        Ok(Self {
            stream: DeltaHandleStream::new(stream),
        })
    }

    pub fn stream(&self) -> DeltaHandleStream {
        self.stream.clone()
    }
}

fn merge_output_deltas<K>(
    target: &mut HashMap<(K, Vec<i64>), i64>,
    updates: HashMap<(K, Vec<i64>), i64>,
) where
    K: Clone + Eq + Hash,
{
    for (pair, delta) in updates {
        if delta == 0 {
            continue;
        }
        let entry = target.entry(pair.clone()).or_insert(0);
        *entry += delta;
        if *entry == 0 {
            target.remove(&pair);
        }
    }
}

#[derive(Clone)]
struct ZSetHandleGroup {
    default: ZSetHandle,
}

#[async_trait::async_trait]
impl AbelianGroup<ZSetHandle> for ZSetHandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

static NEXT_WINDOW_COUNT_AGGREGATE_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use crate::operators::count_aggregate::CountAggregateSlotUpdate;
    use crate::storage::KeyValueTable;
    use crate::storage::SlateTable;
    use crate::storage::dictionary::Dictionary;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::Ordering;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open("window_count_aggregate", store)
                .await
                .expect("open SlateDB"),
        )
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
                .expect("intern key for window count test");
            buckets
                .entry((id >> 48) as u16)
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
            .expect("build versioned input");
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create input version");
        versioned.handle_for_version(version)
    }

    #[tokio::test]
    async fn window_count_aggregate_evicts_expired_windows_on_watermark_advance() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "window_count_input", None)
                .await
                .expect("input dict"),
        );
        let state = RelationState::<(WindowKey<i64>, GroupedCountState)>::empty(
            table.clone(),
            "window_count_state".to_string(),
        )
        .await
        .expect("count state");
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, Vec<i64>)>::with_table(
                table.clone(),
                "window_count_output",
                None,
            )
            .await
            .expect("output dict"),
        );
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "window_count_output".to_string(),
        )
        .await
        .expect("output zset");

        let watermark = Arc::new(AtomicI64::new(-1));
        let count_op =
            CountAggregateOp::<WindowKey<i64>, WindowCountInput<i64, i64>, i64>::new_batch(
                state,
                table.clone(),
                Arc::new(|rows: &[(WindowCountInput<i64, i64>, i64)]| {
                    rows.iter()
                        .map(|(row, weight)| {
                            (
                                CountAggregateRow {
                                    key: row.window_key.clone(),
                                    slots: vec![CountAggregateSlotUpdate::<i64>::Linear(1)],
                                },
                                *weight,
                            )
                        })
                        .collect()
                }),
                output,
                vec![CountAggregateSlotKind::Linear],
                None,
            );
        let mut op = WindowCountAggregateOp {
            table: table.clone(),
            dict_cache: HashMap::new(),
            window_extractor: Arc::new(|rows: &[(i64, i64)]| {
                rows.iter()
                    .map(|(row, weight)| (*row, *weight, 0_i64, *row))
                    .collect()
            }),
            count_op,
            watermark: Arc::clone(&watermark),
            window_size: 1_000,
            window_slide: 1_000,
            allowed_lateness_ms: 0,
        };

        let input_handle = stage_version(
            input_dict.clone(),
            table.clone(),
            "window_count_input",
            &[(1_000_i64, 1)],
        )
        .await;

        let mut cache = HashMap::new();
        let step_one = materialize_zset_handle::<(WindowKey<i64>, Vec<i64>)>(
            table.clone(),
            &mut cache,
            &op.on_step(&[input_handle])
                .await
                .expect("run t1")
                .expect("t1 output"),
        )
        .await
        .expect("materialize t1");
        assert_eq!(
            step_one,
            HashMap::from([(
                (
                    WindowKey {
                        start: 1_000,
                        end: 2_000,
                        key: 0,
                    },
                    vec![1],
                ),
                1,
            )])
        );

        watermark.store(3_000, Ordering::Relaxed);
        let step_two = materialize_zset_handle::<(WindowKey<i64>, Vec<i64>)>(
            table.clone(),
            &mut cache,
            &op.on_step(&[ZSetHandle {
                ns: "window_count_input".to_string(),
                version: 0,
            }])
            .await
            .expect("run t2")
            .expect("t2 output"),
        )
        .await
        .expect("materialize t2");
        assert_eq!(
            step_two,
            HashMap::from([(
                (
                    WindowKey {
                        start: 1_000,
                        end: 2_000,
                        key: 0,
                    },
                    vec![1],
                ),
                -1,
            )])
        );
    }
}
