use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::Context;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::runtime::{HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, delta_zset_handle_batch,
    publish_scheduled_value, publish_transient_zset_batch, push_value_in_place,
};

type FilterMapTransform<K, R> = Arc<dyn Fn(&K) -> Option<R> + Send + Sync>;
type FilterMapBatchTransform<K, R> =
    Arc<dyn Fn(&[(K, i64)]) -> anyhow::Result<Vec<(R, i64)>> + Send + Sync>;

/// Filter+map wrapper that evaluates a fused row transform over handle streams.
/// The transform returns `None` to drop a row or `Some(mapped_key)` to emit it.
pub struct DbspFilterMap {
    stream: DeltaHandleStream,
}

impl DbspFilterMap {
    pub async fn new<K, R, F>(
        input: &DeltaHandleStream,
        transform: F,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        F: Fn(&K) -> Option<R> + Send + Sync + Clone + 'static,
    {
        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let op_id = NEXT_FILTER_MAP_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("filter_map_output_{op_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let output_dict = Arc::new(
            Dictionary::<R>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for filter_map")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for filter_map")?;
        let state = Arc::new(AsyncMutex::new(FilterMapState {
            transform: Arc::new(transform),
            table: table.clone(),
            output,
            dict_cache: HashMap::new(),
            logical_work: metrics::LogicalWorkCollector::default(),
        }));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });
        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut guard = state.lock().await;
                guard.on_step(ts as i64, &handle).await?
            };
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "filter_map_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let state = Arc::clone(&state);
            let writer = Arc::clone(&writer);
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "filter_map runtime expected 1 handle, got {}",
                        handles_vec.len()
                    ));
                }
                if ts <= horizon {
                    let mut writer_guard = writer.lock().await;
                    publish_scheduled_value(&mut writer_guard, ts).await?;
                    return Ok(());
                }
                let mut state_guard = state.lock().await;
                let out_handle = state_guard.on_step(ts, &handles_vec[0]).await?;
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
                    report_runtime_error(&error_handler, "filter_map", err);
                    break;
                }
            }
        });

        Ok(Self {
            stream: DeltaHandleStream::new(stream),
        })
    }

    /// Batch variant of filter+map that receives the whole input delta for a tick.
    ///
    /// This allows callers to apply vectorized transforms over Arrow batches instead of
    /// invoking a row closure per input key.
    pub async fn new_batch<K, R, F>(
        input: &DeltaHandleStream,
        transform: F,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        F: Fn(&[(K, i64)]) -> anyhow::Result<Vec<(R, i64)>> + Send + Sync + Clone + 'static,
    {
        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let op_id = NEXT_FILTER_MAP_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("filter_map_output_{op_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let output_dict = Arc::new(
            Dictionary::<R>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for filter_map batch")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for filter_map batch")?;
        let state = Arc::new(AsyncMutex::new(FilterMapBatchState {
            transform: Arc::new(transform),
            table: table.clone(),
            output,
            dict_cache: HashMap::new(),
            logical_work: metrics::LogicalWorkCollector::default(),
        }));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });
        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut guard = state.lock().await;
                guard.on_step(ts as i64, &handle).await?
            };
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "filter_map_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let state = Arc::clone(&state);
            let writer = Arc::clone(&writer);
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "filter_map runtime expected 1 handle, got {}",
                        handles_vec.len()
                    ));
                }
                if ts <= horizon {
                    let mut writer_guard = writer.lock().await;
                    publish_scheduled_value(&mut writer_guard, ts).await?;
                    return Ok(());
                }
                let mut state_guard = state.lock().await;
                let out_handle = state_guard.on_step(ts, &handles_vec[0]).await?;
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
                    report_runtime_error(&error_handler, "filter_map_batch", err);
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

struct FilterMapState<K, R>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    transform: FilterMapTransform<K, R>,
    table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<R>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K, R> FilterMapState<K, R>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    #[cfg(test)]
    fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn on_step(&mut self, ts: i64, input_handle: &ZSetHandle) -> anyhow::Result<ZSetHandle> {
        let total_start = Instant::now();
        let load_start = Instant::now();
        let delta_values =
            delta_zset_handle_batch::<K>(self.table.clone(), &mut self.dict_cache, input_handle)
                .await
                .context("load input delta for filter_map")?;
        let input_delta_rows = delta_values.len();
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(input_delta_rows);
        let load_ms = load_start.elapsed().as_millis() as u64;

        let transform_start = Instant::now();
        let mut projected: HashMap<R, i64> = HashMap::new();
        for (key, weight) in delta_values.iter() {
            let Some(out_key) = (self.transform)(key) else {
                continue;
            };
            let entry = projected.entry(out_key.clone()).or_insert(0);
            *entry += *weight;
            if *entry == 0 {
                projected.remove(&out_key);
            }
        }
        let transform_ms = transform_start.elapsed().as_millis() as u64;
        let output_delta_rows = projected.len();

        if projected.is_empty() {
            tracing::debug!(
                ts,
                input_ns = %input_handle.ns,
                input_version = input_handle.version,
                input_delta_rows,
                output_delta_rows,
                load_ms,
                transform_ms,
                total_ms = total_start.elapsed().as_millis() as u64,
                "filter_map operator timing (no output)"
            );
            self.logical_work.finish_tick(work);
            return Ok(self.output.handle_for_version(0));
        }
        work.record_output_delta_rows(output_delta_rows);
        work.record_persisted_rows(output_delta_rows);

        let output_apply_start = Instant::now();
        let projected = projected.into_iter().collect::<Vec<_>>();
        let output_handle = apply_deltas_to_versioned(&mut self.output, &projected)
            .await
            .context("persist filter_map delta output")?;
        publish_transient_zset_batch(&output_handle, Arc::new(projected));
        let output_apply_ms = output_apply_start.elapsed().as_millis() as u64;
        tracing::debug!(
            ts,
            input_ns = %input_handle.ns,
            input_version = input_handle.version,
            input_delta_rows,
            output_delta_rows,
            output_ns = %self.output.namespace(),
            output_version = output_handle.version,
            load_ms,
            transform_ms,
            output_apply_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "filter_map operator timing"
        );
        self.logical_work.finish_tick(work);
        Ok(output_handle)
    }
}

struct FilterMapBatchState<K, R>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    transform: FilterMapBatchTransform<K, R>,
    table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<R>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K, R> FilterMapBatchState<K, R>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    #[cfg(test)]
    fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn on_step(&mut self, ts: i64, input_handle: &ZSetHandle) -> anyhow::Result<ZSetHandle> {
        let total_start = Instant::now();
        let load_start = Instant::now();
        let delta_values =
            delta_zset_handle_batch::<K>(self.table.clone(), &mut self.dict_cache, input_handle)
                .await
                .context("load input delta for filter_map batch")?;
        let input_delta_rows = delta_values.len();
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(input_delta_rows);
        let load_ms = load_start.elapsed().as_millis() as u64;

        let transform_start = Instant::now();
        let projected =
            (self.transform)(delta_values.as_ref()).context("run filter_map batch transform")?;
        let transform_ms = transform_start.elapsed().as_millis() as u64;
        let output_delta_rows = projected.len();

        if projected.is_empty() {
            tracing::debug!(
                ts,
                input_ns = %input_handle.ns,
                input_version = input_handle.version,
                input_delta_rows,
                output_delta_rows,
                load_ms,
                transform_ms,
                total_ms = total_start.elapsed().as_millis() as u64,
                "filter_map_batch operator timing (no output)"
            );
            self.logical_work.finish_tick(work);
            return Ok(self.output.handle_for_version(0));
        }
        work.record_output_delta_rows(output_delta_rows);
        work.record_persisted_rows(output_delta_rows);

        let output_apply_start = Instant::now();
        let output_handle = apply_deltas_to_versioned(&mut self.output, &projected)
            .await
            .context("persist filter_map batch delta output")?;
        publish_transient_zset_batch(&output_handle, Arc::new(projected));
        let output_apply_ms = output_apply_start.elapsed().as_millis() as u64;
        tracing::debug!(
            ts,
            input_ns = %input_handle.ns,
            input_version = input_handle.version,
            input_delta_rows,
            output_delta_rows,
            output_ns = %self.output.namespace(),
            output_version = output_handle.version,
            load_ms,
            transform_ms,
            output_apply_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "filter_map_batch operator timing"
        );
        self.logical_work.finish_tick(work);
        Ok(output_handle)
    }
}

async fn apply_deltas_to_versioned<R>(
    versioned: &mut VersionedZSet<R>,
    deltas: &[(R, i64)],
) -> anyhow::Result<ZSetHandle>
where
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let total_start = Instant::now();
    let input_rows = deltas.len();
    let stage_start = Instant::now();
    let staged: Vec<(R, i64)> = deltas
        .iter()
        .filter(|(_, delta)| *delta != 0)
        .map(|(row, delta)| (row.clone(), *delta))
        .collect();
    let stage_ms = stage_start.elapsed().as_millis() as u64;
    if staged.is_empty() {
        tracing::debug!(
            namespace = %versioned.namespace(),
            input_rows,
            staged_rows = 0,
            stage_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "filter_map apply_deltas_to_versioned breakdown (empty staged)"
        );
        return Ok(versioned
            .current_handle()
            .unwrap_or_else(|| versioned.handle_for_version(0)));
    }

    if versioned.uses_replayable_persistence() {
        let batch = Arc::new(staged);
        let handle = versioned.publish_replayable_batch(Arc::clone(&batch));
        tracing::debug!(
            namespace = %versioned.namespace(),
            input_rows,
            staged_rows = batch.len(),
            stage_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "filter_map apply_deltas_to_versioned breakdown (replayable)"
        );
        return Ok(handle);
    }

    let split_start = Instant::now();
    let mut keys = Vec::with_capacity(staged.len());
    let mut weights = Vec::with_capacity(staged.len());
    for (key, delta) in staged {
        keys.push(key);
        weights.push(delta);
    }
    let split_ms = split_start.elapsed().as_millis() as u64;

    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let dict = versioned.dictionary();
    let intern_start = Instant::now();
    let ids = dict
        .intern_many_values_unique_owned(keys)
        .await
        .context("intern keys while staging filter_map delta")?;
    let intern_ms = intern_start.elapsed().as_millis() as u64;

    let bucketize_start = Instant::now();
    for (delta, id) in weights.into_iter().zip(ids.into_iter()) {
        buckets.entry(bucket_for(id)).or_default().push((id, delta));
    }
    let bucketize_ms = bucketize_start.elapsed().as_millis() as u64;

    let segment_build_start = Instant::now();
    let mut segments = Vec::new();
    let mut segment_rows = 0usize;
    for (bucket, mut bucket_deltas) in buckets {
        bucket_deltas.retain(|(_, delta)| *delta != 0);
        if bucket_deltas.is_empty() {
            continue;
        }
        bucket_deltas.sort_by_key(|(id, _)| *id);
        segment_rows += bucket_deltas.len();
        segments.push(SegmentRecord {
            id: 0,
            bucket,
            deltas: bucket_deltas,
        });
    }
    let segment_build_ms = segment_build_start.elapsed().as_millis() as u64;

    let mut batch = WriteBatch::new();
    let enqueue_start = Instant::now();
    let plan = versioned
        .enqueue_version_with_base(segments, None, 0, &mut batch)
        .await
        .context("schedule filter_map version update")?;
    let enqueue_ms = enqueue_start.elapsed().as_millis() as u64;

    let write_start = Instant::now();
    versioned
        .table()
        .write_batch(batch)
        .await
        .context("write filter_map version update")?;
    let write_ms = write_start.elapsed().as_millis() as u64;

    let apply_plan_start = Instant::now();
    versioned.apply_version_plan(&plan);
    let apply_plan_ms = apply_plan_start.elapsed().as_millis() as u64;

    tracing::debug!(
        namespace = %versioned.namespace(),
        input_rows,
        staged_rows = segment_rows,
        segment_count = plan.manifest.buckets.len(),
        stage_ms,
        split_ms,
        intern_ms,
        bucketize_ms,
        segment_build_ms,
        enqueue_ms,
        write_ms,
        apply_plan_ms,
        total_ms = total_start.elapsed().as_millis() as u64,
        "filter_map apply_deltas_to_versioned breakdown"
    );

    Ok(versioned.handle_for_version(plan.version))
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

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

static NEXT_FILTER_MAP_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::util::{
        delta_zset_handle_batch, materialize_zset_handle, publish_transient_zset_batch,
    };
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::sync::atomic::{AtomicU64, Ordering};

    static FILTER_MAP_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn next_suffix() -> u64 {
        FILTER_MAP_TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    async fn build_db(suffix: u64) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open(format!("filter_map_test_{suffix}"), store)
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
            + std::hash::Hash
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
                .expect("intern key for filter_map test");
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
            .expect("build versioned zset");
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create version");
        versioned.handle_for_version(version)
    }

    fn coalesce_rows(rows: &[(i64, i64)]) -> HashMap<i64, i64> {
        let mut out = HashMap::new();
        for (key, weight) in rows {
            let entry = out.entry(*key).or_insert(0);
            *entry += *weight;
            if *entry == 0 {
                out.remove(key);
            }
        }
        out
    }

    #[tokio::test]
    async fn zset_handle_group_and_bucket_helpers_work() {
        let default = ZSetHandle {
            ns: "group-default".to_string(),
            version: 7,
        };
        let group = ZSetHandleGroup {
            default: default.clone(),
        };

        let a = ZSetHandle {
            ns: "a".to_string(),
            version: 1,
        };
        let b = ZSetHandle {
            ns: "b".to_string(),
            version: 2,
        };

        assert_eq!(group.add(&a, &b).await, a);
        assert_eq!(group.neg(&b).await, b);
        assert_eq!(group.identity().await, default);

        assert_eq!(bucket_for(0), 0);
        assert_eq!(bucket_for(1 << 48), 1);
        assert_eq!(bucket_for(u64::MAX), u16::MAX);
    }

    #[tokio::test]
    async fn apply_deltas_to_versioned_covers_persistent_replayable_and_empty_paths() {
        let suffix = next_suffix();
        let db = build_db(suffix).await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));

        let ns = format!("apply_versioned_{suffix}");
        let dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), ns.clone(), None)
                .await
                .expect("dict"),
        );
        let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), ns.clone())
            .await
            .expect("zset");

        let handle1 = apply_deltas_to_versioned(&mut versioned, &[(1, 1), (2, 2), (1, -1), (3, 0)])
            .await
            .expect("apply persistent deltas");
        let mut cache = HashMap::new();
        let materialized1 = materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle1)
            .await
            .expect("materialize handle1");
        assert_eq!(materialized1, HashMap::from([(2_i64, 2_i64)]));

        let handle2 = apply_deltas_to_versioned(&mut versioned, &[(2, -2), (5, 0)])
            .await
            .expect("apply second persistent deltas");
        let materialized2 = materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle2)
            .await
            .expect("materialize handle2");
        assert_eq!(materialized2, HashMap::from([(2_i64, -2_i64)]));

        let handle3 = apply_deltas_to_versioned(&mut versioned, &[(9, 0)])
            .await
            .expect("apply empty staged deltas");
        assert_eq!(handle3.version, handle2.version);

        let replay_ns = format!("apply_versioned_replay_{suffix}");
        let replay_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), replay_ns.clone(), None)
                .await
                .expect("replay dict"),
        );
        let mut replay = VersionedZSet::new(replay_dict, table.clone(), replay_ns)
            .await
            .expect("replay zset");
        replay.enable_replayable_persistence();
        let replay_handle = apply_deltas_to_versioned(&mut replay, &[(11, 3), (12, -1)])
            .await
            .expect("apply replayable deltas");
        let replay_rows =
            delta_zset_handle_batch::<i64>(table, &mut HashMap::new(), &replay_handle)
                .await
                .expect("delta replay rows");
        assert_eq!(
            coalesce_rows(replay_rows.as_ref()),
            HashMap::from([(11_i64, 3_i64), (12_i64, -1_i64)])
        );
    }

    #[tokio::test]
    async fn filter_map_state_on_step_projects_filters_and_persists_output() {
        let suffix = next_suffix();
        let db = build_db(suffix).await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));

        let input_ns = format!("filter_map_state_input_{suffix}");
        let output_ns = format!("filter_map_state_output_{suffix}");
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .expect("output dict"),
        );
        let output = VersionedZSet::new(output_dict.clone(), table.clone(), output_ns.clone())
            .await
            .expect("output zset");

        let mut state = FilterMapState {
            transform: Arc::new(|value: &i64| (value % 2 == 0).then_some(value * 10)),
            table: table.clone(),
            output,
            dict_cache: HashMap::new(),
            logical_work: metrics::LogicalWorkCollector::default(),
        };

        let input_h1 = stage_version(
            input_dict.clone(),
            table.clone(),
            input_ns.as_str(),
            &[(1, 1), (2, 3), (4, -1)],
        )
        .await;
        let out_h1 = state.on_step(1, &input_h1).await.expect("state step 1");
        let out_rows_1 =
            delta_zset_handle_batch::<i64>(table.clone(), &mut HashMap::new(), &out_h1)
                .await
                .expect("output rows 1");
        assert_eq!(
            coalesce_rows(out_rows_1.as_ref()),
            HashMap::from([(20_i64, 3_i64), (40_i64, -1_i64)])
        );
        let work1 = state.last_logical_work();
        assert_eq!(work1.input_delta_rows, 3);
        assert_eq!(work1.output_delta_rows, 2);
        assert_eq!(work1.persisted_rows, 2);

        let input_h2 = stage_version(
            input_dict.clone(),
            table.clone(),
            input_ns.as_str(),
            &[(1, 1), (3, -2)],
        )
        .await;
        let out_h2 = state.on_step(2, &input_h2).await.expect("state step 2");
        assert_eq!(out_h2.version, 0);
        let work2 = state.last_logical_work();
        assert_eq!(work2.input_delta_rows, 2);
        assert_eq!(work2.output_delta_rows, 0);
        assert_eq!(work2.state_full_scan_count, 0);

        let input_h3 = stage_version(input_dict, table.clone(), input_ns.as_str(), &[(6, 2)]).await;
        let out_h3 = state.on_step(3, &input_h3).await.expect("state step 3");
        assert!(out_h3.version > 0);
        for version in 1..=600 {
            publish_transient_zset_batch(
                &ZSetHandle {
                    ns: format!("evict_filter_map_state_{suffix}_{version}"),
                    version,
                },
                Arc::new(vec![(version as i64, 1)]),
            );
        }
        let out_rows_3 = delta_zset_handle_batch::<i64>(table, &mut HashMap::new(), &out_h3)
            .await
            .expect("output rows 3 after transient registry churn");
        assert_eq!(
            coalesce_rows(out_rows_3.as_ref()),
            HashMap::from([(60_i64, 2_i64)])
        );
    }

    #[tokio::test]
    async fn filter_map_batch_state_on_step_handles_success_empty_and_error_paths() {
        let suffix = next_suffix();
        let db = build_db(suffix).await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));

        let input_ns = format!("filter_map_batch_input_{suffix}");
        let output_ns = format!("filter_map_batch_output_{suffix}");
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .expect("output dict"),
        );
        let output = VersionedZSet::new(output_dict.clone(), table.clone(), output_ns.clone())
            .await
            .expect("output zset");

        let mut state = FilterMapBatchState {
            transform: Arc::new(|rows: &[(i64, i64)]| {
                Ok(rows
                    .iter()
                    .filter_map(|(value, weight)| {
                        (value % 2 != 0).then_some((value * 100, *weight))
                    })
                    .collect::<Vec<_>>())
            }),
            table: table.clone(),
            output,
            dict_cache: HashMap::new(),
            logical_work: metrics::LogicalWorkCollector::default(),
        };

        let input_h1 = stage_version(
            input_dict.clone(),
            table.clone(),
            input_ns.as_str(),
            &[(1, 2), (2, 1), (3, -1)],
        )
        .await;
        let out_h1 = state
            .on_step(1, &input_h1)
            .await
            .expect("batch state step 1");
        let out_rows_1 =
            delta_zset_handle_batch::<i64>(table.clone(), &mut HashMap::new(), &out_h1)
                .await
                .expect("batch output rows 1");
        assert_eq!(
            coalesce_rows(out_rows_1.as_ref()),
            HashMap::from([(100_i64, 2_i64), (300_i64, -1_i64)])
        );
        let work1 = state.last_logical_work();
        assert_eq!(work1.input_delta_rows, 3);
        assert_eq!(work1.output_delta_rows, 2);
        assert_eq!(work1.persisted_rows, 2);

        let input_h2 = stage_version(
            input_dict.clone(),
            table.clone(),
            input_ns.as_str(),
            &[(2, 5), (4, 1)],
        )
        .await;
        let out_h2 = state
            .on_step(2, &input_h2)
            .await
            .expect("batch state step 2");
        assert_eq!(out_h2.version, 0);
        let work2 = state.last_logical_work();
        assert_eq!(work2.input_delta_rows, 2);
        assert_eq!(work2.output_delta_rows, 0);
        assert_eq!(work2.state_full_scan_count, 0);

        let input_h3 = stage_version(
            input_dict.clone(),
            table.clone(),
            input_ns.as_str(),
            &[(5, 3)],
        )
        .await;
        let out_h3 = state
            .on_step(3, &input_h3)
            .await
            .expect("batch state step 3");
        assert!(out_h3.version > 0);
        for version in 1..=600 {
            publish_transient_zset_batch(
                &ZSetHandle {
                    ns: format!("evict_filter_map_batch_{suffix}_{version}"),
                    version,
                },
                Arc::new(vec![(version as i64, 1)]),
            );
        }
        let out_rows_3 =
            delta_zset_handle_batch::<i64>(table.clone(), &mut HashMap::new(), &out_h3)
                .await
                .expect("batch output rows 3 after transient registry churn");
        assert_eq!(
            coalesce_rows(out_rows_3.as_ref()),
            HashMap::from([(500, 3)])
        );

        let output_err_ns = format!("filter_map_batch_output_err_{suffix}");
        let output_err_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), output_err_ns.clone(), None)
                .await
                .expect("output err dict"),
        );
        let output_err = VersionedZSet::new(output_err_dict, table.clone(), output_err_ns)
            .await
            .expect("output err zset");
        let mut err_state = FilterMapBatchState {
            transform: Arc::new(|rows: &[(i64, i64)]| {
                if rows.iter().any(|(value, _)| *value < 0) {
                    anyhow::bail!("negative keys not allowed")
                }
                Ok(Vec::new())
            }),
            table: table.clone(),
            output: output_err,
            dict_cache: HashMap::new(),
            logical_work: metrics::LogicalWorkCollector::default(),
        };

        let input_h_err = stage_version(input_dict, table, input_ns.as_str(), &[(-1, 1)]).await;
        let err = err_state
            .on_step(4, &input_h_err)
            .await
            .expect_err("batch transform should fail");
        assert!(err.to_string().contains("run filter_map batch transform"));
    }

    async fn run_filter_map_batch_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
        let suffix = next_suffix();
        let db = build_db(suffix).await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));

        let input_ns = format!("filter_map_batch_history_input_{history_rows}_{suffix}");
        let output_ns = format!("filter_map_batch_history_output_{history_rows}_{suffix}");
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
                .await
                .expect("input dict"),
        );
        let output = VersionedZSet::new(
            Arc::new(
                Dictionary::<i64>::with_table(table.clone(), output_ns.clone(), None)
                    .await
                    .expect("output dict"),
            ),
            table.clone(),
            output_ns,
        )
        .await
        .expect("output zset");
        let mut state = FilterMapBatchState {
            transform: Arc::new(|rows: &[(i64, i64)]| {
                Ok(rows
                    .iter()
                    .filter_map(|(value, weight)| (value % 2 == 0).then_some((value * 10, *weight)))
                    .collect::<Vec<_>>())
            }),
            table: table.clone(),
            output,
            dict_cache: HashMap::new(),
            logical_work: metrics::LogicalWorkCollector::default(),
        };

        let history = (0..history_rows)
            .map(|idx| (1_000_000 + idx * 2, 1))
            .collect::<Vec<_>>();
        let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
        state.on_step(1, &seed).await.expect("seed filter_map");

        let fixed = stage_version(input_dict, table.clone(), &input_ns, &[(8, 1)]).await;
        let output = state.on_step(2, &fixed).await.expect("fixed filter_map");
        let materialized = delta_zset_handle_batch::<i64>(table, &mut HashMap::new(), &output)
            .await
            .expect("materialize fixed filter_map");
        assert_eq!(
            coalesce_rows(materialized.as_ref()),
            HashMap::from([(80, 1)])
        );

        state.last_logical_work()
    }

    #[tokio::test]
    async fn filter_map_batch_logical_work_is_delta_local() {
        let baseline = run_filter_map_batch_history_probe(8).await;
        for history_rows in [128, 1024] {
            let actual = run_filter_map_batch_history_probe(history_rows).await;
            assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
            assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
            assert_eq!(actual.persisted_rows, baseline.persisted_rows);
            assert_eq!(actual.state_full_scan_count, 0);
            assert_eq!(actual.cache_rebuild_rows, 0);
        }

        assert_eq!(baseline.input_delta_rows, 1);
        assert_eq!(baseline.output_delta_rows, 1);
        assert_eq!(baseline.persisted_rows, 1);
    }
}
