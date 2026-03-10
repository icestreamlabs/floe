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
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::runtime::{HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error};
use crate::stream::util::{
    build_derived_stream, collect_values, delta_zset_handle, push_value_in_place,
    set_default_in_place,
};

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
        let op_id = NEXT_FILTER_MAP_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("filter_map_output_{op_id}");

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
        }));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: ZSetHandle {
                ns: output_ns.clone(),
                version: 0,
            },
        });
        let mut stream =
            build_derived_stream(table.clone(), handle_group, "filter_map_output_stream/").await?;
        set_default_in_place(
            &mut stream,
            ZSetHandle {
                ns: output_ns,
                version: 0,
            },
        );

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        // Seed downstream with any already-materialized input handles.
        let history = collect_values(input, input.current_time()).await?;
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut guard = state.lock().await;
                guard.on_step(ts as i64, &handle).await?
            };
            if let Some(out_handle) = out_handle {
                let mut writer_guard = writer.lock().await;
                push_value_in_place(&mut writer_guard, out_handle);
                writer_guard.flush().await?;
            }
        }

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
                let mut state_guard = state.lock().await;
                if let Some(out_handle) = state_guard.on_step(ts, &handles_vec[0]).await? {
                    let mut writer_guard = writer.lock().await;
                    push_value_in_place(&mut writer_guard, out_handle);
                    writer_guard.flush().await?;
                }
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

        stream.flush().await?;
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
        F: Fn(Vec<(K, i64)>) -> anyhow::Result<HashMap<R, i64>> + Send + Sync + Clone + 'static,
    {
        let table = input.table();
        let op_id = NEXT_FILTER_MAP_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("filter_map_output_{op_id}");

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
        }));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: ZSetHandle {
                ns: output_ns.clone(),
                version: 0,
            },
        });
        let mut stream =
            build_derived_stream(table.clone(), handle_group, "filter_map_output_stream/").await?;
        set_default_in_place(
            &mut stream,
            ZSetHandle {
                ns: output_ns,
                version: 0,
            },
        );

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        // Seed downstream with any already-materialized input handles.
        let history = collect_values(input, input.current_time()).await?;
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut guard = state.lock().await;
                guard.on_step(ts as i64, &handle).await?
            };
            if let Some(out_handle) = out_handle {
                let mut writer_guard = writer.lock().await;
                push_value_in_place(&mut writer_guard, out_handle);
                writer_guard.flush().await?;
            }
        }

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
                let mut state_guard = state.lock().await;
                if let Some(out_handle) = state_guard.on_step(ts, &handles_vec[0]).await? {
                    let mut writer_guard = writer.lock().await;
                    push_value_in_place(&mut writer_guard, out_handle);
                    writer_guard.flush().await?;
                }
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

        stream.flush().await?;
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
    transform: Arc<dyn Fn(&K) -> Option<R> + Send + Sync>,
    table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<R>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
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
    async fn on_step(
        &mut self,
        ts: i64,
        input_handle: &ZSetHandle,
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let total_start = Instant::now();
        let load_start = Instant::now();
        let delta_values =
            delta_zset_handle::<K>(self.table.clone(), &mut self.dict_cache, input_handle)
                .await
                .context("load input delta for filter_map")?;
        let input_delta_rows = delta_values.len();
        let load_ms = load_start.elapsed().as_millis() as u64;

        let transform_start = Instant::now();
        let mut projected: HashMap<R, i64> = HashMap::new();
        for (key, weight) in delta_values {
            let Some(out_key) = (self.transform)(&key) else {
                continue;
            };
            let entry = projected.entry(out_key.clone()).or_insert(0);
            *entry += weight;
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
            return Ok(None);
        }

        let output_apply_start = Instant::now();
        let output_handle = apply_deltas_to_versioned(&mut self.output, &projected)
            .await
            .context("persist filter_map delta output")?;
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
        Ok(Some(output_handle))
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
    transform: Arc<dyn Fn(Vec<(K, i64)>) -> anyhow::Result<HashMap<R, i64>> + Send + Sync>,
    table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<R>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
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
    async fn on_step(
        &mut self,
        ts: i64,
        input_handle: &ZSetHandle,
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let total_start = Instant::now();
        let load_start = Instant::now();
        let delta_values =
            delta_zset_handle::<K>(self.table.clone(), &mut self.dict_cache, input_handle)
                .await
                .context("load input delta for filter_map batch")?;
        let input_delta_rows = delta_values.len();
        let load_ms = load_start.elapsed().as_millis() as u64;

        let transform_start = Instant::now();
        let projected = (self.transform)(delta_values).context("run filter_map batch transform")?;
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
            return Ok(None);
        }

        let output_apply_start = Instant::now();
        let output_handle = apply_deltas_to_versioned(&mut self.output, &projected)
            .await
            .context("persist filter_map batch delta output")?;
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
        Ok(Some(output_handle))
    }
}

async fn apply_deltas_to_versioned<R>(
    versioned: &mut VersionedZSet<R>,
    deltas: &HashMap<R, i64>,
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
    let staged: Vec<(&R, i64)> = deltas
        .iter()
        .filter_map(|(key, delta)| (*delta != 0).then_some((key, *delta)))
        .collect();
    if staged.is_empty() {
        return Ok(versioned
            .current_handle()
            .unwrap_or_else(|| versioned.handle_for_version(0)));
    }

    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let dict = versioned.dictionary();
    let ids = dict
        .intern_many_values_unique(staged.iter().map(|(key, _)| *key))
        .await
        .context("intern keys while staging filter_map delta")?;
    for ((_, delta), id) in staged.iter().zip(ids.into_iter()) {
        buckets
            .entry(bucket_for(id))
            .or_default()
            .push((id, *delta));
    }

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

    let mut batch = WriteBatch::new();
    let plan = versioned
        .enqueue_version_with_base(segments, None, 0, &mut batch)
        .await
        .context("schedule filter_map version update")?;
    versioned
        .table()
        .write_batch(batch)
        .await
        .context("write filter_map version update")?;

    versioned.apply_version_plan(&plan);
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
