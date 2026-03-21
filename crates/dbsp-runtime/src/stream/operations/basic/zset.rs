use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::runtime::HandleOperatorRuntime;
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, delta_handle_namespace, delta_zset_handle,
    next_lifted_zset_namespace, open_delta_handle_stream, publish_scheduled_value,
    push_value_in_place,
};
use slatedb::WriteBatch;

use super::super::zset_integral::integrate_zset_handle_stream;

/// Single-level helper: integrates a `Stream<ZSetHandle>` into cumulative state,
/// returning another `Stream<ZSetHandle>` that carries integrated deltas only.
pub async fn integrate_zset_stream<K>(input: &Stream<ZSetHandle>) -> Result<Stream<ZSetHandle>>
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
{
    integrate_zset_handle_stream::<K>(input).await
}

/// Single-level helper: differentiates `Stream<ZSetHandle>` by emitting the
/// per-step Z-set deltas in a new `Stream<ZSetHandle>`.
pub async fn differentiate_zset_stream<K>(input: &Stream<ZSetHandle>) -> Result<Stream<ZSetHandle>>
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
{
    let delta_input = open_delta_handle_stream(input).await?;
    let frontier = delta_input.current_time();
    let horizon = delta_input.semantic_horizon();
    let handles = collect_values(&delta_input, horizon).await?;
    let table = input.table();
    let namespace = next_lifted_zset_namespace("stream_diff_zset/");
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for differentiate_zset_stream")?,
    );
    let mut versioned = VersionedZSet::new(dict, table.clone(), namespace.clone())
        .await
        .context("create versioned zset for diff stream")?;

    let mut cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut output_handles = Vec::with_capacity(handles.len());

    for handle in handles {
        let deltas = delta_zset_handle::<K>(table.clone(), &mut cache, &handle)
            .await
            .context("delta iterate zset handle for diff")?;
        let next_handle = if deltas.is_empty() {
            versioned.handle_for_version(0)
        } else {
            apply_delta_version(&mut versioned, deltas)
                .await
                .context("persist diff zset version update")?
        };

        output_handles.push(next_handle.clone());
    }

    let default_handle = versioned.handle_for_version(0);
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_exact_stream_from_values(
        table.clone(),
        handle_group,
        "stream_diff_zset_handles/",
        frontier,
        horizon,
        &output_handles,
        default_handle,
    )
    .await?;

    result_stream.flush().await?;
    Ok(result_stream)
}

/// Live variant of `differentiate_zset_stream` that converts an incoming snapshot
/// handle stream into a delta stream, driving a background runtime to process new
/// handles as they arrive.
pub async fn differentiate_zset_stream_live<K>(
    input: &Stream<ZSetHandle>,
) -> Result<Stream<ZSetHandle>>
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
{
    let table = input.table();
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    let namespace = next_lifted_zset_namespace("stream_live_diff/");
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for live differentiate_zset_stream")?,
    );
    let mut versioned = VersionedZSet::new(dict, table.clone(), namespace.clone())
        .await
        .context("create versioned zset for live diff")?;
    let delta_stream = open_delta_handle_stream(input).await?;
    let handles = collect_values(input, horizon).await?;
    let delta_handles = collect_values(&delta_stream, horizon).await?;
    let mut delta_cache = HashMap::new();
    let mut previous_snapshot: Option<ZSetHandle> = None;
    let mut output_handles = Vec::with_capacity(handles.len());

    for (index, handle) in handles.iter().enumerate() {
        let deltas = resolve_live_diff_step_deltas::<K>(
            table.clone(),
            &mut delta_cache,
            handle,
            previous_snapshot.as_ref(),
            delta_handles.get(index),
        )
        .await
        .context("resolve snapshot deltas for live diff history")?;
        previous_snapshot = Some(handle.clone());
        let out_handle = if deltas.is_empty() {
            versioned.handle_for_version(0)
        } else {
            apply_delta_version(&mut versioned, deltas)
                .await
                .context("persist live diff history version")?
        };
        output_handles.push(out_handle);
    }

    let default_handle = versioned.handle_for_version(0);
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut out_stream = build_exact_stream_from_values(
        table.clone(),
        handle_group,
        "stream_live_diff_handles/",
        frontier,
        horizon,
        &output_handles,
        default_handle,
    )
    .await?;
    out_stream.flush().await?;

    let state = Arc::new(Mutex::new(LiveDiffState {
        versioned,
        delta_stream,
        delta_cache,
        previous_snapshot,
    }));

    let writer = Arc::new(Mutex::new(out_stream.clone()));
    let state_clone = Arc::clone(&state);
    let scheduled_horizon = horizon;
    let mut runtime = HandleOperatorRuntime::new(vec![input.clone()], move |ts, handles| {
        let state = Arc::clone(&state_clone);
        let writer = Arc::clone(&writer);
        let snapshot_handle = handles[0].clone();
        Box::pin(async move {
            if ts <= scheduled_horizon {
                let mut writer_guard = writer.lock().await;
                publish_scheduled_handle(&mut writer_guard, ts).await?;
                return Ok(());
            }

            let mut guard = state.lock().await;
            let previous_snapshot = guard.previous_snapshot.clone();
            let delta_handle = guard
                .delta_stream
                .get(ts)
                .await
                .context("load live diff delta handle")?;
            let deltas = resolve_live_diff_step_deltas::<K>(
                guard.versioned.table(),
                &mut guard.delta_cache,
                &snapshot_handle,
                previous_snapshot.as_ref(),
                Some(&delta_handle),
            )
            .await
            .context("delta iterate input handle for live diff")?;
            guard.previous_snapshot = Some(snapshot_handle.clone());
            let out_handle = if deltas.is_empty() {
                guard.versioned.handle_for_version(0)
            } else {
                apply_delta_version(&mut guard.versioned, deltas)
                    .await
                    .context("persist live diff version")?
            };

            let mut writer_guard = writer.lock().await;
            push_value_in_place(&mut writer_guard, out_handle);
            writer_guard.flush().await?;
            Ok(())
        })
    });

    tokio::spawn(async move {
        loop {
            if let Err(err) = runtime.step().await {
                tracing::error!(
                    error = %err,
                    "live differentiate_zset_stream runtime terminated"
                );
                break;
            }
        }
    });

    Ok(out_stream)
}

struct LiveDiffState<K>
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
{
    versioned: VersionedZSet<K>,
    delta_stream: Stream<ZSetHandle>,
    delta_cache: HashMap<String, Arc<Dictionary<K>>>,
    previous_snapshot: Option<ZSetHandle>,
}

async fn resolve_live_diff_step_deltas<K>(
    table: Arc<dyn crate::storage::KeyValueTable>,
    cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    snapshot_handle: &ZSetHandle,
    previous_snapshot: Option<&ZSetHandle>,
    candidate_delta: Option<&ZSetHandle>,
) -> Result<Vec<(K, i64)>>
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
{
    if previous_snapshot == Some(snapshot_handle) {
        return Ok(Vec::new());
    }

    let expected_ns = delta_handle_namespace(&snapshot_handle.ns);
    if let Some(candidate) = candidate_delta
        && candidate.ns == expected_ns
    {
        return delta_zset_handle::<K>(table, cache, candidate)
            .await
            .context("read candidate live diff delta handle");
    }

    let fallback = ZSetHandle {
        ns: expected_ns,
        version: snapshot_handle.version,
    };
    match delta_zset_handle::<K>(table, cache, &fallback).await {
        Ok(deltas) => Ok(deltas),
        Err(err) if is_missing_manifest(&err) => Ok(Vec::new()),
        Err(err) => Err(err).context("read fallback live diff delta handle"),
    }
}

async fn publish_scheduled_handle(stream: &mut Stream<ZSetHandle>, ts: i64) -> Result<()> {
    publish_scheduled_value(stream, ts)
        .await
        .with_context(|| format!("publish scheduled live diff handle at {ts}"))
}

fn is_missing_manifest(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}");
    message.contains("manifest version")
        || message.contains("not found for namespace")
        || message.contains("not found")
}

async fn apply_delta_version<K>(
    versioned: &mut VersionedZSet<K>,
    deltas: Vec<(K, i64)>,
) -> Result<ZSetHandle>
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
{
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let dict = versioned.dictionary();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        if delta == 0 {
            continue;
        }
        let id = dict_batch
            .intern(&key)
            .await
            .context("intern key while staging live diff delta")?;
        buckets.entry(bucket_for(id)).or_default().push((id, delta));
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

    if segments.is_empty() {
        return Ok(versioned
            .current_handle()
            .unwrap_or_else(|| versioned.handle_for_version(0)));
    }

    let mut batch = WriteBatch::new();
    let plan = versioned
        .enqueue_version_with_base(segments, None, 0, &mut batch)
        .await
        .context("enqueue live diff version")?;
    versioned
        .table()
        .write_batch(batch)
        .await
        .context("write live diff version")?;

    versioned.apply_version_plan(&plan);
    Ok(versioned.handle_for_version(plan.version))
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
