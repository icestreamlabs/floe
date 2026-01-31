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
    build_derived_stream, collect_values, compute_delta, materialize_zset_handle,
    next_lifted_zset_namespace, push_value_in_place, set_default_in_place,
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
    let handles = collect_values(input, input.current_time()).await?;
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

    let mut cache = HashMap::new();
    let mut previous = HashMap::new();
    let mut output_handles = Vec::with_capacity(handles.len());

    for handle in handles {
        let current = materialize_zset_handle::<K>(table.clone(), &mut cache, &handle)
            .await
            .context("materialize zset handle for diff")?;
        let deltas = compute_delta(&previous, &current);
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
                .context("intern key while staging diff delta")?;
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

        let next_handle = if segments.is_empty() {
            versioned
                .current_handle()
                .unwrap_or_else(|| versioned.handle_for_version(0))
        } else {
            let mut batch = WriteBatch::new();
            let plan = versioned
                .enqueue_version_with_base(segments, None, 0, &mut batch)
                .await
                .context("schedule diff zset version update")?;

            versioned
                .table()
                .write_batch(batch)
                .await
                .context("write diff zset version update")?;

            let mut cleanup = WriteBatch::new();
            cleanup.delete(versioned.intent_key_bytes());
            versioned
                .table()
                .write_batch(cleanup)
                .await
                .context("clear diff zset intent")?;

            versioned.apply_version_plan(&plan);
            versioned.handle_for_version(plan.version)
        };

        output_handles.push(next_handle.clone());
        previous = current;
    }

    let default_handle = output_handles
        .first()
        .cloned()
        .unwrap_or_else(|| versioned.handle_for_version(0));
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, "stream_diff_zset_handles/").await?;

    if output_handles.is_empty() {
        set_default_in_place(&mut result_stream, default_handle);
    } else {
        set_default_in_place(&mut result_stream, output_handles[0].clone());
        for handle in output_handles.iter().skip(1) {
            push_value_in_place(&mut result_stream, handle.clone());
        }
        if let Some(last) = output_handles.last() {
            set_default_in_place(&mut result_stream, last.clone());
        }
    }

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
    let namespace = next_lifted_zset_namespace("stream_live_diff/");
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for live differentiate_zset_stream")?,
    );
    let versioned = VersionedZSet::new(dict, table.clone(), namespace.clone())
        .await
        .context("create versioned zset for live diff")?;
    let state = Arc::new(Mutex::new(LiveDiffState {
        versioned,
        prev: HashMap::new(),
        materialize_cache: HashMap::new(),
    }));

    let mut guard = state.lock().await;
    let default_handle = guard.versioned.handle_for_version(0);
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut out_stream =
        build_derived_stream(table.clone(), handle_group, "stream_live_diff_handles/").await?;
    set_default_in_place(&mut out_stream, default_handle.clone());

    // Drain history up to current time to seed the live diff stream.
    let input_handles = collect_values(input, input.current_time()).await?;
    let mut prev = HashMap::new();
    for handle in input_handles {
        let current = materialize_zset_handle::<K>(
            guard.versioned.table(),
            &mut guard.materialize_cache,
            &handle,
        )
        .await
        .context("materialize input handle for live diff history")?;
        let deltas = compute_delta(&prev, &current);
        let out_handle = if deltas.is_empty() {
            guard.versioned.handle_for_version(0)
        } else {
            apply_delta_version(&mut guard.versioned, deltas)
                .await
                .context("persist live diff history version")?
        };
        push_value_in_place(&mut out_stream, out_handle.clone());
        prev = current;
    }
    // Keep the latest integrated state for incremental steps.
    guard.prev = prev;
    if let Some(last) = out_stream.to_vec().await?.last() {
        set_default_in_place(&mut out_stream, last.clone());
    }
    out_stream.flush().await?;
    drop(guard);

    let writer = Arc::new(Mutex::new(out_stream.clone()));
    let state_clone = Arc::clone(&state);
    let mut runtime = HandleOperatorRuntime::new(vec![input.clone()], move |_, handles| {
        let state = Arc::clone(&state_clone);
        let writer = Arc::clone(&writer);
        let handle = handles[0].clone();
        Box::pin(async move {
            let mut guard = state.lock().await;
            let current = materialize_zset_handle::<K>(
                guard.versioned.table(),
                &mut guard.materialize_cache,
                &handle,
            )
            .await
            .context("materialize input handle for live diff")?;
            let deltas = compute_delta(&guard.prev, &current);
            guard.prev = current;
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
    prev: HashMap<K, i64>,
    materialize_cache: HashMap<String, Arc<Dictionary<K>>>,
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

    let mut cleanup = WriteBatch::new();
    cleanup.delete(versioned.intent_key_bytes());
    versioned
        .table()
        .write_batch(cleanup)
        .await
        .context("clear live diff intent")?;

    versioned.apply_version_plan(&plan);
    Ok(versioned.handle_for_version(plan.version))
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
