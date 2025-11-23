use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::Mutex;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use slatedb::WriteBatch;

use super::super::addition::StreamAddition;
use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::{
    build_derived_stream, collect_values, compute_delta, materialize_zset_handle,
    next_lifted_zset_namespace, push_value_in_place, set_default_in_place,
};
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::stream::runtime::HandleOperatorRuntime;
use super::zset_integral::integrate_zset_handle_stream;

pub async fn delay<T>(input: &Stream<T>) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(input, input.current_time()).await?;
    let mut result = build_derived_stream(input.table(), input.group(), "stream_delay/").await?;

    let mut last_output = None;
    for t in 1..=input.current_time() {
        let value = values[(t - 1) as usize].clone();
        push_value_in_place(&mut result, value.clone());
        last_output = Some(value);
    }

    if let Some(last) = last_output {
        set_default_in_place(&mut result, last);
    }

    Ok(result)
}

pub async fn differentiate<T>(input: &Stream<T>) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(input, input.current_time()).await?;
    let group = input.group();
    let mut result = build_derived_stream(input.table(), group.clone(), "stream_diff/").await?;

    if let Some(first) = values.first() {
        let mut last_output = first.clone();
        set_default_in_place(&mut result, first.clone());

        for t in 1..=input.current_time() {
            let current = &values[t as usize];
            let previous = &values[(t - 1) as usize];
            let neg_prev = group.neg(previous).await;
            let diff = group.add(current, &neg_prev).await;
            last_output = diff.clone();
            push_value_in_place(&mut result, diff);
        }

        set_default_in_place(&mut result, last_output);
    }

    Ok(result)
}

pub async fn integrate<T>(input: &Stream<T>) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(input, input.current_time()).await?;
    let group = input.group();
    let mut result =
        build_derived_stream(input.table(), group.clone(), "stream_integrate/").await?;

    if let Some(first) = values.first() {
        let mut acc = first.clone();
        set_default_in_place(&mut result, acc.clone());

        for t in 1..=input.current_time() {
            let current = &values[t as usize];
            acc = group.add(&acc, current).await;
            push_value_in_place(&mut result, acc.clone());
        }

        set_default_in_place(&mut result, acc);
    }

    Ok(result)
}

/// Single-level helper: integrates a `Stream<ZSetHandle>` into cumulative state,
/// returning another `Stream<ZSetHandle>` that carries integrated deltas only.
pub async fn integrate_zset_stream<K>(
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
    integrate_zset_handle_stream::<K>(input).await
}

/// Single-level helper: differentiates `Stream<ZSetHandle>` by emitting the
/// per-step Z-set deltas in a new `Stream<ZSetHandle>`.
pub async fn differentiate_zset_stream<K>(
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

    let mut cache = std::collections::HashMap::new();
    let mut previous = std::collections::HashMap::new();
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
            cleanup.delete(versioned.intent_key_bytes().to_vec());
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
        prev: std::collections::HashMap::new(),
        materialize_cache: std::collections::HashMap::new(),
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
    let mut prev = std::collections::HashMap::new();
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
    let mut runtime = HandleOperatorRuntime::new(vec![input.clone()], move |_ts, handles| {
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
                eprintln!("live differentiate_zset_stream runtime terminated: {err}");
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
    prev: std::collections::HashMap<K, i64>,
    materialize_cache: std::collections::HashMap<String, Arc<Dictionary<K>>>,
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
        return Ok(
            versioned
                .current_handle()
                .unwrap_or_else(|| versioned.handle_for_version(0)),
        );
    }

    let mut batch = slatedb::WriteBatch::new();
    let plan = versioned
        .enqueue_version_with_base(segments, None, 0, &mut batch)
        .await
        .context("enqueue live diff version")?;
    versioned
        .table()
        .write_batch(batch)
        .await
        .context("write live diff version")?;

    let mut cleanup = slatedb::WriteBatch::new();
    cleanup.delete(versioned.intent_key_bytes().to_vec());
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

pub async fn lift1<I, O, F>(
    input: &Stream<I>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    I: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    I::Archived: RkyvDeserialize<I, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&I) -> O + Send + Sync,
{
    let values = collect_values(input, input.current_time()).await?;
    let mut result =
        build_derived_stream(input.table(), output_group.clone(), "stream_lift1/").await?;

    if let Some(first) = values.first() {
        let mut last = function(first);
        set_default_in_place(&mut result, last.clone());

        for t in 1..=input.current_time() {
            let value = function(&values[t as usize]);
            last = value.clone();
            push_value_in_place(&mut result, value);
        }

        set_default_in_place(&mut result, last);
    }

    Ok(result)
}

pub async fn lift2<L, R, O, F>(
    left: &Stream<L>,
    right: &Stream<R>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    L: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&L, &R) -> O + Send + Sync,
{
    let frontier = left.current_time().max(right.current_time());
    let left_values = collect_values(left, frontier).await?;
    let right_values = collect_values(right, frontier).await?;
    let mut result =
        build_derived_stream(left.table(), output_group.clone(), "stream_lift2/").await?;

    if let Some((first_left, first_right)) = left_values.first().zip(right_values.first()) {
        let mut last = function(first_left, first_right);
        set_default_in_place(&mut result, last.clone());

        for t in 1..=frontier {
            let value = function(&left_values[t as usize], &right_values[t as usize]);
            last = value.clone();
            push_value_in_place(&mut result, value);
        }

        set_default_in_place(&mut result, last);
    }

    Ok(result)
}

pub async fn incrementalize2<T, R, O, F>(
    left: &Stream<T>,
    right: &Stream<R>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&T, &R) -> O + Send + Sync + Clone + 'static,
{
    let integrated_left = integrate(left).await?;
    let delayed_integrated_left = delay(&integrated_left).await?;

    let integrated_right = integrate(right).await?;
    let delayed_integrated_right = delay(&integrated_right).await?;

    let f_ab = lift2(left, right, output_group.clone(), function.clone()).await?;
    let f_a_delayed_b = lift2(
        left,
        &delayed_integrated_right,
        output_group.clone(),
        function.clone(),
    )
    .await?;
    let f_delayed_a_b = lift2(
        &delayed_integrated_left,
        right,
        output_group.clone(),
        function,
    )
    .await?;

    let addition = StreamAddition::from_stream(&f_ab);
    let partial = addition.add(&f_ab, &f_a_delayed_b).await;
    let summed = addition.add(&partial, &f_delayed_a_b).await;
    Ok(summed)
}

pub async fn stream_introduction<T>(
    table: Arc<dyn KeyValueTable>,
    group: Arc<dyn AbelianGroup<T>>,
    value: T,
) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut stream = build_derived_stream(table, group.clone(), "stream_intro/").await?;
    set_default_in_place(&mut stream, value);
    stream.flush().await?;
    Ok(stream)
}

pub async fn stream_elimination<T>(stream: &Stream<T>) -> Result<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(stream, stream.current_time()).await?;
    let group = stream.group();
    let mut acc = group.identity().await;
    for value in values {
        acc = group.add(&acc, &value).await;
    }
    Ok(acc)
}
