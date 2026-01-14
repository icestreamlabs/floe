use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::time::sleep;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::super::core::stream::Stream;
use super::super::cursor::StreamCursor;
use super::super::groups::HandleGroup;
use super::super::runtime::HandleOperatorRuntime;
use super::super::util::{
    LIFTED_JOIN_STREAM_PREFIX, LIFTED_JOIN_ZSET_PREFIX, LIFTED_PROJECT_STREAM_PREFIX,
    LIFTED_PROJECT_ZSET_PREFIX, LIFTED_SELECT_STREAM_PREFIX, LIFTED_SELECT_ZSET_PREFIX,
    build_derived_stream, collect_values, compute_delta, materialize_zset_handle,
    next_lifted_zset_namespace, push_value_in_place, set_default_in_place,
};
use super::super::zset_stream::{StreamRetention, ZSetStream};
use crate::storage::KeyValueTable;

pub async fn lifted_select_zset_stream<K, P>(
    input: &Stream<ZSetHandle>,
    predicate: P,
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
    P: Fn(&K) -> bool + Send + Sync + Clone + 'static,
{
    let table = input.table();
    let namespace = next_lifted_zset_namespace(LIFTED_SELECT_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted select")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted select")?;

    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(zset_stream.current_handle().clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_SELECT_STREAM_PREFIX).await?;

    let mut dict_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut previous: HashMap<K, i64> = HashMap::new();
    let mut initialized = false;
    let mut writer = result_stream.clone();
    let predicate: Arc<P> = Arc::new(predicate);

    let handles = collect_values(input, input.current_time()).await?;
    for handle in handles {
        let output_handle = select_handle(
            table.clone(),
            &predicate,
            &mut dict_cache,
            &mut previous,
            &mut zset_stream,
            &handle,
        )
        .await?;
        publish_handle(&mut writer, output_handle, &mut initialized).await?;
    }

    result_stream.flush().await?;

    let mut cursor = StreamCursor::new(input.clone());
    tokio::spawn(async move {
        let mut dict_cache = dict_cache;
        let mut previous = previous;
        let mut zset_stream = zset_stream;
        let mut writer = writer;
        let mut initialized = initialized;
        loop {
            match cursor.next().await {
                Ok((_, handle)) => {
                    match select_handle(
                        table.clone(),
                        &predicate,
                        &mut dict_cache,
                        &mut previous,
                        &mut zset_stream,
                        &handle,
                    )
                    .await
                    {
                        Ok(output_handle) => {
                            if let Err(err) =
                                publish_handle(&mut writer, output_handle, &mut initialized).await
                            {
                                tracing::error!(
                                    error = %err,
                                    "failed to publish lifted select handle"
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                namespace = %handle.ns,
                                error = %err,
                                "failed to compute lifted select handle"
                            );
                            break;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "select input stream closed unexpectedly"
                    );
                    break;
                }
            }
        }
    });

    Ok(result_stream)
}

pub async fn lifted_project_zset_stream<K, R, F>(
    input: &Stream<ZSetHandle>,
    projector: F,
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
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&K) -> R + Send + Sync + Clone + 'static,
{
    let table = input.table();
    let namespace = next_lifted_zset_namespace(LIFTED_PROJECT_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted project")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted project")?;

    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(zset_stream.current_handle().clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_PROJECT_STREAM_PREFIX).await?;

    let mut dict_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut previous: HashMap<R, i64> = HashMap::new();
    let mut initialized = false;
    let mut writer = result_stream.clone();
    let projector: Arc<F> = Arc::new(projector);

    let handles = collect_values(input, input.current_time()).await?;
    for handle in handles {
        let output_handle = project_handle(
            table.clone(),
            &projector,
            &mut dict_cache,
            &mut previous,
            &mut zset_stream,
            &handle,
        )
        .await?;
        publish_handle(&mut writer, output_handle, &mut initialized).await?;
    }

    result_stream.flush().await?;

    let mut cursor = StreamCursor::new(input.clone());
    tokio::spawn(async move {
        let mut dict_cache = dict_cache;
        let mut previous = previous;
        let mut zset_stream = zset_stream;
        let mut writer = writer;
        let mut initialized = initialized;
        loop {
            match cursor.next().await {
                Ok((_, handle)) => {
                    match project_handle(
                        table.clone(),
                        &projector,
                        &mut dict_cache,
                        &mut previous,
                        &mut zset_stream,
                        &handle,
                    )
                    .await
                    {
                        Ok(output_handle) => {
                            if let Err(err) =
                                publish_handle(&mut writer, output_handle, &mut initialized).await
                            {
                                tracing::error!(
                                    error = %err,
                                    "failed to publish lifted project handle"
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                namespace = %handle.ns,
                                error = %err,
                                "failed to compute lifted project handle"
                            );
                            break;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "project input stream closed unexpectedly"
                    );
                    break;
                }
            }
        }
    });

    Ok(result_stream)
}

pub async fn lifted_join_zset_stream<L, R, O, P, F>(
    left: &Stream<ZSetHandle>,
    right: &Stream<ZSetHandle>,
    predicate: P,
    projector: F,
) -> Result<Stream<ZSetHandle>>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
    F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
{
    let left_handles = collect_values(left, left.current_time()).await?;
    let right_handles = collect_values(right, right.current_time()).await?;
    let total = left_handles.len().min(right_handles.len());
    let table = left.table();
    let namespace = next_lifted_zset_namespace(LIFTED_JOIN_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted join")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted join")?;

    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(zset_stream.current_handle().clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_JOIN_STREAM_PREFIX).await?;

    let mut left_cache: HashMap<String, Arc<Dictionary<L>>> = HashMap::new();
    let mut right_cache: HashMap<String, Arc<Dictionary<R>>> = HashMap::new();
    let mut previous: HashMap<O, i64> = HashMap::new();
    let mut initialized = false;
    let mut writer = result_stream.clone();
    let predicate: Arc<P> = Arc::new(predicate);
    let projector: Arc<F> = Arc::new(projector);

    for t in 0..total {
        let mut join_ctx = JoinHandleContext {
            table: table.clone(),
            predicate: &predicate,
            projector: &projector,
            left_cache: &mut left_cache,
            right_cache: &mut right_cache,
            previous: &mut previous,
            zset_stream: &mut zset_stream,
        };
        let output_handle = join_handle(&mut join_ctx, &left_handles[t], &right_handles[t]).await?;
        publish_handle(&mut writer, output_handle, &mut initialized).await?;
    }

    result_stream.flush().await?;

    let mut runtime =
        HandleOperatorRuntime::new(vec![left.clone(), right.clone()], |_, _| async { Ok(()) });
    tokio::spawn(async move {
        let mut left_cache = left_cache;
        let mut right_cache = right_cache;
        let mut previous = previous;
        let mut zset_stream = zset_stream;
        let mut writer = writer;
        let mut initialized = initialized;
        loop {
            match runtime.next_handles().await {
                Ok((_, handles)) => {
                    if handles.len() != 2 {
                        tracing::error!(
                            handle_count = handles.len(),
                            "join runtime produced unexpected handle count"
                        );
                        break;
                    }
                    let mut join_ctx = JoinHandleContext {
                        table: table.clone(),
                        predicate: &predicate,
                        projector: &projector,
                        left_cache: &mut left_cache,
                        right_cache: &mut right_cache,
                        previous: &mut previous,
                        zset_stream: &mut zset_stream,
                    };
                    match join_handle(&mut join_ctx, &handles[0], &handles[1]).await {
                        Ok(output_handle) => {
                            if let Err(err) =
                                publish_handle(&mut writer, output_handle, &mut initialized).await
                            {
                                tracing::error!(
                                    error = %err,
                                    "failed to publish lifted join handle"
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                error = %err,
                                "failed to compute lifted join handle"
                            );
                            break;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "join input streams closed unexpectedly"
                    );
                    break;
                }
            }
        }
    });

    Ok(result_stream)
}

async fn publish_handle(
    stream: &mut Stream<ZSetHandle>,
    handle: ZSetHandle,
    initialized: &mut bool,
) -> Result<()> {
    if !*initialized {
        set_default_in_place(stream, handle.clone());
        *initialized = true;
    } else {
        push_value_in_place(stream, handle.clone());
    }
    set_default_in_place(stream, handle.clone());
    stream.flush().await?;
    Ok(())
}

async fn select_handle<K, P>(
    table: Arc<dyn KeyValueTable>,
    predicate: &Arc<P>,
    dict_cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    previous: &mut HashMap<K, i64>,
    zset_stream: &mut ZSetStream<K>,
    handle: &ZSetHandle,
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
    P: Fn(&K) -> bool + Send + Sync + 'static,
{
    let materialized = materialize_zset_with_retry::<K>(table, dict_cache, handle).await?;

    let mut filtered = HashMap::new();
    for (key, weight) in materialized {
        if predicate(&key) && weight != 0 {
            filtered.insert(key, weight);
        }
    }

    let deltas = compute_delta(previous, &filtered);
    zset_stream.add_deltas(deltas);
    let handle = zset_stream
        .flush()
        .await
        .context("flush lifted select result")?;
    *previous = filtered;
    Ok(handle)
}

async fn project_handle<K, R, F>(
    table: Arc<dyn KeyValueTable>,
    projector: &Arc<F>,
    dict_cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    previous: &mut HashMap<R, i64>,
    zset_stream: &mut ZSetStream<R>,
    handle: &ZSetHandle,
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
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&K) -> R + Send + Sync + 'static,
{
    let materialized = materialize_zset_with_retry::<K>(table, dict_cache, handle).await?;

    let mut projected: HashMap<R, i64> = HashMap::new();
    for (key, weight) in materialized {
        if weight == 0 {
            continue;
        }
        let result_key = projector(&key);
        *projected.entry(result_key).or_insert(0) += weight;
    }
    projected.retain(|_, weight| *weight != 0);

    let deltas = compute_delta(previous, &projected);
    zset_stream.add_deltas(deltas);
    let handle = zset_stream
        .flush()
        .await
        .context("flush lifted project result")?;
    *previous = projected;
    Ok(handle)
}

struct JoinHandleContext<'a, L, R, O, P, F>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    table: Arc<dyn KeyValueTable>,
    predicate: &'a Arc<P>,
    projector: &'a Arc<F>,
    left_cache: &'a mut HashMap<String, Arc<Dictionary<L>>>,
    right_cache: &'a mut HashMap<String, Arc<Dictionary<R>>>,
    previous: &'a mut HashMap<O, i64>,
    zset_stream: &'a mut ZSetStream<O>,
}

async fn join_handle<L, R, O, P, F>(
    ctx: &mut JoinHandleContext<'_, L, R, O, P, F>,
    left_handle: &ZSetHandle,
    right_handle: &ZSetHandle,
) -> Result<ZSetHandle>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Fn(&L, &R) -> bool + Send + Sync + 'static,
    F: Fn(&L, &R) -> O + Send + Sync + 'static,
{
    let left_map =
        materialize_zset_with_retry::<L>(ctx.table.clone(), ctx.left_cache, left_handle).await?;
    let right_map =
        materialize_zset_with_retry::<R>(ctx.table.clone(), ctx.right_cache, right_handle).await?;

    let mut joined: HashMap<O, i64> = HashMap::new();
    for (left_key, &left_weight) in &left_map {
        if left_weight == 0 {
            continue;
        }
        for (right_key, &right_weight) in &right_map {
            if right_weight == 0 {
                continue;
            }
            if (ctx.predicate)(left_key, right_key) {
                let projected = (ctx.projector)(left_key, right_key);
                *joined.entry(projected).or_insert(0) += left_weight * right_weight;
            }
        }
    }
    joined.retain(|_, weight| *weight != 0);

    let deltas = compute_delta(ctx.previous, &joined);
    ctx.zset_stream.add_deltas(deltas);
    let handle = ctx
        .zset_stream
        .flush()
        .await
        .context("flush lifted join result")?;
    *ctx.previous = joined;
    Ok(handle)
}

async fn materialize_zset_with_retry<K>(
    table: Arc<dyn KeyValueTable>,
    cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    handle: &ZSetHandle,
) -> Result<HashMap<K, i64>>
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
    let mut last_err = None;
    for _ in 0..80 {
        match materialize_zset_handle::<K>(table.clone(), cache, handle).await {
            Ok(map) => return Ok(map),
            Err(err) => {
                last_err = Some(err);
                sleep(Duration::from_millis(25)).await;
            }
        }
    }
    Err(last_err.expect("at least one materialize attempt"))
}
