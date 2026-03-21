use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::groups::HandleGroup;
use crate::stream::runtime::HandleOperatorRuntime;
use crate::stream::util::{
    LIFTED_JOIN_STREAM_PREFIX, LIFTED_JOIN_ZSET_PREFIX, build_exact_stream_from_values,
    collect_values, next_lifted_zset_namespace, open_delta_handle_stream,
};
use crate::stream::{Stream, StreamRetention, ZSetStream};

use super::helpers::{
    delta_for_snapshot_step_with_retry, publish_handle, publish_scheduled_handle,
};

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
    let frontier = left.current_time().max(right.current_time());
    let horizon = left.semantic_horizon().max(right.semantic_horizon());
    let left_handles = collect_values(left, horizon).await?;
    let right_handles = collect_values(right, horizon).await?;
    let left_delta_stream = open_delta_handle_stream(left).await?;
    let right_delta_stream = open_delta_handle_stream(right).await?;
    let left_delta_handles = collect_values(&left_delta_stream, horizon).await?;
    let right_delta_handles = collect_values(&right_delta_stream, horizon).await?;
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

    let mut left_cache: HashMap<String, Arc<Dictionary<L>>> = HashMap::new();
    let mut right_cache: HashMap<String, Arc<Dictionary<R>>> = HashMap::new();
    let mut left_state: HashMap<L, i64> = HashMap::new();
    let mut right_state: HashMap<R, i64> = HashMap::new();
    let mut previous_left_handle: Option<ZSetHandle> = None;
    let mut previous_right_handle: Option<ZSetHandle> = None;
    let predicate: Arc<P> = Arc::new(predicate);
    let projector: Arc<F> = Arc::new(projector);
    let mut output_handles = Vec::with_capacity((horizon + 1) as usize);

    for t in 0..=horizon {
        let mut join_ctx = JoinHandleContext {
            table: table.clone(),
            predicate: &predicate,
            projector: &projector,
            left_cache: &mut left_cache,
            right_cache: &mut right_cache,
            left_state: &mut left_state,
            right_state: &mut right_state,
            previous_left_handle: &mut previous_left_handle,
            previous_right_handle: &mut previous_right_handle,
            zset_stream: &mut zset_stream,
        };
        let left_candidate = left_delta_handles.get(t as usize);
        let right_candidate = right_delta_handles.get(t as usize);
        let output_handle = join_handle(
            &mut join_ctx,
            &left_handles[t as usize],
            &right_handles[t as usize],
            left_candidate,
            right_candidate,
        )
        .await?;
        output_handles.push(output_handle);
    }

    let left_default_handle = left.default_value();
    let right_default_handle = right.default_value();
    let default_handle = left_handles
        .iter()
        .zip(right_handles.iter())
        .zip(output_handles.iter())
        .find_map(|((left_handle, right_handle), derived)| {
            if *left_handle == left_default_handle && *right_handle == right_default_handle {
                Some(derived.clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            output_handles
                .last()
                .cloned()
                .unwrap_or_else(|| zset_stream.current_handle().clone())
        });
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_exact_stream_from_values(
        table.clone(),
        handle_group,
        LIFTED_JOIN_STREAM_PREFIX,
        frontier,
        horizon,
        &output_handles,
        default_handle,
    )
    .await?;
    result_stream.flush().await?;
    let writer = result_stream.clone();

    let mut runtime =
        HandleOperatorRuntime::new(vec![left.clone(), right.clone()], |_, _| async { Ok(()) });
    tokio::spawn(async move {
        let mut left_cache = left_cache;
        let mut right_cache = right_cache;
        let mut left_state = left_state;
        let mut right_state = right_state;
        let mut previous_left_handle = previous_left_handle;
        let mut previous_right_handle = previous_right_handle;
        let mut zset_stream = zset_stream;
        let mut left_delta_stream = left_delta_stream;
        let mut right_delta_stream = right_delta_stream;
        let mut writer = writer;
        let mut initialized = true;
        let scheduled_horizon = horizon;
        loop {
            match runtime.next_handles().await {
                Ok((ts, handles)) => {
                    if ts <= scheduled_horizon {
                        if let Err(err) = publish_scheduled_handle(&mut writer, ts).await {
                            tracing::error!(
                                error = %err,
                                timestamp = ts,
                                "failed to publish scheduled lifted join handle"
                            );
                            break;
                        }
                        continue;
                    }
                    if handles.len() != 2 {
                        tracing::error!(
                            handle_count = handles.len(),
                            "join runtime produced unexpected handle count"
                        );
                        break;
                    }
                    let left_delta_handle = match left_delta_stream.get(ts).await {
                        Ok(handle) => Some(handle),
                        Err(err) => {
                            tracing::debug!(
                                error = %err,
                                "left lifted join delta lookup failed; falling back to handle namespace"
                            );
                            None
                        }
                    };
                    let right_delta_handle = match right_delta_stream.get(ts).await {
                        Ok(handle) => Some(handle),
                        Err(err) => {
                            tracing::debug!(
                                error = %err,
                                "right lifted join delta lookup failed; falling back to handle namespace"
                            );
                            None
                        }
                    };
                    let mut join_ctx = JoinHandleContext {
                        table: table.clone(),
                        predicate: &predicate,
                        projector: &projector,
                        left_cache: &mut left_cache,
                        right_cache: &mut right_cache,
                        left_state: &mut left_state,
                        right_state: &mut right_state,
                        previous_left_handle: &mut previous_left_handle,
                        previous_right_handle: &mut previous_right_handle,
                        zset_stream: &mut zset_stream,
                    };
                    match join_handle(
                        &mut join_ctx,
                        &handles[0],
                        &handles[1],
                        left_delta_handle.as_ref(),
                        right_delta_handle.as_ref(),
                    )
                    .await
                    {
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
    left_state: &'a mut HashMap<L, i64>,
    right_state: &'a mut HashMap<R, i64>,
    previous_left_handle: &'a mut Option<ZSetHandle>,
    previous_right_handle: &'a mut Option<ZSetHandle>,
    zset_stream: &'a mut ZSetStream<O>,
}

async fn join_handle<L, R, O, P, F>(
    ctx: &mut JoinHandleContext<'_, L, R, O, P, F>,
    left_snapshot: &ZSetHandle,
    right_snapshot: &ZSetHandle,
    left_candidate_delta: Option<&ZSetHandle>,
    right_candidate_delta: Option<&ZSetHandle>,
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
    let left_delta = delta_for_snapshot_step_with_retry::<L>(
        ctx.table.clone(),
        ctx.left_cache,
        left_snapshot,
        ctx.previous_left_handle.as_ref(),
        left_candidate_delta,
    )
    .await?;
    *ctx.previous_left_handle = Some(left_snapshot.clone());
    let right_delta = delta_for_snapshot_step_with_retry::<R>(
        ctx.table.clone(),
        ctx.right_cache,
        right_snapshot,
        ctx.previous_right_handle.as_ref(),
        right_candidate_delta,
    )
    .await?;
    *ctx.previous_right_handle = Some(right_snapshot.clone());

    let mut joined_delta: HashMap<O, i64> = HashMap::new();
    for (left_key, left_diff) in &left_delta {
        if *left_diff == 0 {
            continue;
        }
        for (right_key, right_weight) in ctx.right_state.iter() {
            if *right_weight == 0 {
                continue;
            }
            if (ctx.predicate)(left_key, right_key) {
                let projected = (ctx.projector)(left_key, right_key);
                *joined_delta.entry(projected).or_insert(0) += *left_diff * *right_weight;
            }
        }
    }

    for (right_key, right_diff) in &right_delta {
        if *right_diff == 0 {
            continue;
        }
        for (left_key, left_weight) in ctx.left_state.iter() {
            if *left_weight == 0 {
                continue;
            }
            if (ctx.predicate)(left_key, right_key) {
                let projected = (ctx.projector)(left_key, right_key);
                *joined_delta.entry(projected).or_insert(0) += *left_weight * *right_diff;
            }
        }
    }

    for (left_key, left_diff) in &left_delta {
        if *left_diff == 0 {
            continue;
        }
        for (right_key, right_diff) in &right_delta {
            if *right_diff == 0 {
                continue;
            }
            if (ctx.predicate)(left_key, right_key) {
                let projected = (ctx.projector)(left_key, right_key);
                *joined_delta.entry(projected).or_insert(0) += *left_diff * *right_diff;
            }
        }
    }
    joined_delta.retain(|_, weight| *weight != 0);

    ctx.zset_stream.add_deltas(joined_delta.into_iter());
    let handle = ctx
        .zset_stream
        .flush()
        .await
        .context("flush lifted join result")?;
    apply_deltas(ctx.left_state, left_delta);
    apply_deltas(ctx.right_state, right_delta);
    Ok(handle)
}

fn apply_deltas<K>(state: &mut HashMap<K, i64>, deltas: Vec<(K, i64)>)
where
    K: Eq + Hash,
{
    for (key, delta) in deltas {
        if delta == 0 {
            continue;
        }
        match state.entry(key) {
            Entry::Occupied(mut occupied) => {
                let updated = *occupied.get() + delta;
                if updated == 0 {
                    occupied.remove();
                } else {
                    *occupied.get_mut() = updated;
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(delta);
            }
        }
    }
}
