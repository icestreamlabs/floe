use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::KeyValueTable;
use crate::stream::{Stream, StreamRetention, ZSetStream};
use crate::stream::groups::HandleGroup;
use crate::stream::runtime::HandleOperatorRuntime;
use crate::stream::util::{
    LIFTED_JOIN_STREAM_PREFIX, LIFTED_JOIN_ZSET_PREFIX, build_derived_stream, collect_values,
    compute_delta, next_lifted_zset_namespace,
};

use super::helpers::{materialize_zset_with_retry, publish_handle};

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
    let left_map = materialize_zset_with_retry::<L>(
        ctx.table.clone(),
        ctx.left_cache,
        left_handle,
    )
    .await?;
    let right_map = materialize_zset_with_retry::<R>(
        ctx.table.clone(),
        ctx.right_cache,
        right_handle,
    )
    .await?;

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
