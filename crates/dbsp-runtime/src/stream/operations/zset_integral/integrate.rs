use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::groups::HandleGroup;
use crate::stream::util::{
    ZSET_INTEGRAL_PREFIX, ZSET_INTEGRAL_STREAM_PREFIX, build_exact_stream_from_values,
    collect_values, delta_handle_namespace, delta_zset_handle, next_lifted_zset_namespace,
    open_delta_handle_stream, resolve_apply_handle_op,
};
use crate::stream::{Stream, StreamRetention, ZSetStream};

pub async fn lifted_integrate_zset<K>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<ZSetHandle>>,
) -> Result<Stream<StreamHandle>>
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
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { integrate_zset_handle_stream::<K>(&inner).await },
        "stream_lift_integrate/",
    )
    .await
}

pub(crate) async fn integrate_zset_handle_stream<K>(
    stream: &Stream<ZSetHandle>,
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
    let delta_stream = open_delta_handle_stream(stream).await?;
    let frontier = stream.current_time().max(delta_stream.current_time());
    let horizon = stream
        .semantic_horizon()
        .max(delta_stream.semantic_horizon());
    let handles = collect_values(stream, horizon).await?;
    let delta_handles = collect_values(&delta_stream, horizon).await?;
    let table = stream.table();
    let namespace = next_lifted_zset_namespace(ZSET_INTEGRAL_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for integrated zset stream")?,
    );
    let mut aggregator = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create aggregator for integrated zset stream")?;

    let mut caches: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut result_handles = Vec::with_capacity(handles.len());
    let mut previous_handle: Option<ZSetHandle> = None;

    for (index, handle) in handles.iter().enumerate() {
        let deltas = resolve_step_deltas::<K>(
            table.clone(),
            &mut caches,
            handle,
            previous_handle.as_ref(),
            delta_handles.get(index),
        )
        .await
        .context("resolve zset deltas for integration")?;
        previous_handle = Some(handle.clone());
        aggregator.add_deltas(deltas);
        let handle = aggregator
            .flush()
            .await
            .context("flush integrated zset stream")?;
        result_handles.push(handle);
    }

    let default_handle = aggregator.current_handle().clone();
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_exact_stream_from_values(
        table,
        handle_group,
        ZSET_INTEGRAL_STREAM_PREFIX,
        frontier,
        horizon,
        &result_handles,
        default_handle,
    )
    .await?;

    result_stream.flush().await?;
    Ok(result_stream)
}

async fn resolve_step_deltas<K>(
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
            .context("read candidate integration delta handle");
    }

    let fallback = ZSetHandle {
        ns: expected_ns,
        version: snapshot_handle.version,
    };
    match delta_zset_handle::<K>(table, cache, &fallback).await {
        Ok(deltas) => Ok(deltas),
        Err(err) if is_missing_manifest(&err) => Ok(Vec::new()),
        Err(err) => Err(err).context("read fallback integration delta handle"),
    }
}

fn is_missing_manifest(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}");
    message.contains("manifest version")
        || message.contains("not found for namespace")
        || message.contains("not found")
}
