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
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::groups::HandleGroup;
use crate::stream::util::{
    LIFTED_SELECT_STREAM_PREFIX, LIFTED_SELECT_ZSET_PREFIX, build_derived_stream, collect_values,
    next_lifted_zset_namespace, open_delta_handle_stream,
};
use crate::stream::{Stream, StreamCursor, StreamRetention, ZSetStream};

use super::helpers::{delta_for_snapshot_step_with_retry, publish_handle};

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
    let delta_input = open_delta_handle_stream(input).await?;
    let mut initialized = false;
    let mut writer = result_stream.clone();
    let predicate: Arc<P> = Arc::new(predicate);

    let handles = collect_values(input, input.current_time()).await?;
    let delta_handles = collect_values(&delta_input, input.current_time()).await?;
    let mut previous_input_handle: Option<ZSetHandle> = None;
    for (index, handle) in handles.iter().enumerate() {
        let candidate_delta = delta_handles.get(index);
        let output_handle = select_handle(
            table.clone(),
            &predicate,
            &mut dict_cache,
            &mut zset_stream,
            handle,
            &mut previous_input_handle,
            candidate_delta,
        )
        .await?;
        publish_handle(&mut writer, output_handle, &mut initialized).await?;
    }

    result_stream.flush().await?;

    let mut cursor = StreamCursor::new(input.clone());
    tokio::spawn(async move {
        let mut dict_cache = dict_cache;
        let mut zset_stream = zset_stream;
        let mut delta_input = delta_input;
        let mut writer = writer;
        let mut initialized = initialized;
        let mut previous_input_handle = previous_input_handle;
        loop {
            match cursor.next().await {
                Ok((ts, handle)) => {
                    let delta_handle = match delta_input.get(ts).await {
                        Ok(handle) => Some(handle),
                        Err(err) => {
                            tracing::debug!(
                                error = %err,
                                timestamp = ts,
                                "lifted select delta stream lookup failed; falling back to handle namespace"
                            );
                            None
                        }
                    };
                    match select_handle(
                        table.clone(),
                        &predicate,
                        &mut dict_cache,
                        &mut zset_stream,
                        &handle,
                        &mut previous_input_handle,
                        delta_handle.as_ref(),
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

async fn select_handle<K, P>(
    table: Arc<dyn KeyValueTable>,
    predicate: &Arc<P>,
    dict_cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    zset_stream: &mut ZSetStream<K>,
    snapshot_handle: &ZSetHandle,
    previous_snapshot: &mut Option<ZSetHandle>,
    candidate_delta_handle: Option<&ZSetHandle>,
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
    let deltas = delta_for_snapshot_step_with_retry::<K>(
        table,
        dict_cache,
        snapshot_handle,
        previous_snapshot.as_ref(),
        candidate_delta_handle,
    )
    .await?;
    *previous_snapshot = Some(snapshot_handle.clone());
    let mut filtered_deltas: HashMap<K, i64> = HashMap::new();
    for (key, delta) in deltas {
        if delta == 0 || !predicate(&key) {
            continue;
        }
        *filtered_deltas.entry(key).or_insert(0) += delta;
    }
    filtered_deltas.retain(|_, delta| *delta != 0);

    zset_stream.add_deltas(filtered_deltas.into_iter());
    let handle = zset_stream
        .flush()
        .await
        .context("flush lifted select result")?;
    Ok(handle)
}
