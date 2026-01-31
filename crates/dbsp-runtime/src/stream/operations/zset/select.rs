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
    compute_delta, next_lifted_zset_namespace,
};
use crate::stream::{Stream, StreamCursor, StreamRetention, ZSetStream};

use super::helpers::{materialize_zset_with_retry, publish_handle};

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
