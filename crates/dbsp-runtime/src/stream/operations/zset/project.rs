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
use crate::stream::{Stream, StreamCursor, StreamRetention, ZSetStream};
use crate::stream::groups::HandleGroup;
use crate::stream::util::{
    LIFTED_PROJECT_STREAM_PREFIX, LIFTED_PROJECT_ZSET_PREFIX, build_derived_stream,
    collect_values, compute_delta, next_lifted_zset_namespace,
};

use super::helpers::{materialize_zset_with_retry, publish_handle};

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
