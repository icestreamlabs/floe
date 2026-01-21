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
use crate::stream::groups::HandleGroup;
use crate::stream::util::{
    LIFTED_H_STREAM_PREFIX, LIFTED_H_ZSET_PREFIX, build_derived_stream, collect_values,
    compute_delta, materialize_zset_handle, next_lifted_zset_namespace, push_value_in_place,
    set_default_in_place,
};
use crate::stream::{Stream, StreamRetention, ZSetStream};

pub async fn lifted_h_zset_stream<K>(
    diff_stream: &Stream<ZSetHandle>,
    integrated_stream: &Stream<ZSetHandle>,
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
    let diff_handles = collect_values(diff_stream, diff_stream.current_time()).await?;
    let state_handles = collect_values(integrated_stream, integrated_stream.current_time()).await?;
    let total = diff_handles.len().min(state_handles.len());
    let table = diff_stream.table();
    let namespace = next_lifted_zset_namespace(LIFTED_H_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted H")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted H")?;

    let mut diff_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut state_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut previous: HashMap<K, i64> = HashMap::new();
    let mut output_handles = Vec::with_capacity(total);

    for t in 0..total {
        let diff_map =
            materialize_zset_handle::<K>(table.clone(), &mut diff_cache, &diff_handles[t]).await?;
        let state_map =
            materialize_zset_handle::<K>(table.clone(), &mut state_cache, &state_handles[t])
                .await?;

        let mut distincted = HashMap::new();
        for (key, &diff_weight) in &diff_map {
            let state_weight = state_map.get(key).copied().unwrap_or(0);
            let coalesced = diff_weight + state_weight;
            if state_weight > 0 && coalesced <= 0 {
                distincted.insert(key.clone(), -1);
                continue;
            }
            if state_weight <= 0 && coalesced > 0 {
                distincted.insert(key.clone(), 1);
                continue;
            }
            if state_weight == 0 && diff_weight > 0 {
                distincted.insert(key.clone(), 1);
            }
        }

        let deltas = compute_delta(&previous, &distincted);
        zset_stream.add_deltas(deltas);
        let handle = zset_stream.flush().await.context("flush lifted H result")?;
        output_handles.push(handle);
        previous = distincted;
    }

    let default_handle = output_handles
        .first()
        .cloned()
        .unwrap_or_else(|| zset_stream.current_handle().clone());
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_H_STREAM_PREFIX).await?;

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
