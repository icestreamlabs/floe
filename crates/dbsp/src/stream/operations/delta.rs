use std::collections::HashMap;
use std::convert::TryFrom;
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

use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::{
    DELTA_LIFTED_JOIN_STREAM_PREFIX, ZSET_SUM_PREFIX, build_derived_stream, compute_delta,
    materialize_zset_handle, next_lifted_zset_namespace, push_value_in_place, set_default_in_place,
};
use super::super::zset_stream::{StreamRetention, ZSetStream};
use super::lifted::lifted_delay;
use super::zset_integral::lifted_integrate_zset;

pub async fn delta_lifted_delta_lifted_join<L, R, O, P, F>(
    left: &Stream<StreamHandle>,
    right: &Stream<StreamHandle>,
    predicate: P,
    projector: F,
) -> Result<Stream<StreamHandle>>
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
    P: Fn(&L, &R) -> bool + Send + Sync + Clone,
    F: Fn(&L, &R) -> O + Send + Sync + Clone,
{
    let left_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(ZSetHandle {
            ns: left.default.ns.clone(),
            version: 0,
        }));
    let right_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(ZSetHandle {
            ns: right.default.ns.clone(),
            version: 0,
        }));

    let int_l = lifted_integrate_zset::<L>(left, left_inner_group.clone()).await?;
    let d_int_l = lifted_delay(&int_l, left_inner_group.clone()).await?;
    let i_int_l = lifted_integrate_zset::<L>(&int_l, left_inner_group.clone()).await?;

    let int_r = lifted_integrate_zset::<R>(right, right_inner_group.clone()).await?;
    let d_int_r = lifted_delay(&int_r, right_inner_group.clone()).await?;
    let i_int_r = lifted_integrate_zset::<R>(&int_r, right_inner_group.clone()).await?;
    let d_i_int_r = lifted_delay(&i_int_r, right_inner_group.clone()).await?;

    let join1 = super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &d_int_l,
        &d_int_r,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join2 = super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &i_int_l,
        right,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join3 = super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &int_l,
        &d_int_r,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join4 = super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        left, &d_i_int_r, predicate, projector,
    )
    .await?;

    let mut total_ts = join1
        .timestamp
        .min(join2.timestamp)
        .min(join3.timestamp)
        .min(join4.timestamp);
    if total_ts < 0 {
        total_ts = 0;
    }

    let table = left.table.clone();

    let mut components = [join1, join2, join3, join4];
    let mut caches: Vec<HashMap<String, Arc<Dictionary<O>>>> =
        vec![HashMap::new(); components.len()];

    let ns = next_lifted_zset_namespace(ZSET_SUM_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), ns.clone(), None)
            .await
            .context("build dictionary for delta lifted join")?,
    );
    let mut aggregator = ZSetStream::new(dict, table.clone(), ns, StreamRetention::None)
        .await
        .context("create aggregator stream for delta lifted join")?;

    let mut previous: HashMap<O, i64> = HashMap::new();
    let capacity = usize::try_from(total_ts.saturating_add(1)).unwrap_or(usize::MAX);
    let mut aggregated_handles = Vec::with_capacity(capacity);

    for t in 0..=total_ts {
        let mut combined: HashMap<O, i64> = HashMap::new();

        for (idx, component) in components.iter_mut().enumerate() {
            let handle = component
                .get(t)
                .await
                .context("read component handle for delta lifted join")?;
            let inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
                Arc::new(HandleGroup::new(ZSetHandle {
                    ns: handle.ns.clone(),
                    version: 0,
                }));
            let mut resolved = component
                .resolve_handle(&handle, inner_group)
                .await
                .context("resolve component inner stream")?;
            let zset_handle = resolved
                .latest()
                .await
                .context("read component zset handle")?;
            let map = materialize_zset_handle::<O>(table.clone(), &mut caches[idx], &zset_handle)
                .await
                .context("materialize component zset")?;

            for (key, weight) in map {
                let entry = combined.entry(key).or_insert(0);
                *entry = (*entry).saturating_add(weight);
            }
        }

        combined.retain(|_, weight| *weight != 0);
        let deltas = compute_delta(&previous, &combined);
        aggregator.add_deltas(deltas);
        aggregator
            .flush()
            .await
            .context("flush aggregated zset stream")?;
        previous = combined;

        aggregated_handles.push(aggregator.stream.handle());
    }

    let fallback_handle = aggregator.stream.handle();
    let default_handle = aggregated_handles
        .first()
        .cloned()
        .unwrap_or(fallback_handle);
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, DELTA_LIFTED_JOIN_STREAM_PREFIX).await?;

    if aggregated_handles.is_empty() {
        set_default_in_place(&mut result_stream, default_handle);
    } else {
        set_default_in_place(&mut result_stream, aggregated_handles[0].clone());
        for handle in aggregated_handles.iter().skip(1) {
            push_value_in_place(&mut result_stream, handle.clone());
        }
        if let Some(last) = aggregated_handles.last() {
            set_default_in_place(&mut result_stream, last.clone());
        }
    }

    result_stream.flush().await?;
    Ok(result_stream)
}
