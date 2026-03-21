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

use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::util::{
    DELTA_LIFTED_JOIN_STREAM_PREFIX, ZSET_SUM_PREFIX, build_exact_stream_from_values,
    compute_delta, materialize_zset_handle, next_lifted_zset_namespace,
};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};

use super::super::lifted::lifted_delay;
use super::super::zset_integral::lifted_integrate_zset;

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
    P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
    F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
{
    let left_default = left.default_value();
    let right_default = right.default_value();
    let left_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(ZSetHandle {
            ns: left_default.ns.clone(),
            version: 0,
        }));
    let right_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(ZSetHandle {
            ns: right_default.ns.clone(),
            version: 0,
        }));

    let int_l = lifted_integrate_zset::<L>(left, left_inner_group.clone()).await?;
    let d_int_l = lifted_delay(&int_l, left_inner_group.clone()).await?;
    let i_int_l = lifted_integrate_zset::<L>(&int_l, left_inner_group.clone()).await?;

    let int_r = lifted_integrate_zset::<R>(right, right_inner_group.clone()).await?;
    let d_int_r = lifted_delay(&int_r, right_inner_group.clone()).await?;
    let i_int_r = lifted_integrate_zset::<R>(&int_r, right_inner_group.clone()).await?;
    let d_i_int_r = lifted_delay(&i_int_r, right_inner_group.clone()).await?;

    let join1 = super::super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &d_int_l,
        &d_int_r,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join2 = super::super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &i_int_l,
        right,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join3 = super::super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &int_l,
        &d_int_r,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join4 = super::super::lifted_lifted::lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        left, &d_i_int_r, predicate, projector,
    )
    .await?;

    let frontier = join1
        .current_time()
        .max(join2.current_time())
        .max(join3.current_time())
        .max(join4.current_time());
    let horizon = join1
        .semantic_horizon()
        .max(join2.semantic_horizon())
        .max(join3.semantic_horizon())
        .max(join4.semantic_horizon());

    let table = left.table();

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
    let capacity = usize::try_from(horizon.saturating_add(1)).unwrap_or(usize::MAX);
    let mut aggregated_handles = Vec::with_capacity(capacity);

    for t in 0..=horizon {
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

    let default_handle = aggregator.stream.handle();
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    tracing::debug!(
        handle_count = aggregated_handles.len(),
        latest_version = aggregated_handles
            .last()
            .map(|h| h.frontier)
            .unwrap_or_default(),
        "delta_lifted_delta_lifted_join aggregated handles"
    );
    let mut result_stream = build_exact_stream_from_values(
        table.clone(),
        handle_group,
        DELTA_LIFTED_JOIN_STREAM_PREFIX,
        frontier,
        horizon,
        &aggregated_handles,
        default_handle,
    )
    .await?;

    result_stream.flush().await?;
    Ok(result_stream)
}
