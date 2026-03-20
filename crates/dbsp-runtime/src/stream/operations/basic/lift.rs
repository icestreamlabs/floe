use std::sync::Arc;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::Stream;
use crate::stream::util::{build_exact_stream_from_values, collect_values};

use super::time::{delay, integrate};
use crate::stream::addition::StreamAddition;

pub async fn lift1<I, O, F>(
    input: &Stream<I>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    I: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    I::Archived: RkyvDeserialize<I, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&I) -> O + Send + Sync,
{
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    let values = collect_values(input, horizon).await?;
    let mut mapped = Vec::with_capacity((horizon + 1) as usize);
    for value in &values {
        mapped.push(function(value));
    }
    let tail_default = function(&input.default_value());

    build_exact_stream_from_values(
        input.table(),
        output_group,
        "stream_lift1/",
        frontier,
        horizon,
        &mapped,
        tail_default,
    )
    .await
}

pub async fn lift2<L, R, O, F>(
    left: &Stream<L>,
    right: &Stream<R>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    L: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&L, &R) -> O + Send + Sync,
{
    let frontier = left.current_time().max(right.current_time());
    let horizon = left.semantic_horizon().max(right.semantic_horizon());
    let left_values = collect_values(left, horizon).await?;
    let right_values = collect_values(right, horizon).await?;
    let mut combined = Vec::with_capacity((horizon + 1) as usize);
    for t in 0..=horizon {
        combined.push(function(
            &left_values[t as usize],
            &right_values[t as usize],
        ));
    }
    let tail_default = function(&left.default_value(), &right.default_value());

    build_exact_stream_from_values(
        left.table(),
        output_group,
        "stream_lift2/",
        frontier,
        horizon,
        &combined,
        tail_default,
    )
    .await
}

pub async fn incrementalize2<T, R, O, F>(
    left: &Stream<T>,
    right: &Stream<R>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&T, &R) -> O + Send + Sync + Clone + 'static,
{
    let integrated_left = integrate(left).await?;
    let delayed_integrated_left = delay(&integrated_left).await?;

    let integrated_right = integrate(right).await?;
    let delayed_integrated_right = delay(&integrated_right).await?;

    let f_ab = lift2(left, right, output_group.clone(), function.clone()).await?;
    let f_a_delayed_b = lift2(
        left,
        &delayed_integrated_right,
        output_group.clone(),
        function.clone(),
    )
    .await?;
    let f_delayed_a_b = lift2(
        &delayed_integrated_left,
        right,
        output_group.clone(),
        function,
    )
    .await?;

    let addition = StreamAddition::from_stream(&f_ab);
    let partial = addition.add(&f_ab, &f_a_delayed_b).await;
    let summed = addition.add(&partial, &f_delayed_a_b).await;
    Ok(summed)
}
