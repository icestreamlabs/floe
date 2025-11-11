use std::sync::Arc;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::super::addition::StreamAddition;
use super::super::core::stream::Stream;
use super::super::util::{
    build_derived_stream, collect_values, push_value_in_place, set_default_in_place,
};

pub async fn delay<T>(input: &Stream<T>) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(input, input.current_time()).await?;
    let mut result = build_derived_stream(input.table(), input.group(), "stream_delay/").await?;

    let mut last_output = None;
    for t in 1..=input.current_time() {
        let value = values[(t - 1) as usize].clone();
        push_value_in_place(&mut result, value.clone());
        last_output = Some(value);
    }

    if let Some(last) = last_output {
        set_default_in_place(&mut result, last);
    }

    Ok(result)
}

pub async fn differentiate<T>(input: &Stream<T>) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(input, input.current_time()).await?;
    let group = input.group();
    let mut result = build_derived_stream(input.table(), group.clone(), "stream_diff/").await?;

    if let Some(first) = values.first() {
        let mut last_output = first.clone();
        set_default_in_place(&mut result, first.clone());

        for t in 1..=input.current_time() {
            let current = &values[t as usize];
            let previous = &values[(t - 1) as usize];
            let neg_prev = group.neg(previous).await;
            let diff = group.add(current, &neg_prev).await;
            last_output = diff.clone();
            push_value_in_place(&mut result, diff);
        }

        set_default_in_place(&mut result, last_output);
    }

    Ok(result)
}

pub async fn integrate<T>(input: &Stream<T>) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(input, input.current_time()).await?;
    let group = input.group();
    let mut result =
        build_derived_stream(input.table(), group.clone(), "stream_integrate/").await?;

    if let Some(first) = values.first() {
        let mut acc = first.clone();
        set_default_in_place(&mut result, acc.clone());

        for t in 1..=input.current_time() {
            let current = &values[t as usize];
            acc = group.add(&acc, current).await;
            push_value_in_place(&mut result, acc.clone());
        }

        set_default_in_place(&mut result, acc);
    }

    Ok(result)
}

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
    let values = collect_values(input, input.current_time()).await?;
    let mut result =
        build_derived_stream(input.table(), output_group.clone(), "stream_lift1/").await?;

    if let Some(first) = values.first() {
        let mut last = function(first);
        set_default_in_place(&mut result, last.clone());

        for t in 1..=input.current_time() {
            let value = function(&values[t as usize]);
            last = value.clone();
            push_value_in_place(&mut result, value);
        }

        set_default_in_place(&mut result, last);
    }

    Ok(result)
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
    let left_values = collect_values(left, frontier).await?;
    let right_values = collect_values(right, frontier).await?;
    let mut result =
        build_derived_stream(left.table(), output_group.clone(), "stream_lift2/").await?;

    if let Some((first_left, first_right)) = left_values.first().zip(right_values.first()) {
        let mut last = function(first_left, first_right);
        set_default_in_place(&mut result, last.clone());

        for t in 1..=frontier {
            let value = function(&left_values[t as usize], &right_values[t as usize]);
            last = value.clone();
            push_value_in_place(&mut result, value);
        }

        set_default_in_place(&mut result, last);
    }

    Ok(result)
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

pub async fn stream_introduction<T>(
    table: Arc<dyn KeyValueTable>,
    group: Arc<dyn AbelianGroup<T>>,
    value: T,
) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut stream = build_derived_stream(table, group.clone(), "stream_intro/").await?;
    set_default_in_place(&mut stream, value);
    stream.flush().await?;
    Ok(stream)
}

pub async fn stream_elimination<T>(stream: &Stream<T>) -> Result<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let values = collect_values(stream, stream.current_time()).await?;
    let group = stream.group();
    let mut acc = group.identity().await;
    for value in values {
        acc = group.add(&acc, &value).await;
    }
    Ok(acc)
}
