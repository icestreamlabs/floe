use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::Stream;
use crate::stream::util::{
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
