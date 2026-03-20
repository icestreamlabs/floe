use anyhow::{Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::Stream;
use crate::stream::util::{build_exact_stream_from_values, collect_values};

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
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    let input_values = collect_values(input, horizon).await?;
    let mut delayed_values = Vec::with_capacity((horizon + 2) as usize);
    delayed_values.push(input_values[0].clone());
    for t in 1..=horizon + 1 {
        delayed_values.push(input_values[(t - 1) as usize].clone());
    }
    build_exact_stream_from_values(
        input.table(),
        input.group(),
        "stream_delay/",
        frontier,
        horizon + 1,
        &delayed_values,
        input.default_value(),
    )
    .await
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
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    let values = collect_values(input, horizon).await?;
    let group = input.group();
    let tail_value = input.default_value();
    let mut diff_values = Vec::with_capacity((horizon + 2) as usize);
    diff_values.push(values[0].clone());
    for t in 1..=horizon {
        let current = &values[t as usize];
        let previous = &values[(t - 1) as usize];
        let neg_prev = group.neg(previous).await;
        diff_values.push(group.add(current, &neg_prev).await);
    }
    let neg_last = group.neg(values.last().expect("values non-empty")).await;
    diff_values.push(group.add(&tail_value, &neg_last).await);

    build_exact_stream_from_values(
        input.table(),
        group.clone(),
        "stream_diff/",
        frontier,
        horizon + 1,
        &diff_values,
        group.identity().await,
    )
    .await
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
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    let values = collect_values(input, horizon).await?;
    let group = input.group();
    let identity = group.identity().await;
    let tail_value = input.default_value();
    if tail_value != identity {
        return Err(anyhow!(
            "integrate requires an eventually-identity input stream for exact semantics"
        ));
    }
    let mut integrated_values = Vec::with_capacity((horizon + 1) as usize);
    let mut acc = values[0].clone();
    integrated_values.push(acc.clone());
    for t in 1..=horizon {
        let current = &values[t as usize];
        acc = group.add(&acc, current).await;
        integrated_values.push(acc.clone());
    }

    build_exact_stream_from_values(
        input.table(),
        group,
        "stream_integrate/",
        frontier,
        horizon,
        &integrated_values,
        acc,
    )
    .await
}
