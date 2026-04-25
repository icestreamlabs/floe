use std::sync::Arc;

use anyhow::{Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::Stream;
use crate::stream::util::{build_derived_stream, collect_values, set_default_in_place};

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
    let horizon = stream.semantic_horizon();
    let values = collect_values(stream, horizon).await?;
    let group = stream.group();
    let mut acc = group.identity().await;
    if stream.default_value() != acc {
        return Err(anyhow!(
            "stream_elimination requires an eventually-identity input stream for exact semantics"
        ));
    }
    for value in values {
        acc = group.add(&acc, &value).await;
    }
    Ok(acc)
}

pub async fn stream_elimination_range<T>(
    stream: &Stream<T>,
    start: i64,
    end_inclusive: i64,
) -> Result<T>
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
    if start < 0 {
        return Err(anyhow!("stream_elimination_range start cannot be negative"));
    }
    if end_inclusive < start {
        return Err(anyhow!(
            "stream_elimination_range end must be greater than or equal to start"
        ));
    }

    let group = stream.group();
    let mut acc = group.identity().await;
    let mut stream = stream.clone();
    for timestamp in start..=end_inclusive {
        let value = stream.get(timestamp).await?;
        acc = group.add(&acc, &value).await;
    }
    Ok(acc)
}

pub async fn stream_elimination_prefix<T>(stream: &Stream<T>, end_inclusive: i64) -> Result<T>
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
    stream_elimination_range(stream, 0, end_inclusive).await
}
