use std::sync::Arc;

use anyhow::Result;
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
    let values = collect_values(stream, stream.current_time()).await?;
    let group = stream.group();
    let mut acc = group.identity().await;
    for value in values {
        acc = group.add(&acc, &value).await;
    }
    Ok(acc)
}
