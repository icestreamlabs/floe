use std::sync::Arc;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::util::{build_exact_stream_from_values, collect_values};

use super::super::basic::stream_introduction;

pub async fn lifted_stream_introduction<T>(stream: &Stream<T>) -> Result<Stream<StreamHandle>>
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
    let frontier = stream.current_time();
    let horizon = stream.semantic_horizon();
    let values = collect_values(stream, horizon).await?;
    let group = stream.group();
    let table = stream.table();

    let mut outputs = Vec::with_capacity(values.len());
    for value in &values {
        let mut introduced =
            stream_introduction(table.clone(), group.clone(), value.clone()).await?;
        introduced.flush().await?;
        outputs.push(introduced.handle());
    }

    let mut default_stream =
        stream_introduction(table.clone(), group.clone(), stream.default_value()).await?;
    default_stream.flush().await?;
    let default_handle = default_stream.handle();

    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    build_exact_stream_from_values(
        table,
        handle_group,
        "stream_lift_intro/",
        frontier,
        horizon,
        &outputs,
        default_handle,
    )
    .await
}
