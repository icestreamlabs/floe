use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::Stream;
use crate::stream::util::{build_exact_stream_from_values, collect_values};

pub async fn lifted_stream_elimination<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
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
    let frontier = stream.current_time();
    let horizon = stream.semantic_horizon();
    let handles = collect_values(stream, horizon).await?;
    let mut outputs = Vec::with_capacity(handles.len());
    for handle in &handles {
        let inner = stream
            .resolve_handle(handle, inner_group.clone())
            .await
            .context("resolve handle for lifted stream elimination")?;
        let mut resolved = inner;
        let latest = resolved
            .latest()
            .await
            .context("load latest handle for lifted stream elimination")?;
        outputs.push(latest);
    }

    let default_inner = stream
        .resolve_handle(&stream.default_value(), inner_group.clone())
        .await
        .context("resolve default handle for lifted stream elimination")?;
    let mut default_resolved = default_inner;
    let default_value = default_resolved
        .latest()
        .await
        .context("load latest default handle for lifted stream elimination")?;

    build_exact_stream_from_values(
        stream.table(),
        inner_group,
        "stream_lift_elim/",
        frontier,
        horizon,
        &outputs,
        default_value,
    )
    .await
}
