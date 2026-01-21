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
use crate::stream::util::{
    build_derived_stream, collect_values, push_value_in_place, set_default_in_place,
};

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
    let handles = collect_values(stream, stream.current_time()).await?;
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

    let default_value = if let Some(first) = outputs.first() {
        first.clone()
    } else {
        inner_group.identity().await
    };

    let mut result =
        build_derived_stream(stream.table(), inner_group.clone(), "stream_lift_elim/").await?;

    if outputs.is_empty() {
        set_default_in_place(&mut result, default_value);
    } else {
        set_default_in_place(&mut result, outputs[0].clone());
        for value in outputs.iter().skip(1) {
            push_value_in_place(&mut result, value.clone());
        }
        if let Some(last) = outputs.last() {
            set_default_in_place(&mut result, last.clone());
        }
    }

    Ok(result)
}
