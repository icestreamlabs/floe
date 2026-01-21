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
use crate::stream::util::{
    build_derived_stream, collect_values, push_value_in_place, set_default_in_place,
};

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
    let values = collect_values(stream, stream.current_time()).await?;
    let group = stream.group();
    let table = stream.table();

    let mut outputs = Vec::with_capacity(values.len());
    for value in &values {
        let mut introduced =
            stream_introduction(table.clone(), group.clone(), value.clone()).await?;
        introduced.flush().await?;
        outputs.push(introduced.handle());
    }

    let default_handle = if let Some(first) = outputs.first() {
        first.clone()
    } else {
        let identity = group.identity().await;
        let mut identity_stream =
            stream_introduction(table.clone(), group.clone(), identity).await?;
        identity_stream.flush().await?;
        identity_stream.handle()
    };

    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result = build_derived_stream(table, handle_group, "stream_lift_intro/").await?;

    if outputs.is_empty() {
        set_default_in_place(&mut result, default_handle);
    } else {
        set_default_in_place(&mut result, outputs[0].clone());
        for handle in outputs.iter().skip(1) {
            push_value_in_place(&mut result, handle.clone());
        }
        if let Some(last) = outputs.last() {
            set_default_in_place(&mut result, last.clone());
        }
    }

    Ok(result)
}
