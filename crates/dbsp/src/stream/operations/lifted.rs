use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::{
    build_derived_stream, collect_values, push_value_in_place, resolve_apply_handle_op,
    set_default_in_place,
};
use super::basic::{delay, differentiate, integrate, stream_elimination, stream_introduction};

pub async fn lifted_delay<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
) -> Result<Stream<StreamHandle>>
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
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { delay(&inner).await },
        "stream_lift_delay/",
    )
    .await
}

pub async fn lifted_integrate<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
) -> Result<Stream<StreamHandle>>
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
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { integrate(&inner).await },
        "stream_lift_integrate/",
    )
    .await
}

pub async fn lifted_differentiate<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
) -> Result<Stream<StreamHandle>>
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
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { differentiate(&inner).await },
        "stream_lift_differentiate/",
    )
    .await
}

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
        outputs.push(stream_elimination(&inner).await?);
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
