use std::hash::Hash;
use std::sync::Arc;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::util::{LIFTED_PROJECT_STREAM_PREFIX, apply_on_resolved_handles};

use super::super::zset::lifted_project_zset_stream;

pub async fn lifted_lifted_project_zset_stream<K, R, F>(
    input: &Stream<StreamHandle>,
    projector: F,
) -> Result<Stream<StreamHandle>>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&K) -> R + Send + Sync + Clone + 'static,
{
    let mut input_for_identity = input.clone();
    let first_handle = input_for_identity.get(0).await?;
    let inner_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: first_handle.ns,
        version: 0,
    }));
    apply_on_resolved_handles(
        input,
        inner_group,
        LIFTED_PROJECT_STREAM_PREFIX,
        move |inner| {
            let projector = projector.clone();
            async move { lifted_project_zset_stream::<K, R, _>(&inner, projector).await }
        },
    )
    .await
}
