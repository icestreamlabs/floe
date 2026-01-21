use std::sync::Arc;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::Stream;

impl Stream<StreamHandle> {
    pub async fn resolve_handle<U>(
        &self,
        handle: &StreamHandle,
        group: Arc<dyn AbelianGroup<U>>,
    ) -> Result<Stream<U>>
    where
        U: Archive
            + Clone
            + PartialEq
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        U::Archived: RkyvDeserialize<U, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        Stream::open_at_with_table(self.table(), handle.ns.clone(), group, handle.frontier).await
    }

    pub async fn resolve_latest<U>(&mut self, group: Arc<dyn AbelianGroup<U>>) -> Result<Stream<U>>
    where
        U: Archive
            + Clone
            + PartialEq
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        U::Archived: RkyvDeserialize<U, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        let handle = self.latest().await?;
        self.resolve_handle(&handle, group).await
    }
}
