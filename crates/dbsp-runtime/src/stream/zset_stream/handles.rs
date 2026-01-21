use std::hash::Hash;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::handles::{ZSetHandle, ZSetHandleView};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::ZSetStream;
use super::super::roles::{DeltaHandleStream, SnapshotHandleStream};

impl<K> ZSetStream<K>
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
{
    pub fn current_handle(&self) -> &ZSetHandle {
        &self.current_handle
    }

    pub fn handle_view(&self, handle: &ZSetHandle) -> ZSetHandleView<K> {
        ZSetHandleView::new(
            self.versioned.dictionary(),
            self.versioned.table(),
            handle.ns.clone(),
            handle.version,
        )
    }

    pub fn latest_view(&self) -> ZSetHandleView<K> {
        self.handle_view(&self.current_handle)
    }

    pub fn handle_stream(&self) -> SnapshotHandleStream {
        SnapshotHandleStream::new(self.stream.clone())
    }

    pub fn delta_handle_stream(&self) -> DeltaHandleStream {
        DeltaHandleStream::new(self.delta_stream.clone())
    }

    /// Publishes an externally produced [`ZSetHandle`] into this stream
    /// without mutating the underlying [`VersionedZSet`].
    pub async fn publish_handle(&mut self, handle: ZSetHandle) -> Result<()> {
        self.current_handle = handle.clone();
        self.stream
            .send(handle.clone())
            .await
            .context("publish handle to stream")?;
        if self.stream.default_value() != handle {
            self.stream.set_default_in_place(handle.clone());
        }
        self.stream.flush().await.context("flush handle stream")?;
        Ok(())
    }
}
