use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::dictionary::Dictionary;
use std::hash::Hash;
use std::sync::Arc;

/// Canonical relation state at a logical time boundary.
pub struct RelationState<K>
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
    /// Integrated `R_t` for the relation.
    pub integrated: VersionedZSet<K>,
    /// Handle pointing at the integrated version.
    pub latest_handle: ZSetHandle,
}

impl<K> RelationState<K>
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
    pub fn update(&mut self, integrated: VersionedZSet<K>, handle: ZSetHandle) {
        self.integrated = integrated;
        self.latest_handle = handle;
    }

    pub fn update_handle(&mut self, handle: ZSetHandle) {
        self.latest_handle = handle;
    }

    pub fn dictionary(&self) -> Arc<Dictionary<K>> {
        self.integrated.dictionary()
    }
}
