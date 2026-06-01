use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::super::ZSET_PREFIX;
use super::{
    CompactionPolicy, SegmentId, VersionChainStats, VersionedZSet, VersionedZSetPersistence,
    ZSetVersionManifest,
};

impl CompactionPolicy {
    pub const fn disabled() -> Self {
        Self {
            max_chain_len: usize::MAX,
            max_segments: usize::MAX,
            max_bucket_segments: usize::MAX,
        }
    }

    pub fn is_disabled(self) -> bool {
        self.max_chain_len == usize::MAX
            && self.max_segments == usize::MAX
            && self.max_bucket_segments == usize::MAX
    }

    pub fn should_compact(self, stats: VersionChainStats) -> bool {
        stats.version_count >= self.max_chain_len
            || stats.segment_count >= self.max_segments
            || stats.max_bucket_segment_count >= self.max_bucket_segments
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            max_chain_len: 512,
            max_segments: 4096,
            max_bucket_segments: 512,
        }
    }
}

#[allow(dead_code)]
impl<K> VersionedZSet<K>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    /// Placeholder constructor for future implementation. The layout will bucket segments by the
    /// high bits of the interned key ID to keep manifest fan-out small while supporting efficient
    /// scans.
    pub async fn new(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
    ) -> Result<Self> {
        Self::new_with_persistence(dict, table, namespace, VersionedZSetPersistence::Immediate)
            .await
    }

    pub async fn new_with_persistence(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        persistence: VersionedZSetPersistence,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let mut manifest_prefix = ZSET_PREFIX.as_bytes().to_vec();
        manifest_prefix.extend_from_slice(namespace.as_bytes());
        manifest_prefix.extend_from_slice(b"/manifest_arrow/");

        let mut segment_prefix = ZSET_PREFIX.as_bytes().to_vec();
        segment_prefix.extend_from_slice(namespace.as_bytes());
        segment_prefix.extend_from_slice(b"/seg_arrow/");

        let mut intent_key = manifest_prefix.clone();
        intent_key.extend_from_slice(b"intent");

        let mut versioned = Self {
            dict,
            table,
            namespace,
            manifest_prefix,
            segment_prefix,
            current_version: 0,
            persisted_version: 0,
            intent_key,
            manifest: None,
            next_segment_id: 1,
            persistence,
        };

        versioned.refresh_state().await?;
        Ok(versioned)
    }

    pub async fn open_for_handle(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let instance = Self::new(dict, table, namespace.clone()).await?;
        if version != 0 {
            instance
                .load_manifest_record(version)
                .await
                .with_context(|| {
                    anyhow!(
                        "manifest version {version} not found for namespace {}",
                        namespace
                    )
                })?;
        }
        Ok(instance)
    }

    pub fn manifest(&self) -> Option<&ZSetVersionManifest> {
        self.manifest.as_ref()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn dictionary(&self) -> Arc<Dictionary<K>> {
        self.dict.clone()
    }

    pub(crate) fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }

    pub fn handle_for_version(&self, version: u64) -> ZSetHandle {
        ZSetHandle {
            ns: self.namespace.clone(),
            version,
        }
    }

    pub fn current_handle(&self) -> Option<ZSetHandle> {
        if self.current_version == 0 {
            None
        } else {
            Some(self.handle_for_version(self.current_version))
        }
    }

    pub fn persisted_handle(&self) -> Option<ZSetHandle> {
        if self.persisted_version == 0 {
            None
        } else {
            Some(self.handle_for_version(self.persisted_version))
        }
    }

    pub fn persistence(&self) -> VersionedZSetPersistence {
        self.persistence
    }

    pub fn enable_replayable_persistence(&mut self) {
        self.persistence = VersionedZSetPersistence::Replayable;
    }

    pub fn uses_replayable_persistence(&self) -> bool {
        matches!(self.persistence, VersionedZSetPersistence::Replayable)
    }

    #[cfg(test)]
    pub(crate) fn manifest_prefix_bytes(&self) -> &[u8] {
        &self.manifest_prefix
    }

    #[cfg(test)]
    pub(crate) fn segment_prefix_bytes(&self) -> &[u8] {
        &self.segment_prefix
    }

    #[cfg(test)]
    pub(crate) async fn manifest_record(&self, version: u64) -> Result<ZSetVersionManifest> {
        self.load_manifest_record(version).await
    }

    #[cfg(test)]
    pub(crate) fn intent_key_bytes(&self) -> &[u8] {
        &self.intent_key
    }

    pub(crate) async fn adopt_persisted_version(&mut self, version: u64) -> Result<()> {
        if version == 0 {
            self.current_version = 0;
            self.persisted_version = 0;
            self.manifest = None;
            return Ok(());
        }

        let manifest = self.load_manifest_record(version).await?;
        let next_segment_id = manifest
            .buckets
            .values()
            .flat_map(|segments| segments.iter().copied())
            .max()
            .map(|id| id.saturating_add(1))
            .unwrap_or(1);
        self.current_version = version;
        self.persisted_version = version;
        self.next_segment_id = self.next_segment_id.max(next_segment_id);
        self.manifest = Some(manifest);
        Ok(())
    }

    pub(super) fn allocate_segment_id(&mut self) -> SegmentId {
        let id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        id
    }
}

#[cfg(test)]
mod tests {
    use super::{CompactionPolicy, VersionChainStats};

    #[test]
    fn compaction_policy_triggers_on_bucket_depth() {
        let policy = CompactionPolicy {
            max_chain_len: usize::MAX,
            max_segments: usize::MAX,
            max_bucket_segments: 3,
        };
        let stats = VersionChainStats {
            version_count: 1,
            segment_count: 2,
            max_bucket_segment_count: 3,
        };
        assert!(policy.should_compact(stats));
    }
}
