use std::hash::Hash;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::storage::dictionary::KeyIntern;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::{SegmentRecord, VersionedZSet};

#[allow(dead_code)]
impl<K> VersionedZSet<K>
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
    pub async fn compact_current(&mut self) -> Result<u64>
    where
        K: Clone,
    {
        let previous_version = self.current_version;
        let view = self.materialize().await?;
        if view.is_empty() {
            return Err(anyhow!("cannot compact empty version"));
        }

        let mut deltas = Vec::with_capacity(view.len());
        for (key, weight) in view {
            let id = self
                .dict
                .intern(&key)
                .await
                .context("intern key during compaction")?;
            deltas.push((id, weight));
        }

        let record = SegmentRecord {
            id: self.allocate_segment_id(),
            bucket: 0,
            deltas,
        };

        let new_version = self
            .create_version_with_base(vec![record], None)
            .await
            .context("create compacted version")?;

        if previous_version != 0 {
            self.release_version(previous_version)
                .await
                .context("release previous version during compaction")?;
        }

        Ok(new_version)
    }
}
