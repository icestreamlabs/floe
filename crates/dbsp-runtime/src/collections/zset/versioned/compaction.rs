use std::collections::BTreeMap;
use std::hash::Hash;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::metrics;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::{SegmentRecord, VersionedZSet};

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
        let compaction_start = Instant::now();
        let previous_version = self.persisted_version;
        let new_version = self
            .compact_current_detached()
            .await
            .context("create compacted version")?;

        if previous_version != 0 {
            self.release_version(previous_version)
                .await
                .context("release previous version during compaction")?;
        }

        metrics::observe_foreground_compaction_latency_ms(
            compaction_start.elapsed().as_millis() as u64
        );
        Ok(new_version)
    }

    pub async fn compact_current_detached_segments(&mut self) -> Result<Vec<SegmentRecord>>
    where
        K: Clone,
    {
        if self.persisted_version == 0 {
            return Err(anyhow!("cannot compact empty version"));
        }

        let mut manifests = Vec::new();
        let mut cursor = Some(self.persisted_version);
        while let Some(version) = cursor {
            let manifest = if version == self.persisted_version {
                self.manifest
                    .clone()
                    .ok_or_else(|| anyhow!("missing current manifest for compaction"))?
            } else {
                self.load_manifest_record(version)
                    .await
                    .with_context(|| format!("load manifest {version} during compaction"))?
            };
            cursor = manifest.base;
            manifests.push(manifest);
        }
        manifests.reverse();

        let mut merged_by_bucket: BTreeMap<u16, BTreeMap<u64, i64>> = BTreeMap::new();
        for manifest in manifests {
            for (bucket, segments) in manifest.buckets {
                for segment_id in segments {
                    let record =
                        self.load_segment(bucket, segment_id)
                            .await
                            .with_context(|| {
                                format!(
                                    "load segment {segment_id} in bucket {bucket} during compaction"
                                )
                            })?;
                    let bucket_state = merged_by_bucket.entry(bucket).or_default();
                    for (key_id, delta) in record.deltas {
                        let next = bucket_state
                            .get(&key_id)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(delta);
                        if next == 0 {
                            bucket_state.remove(&key_id);
                        } else {
                            bucket_state.insert(key_id, next);
                        }
                    }
                }
            }
        }

        let mut compacted_segments = Vec::new();
        for (bucket, bucket_state) in merged_by_bucket {
            if bucket_state.is_empty() {
                continue;
            }
            compacted_segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas: bucket_state.into_iter().collect(),
            });
        }

        if compacted_segments.is_empty() {
            return Err(anyhow!("cannot compact empty version"));
        }

        Ok(compacted_segments)
    }

    pub async fn compact_current_detached(&mut self) -> Result<u64>
    where
        K: Clone,
    {
        let segments = self.compact_current_detached_segments().await?;
        self.create_version_with_base(segments, None)
            .await
            .context("create compacted version")
    }
}
