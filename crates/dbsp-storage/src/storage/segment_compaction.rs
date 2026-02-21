use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};

use crate::storage::KeyValueTable;
use crate::storage::manifest::{DataManifest, ManifestStatistics, ManifestStore};
use crate::storage::segment::{ArrowSegmentStore, SegmentWriteStats};

#[derive(Debug, Clone, Copy)]
pub struct SegmentCompactionPolicy {
    pub min_input_segments: usize,
}

impl Default for SegmentCompactionPolicy {
    fn default() -> Self {
        Self {
            min_input_segments: 2,
        }
    }
}

pub struct SegmentCompactor {
    segment_store: ArrowSegmentStore,
    manifest_store: ManifestStore<DataManifest>,
    policy: SegmentCompactionPolicy,
}

impl SegmentCompactor {
    pub fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        policy: SegmentCompactionPolicy,
    ) -> Self {
        let namespace = namespace.into();
        Self {
            segment_store: ArrowSegmentStore::new(table.clone(), namespace.clone()),
            manifest_store: ManifestStore::<DataManifest>::data(table, namespace),
            policy,
        }
    }

    /// Compacts all currently visible segments if policy thresholds are met.
    pub async fn compact_once(&self) -> Result<Option<DataManifest>> {
        let segment_ids = self
            .segment_store
            .list_segment_ids()
            .await
            .context("list segment ids for compaction")?;
        if segment_ids.len() < self.policy.min_input_segments {
            return Ok(None);
        }
        let manifest = self
            .compact_segments(&segment_ids)
            .await
            .context("compact candidate segment set")?;
        Ok(Some(manifest))
    }

    /// Performs copy-on-write segment compaction and atomically publishes a replacement manifest.
    pub async fn compact_segments(&self, input_segment_ids: &[u64]) -> Result<DataManifest> {
        if input_segment_ids.len() < 2 {
            bail!("segment compaction requires at least two input segments");
        }

        let mut input_segments = Vec::with_capacity(input_segment_ids.len());
        for segment_id in input_segment_ids {
            let segment = self
                .segment_store
                .read_segment(*segment_id)
                .await
                .with_context(|| format!("load input segment {segment_id}"))?
                .ok_or_else(|| anyhow!("input segment {segment_id} not found"))?;
            input_segments.push(segment);
        }

        let schema = input_segments[0].schema.clone();
        for segment in &input_segments[1..] {
            if segment.schema.as_ref() != schema.as_ref() {
                bail!("all segments must have the same Arrow schema for compaction");
            }
        }

        let output_segment_id = self.next_segment_id().await?;
        let mut merged_batches = Vec::new();
        let mut total_rows = 0_u64;
        let mut total_bytes = 0_u64;
        let mut min_key_hash = u64::MAX;
        let mut max_key_hash = 0_u64;
        let mut weighted_tombstones = 0.0_f64;

        for segment in &input_segments {
            merged_batches.extend(segment.batches.clone());
            total_rows = total_rows.saturating_add(segment.metadata.row_count);
            total_bytes = total_bytes.saturating_add(segment.metadata.byte_size);
            min_key_hash = min_key_hash.min(segment.metadata.min_key_hash);
            max_key_hash = max_key_hash.max(segment.metadata.max_key_hash);
            weighted_tombstones +=
                segment.metadata.tombstone_ratio * segment.metadata.row_count as f64;
        }

        let tombstone_ratio = if total_rows == 0 {
            0.0
        } else {
            weighted_tombstones / total_rows as f64
        };
        let stats = SegmentWriteStats::new(min_key_hash, max_key_hash, tombstone_ratio)
            .context("build compacted segment statistics")?;
        self.segment_store
            .write_segment(output_segment_id, schema, &merged_batches, stats)
            .await
            .with_context(|| format!("write compacted segment {output_segment_id}"))?;

        let latest = self
            .manifest_store
            .latest_manifest()
            .await
            .context("load latest data manifest before compaction publish")?;
        let base = latest.as_ref().map(|manifest| manifest.version);
        let next_version = base.unwrap_or(0).saturating_add(1);

        let mut next_segments = latest
            .map(|manifest| manifest.segments)
            .unwrap_or_else(|| input_segment_ids.to_vec());
        let to_remove: HashSet<u64> = input_segment_ids.iter().copied().collect();
        next_segments.retain(|segment_id| !to_remove.contains(segment_id));
        next_segments.push(output_segment_id);
        next_segments.sort_unstable();
        next_segments.dedup();

        let statistics = self
            .manifest_statistics(&next_segments)
            .await
            .context("compute replacement manifest statistics")?;
        let manifest = DataManifest {
            version: next_version,
            base,
            reference_count: 1,
            statistics,
            segments: next_segments,
        };

        self.manifest_store
            .publish_manifest(&manifest)
            .await
            .context("publish compacted data manifest")?;

        Ok(manifest)
    }

    async fn next_segment_id(&self) -> Result<u64> {
        let existing = self
            .segment_store
            .list_segment_ids()
            .await
            .context("scan existing segments for id allocation")?;
        Ok(existing.into_iter().max().unwrap_or(0).saturating_add(1))
    }

    async fn manifest_statistics(&self, segment_ids: &[u64]) -> Result<ManifestStatistics> {
        let mut row_count = 0_u64;
        let mut total_bytes = 0_u64;
        let mut weighted_tombstones = 0.0_f64;

        for segment_id in segment_ids {
            let segment = self
                .segment_store
                .read_segment(*segment_id)
                .await
                .with_context(|| format!("load segment {segment_id} for manifest statistics"))?
                .ok_or_else(|| anyhow!("segment {segment_id} missing for manifest statistics"))?;
            row_count = row_count.saturating_add(segment.metadata.row_count);
            total_bytes = total_bytes.saturating_add(segment.metadata.byte_size);
            weighted_tombstones +=
                segment.metadata.tombstone_ratio * segment.metadata.row_count as f64;
        }

        let tombstone_ratio = if row_count == 0 {
            0.0
        } else {
            weighted_tombstones / row_count as f64
        };
        ManifestStatistics::new(
            segment_ids.len() as u64,
            row_count,
            total_bytes,
            tombstone_ratio,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use object_store::memory::InMemory;
    use slatedb::Db;

    use crate::storage::SlateTable;
    use crate::storage::manifest::{DataManifest, ManifestStatistics, ManifestStore};
    use crate::storage::segment::{ArrowSegmentStore, SegmentWriteStats};

    use super::{SegmentCompactionPolicy, SegmentCompactor};

    async fn build_table(name: &str) -> Arc<dyn crate::storage::KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    fn row_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key_hash", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ]))
    }

    fn batch(schema: SchemaRef, rows: &[(u64, i64, i64)]) -> RecordBatch {
        let hashes: Vec<u64> = rows.iter().map(|(hash, _, _)| *hash).collect();
        let values: Vec<i64> = rows.iter().map(|(_, value, _)| *value).collect();
        let deltas: Vec<i64> = rows.iter().map(|(_, _, delta)| *delta).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(hashes)) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
                Arc::new(Int64Array::from(deltas)) as ArrayRef,
            ],
        )
        .expect("build batch")
    }

    #[tokio::test]
    async fn compacts_segments_and_publishes_replacement_manifest() {
        let table = build_table("segment-compactor-basic").await;
        let namespace = "segment-compactor-basic";
        let segment_store = ArrowSegmentStore::new(table.clone(), namespace);
        let manifest_store = ManifestStore::<DataManifest>::data(table.clone(), namespace);
        let schema = row_schema();

        segment_store
            .write_segment(
                1,
                Arc::clone(&schema),
                &[batch(Arc::clone(&schema), &[(1, 10, 1)])],
                SegmentWriteStats::new(1, 1, 0.0).expect("stats"),
            )
            .await
            .expect("write segment one");
        segment_store
            .write_segment(
                2,
                Arc::clone(&schema),
                &[batch(Arc::clone(&schema), &[(2, 20, 1)])],
                SegmentWriteStats::new(2, 2, 0.0).expect("stats"),
            )
            .await
            .expect("write segment two");

        manifest_store
            .publish_manifest(&DataManifest {
                version: 1,
                base: None,
                reference_count: 1,
                statistics: ManifestStatistics::new(2, 2, 0, 0.0).expect("stats"),
                segments: vec![1, 2],
            })
            .await
            .expect("publish initial manifest");

        let compactor =
            SegmentCompactor::new(table.clone(), namespace, SegmentCompactionPolicy::default());
        let replacement = compactor
            .compact_segments(&[1, 2])
            .await
            .expect("compact segments");

        assert_eq!(replacement.version, 2);
        assert_eq!(replacement.base, Some(1));
        assert_eq!(replacement.segments, vec![3]);
        assert!(
            segment_store
                .read_segment(1)
                .await
                .expect("read old segment")
                .is_some(),
            "old segment should remain for snapshot isolation"
        );
        assert!(
            segment_store
                .read_segment(2)
                .await
                .expect("read old segment")
                .is_some(),
            "old segment should remain for snapshot isolation"
        );
        assert!(
            segment_store
                .read_segment(3)
                .await
                .expect("read compacted segment")
                .is_some(),
            "new compacted segment should exist"
        );
    }

    #[tokio::test]
    async fn compact_once_respects_policy_threshold() {
        let table = build_table("segment-compactor-policy").await;
        let namespace = "segment-compactor-policy";
        let segment_store = ArrowSegmentStore::new(table.clone(), namespace);
        let schema = row_schema();
        segment_store
            .write_segment(
                1,
                Arc::clone(&schema),
                &[batch(schema, &[(1, 10, 1)])],
                SegmentWriteStats::new(1, 1, 0.0).expect("stats"),
            )
            .await
            .expect("write segment");

        let compactor = SegmentCompactor::new(
            table,
            namespace,
            SegmentCompactionPolicy {
                min_input_segments: 2,
            },
        );
        let outcome = compactor.compact_once().await.expect("compact once");
        assert!(outcome.is_none());
    }
}
