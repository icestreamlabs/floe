use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use slatedb::config::ScanOptions;
use xxhash_rust::xxh3::xxh3_64;

use crate::storage::KeyValueTable;
use crate::storage::encoding;
use crate::storage::keyspace;
use crate::storage::manifest::{
    DataManifest, IndexManifest, IntentRecoveryOutcome, ManifestLayer, ManifestStore,
};
use crate::storage::segment::ArrowSegmentStore;

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct ManifestReference {
    pub layer: ManifestLayer,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct ManifestPin {
    pub pin_id: String,
    pub pinned_at_ms: u64,
    pub manifests: Vec<ManifestReference>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReachabilityGraph {
    pub data_manifest_versions: BTreeSet<u64>,
    pub index_manifest_versions: BTreeSet<u64>,
    pub data_segments: BTreeSet<u64>,
    pub index_l0_segments: BTreeSet<u64>,
    pub index_l1_blocks: BTreeSet<u64>,
}

pub struct ManifestPinStore {
    table: Arc<dyn KeyValueTable>,
    pin_prefix: Vec<u8>,
}

impl ManifestPinStore {
    pub fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self {
            table,
            pin_prefix: keyspace::gc_pin_prefix(&namespace.into()),
        }
    }

    pub async fn pin(
        &self,
        pin_id: impl Into<String>,
        manifests: Vec<ManifestReference>,
    ) -> Result<()> {
        let pin_id = pin_id.into();
        let record = ManifestPin {
            pin_id: pin_id.clone(),
            pinned_at_ms: unix_epoch_millis(),
            manifests,
        };
        let encoded = encoding::encode(&record).context("encode manifest pin record")?;
        self.table
            .put(&self.pin_key(&pin_id), &encoded)
            .await
            .with_context(|| format!("persist manifest pin '{pin_id}'"))
    }

    pub async fn unpin(&self, pin_id: &str) -> Result<()> {
        self.table
            .delete(&self.pin_key(pin_id))
            .await
            .with_context(|| format!("delete manifest pin '{pin_id}'"))
    }

    pub async fn list_pins(&self) -> Result<Vec<ManifestPin>> {
        let entries = self
            .table
            .scan_prefix(&self.pin_prefix, &ScanOptions::default())
            .await
            .context("scan manifest pins")?;
        let mut pins = Vec::with_capacity(entries.len());
        for (_, bytes) in entries {
            pins.push(
                encoding::decode::<ManifestPin>(&bytes).context("decode manifest pin record")?,
            );
        }
        pins.sort_by(|left, right| left.pin_id.cmp(&right.pin_id));
        Ok(pins)
    }

    fn pin_key(&self, pin_id: &str) -> Vec<u8> {
        let mut key = self.pin_prefix.clone();
        key.extend_from_slice(pin_id.as_bytes());
        key
    }
}

pub struct ManifestReachabilityTracker {
    data_manifest_store: ManifestStore<DataManifest>,
    index_manifest_store: ManifestStore<IndexManifest>,
    pin_store: ManifestPinStore,
}

impl ManifestReachabilityTracker {
    pub fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        Self {
            data_manifest_store: ManifestStore::<DataManifest>::data(
                table.clone(),
                namespace.clone(),
            ),
            index_manifest_store: ManifestStore::<IndexManifest>::index(
                table.clone(),
                namespace.clone(),
            ),
            pin_store: ManifestPinStore::new(table, namespace),
        }
    }

    pub async fn pin_manifest(
        &self,
        pin_id: impl Into<String>,
        manifests: Vec<ManifestReference>,
    ) -> Result<()> {
        self.pin_store.pin(pin_id, manifests).await
    }

    pub async fn unpin_manifest(&self, pin_id: &str) -> Result<()> {
        self.pin_store.unpin(pin_id).await
    }

    pub async fn list_pins(&self) -> Result<Vec<ManifestPin>> {
        self.pin_store.list_pins().await
    }

    pub async fn compute_reachability(&self) -> Result<ReachabilityGraph> {
        let mut graph = ReachabilityGraph::default();

        if let Some(latest) = self
            .data_manifest_store
            .latest_manifest()
            .await
            .context("load latest data manifest for reachability")?
        {
            self.collect_data_chain(latest.version, &mut graph).await?;
        }

        if let Some(latest) = self
            .index_manifest_store
            .latest_manifest()
            .await
            .context("load latest index manifest for reachability")?
        {
            self.collect_index_chain(latest.version, &mut graph).await?;
        }

        for pin in self.pin_store.list_pins().await? {
            for reference in pin.manifests {
                match reference.layer {
                    ManifestLayer::Data => {
                        self.collect_data_chain(reference.version, &mut graph)
                            .await?
                    }
                    ManifestLayer::Index => {
                        self.collect_index_chain(reference.version, &mut graph)
                            .await?
                    }
                }
            }
        }

        Ok(graph)
    }

    async fn collect_data_chain(
        &self,
        start_version: u64,
        graph: &mut ReachabilityGraph,
    ) -> Result<()> {
        let mut current = Some(start_version);
        while let Some(version) = current {
            if !graph.data_manifest_versions.insert(version) {
                break;
            }
            let Some(manifest) = self
                .data_manifest_store
                .load_manifest(version)
                .await
                .with_context(|| format!("load data manifest {version}"))?
            else {
                break;
            };
            for segment_id in manifest.segments {
                graph.data_segments.insert(segment_id);
            }
            current = manifest.base;
        }
        Ok(())
    }

    async fn collect_index_chain(
        &self,
        start_version: u64,
        graph: &mut ReachabilityGraph,
    ) -> Result<()> {
        let mut current = Some(start_version);
        while let Some(version) = current {
            if !graph.index_manifest_versions.insert(version) {
                break;
            }
            let Some(manifest) = self
                .index_manifest_store
                .load_manifest(version)
                .await
                .with_context(|| format!("load index manifest {version}"))?
            else {
                break;
            };
            for segment in manifest.l0_segments {
                graph.index_l0_segments.insert(segment);
            }
            for block in manifest.l1_blocks {
                graph.index_l1_blocks.insert(block);
            }
            current = manifest.base;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcPolicy {
    pub grace_period: Duration,
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self {
            grace_period: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepStats {
    pub marked: usize,
    pub deleted: usize,
    pub skipped_reachable: usize,
    pub recovered_intents: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
enum ArtifactKind {
    DataManifest,
    IndexManifest,
    DataSegment,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct ArtifactTombstone {
    artifact_key: Vec<u8>,
    marked_at_ms: u64,
    kind: ArtifactKind,
}

pub struct GcService {
    table: Arc<dyn KeyValueTable>,
    data_manifest_store: ManifestStore<DataManifest>,
    index_manifest_store: ManifestStore<IndexManifest>,
    segment_store: ArrowSegmentStore,
    tracker: ManifestReachabilityTracker,
    tombstone_prefix: Vec<u8>,
    policy: GcPolicy,
}

impl GcService {
    pub fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        policy: GcPolicy,
    ) -> Self {
        let namespace = namespace.into();
        Self {
            data_manifest_store: ManifestStore::<DataManifest>::data(
                table.clone(),
                namespace.clone(),
            ),
            index_manifest_store: ManifestStore::<IndexManifest>::index(
                table.clone(),
                namespace.clone(),
            ),
            segment_store: ArrowSegmentStore::new(table.clone(), namespace.clone()),
            tracker: ManifestReachabilityTracker::new(table.clone(), namespace.clone()),
            tombstone_prefix: keyspace::gc_tombstone_prefix(&namespace),
            table,
            policy,
        }
    }

    pub async fn pin_manifest(
        &self,
        pin_id: impl Into<String>,
        manifests: Vec<ManifestReference>,
    ) -> Result<()> {
        self.tracker.pin_manifest(pin_id, manifests).await
    }

    pub async fn unpin_manifest(&self, pin_id: &str) -> Result<()> {
        self.tracker.unpin_manifest(pin_id).await
    }

    pub async fn list_pins(&self) -> Result<Vec<ManifestPin>> {
        self.tracker.list_pins().await
    }

    pub async fn compute_reachability(&self) -> Result<ReachabilityGraph> {
        self.tracker.compute_reachability().await
    }

    pub async fn recover_startup(&self) -> Result<(ReachabilityGraph, usize)> {
        let recovered_intents = self
            .recover_manifest_intents()
            .await
            .context("recover intents on startup")?;
        let graph = self
            .tracker
            .compute_reachability()
            .await
            .context("refresh reachability graph on startup")?;
        Ok((graph, recovered_intents))
    }

    pub async fn sweep_once(&self) -> Result<SweepStats> {
        let recovered_intents = self
            .recover_manifest_intents()
            .await
            .context("recover intents before sweep")?;
        let graph = self
            .tracker
            .compute_reachability()
            .await
            .context("compute reachability for sweep")?;

        let now_ms = unix_epoch_millis();
        let grace_ms = self
            .policy
            .grace_period
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let mut marked = 0_usize;

        for version in self
            .data_manifest_store
            .list_versions()
            .await
            .context("list data manifests for sweep")?
        {
            if graph.data_manifest_versions.contains(&version) {
                continue;
            }
            let artifact_key = self.data_manifest_store.key_for_version(version);
            if self
                .ensure_tombstone(&artifact_key, ArtifactKind::DataManifest, now_ms)
                .await?
            {
                marked += 1;
            }
        }

        for version in self
            .index_manifest_store
            .list_versions()
            .await
            .context("list index manifests for sweep")?
        {
            if graph.index_manifest_versions.contains(&version) {
                continue;
            }
            let artifact_key = self.index_manifest_store.key_for_version(version);
            if self
                .ensure_tombstone(&artifact_key, ArtifactKind::IndexManifest, now_ms)
                .await?
            {
                marked += 1;
            }
        }

        for segment_id in self
            .segment_store
            .list_segment_ids()
            .await
            .context("list segments for sweep")?
        {
            if graph.data_segments.contains(&segment_id) {
                continue;
            }
            let artifact_key = self.segment_store.key_for_segment(segment_id);
            if self
                .ensure_tombstone(&artifact_key, ArtifactKind::DataSegment, now_ms)
                .await?
            {
                marked += 1;
            }
        }

        let reachable_keys = self.reachable_artifact_keys(&graph);
        let tombstones = self.load_tombstones().await?;
        let mut deleted = 0_usize;
        let mut skipped_reachable = 0_usize;
        for (tombstone_key, tombstone) in tombstones {
            if reachable_keys.contains(&tombstone.artifact_key) {
                self.table
                    .delete(&tombstone_key)
                    .await
                    .context("clear tombstone for re-reachable artifact")?;
                skipped_reachable += 1;
                continue;
            }

            if now_ms.saturating_sub(tombstone.marked_at_ms) < grace_ms {
                continue;
            }

            self.table
                .delete(&tombstone.artifact_key)
                .await
                .context("delete unreachable artifact")?;
            self.table
                .delete(&tombstone_key)
                .await
                .context("delete processed tombstone")?;
            deleted += 1;
        }

        Ok(SweepStats {
            marked,
            deleted,
            skipped_reachable,
            recovered_intents,
        })
    }

    async fn recover_manifest_intents(&self) -> Result<usize> {
        let mut recovered = 0_usize;
        let data_recovery = self
            .data_manifest_store
            .recover_publish_intent()
            .await
            .context("recover data manifest intent")?;
        if !matches!(data_recovery, IntentRecoveryOutcome::NoPendingIntent) {
            recovered += 1;
        }

        let index_recovery = self
            .index_manifest_store
            .recover_publish_intent()
            .await
            .context("recover index manifest intent")?;
        if !matches!(index_recovery, IntentRecoveryOutcome::NoPendingIntent) {
            recovered += 1;
        }

        Ok(recovered)
    }

    async fn ensure_tombstone(
        &self,
        artifact_key: &[u8],
        kind: ArtifactKind,
        now_ms: u64,
    ) -> Result<bool> {
        let tombstone_key = self.tombstone_key(artifact_key);
        if self
            .table
            .get(&tombstone_key)
            .await
            .context("lookup tombstone key")?
            .is_some()
        {
            return Ok(false);
        }

        let tombstone = ArtifactTombstone {
            artifact_key: artifact_key.to_vec(),
            marked_at_ms: now_ms,
            kind,
        };
        let encoded = encoding::encode(&tombstone).context("encode tombstone record")?;
        self.table
            .put(&tombstone_key, &encoded)
            .await
            .context("persist tombstone")?;
        Ok(true)
    }

    async fn load_tombstones(&self) -> Result<Vec<(Vec<u8>, ArtifactTombstone)>> {
        let entries = self
            .table
            .scan_prefix(&self.tombstone_prefix, &ScanOptions::default())
            .await
            .context("scan tombstones")?;
        let mut tombstones = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let tombstone =
                encoding::decode::<ArtifactTombstone>(&value).context("decode tombstone")?;
            tombstones.push((key, tombstone));
        }
        Ok(tombstones)
    }

    fn reachable_artifact_keys(&self, graph: &ReachabilityGraph) -> HashSet<Vec<u8>> {
        let mut keys = HashSet::new();
        for version in &graph.data_manifest_versions {
            keys.insert(self.data_manifest_store.key_for_version(*version));
        }
        for version in &graph.index_manifest_versions {
            keys.insert(self.index_manifest_store.key_for_version(*version));
        }
        for segment_id in &graph.data_segments {
            keys.insert(self.segment_store.key_for_segment(*segment_id));
        }
        keys
    }

    fn tombstone_key(&self, artifact_key: &[u8]) -> Vec<u8> {
        keyspace::key_with_u64(&self.tombstone_prefix, xxh3_64(artifact_key))
    }
}

fn unix_epoch_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u128::from(u64::MAX)) as u64,
        Err(_) => 0,
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
    use crate::storage::manifest::ManifestStatistics;
    use crate::storage::segment::SegmentWriteStats;

    use super::*;

    async fn build_table(name: &str) -> Arc<dyn crate::storage::KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    fn stats(object_count: u64) -> ManifestStatistics {
        ManifestStatistics::new(object_count, object_count, object_count * 16, 0.0)
            .expect("manifest statistics")
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
    async fn pin_state_persists_across_restart() {
        let table = build_table("gc-pins-restart").await;
        let namespace = "gc-pins-restart";

        let tracker = ManifestReachabilityTracker::new(table.clone(), namespace);
        tracker
            .pin_manifest(
                "reader-1",
                vec![ManifestReference {
                    layer: ManifestLayer::Data,
                    version: 7,
                }],
            )
            .await
            .expect("pin manifest");

        let reopened = ManifestReachabilityTracker::new(table, namespace);
        let pins = reopened.list_pins().await.expect("list persisted pins");
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].pin_id, "reader-1");
        assert_eq!(pins[0].manifests.len(), 1);
        assert_eq!(pins[0].manifests[0].layer, ManifestLayer::Data);
        assert_eq!(pins[0].manifests[0].version, 7);
    }

    #[tokio::test]
    async fn reachability_includes_heads_and_pinned_roots() {
        let table = build_table("gc-reachability").await;
        let namespace = "gc-reachability";

        let data_store = ManifestStore::<DataManifest>::data(table.clone(), namespace);
        data_store
            .publish_manifest(&DataManifest {
                version: 1,
                base: None,
                reference_count: 1,
                statistics: stats(1),
                segments: vec![10],
            })
            .await
            .expect("publish data manifest v1");
        data_store
            .publish_manifest(&DataManifest {
                version: 2,
                base: None,
                reference_count: 1,
                statistics: stats(1),
                segments: vec![20],
            })
            .await
            .expect("publish data manifest v2");

        let index_store = ManifestStore::<IndexManifest>::index(table.clone(), namespace);
        index_store
            .publish_manifest(&IndexManifest {
                version: 1,
                base: None,
                reference_count: 1,
                statistics: stats(2),
                l0_segments: vec![100],
                l1_blocks: vec![200],
            })
            .await
            .expect("publish index manifest v1");
        index_store
            .publish_manifest(&IndexManifest {
                version: 2,
                base: None,
                reference_count: 1,
                statistics: stats(2),
                l0_segments: vec![110],
                l1_blocks: vec![210],
            })
            .await
            .expect("publish index manifest v2");

        let tracker = ManifestReachabilityTracker::new(table, namespace);
        tracker
            .pin_manifest(
                "reader",
                vec![
                    ManifestReference {
                        layer: ManifestLayer::Data,
                        version: 1,
                    },
                    ManifestReference {
                        layer: ManifestLayer::Index,
                        version: 1,
                    },
                ],
            )
            .await
            .expect("pin older manifests");

        let graph = tracker
            .compute_reachability()
            .await
            .expect("compute reachability");

        assert_eq!(
            graph.data_manifest_versions,
            BTreeSet::from_iter([1_u64, 2_u64])
        );
        assert_eq!(graph.data_segments, BTreeSet::from_iter([10_u64, 20_u64]));
        assert_eq!(
            graph.index_manifest_versions,
            BTreeSet::from_iter([1_u64, 2_u64])
        );
        assert_eq!(
            graph.index_l0_segments,
            BTreeSet::from_iter([100_u64, 110_u64])
        );
        assert_eq!(
            graph.index_l1_blocks,
            BTreeSet::from_iter([200_u64, 210_u64])
        );
    }

    #[tokio::test]
    async fn unpin_removes_manifest_pin() {
        let table = build_table("gc-pin-delete").await;
        let namespace = "gc-pin-delete";
        let tracker = ManifestReachabilityTracker::new(table, namespace);
        tracker
            .pin_manifest(
                "reader-2",
                vec![ManifestReference {
                    layer: ManifestLayer::Index,
                    version: 3,
                }],
            )
            .await
            .expect("pin manifest");
        tracker
            .unpin_manifest("reader-2")
            .await
            .expect("unpin manifest");
        assert!(
            tracker.list_pins().await.expect("list pins").is_empty(),
            "pin list should be empty after unpin"
        );
    }

    #[tokio::test]
    async fn sweep_reclaims_unreachable_artifacts_after_unpin() {
        let table = build_table("gc-sweep").await;
        let namespace = "gc-sweep";
        let segment_store = ArrowSegmentStore::new(table.clone(), namespace);
        let data_store = ManifestStore::<DataManifest>::data(table.clone(), namespace);
        let schema = row_schema();

        segment_store
            .write_segment(
                10,
                Arc::clone(&schema),
                &[batch(Arc::clone(&schema), &[(1, 10, 1)])],
                SegmentWriteStats::new(1, 1, 0.0).expect("stats"),
            )
            .await
            .expect("write segment 10");
        segment_store
            .write_segment(
                20,
                Arc::clone(&schema),
                &[batch(schema, &[(2, 20, 1)])],
                SegmentWriteStats::new(2, 2, 0.0).expect("stats"),
            )
            .await
            .expect("write segment 20");

        data_store
            .publish_manifest(&DataManifest {
                version: 1,
                base: None,
                reference_count: 1,
                statistics: stats(1),
                segments: vec![10],
            })
            .await
            .expect("publish data manifest v1");
        data_store
            .publish_manifest(&DataManifest {
                version: 2,
                base: None,
                reference_count: 1,
                statistics: stats(1),
                segments: vec![20],
            })
            .await
            .expect("publish data manifest v2");

        let service = GcService::new(
            table.clone(),
            namespace,
            GcPolicy {
                grace_period: Duration::ZERO,
            },
        );
        service
            .pin_manifest(
                "reader",
                vec![ManifestReference {
                    layer: ManifestLayer::Data,
                    version: 1,
                }],
            )
            .await
            .expect("pin old manifest");

        let first = service.sweep_once().await.expect("run first sweep");
        assert_eq!(first.deleted, 0, "pinned manifest should not be reclaimed");

        service
            .unpin_manifest("reader")
            .await
            .expect("unpin old manifest");
        let second = service.sweep_once().await.expect("run second sweep");
        assert!(
            second.deleted >= 2,
            "manifest and segment should be reclaimed"
        );

        assert!(
            segment_store
                .read_segment(10)
                .await
                .expect("read reclaimed segment")
                .is_none(),
            "unreachable segment should be deleted"
        );
        assert!(
            data_store
                .load_manifest(1)
                .await
                .expect("read reclaimed manifest")
                .is_none(),
            "unreachable manifest should be deleted"
        );
        assert!(
            data_store
                .load_manifest(2)
                .await
                .expect("read live manifest")
                .is_some(),
            "latest manifest should remain"
        );
    }

    #[tokio::test]
    async fn sweep_cleans_stale_manifest_intents() {
        let table = build_table("gc-stale-intents").await;
        let namespace = "gc-stale-intents";
        let data_store = ManifestStore::<DataManifest>::data(table.clone(), namespace);
        let index_store = ManifestStore::<IndexManifest>::index(table.clone(), namespace);

        data_store
            .begin_publish_intent(7)
            .await
            .expect("write data intent");
        index_store
            .begin_publish_intent(11)
            .await
            .expect("write index intent");

        let service = GcService::new(
            table,
            namespace,
            GcPolicy {
                grace_period: Duration::ZERO,
            },
        );
        let stats = service.sweep_once().await.expect("run sweep");
        assert_eq!(stats.recovered_intents, 2);

        assert_eq!(
            data_store
                .pending_intent_version()
                .await
                .expect("read data intent"),
            None
        );
        assert_eq!(
            index_store
                .pending_intent_version()
                .await
                .expect("read index intent"),
            None
        );
    }

    #[tokio::test]
    async fn recover_startup_handles_committed_intent_after_crash() {
        let table = build_table("gc-recover-committed").await;
        let namespace = "gc-recover-committed";
        let data_store = ManifestStore::<DataManifest>::data(table.clone(), namespace);

        data_store
            .publish_manifest(&DataManifest {
                version: 1,
                base: None,
                reference_count: 1,
                statistics: stats(1),
                segments: vec![10],
            })
            .await
            .expect("publish baseline manifest");

        let pending = DataManifest {
            version: 2,
            base: Some(1),
            reference_count: 1,
            statistics: stats(1),
            segments: vec![20],
        };
        data_store
            .begin_publish_intent(pending.version)
            .await
            .expect("write publish intent");
        data_store
            .commit_manifest(&pending)
            .await
            .expect("commit manifest without finalize");

        let recovered = GcService::new(table, namespace, GcPolicy::default());
        let (graph, recovered_intents) = recovered
            .recover_startup()
            .await
            .expect("recover startup state");

        assert_eq!(recovered_intents, 1);
        assert_eq!(
            graph.data_manifest_versions,
            BTreeSet::from_iter([1_u64, 2_u64])
        );
        assert_eq!(graph.data_segments, BTreeSet::from_iter([10_u64, 20_u64]));
    }

    #[tokio::test]
    async fn recover_startup_clears_orphaned_intent_after_crash() {
        let table = build_table("gc-recover-orphan").await;
        let namespace = "gc-recover-orphan";
        let data_store = ManifestStore::<DataManifest>::data(table.clone(), namespace);
        data_store
            .begin_publish_intent(99)
            .await
            .expect("write orphan intent");

        let recovered = GcService::new(table, namespace, GcPolicy::default());
        let (_, recovered_intents) = recovered
            .recover_startup()
            .await
            .expect("recover startup state");
        assert_eq!(recovered_intents, 1);
        assert_eq!(
            data_store
                .pending_intent_version()
                .await
                .expect("read pending intent"),
            None
        );
        assert!(
            data_store
                .latest_manifest()
                .await
                .expect("load latest manifest")
                .is_none(),
            "orphan intent should not create a manifest"
        );
    }
}
