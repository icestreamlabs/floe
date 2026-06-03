use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use super::table::prefix_bounds;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::keyspace;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum ManifestLayer {
    Data,
    Index,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct ManifestStatistics {
    pub object_count: u64,
    pub row_count: u64,
    pub total_bytes: u64,
    pub tombstone_ratio: f64,
}

impl ManifestStatistics {
    pub fn new(
        object_count: u64,
        row_count: u64,
        total_bytes: u64,
        tombstone_ratio: f64,
    ) -> Result<Self> {
        if !(0.0..=1.0).contains(&tombstone_ratio) {
            bail!("tombstone_ratio must be between 0.0 and 1.0");
        }
        Ok(Self {
            object_count,
            row_count,
            total_bytes,
            tombstone_ratio,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct DataManifest {
    pub version: u64,
    pub base: Option<u64>,
    pub reference_count: u64,
    pub statistics: ManifestStatistics,
    pub segments: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct IndexManifest {
    pub version: u64,
    pub base: Option<u64>,
    pub reference_count: u64,
    pub statistics: ManifestStatistics,
    pub l0_segments: Vec<u64>,
    pub l1_blocks: Vec<u64>,
}

pub trait ManifestRecord:
    Archive + Clone + Send + Sync + 'static + for<'a> RkyvSerialize<RkyvSerializer<'a>>
where
    Self::Archived: RkyvDeserialize<Self, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    fn version(&self) -> u64;
}

impl ManifestRecord for DataManifest {
    fn version(&self) -> u64 {
        self.version
    }
}

impl ManifestRecord for IndexManifest {
    fn version(&self) -> u64 {
        self.version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct ManifestIntent {
    version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentRecoveryOutcome {
    NoPendingIntent,
    ClearedCommittedIntent { version: u64 },
    ClearedOrphanedIntent { version: u64 },
}

pub struct ManifestStore<M>
where
    M: ManifestRecord,
    M::Archived: RkyvDeserialize<M, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    manifest_prefix: Vec<u8>,
    intent_key: Vec<u8>,
    marker: PhantomData<M>,
}

impl<M> ManifestStore<M>
where
    M: ManifestRecord,
    M::Archived: RkyvDeserialize<M, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub async fn begin_publish_intent(&self, version: u64) -> Result<()> {
        let encoded =
            encoding::encode(&ManifestIntent { version }).context("encode manifest intent")?;
        self.table
            .put(&self.intent_key, &encoded)
            .await
            .with_context(|| format!("write manifest intent for version {version}"))
    }

    pub async fn commit_manifest(&self, manifest: &M) -> Result<()> {
        let key = self.manifest_key(manifest.version());
        let encoded = encoding::encode(manifest).context("encode manifest record")?;
        self.table
            .put(&key, &encoded)
            .await
            .with_context(|| format!("write manifest version {}", manifest.version()))
    }

    pub async fn finalize_publish(&self) -> Result<()> {
        self.table
            .delete(&self.intent_key)
            .await
            .context("clear manifest publish intent")
    }

    pub async fn publish_manifest(&self, manifest: &M) -> Result<()> {
        let version = manifest.version();
        let intent =
            encoding::encode(&ManifestIntent { version }).context("encode manifest intent")?;
        let key = self.manifest_key(version);
        let manifest_bytes = encoding::encode(manifest).context("encode manifest record")?;

        let mut batch = WriteBatch::new();
        batch.put(self.intent_key.clone(), intent);
        batch.put(key, manifest_bytes);
        self.table
            .write_batch(batch)
            .await
            .with_context(|| format!("stage manifest publish for version {version}"))?;

        self.finalize_publish()
            .await
            .with_context(|| format!("finalize manifest publish for version {version}"))
    }

    pub async fn recover_publish_intent(&self) -> Result<IntentRecoveryOutcome> {
        let Some(intent_bytes) = self
            .table
            .get_bytes(&self.intent_key)
            .await
            .context("read manifest intent key")?
        else {
            return Ok(IntentRecoveryOutcome::NoPendingIntent);
        };

        let intent: ManifestIntent =
            encoding::decode(intent_bytes.as_ref()).context("decode manifest intent key")?;
        let manifest_exists = self
            .load_manifest(intent.version)
            .await
            .with_context(|| format!("check manifest version {}", intent.version))?
            .is_some();

        self.finalize_publish().await?;

        if manifest_exists {
            Ok(IntentRecoveryOutcome::ClearedCommittedIntent {
                version: intent.version,
            })
        } else {
            Ok(IntentRecoveryOutcome::ClearedOrphanedIntent {
                version: intent.version,
            })
        }
    }

    pub async fn load_manifest(&self, version: u64) -> Result<Option<M>> {
        let key = self.manifest_key(version);
        let Some(bytes) = self
            .table
            .get_bytes(&key)
            .await
            .with_context(|| format!("read manifest version {version}"))?
        else {
            return Ok(None);
        };
        let manifest = encoding::decode::<M>(bytes.as_ref()).context("decode manifest record")?;
        Ok(Some(manifest))
    }

    pub async fn latest_manifest(&self) -> Result<Option<M>> {
        let mut latest_version = None;
        let mut latest_bytes = None;
        let mut visit_entry = |key: &[u8], bytes: &[u8]| -> Result<()> {
            let version = self.version_from_key(key)?;
            if latest_version
                .map(|current| version >= current)
                .unwrap_or(true)
            {
                latest_version = Some(version);
                latest_bytes = Some(bytes.to_vec());
            }
            Ok(())
        };
        self.table
            .scan_range_bytes_for_each(
                prefix_bounds(&self.manifest_prefix),
                &ScanOptions::default(),
                &mut visit_entry,
            )
            .await
            .context("scan manifest prefix for latest manifest")?;

        latest_bytes
            .map(|bytes| {
                let version = latest_version.unwrap_or_default();
                encoding::decode::<M>(bytes.as_slice())
                    .with_context(|| format!("decode manifest version {version}"))
            })
            .transpose()
    }

    pub fn key_for_version(&self, version: u64) -> Vec<u8> {
        self.manifest_key(version)
    }

    pub fn intent_key(&self) -> Vec<u8> {
        self.intent_key.clone()
    }

    pub async fn pending_intent_version(&self) -> Result<Option<u64>> {
        let Some(intent_bytes) = self
            .table
            .get_bytes(&self.intent_key)
            .await
            .context("read manifest intent key")?
        else {
            return Ok(None);
        };
        let intent: ManifestIntent =
            encoding::decode(intent_bytes.as_ref()).context("decode manifest intent key")?;
        Ok(Some(intent.version))
    }

    pub async fn list_versions(&self) -> Result<Vec<u64>> {
        let entries = self
            .table
            .scan_range_bytes(
                prefix_bounds(&self.manifest_prefix),
                &ScanOptions::default(),
            )
            .await
            .context("scan manifest prefix for versions")?;
        entries
            .into_iter()
            .map(|(key, _)| self.version_from_key(&key))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn intent_key_bytes(&self) -> &[u8] {
        &self.intent_key
    }

    fn manifest_key(&self, version: u64) -> Vec<u8> {
        keyspace::key_with_u64(&self.manifest_prefix, version)
    }

    fn version_from_key(&self, key: &[u8]) -> Result<u64> {
        keyspace::parse_u64_key_suffix(&self.manifest_prefix, key)
            .ok_or_else(|| anyhow::anyhow!("invalid manifest key suffix"))
    }
}

impl ManifestStore<DataManifest> {
    pub fn data(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        build_manifest_store(table, namespace, ManifestLayer::Data)
    }
}

impl ManifestStore<IndexManifest> {
    pub fn index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        build_manifest_store(table, namespace, ManifestLayer::Index)
    }
}

fn build_manifest_store<M>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    layer: ManifestLayer,
) -> ManifestStore<M>
where
    M: ManifestRecord,
    M::Archived: RkyvDeserialize<M, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let namespace = namespace.into();
    let manifest_prefix = match layer {
        ManifestLayer::Data => keyspace::data_manifest_prefix(&namespace),
        ManifestLayer::Index => keyspace::index_manifest_prefix(&namespace),
    };
    let intent_key = keyspace::intent_key(&manifest_prefix);

    ManifestStore {
        table,
        manifest_prefix,
        intent_key,
        marker: PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::{Db, WriteBatch};

    use crate::storage::SlateTable;
    use crate::storage::encoding;

    use super::{
        DataManifest, IndexManifest, IntentRecoveryOutcome, ManifestStatistics, ManifestStore,
    };

    async fn build_table(name: &str) -> Arc<dyn crate::storage::KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    fn stats() -> ManifestStatistics {
        ManifestStatistics::new(3, 10, 2048, 0.2).expect("build statistics")
    }

    #[tokio::test]
    async fn publishes_and_reads_data_manifest() {
        let table = build_table("manifest-data").await;
        let store = ManifestStore::<DataManifest>::data(table, "manifest-data");
        let manifest = DataManifest {
            version: 1,
            base: None,
            reference_count: 1,
            statistics: stats(),
            segments: vec![5, 8, 13],
        };

        store
            .publish_manifest(&manifest)
            .await
            .expect("publish data manifest");

        let loaded = store
            .load_manifest(1)
            .await
            .expect("load data manifest")
            .expect("manifest exists");
        assert_eq!(loaded, manifest);
    }

    #[tokio::test]
    async fn publishes_and_reads_index_manifest() {
        let table = build_table("manifest-index").await;
        let store = ManifestStore::<IndexManifest>::index(table, "manifest-index");
        let manifest = IndexManifest {
            version: 7,
            base: Some(6),
            reference_count: 4,
            statistics: stats(),
            l0_segments: vec![1, 2],
            l1_blocks: vec![10, 11],
        };

        store
            .publish_manifest(&manifest)
            .await
            .expect("publish index manifest");

        let latest = store
            .latest_manifest()
            .await
            .expect("latest index manifest")
            .expect("manifest exists");
        assert_eq!(latest, manifest);
    }

    #[tokio::test]
    async fn recovers_pending_committed_intent_on_restart() {
        let table = build_table("manifest-recover-committed").await;
        let store =
            ManifestStore::<DataManifest>::data(table.clone(), "manifest-recover-committed");
        let manifest = DataManifest {
            version: 2,
            base: Some(1),
            reference_count: 2,
            statistics: stats(),
            segments: vec![21],
        };

        store
            .begin_publish_intent(manifest.version)
            .await
            .expect("begin publish intent");
        store
            .commit_manifest(&manifest)
            .await
            .expect("commit manifest without finalize");

        let reopened = ManifestStore::<DataManifest>::data(table, "manifest-recover-committed");
        let outcome = reopened
            .recover_publish_intent()
            .await
            .expect("recover publish intent");
        assert_eq!(
            outcome,
            IntentRecoveryOutcome::ClearedCommittedIntent {
                version: manifest.version
            }
        );
        assert!(
            reopened
                .load_manifest(manifest.version)
                .await
                .expect("load recovered manifest")
                .is_some()
        );
    }

    #[tokio::test]
    async fn recovers_orphan_intent_on_restart() {
        let table = build_table("manifest-recover-orphan").await;
        let store = ManifestStore::<DataManifest>::data(table.clone(), "manifest-recover-orphan");
        store
            .begin_publish_intent(9)
            .await
            .expect("write orphan intent");

        let reopened =
            ManifestStore::<DataManifest>::data(table.clone(), "manifest-recover-orphan");
        let outcome = reopened
            .recover_publish_intent()
            .await
            .expect("recover orphan intent");
        assert_eq!(
            outcome,
            IntentRecoveryOutcome::ClearedOrphanedIntent { version: 9 }
        );

        let no_intent = table
            .get(reopened.intent_key_bytes())
            .await
            .expect("get intent key");
        assert!(no_intent.is_none());
    }

    #[tokio::test]
    async fn restart_keeps_latest_manifest_visible() {
        let table = build_table("manifest-restart-latest").await;
        let store = ManifestStore::<DataManifest>::data(table.clone(), "manifest-restart-latest");
        let v1 = DataManifest {
            version: 1,
            base: None,
            reference_count: 1,
            statistics: stats(),
            segments: vec![1, 2],
        };
        let v2 = DataManifest {
            version: 2,
            base: Some(1),
            reference_count: 1,
            statistics: stats(),
            segments: vec![3, 4],
        };

        store.publish_manifest(&v1).await.expect("publish v1");
        store.publish_manifest(&v2).await.expect("publish v2");

        let reopened = ManifestStore::<DataManifest>::data(table, "manifest-restart-latest");
        assert_eq!(
            reopened
                .recover_publish_intent()
                .await
                .expect("recover on reopen"),
            IntentRecoveryOutcome::NoPendingIntent
        );
        let latest = reopened
            .latest_manifest()
            .await
            .expect("read latest")
            .expect("latest exists");
        assert_eq!(latest.version, 2);
        assert_eq!(latest.base, Some(1));
        assert_eq!(latest.reference_count, 1);
        assert_eq!(latest.segments, vec![3, 4]);
    }

    #[tokio::test]
    async fn manual_intent_payload_stays_compatible() {
        let table = build_table("manifest-intent-compat").await;
        let store = ManifestStore::<DataManifest>::data(table.clone(), "manifest-intent-compat");
        let mut batch = WriteBatch::new();
        let intent =
            encoding::encode(&super::ManifestIntent { version: 11 }).expect("encode intent");
        batch.put(store.intent_key_bytes(), intent);
        table
            .write_batch(batch)
            .await
            .expect("write manual intent payload");

        let outcome = store
            .recover_publish_intent()
            .await
            .expect("recover manual intent");
        assert_eq!(
            outcome,
            IntentRecoveryOutcome::ClearedOrphanedIntent { version: 11 }
        );
    }
}
