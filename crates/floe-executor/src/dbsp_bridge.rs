use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use datafusion::arrow::datatypes::SchemaRef;
use dbsp::collections::CompactionPolicy;
use dbsp::handles::{ZSetHandle, ZSetHandleView};
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::gc::{GcPolicy, GcService, ManifestReachabilityTracker, SweepStats};
use dbsp::storage::manifest::{DataManifest, IndexManifest, ManifestStore};
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::SnapshotHandleStream;
use dbsp::{CompactionSchedulerConfig, StreamRetention, ZSetStream};
use slatedb::Db;

use crate::namespaces;

const MV_SCHEMA_SUFFIX: &str = "/meta/schema.json";
const MV_LOGICAL_VERSION_SUFFIX: &str = "/meta/logical_version.bin";
const DELTA_NAMESPACE_SUFFIX: &str = "/delta";

fn dictionary_namespace(namespace: &str) -> &str {
    namespace
        .strip_suffix(DELTA_NAMESPACE_SUFFIX)
        .unwrap_or(namespace)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStorageSummary {
    pub namespace: String,
    pub data_manifest_version: Option<u64>,
    pub index_manifest_version: Option<u64>,
    pub pinned_handle_count: usize,
    pub reachable_data_manifest_count: usize,
    pub reachable_index_manifest_count: usize,
    pub reachable_segment_count: usize,
}

/// Shared bridge that provisions DBSP-backed views for materialization.
pub struct DbspBridge {
    table: Arc<dyn KeyValueTable>,
    dictionaries: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
    stream_compaction_policy: CompactionPolicy,
    stream_compaction_scheduler: CompactionSchedulerConfig,
    maintenance_paused: bool,
}

impl DbspBridge {
    pub async fn new(db: Arc<Db>) -> Result<Self> {
        Ok(Self {
            table: Arc::new(SlateTable::new(db)),
            dictionaries: HashMap::new(),
            stream_compaction_policy: CompactionPolicy::default(),
            stream_compaction_scheduler: CompactionSchedulerConfig::default(),
            maintenance_paused: false,
        })
    }

    pub fn set_stream_compaction_policy(&mut self, policy: CompactionPolicy) {
        self.stream_compaction_policy = policy;
    }

    pub fn set_stream_compaction_scheduler_config(&mut self, config: CompactionSchedulerConfig) {
        self.stream_compaction_scheduler = config;
    }

    pub fn pause_maintenance(&mut self) {
        self.maintenance_paused = true;
    }

    pub fn resume_maintenance(&mut self) {
        self.maintenance_paused = false;
    }

    pub fn maintenance_paused(&self) -> bool {
        self.maintenance_paused
    }

    async fn dictionary_for(&mut self, namespace: &str) -> Result<Arc<Dictionary<Vec<u8>>>> {
        let dict_namespace = dictionary_namespace(namespace);
        match self.dictionaries.entry(dict_namespace.to_string()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let dict = Arc::new(
                    Dictionary::with_table(self.table.clone(), dict_namespace.to_string(), None)
                        .await?,
                );
                Ok(entry.insert(dict).clone())
            }
        }
    }

    /// Provisions a new [`ZSetStream`] in the provided namespace with the supplied retention policy.
    pub async fn new_stream(
        &mut self,
        namespace: impl Into<String>,
        retention: StreamRetention,
    ) -> Result<ZSetStream<Vec<u8>>> {
        let namespace = namespace.into();
        let dict = self.dictionary_for(&namespace).await?;
        let mut stream = ZSetStream::new(dict, self.table.clone(), namespace, retention).await?;
        stream.set_compaction_policy(self.effective_compaction_policy());
        stream.set_compaction_scheduler_config(self.stream_compaction_scheduler);
        Ok(stream)
    }

    pub async fn new_view(
        &mut self,
        view_name: &str,
        retention: StreamRetention,
    ) -> Result<DbspView> {
        let namespace = namespaces::materialized_view(view_name)?;
        let zset = self.new_stream(namespace.clone(), retention).await?;
        Ok(DbspView {
            name: view_name.to_string(),
            namespace,
            zset,
        })
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }

    pub async fn compact_namespace_once(&mut self, namespace: &str) -> Result<Option<u64>> {
        if self.maintenance_paused {
            return Ok(None);
        }
        let dict = self.dictionary_for(namespace).await?;
        let mut stream = ZSetStream::new(
            dict,
            self.table.clone(),
            namespace.to_string(),
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await?;
        let Some(_) = stream.versioned().current_handle() else {
            return Ok(None);
        };
        let compacted_version = stream.versioned().compact_current().await?;
        Ok(Some(compacted_version))
    }

    pub async fn inspect_namespace_storage(
        &self,
        namespace: &str,
    ) -> Result<NamespaceStorageSummary> {
        let data_store = ManifestStore::<DataManifest>::data(self.table.clone(), namespace);
        let index_store = ManifestStore::<IndexManifest>::index(self.table.clone(), namespace);
        let tracker = ManifestReachabilityTracker::new(self.table.clone(), namespace);

        let data_manifest_version = data_store.latest_manifest().await?.map(|m| m.version);
        let index_manifest_version = index_store.latest_manifest().await?.map(|m| m.version);
        let pins = tracker.list_pins().await?;
        let reachable = tracker.compute_reachability().await?;

        Ok(NamespaceStorageSummary {
            namespace: namespace.to_string(),
            data_manifest_version,
            index_manifest_version,
            pinned_handle_count: pins.len(),
            reachable_data_manifest_count: reachable.data_manifest_versions.len(),
            reachable_index_manifest_count: reachable.index_manifest_versions.len(),
            reachable_segment_count: reachable.data_segments.len(),
        })
    }

    pub async fn run_namespace_gc_once(
        &self,
        namespace: &str,
        policy: GcPolicy,
    ) -> Result<SweepStats> {
        let gc = GcService::new(self.table.clone(), namespace, policy);
        gc.sweep_once().await
    }

    pub async fn save_mv_schema(&self, view_name: &str, schema: SchemaRef) -> Result<()> {
        let key = Self::mv_schema_key(view_name)?;
        let mut payload = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut payload, schema.as_ref())
                .context("encode materialized view schema via Arrow IPC")?;
            writer.finish().context("finalize schema IPC stream")?;
        }
        self.table
            .put(&key, &payload)
            .await
            .with_context(|| format!("persist schema for materialized view '{view_name}'"))
    }

    pub async fn load_mv_schema(&self, view_name: &str) -> Result<Option<SchemaRef>> {
        let key = Self::mv_schema_key(view_name)?;
        let bytes =
            match self.table.get(&key).await.with_context(|| {
                format!("load schema metadata for materialized view '{view_name}'")
            })? {
                Some(bytes) => bytes,
                None => return Ok(None),
            };
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None).with_context(|| {
            format!("decode persisted schema for materialized view '{view_name}'")
        })?;
        Ok(Some(reader.schema()))
    }

    pub async fn save_mv_logical_version(
        &self,
        view_name: &str,
        logical_version: u64,
    ) -> Result<()> {
        let key = Self::mv_logical_version_key(view_name)?;
        self.table
            .put(&key, &logical_version.to_le_bytes())
            .await
            .with_context(|| format!("persist logical version for materialized view '{view_name}'"))
    }

    pub async fn load_mv_logical_version(&self, view_name: &str) -> Result<Option<u64>> {
        let key = Self::mv_logical_version_key(view_name)?;
        let bytes = match self.table.get(&key).await.with_context(|| {
            format!("load logical version metadata for materialized view '{view_name}'")
        })? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        if bytes.len() != std::mem::size_of::<u64>() {
            anyhow::bail!(
                "persisted logical version metadata for materialized view '{}' had invalid length {}",
                view_name,
                bytes.len()
            );
        }
        Ok(Some(u64::from_le_bytes(
            bytes[..std::mem::size_of::<u64>()]
                .try_into()
                .expect("slice width already checked"),
        )))
    }

    fn mv_schema_key(view_name: &str) -> Result<Vec<u8>> {
        let namespace = namespaces::materialized_view(view_name)?;
        Ok(format!("{namespace}{MV_SCHEMA_SUFFIX}").into_bytes())
    }

    fn mv_logical_version_key(view_name: &str) -> Result<Vec<u8>> {
        let namespace = namespaces::materialized_view(view_name)?;
        Ok(format!("{namespace}{MV_LOGICAL_VERSION_SUFFIX}").into_bytes())
    }

    pub async fn handle_view_for(
        &mut self,
        namespace: &str,
        version: u64,
    ) -> Result<ZSetHandleView<Vec<u8>>> {
        let dict = self.dictionary_for(namespace).await?;
        Ok(ZSetHandleView::new(
            dict,
            self.table.clone(),
            namespace.to_string(),
            version,
        ))
    }

    pub async fn latest_view_handle(&mut self, namespace: &str) -> Result<ZSetHandle> {
        let dict = self.dictionary_for(namespace).await?;
        let mut stream = ZSetStream::new(
            dict,
            self.table.clone(),
            namespace.to_string(),
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await?;
        stream.set_compaction_policy(self.effective_compaction_policy());
        stream.set_compaction_scheduler_config(self.stream_compaction_scheduler);
        stream.latest_handle().await
    }

    fn effective_compaction_policy(&self) -> CompactionPolicy {
        if self.maintenance_paused {
            CompactionPolicy::disabled()
        } else {
            self.stream_compaction_policy
        }
    }
}

/// Mutable writer for a specific materialized view.
pub struct DbspView {
    name: String,
    namespace: String,
    zset: ZSetStream<Vec<u8>>,
}

impl DbspView {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn add_delta(&mut self, key: Vec<u8>, diff: i64) {
        self.zset.add_delta(key, diff);
    }

    pub fn add_deltas<I>(&mut self, deltas: I)
    where
        I: IntoIterator<Item = (Vec<u8>, i64)>,
    {
        self.zset.add_deltas(deltas);
    }

    pub async fn flush(&mut self) -> Result<ZSetHandle> {
        self.zset.flush().await
    }

    pub fn set_compaction_policy(&mut self, policy: CompactionPolicy) {
        self.zset.set_compaction_policy(policy);
    }

    pub fn latest_handle_view(&self) -> ZSetHandleView<Vec<u8>> {
        self.zset.latest_view()
    }

    pub fn handle_stream(&self) -> SnapshotHandleStream {
        self.zset.handle_stream()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn build_bridge(name: &str) -> DbspBridge {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        DbspBridge::new(db).await.expect("create bridge")
    }

    #[tokio::test]
    async fn maintenance_pause_resume_controls_one_shot_compaction() {
        let mut bridge = build_bridge("bridge-maintenance-pause").await;
        bridge.set_stream_compaction_policy(CompactionPolicy {
            max_chain_len: 1,
            max_segments: 1,
            max_bucket_segments: 1,
        });

        let namespace = "mv_maint_pause";
        let mut stream = bridge
            .new_stream(
                namespace.to_string(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("create stream");
        stream.add_delta(b"k1".to_vec(), 1);
        stream.flush().await.expect("flush v1");
        stream.add_delta(b"k2".to_vec(), 1);
        stream.flush().await.expect("flush v2");

        bridge.pause_maintenance();
        assert!(bridge.maintenance_paused());
        let skipped = bridge
            .compact_namespace_once(namespace)
            .await
            .expect("compact while paused");
        assert!(
            skipped.is_none(),
            "compaction one-shot should no-op while maintenance is paused"
        );

        bridge.resume_maintenance();
        assert!(!bridge.maintenance_paused());
        let compacted = bridge
            .compact_namespace_once(namespace)
            .await
            .expect("compact after resume");
        assert!(
            compacted.is_some(),
            "compaction one-shot should execute when maintenance is active"
        );
    }

    #[tokio::test]
    async fn maintenance_one_shot_gc_runs_in_paused_mode() {
        let mut bridge = build_bridge("bridge-maintenance-gc").await;
        let namespace = "mv_maint_gc";
        let mut stream = bridge
            .new_stream(
                namespace.to_string(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("create stream");
        stream.add_delta(b"k".to_vec(), 1);
        stream.flush().await.expect("flush");

        bridge.pause_maintenance();
        let stats = bridge
            .run_namespace_gc_once(namespace, GcPolicy::default())
            .await
            .expect("run one-shot gc while paused");
        assert_eq!(stats.recovered_intents, 0);
    }

    #[tokio::test]
    async fn persists_materialized_view_logical_version_metadata() {
        let bridge = build_bridge("bridge-mv-logical-version").await;
        bridge
            .save_mv_logical_version("mv_logical", 42)
            .await
            .expect("persist logical version");

        let loaded = bridge
            .load_mv_logical_version("mv_logical")
            .await
            .expect("load logical version");
        assert_eq!(loaded, Some(42));
    }
}
