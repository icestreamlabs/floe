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
        match self.dictionaries.entry(namespace.to_string()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let dict = Arc::new(
                    Dictionary::with_table(self.table.clone(), namespace.to_string(), None).await?,
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

    fn mv_schema_key(view_name: &str) -> Result<Vec<u8>> {
        let namespace = namespaces::materialized_view(view_name)?;
        Ok(format!("{namespace}{MV_SCHEMA_SUFFIX}").into_bytes())
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

    pub fn latest_handle_view(&self) -> ZSetHandleView<Vec<u8>> {
        self.zset.latest_view()
    }

    pub fn handle_stream(&self) -> SnapshotHandleStream {
        self.zset.handle_stream()
    }
}
