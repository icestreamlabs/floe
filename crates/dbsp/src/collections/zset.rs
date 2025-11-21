use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};

use crate::handles::ZSetHandle;
use crate::storage::dictionary::{Dictionary, KeyIntern};
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::{KeyValueTable, SlateTable};

const ZSET_PREFIX: &str = "zset/";
const SELECT_PREFIX: &str = "zset_select/";
const PROJECT_PREFIX: &str = "zset_project/";
const JOIN_PREFIX: &str = "zset_join/";
const H_PREFIX: &str = "zset_h/";

static SELECT_COUNTER: AtomicU64 = AtomicU64::new(0);
static PROJECT_COUNTER: AtomicU64 = AtomicU64::new(0);
static JOIN_COUNTER: AtomicU64 = AtomicU64::new(0);
static H_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct ZSet<K>
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
    table: Arc<dyn KeyValueTable>,
    dict: Arc<Dictionary<K>>,
    data_prefix: Vec<u8>,
    cache: HashMap<K, i64>,
    pending: HashMap<K, PendingValue>,
}

#[derive(Clone)]
enum PendingValue {
    Upsert(i64),
    Delete,
}

#[allow(dead_code)]
pub type SegmentId = u64;

#[allow(dead_code)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone)]
pub struct SegmentRecord {
    pub id: SegmentId,
    pub bucket: u16,
    pub deltas: Vec<(u64, i64)>,
}

#[allow(dead_code)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone)]
pub struct ZSetVersionManifest {
    pub base: Option<u64>,
    pub buckets: BTreeMap<u16, Vec<SegmentId>>,
    pub reference_count: u64,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct VersionWritePlan {
    pub(crate) version: u64,
    pub(crate) manifest: ZSetVersionManifest,
}

#[allow(dead_code)]
pub struct VersionedZSet<K>
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
    dict: Arc<Dictionary<K>>,
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    manifest_prefix: Vec<u8>,
    segment_prefix: Vec<u8>,
    current_version: u64,
    intent_key: Vec<u8>,
    manifest: Option<ZSetVersionManifest>,
    next_segment_id: SegmentId,
}

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
    /// Placeholder constructor for future implementation. The layout will bucket segments by the
    /// high bits of the interned key ID to keep manifest fan-out small while supporting efficient
    /// scans.
    pub async fn new(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let mut manifest_prefix = ZSET_PREFIX.as_bytes().to_vec();
        manifest_prefix.extend_from_slice(namespace.as_bytes());
        manifest_prefix.extend_from_slice(b"/manifest/");

        let mut segment_prefix = ZSET_PREFIX.as_bytes().to_vec();
        segment_prefix.extend_from_slice(namespace.as_bytes());
        segment_prefix.extend_from_slice(b"/seg/");

        let mut intent_key = manifest_prefix.clone();
        intent_key.extend_from_slice(b"intent");

        let mut versioned = Self {
            dict,
            table,
            namespace,
            manifest_prefix,
            segment_prefix,
            current_version: 0,
            intent_key,
            manifest: None,
            next_segment_id: 1,
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

    fn manifest_key(&self, version: u64) -> Vec<u8> {
        let mut key = self.manifest_prefix.clone();
        key.extend_from_slice(&version.to_be_bytes());
        key
    }

    fn segment_key(&self, bucket: u16, segment: SegmentId) -> Vec<u8> {
        let mut key = self.segment_prefix.clone();
        key.extend_from_slice(&bucket.to_be_bytes());
        key.push(b'/');
        key.extend_from_slice(&segment.to_be_bytes());
        key
    }

    async fn refresh_state(&mut self) -> Result<()> {
        if let Some(intent_bytes) = self.table.get(&self.intent_key).await? {
            if !intent_bytes.is_empty() {
                self.table
                    .delete(&self.intent_key)
                    .await
                    .context("clear stale versioned intent")?;
            }
        }

        let entries = self
            .table
            .scan_range(
                prefix_bounds(&self.manifest_prefix),
                &ScanOptions::default(),
            )
            .await
            .context("scan manifests while refreshing versioned ZSet")?;

        let mut current = None;
        let mut max_version = 0u64;
        let mut max_segment_id = 0u64;

        for (key, bytes) in entries {
            if key.len() != self.manifest_prefix.len() + 8 {
                continue;
            }

            let mut version_bytes = [0u8; 8];
            version_bytes
                .copy_from_slice(&key[self.manifest_prefix.len()..self.manifest_prefix.len() + 8]);
            let version = u64::from_be_bytes(version_bytes);
            let manifest = decode_manifest(&bytes)?;

            for segments in manifest.buckets.values() {
                for id in segments {
                    max_segment_id = max_segment_id.max(*id);
                }
            }

            if version >= max_version {
                max_version = version;
                current = Some(manifest.clone());
            }
        }

        self.current_version = max_version;
        self.manifest = current;
        self.next_segment_id = max_segment_id.saturating_add(1).max(1);
        Ok(())
    }

    pub async fn create_version(&mut self, segments: Vec<SegmentRecord>) -> Result<u64> {
        let base = self.manifest.as_ref().map(|_| self.current_version);
        self.create_version_with_base(segments, base).await
    }

    pub async fn create_version_with_base(
        &mut self,
        segments: Vec<SegmentRecord>,
        base: Option<u64>,
    ) -> Result<u64> {
        let mut batch = WriteBatch::new();
        let plan = self
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await?;

        self.table
            .write_batch(batch)
            .await
            .context("persist versioned ZSet manifest")?;

        let mut clear_intent = WriteBatch::new();
        clear_intent.delete(self.intent_key.clone());
        self.table
            .write_batch(clear_intent)
            .await
            .context("clear versioned intent")?;

        self.apply_version_plan(&plan);

        Ok(plan.version)
    }

    pub(crate) async fn enqueue_version_with_base(
        &mut self,
        segments: Vec<SegmentRecord>,
        base: Option<u64>,
        additional_references: u64,
        batch: &mut WriteBatch,
    ) -> Result<VersionWritePlan> {
        let mut processed = Vec::new();
        for mut record in segments {
            record.deltas.retain(|(_, delta)| *delta != 0);
            if record.deltas.is_empty() {
                continue;
            }

            if record.id == 0 {
                record.id = self.allocate_segment_id();
            } else {
                self.next_segment_id = self.next_segment_id.max(record.id.saturating_add(1));
            }
            record.deltas.sort_by_key(|(id, _)| *id);
            processed.push(record);
        }

        if processed.is_empty() {
            return Err(anyhow!("no deltas to persist in version"));
        }

        batch.put(self.intent_key.clone(), vec![1]);

        let mut buckets = BTreeMap::new();
        for record in &processed {
            let key = self.segment_key(record.bucket, record.id);
            let encoded = encoding::encode(record).context("encode versioned segment")?;
            batch.put(key, encoded);
            buckets
                .entry(record.bucket)
                .or_insert_with(Vec::new)
                .push(record.id);
        }

        for ids in buckets.values_mut() {
            ids.sort_unstable();
        }

        if let Some(base_version) = base {
            let mut base_manifest = self.load_manifest_record(base_version).await?;
            base_manifest.reference_count = base_manifest.reference_count.saturating_add(1);
            let base_bytes = encode_manifest(&base_manifest)?;
            batch.put(self.manifest_key(base_version), base_bytes);
        }

        let next_version = self.current_version.saturating_add(1);
        let manifest = ZSetVersionManifest {
            base,
            buckets,
            reference_count: 1 + additional_references,
        };

        let manifest_bytes = encode_manifest(&manifest)?;
        batch.put(self.manifest_key(next_version), manifest_bytes);

        let highest_id = processed.iter().map(|record| record.id).max().unwrap_or(0);
        self.next_segment_id = self
            .next_segment_id
            .max(highest_id.saturating_add(1))
            .max(1);

        Ok(VersionWritePlan {
            version: next_version,
            manifest,
        })
    }

    pub(crate) fn apply_version_plan(&mut self, plan: &VersionWritePlan) {
        self.current_version = plan.version;
        self.manifest = Some(plan.manifest.clone());
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

    pub async fn materialize(&self) -> Result<HashMap<K, i64>> {
        let mut aggregate = if let Some(base_version) = self.manifest.as_ref().and_then(|m| m.base)
        {
            self.load_version_chain(base_version).await?
        } else {
            HashMap::new()
        };

        if let Some(current) = &self.manifest {
            for (bucket, segments) in &current.buckets {
                for segment_id in segments {
                    let record = self.load_segment(*bucket, *segment_id).await?;
                    for (key_id, delta) in record.deltas {
                        let key = self
                            .dict
                            .resolve(key_id)
                            .await
                            .context("resolve key while materializing version")?;
                        *aggregate.entry(key).or_insert(0) += delta;
                    }
                }
            }
        }

        aggregate.retain(|_, weight| *weight != 0);
        Ok(aggregate)
    }

    pub async fn load_existing_version(&self, version: u64) -> Result<HashMap<K, i64>> {
        if version == 0 {
            return Ok(HashMap::new());
        }
        self.load_version_chain(version).await
    }

    async fn load_version_chain(&self, version: u64) -> Result<HashMap<K, i64>> {
        let mut chain = Vec::new();
        let mut manifests = Vec::new();
        let mut current = Some(version);

        while let Some(v) = current {
            let key = self.manifest_key(v);
            let bytes = self
                .table
                .get(&key)
                .await?
                .ok_or_else(|| anyhow!("manifest version {v} not found"))?;
            let manifest = decode_manifest(&bytes)?;
            chain.push(v);
            manifests.push(manifest.clone());
            current = manifest.base;
        }

        let mut aggregate = HashMap::new();
        for manifest in manifests.into_iter().rev() {
            for (bucket, segments) in manifest.buckets {
                for segment_id in segments {
                    let record = self.load_segment(bucket, segment_id).await?;
                    for (key_id, delta) in record.deltas {
                        let key = self
                            .dict
                            .resolve(key_id)
                            .await
                            .context("resolve key while loading version")?;
                        *aggregate.entry(key).or_insert(0) += delta;
                    }
                }
            }
        }

        Ok(aggregate)
    }

    async fn load_segment(&self, bucket: u16, segment: SegmentId) -> Result<SegmentRecord> {
        let key = self.segment_key(bucket, segment);
        let bytes =
            self.table.get(&key).await?.ok_or_else(|| {
                anyhow!("segment not found for bucket {bucket} segment {segment}")
            })?;
        encoding::decode(&bytes).context("decode segment record")
    }

    async fn load_manifest_record(&self, version: u64) -> Result<ZSetVersionManifest> {
        let key = self.manifest_key(version);
        let bytes = self
            .table
            .get(&key)
            .await?
            .ok_or_else(|| anyhow!("manifest version {version} not found"))?;
        decode_manifest(&bytes)
    }

    async fn store_manifest(&self, version: u64, manifest: &ZSetVersionManifest) -> Result<()> {
        let key = self.manifest_key(version);
        let encoded = encode_manifest(manifest)?;
        self.table
            .put(&key, &encoded)
            .await
            .context("store manifest")
    }

    #[cfg(test)]
    fn manifest_prefix_bytes(&self) -> &[u8] {
        &self.manifest_prefix
    }

    #[cfg(test)]
    fn segment_prefix_bytes(&self) -> &[u8] {
        &self.segment_prefix
    }

    #[cfg(test)]
    pub(crate) async fn manifest_record(&self, version: u64) -> Result<ZSetVersionManifest> {
        self.load_manifest_record(version).await
    }

    pub(crate) fn intent_key_bytes(&self) -> &[u8] {
        &self.intent_key
    }

    pub async fn acquire_version(&self, version: u64) -> Result<()> {
        let mut manifest = self.load_manifest_record(version).await?;
        manifest.reference_count = manifest.reference_count.saturating_add(1);
        self.store_manifest(version, &manifest).await
    }

    pub async fn release_version(&mut self, version: u64) -> Result<()> {
        if version == 0 {
            return Err(anyhow!("cannot release version 0"));
        }

        let mut stack = vec![version];
        let mut needs_refresh = false;

        while let Some(current) = stack.pop() {
            if current == 0 {
                return Err(anyhow!("cannot release version 0"));
            }

            let mut manifest = self.load_manifest_record(current).await?;
            if manifest.reference_count == 0 {
                return Err(anyhow!("manifest {current} has zero reference count"));
            }

            manifest.reference_count -= 1;
            if manifest.reference_count > 0 {
                self.store_manifest(current, &manifest).await?;
                needs_refresh = true;
                break;
            }

            let mut batch = WriteBatch::new();
            for (bucket, segments) in &manifest.buckets {
                for segment_id in segments {
                    batch.delete(self.segment_key(*bucket, *segment_id));
                }
            }
            batch.delete(self.manifest_key(current));
            self.table
                .write_batch(batch)
                .await
                .context("remove manifest and segments")?;

            if let Some(base_version) = manifest.base {
                stack.push(base_version);
            }

            needs_refresh = true;
        }

        if needs_refresh {
            self.refresh_state().await?;
        }

        Ok(())
    }

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

    fn allocate_segment_id(&mut self) -> SegmentId {
        let id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        id
    }

    // TODO:
    // 1. Encode manifests using `storage::encoding::encode` with a leading codec tag so future
    //    versions can evolve the layout.
    // 2. Introduce an intent key similar to streams (`zset/<ns>/manifest/intent`) to guard updates.
    // 3. Wire segment writes to bucket by `u64` key ID (e.g. top 8 bits) to minimize manifest
    //    fan-out, then expose handles that reference `current_version` for readers.
}

impl<K> Clone for ZSet<K>
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
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            dict: self.dict.clone(),
            data_prefix: self.data_prefix.clone(),
            cache: self.cache.clone(),
            pending: self.pending.clone(),
        }
    }
}

impl<K> ZSet<K>
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
    pub async fn new(db: Arc<Db>, namespace: impl Into<String>) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        Self::with_table(table, namespace).await
    }

    pub async fn with_table(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let mut data_prefix = ZSET_PREFIX.as_bytes().to_vec();
        data_prefix.extend_from_slice(namespace.as_bytes());
        data_prefix.push(b'/');

        let dict = Dictionary::with_table(table.clone(), namespace, None)
            .await
            .context("build dictionary for ZSet")?;

        Ok(Self {
            table,
            dict: Arc::new(dict),
            data_prefix,
            cache: HashMap::new(),
            pending: HashMap::new(),
        })
    }

    pub async fn contains(&mut self, key: &K) -> Result<bool> {
        Ok(self.get_weight(key).await? != 0)
    }

    pub async fn get_weight(&mut self, key: &K) -> Result<i64> {
        if let Some(change) = self.pending.get(key) {
            return Ok(match change {
                PendingValue::Upsert(weight) => *weight,
                PendingValue::Delete => 0,
            });
        }

        if let Some(weight) = self.cache.get(key) {
            return Ok(*weight);
        }

        if let Some(id) = self.dict.lookup(key).await? {
            let encoded_key = self.encode_id(id);
            if let Some(bytes) = self.table.get(&encoded_key).await? {
                let weight = decode_weight(bytes.as_ref())?;
                self.cache.insert(key.clone(), weight);
                return Ok(weight);
            }
        }

        Ok(0)
    }

    pub fn set_weight(&mut self, key: K, weight: i64) {
        if weight == 0 {
            self.pending.insert(key.clone(), PendingValue::Delete);
            self.cache.remove(&key);
        } else {
            self.pending
                .insert(key.clone(), PendingValue::Upsert(weight));
            self.cache.insert(key, weight);
        }
    }

    pub async fn add_weight(&mut self, key: K, delta: i64) -> Result<i64> {
        let current = self.get_weight(&key).await?;
        let next = current + delta;
        self.set_weight(key, next);
        Ok(next)
    }

    pub async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let mut pending = HashMap::new();
        std::mem::swap(&mut pending, &mut self.pending);

        let mut batch = WriteBatch::new();
        let mut dirty = false;

        for (key, change) in pending {
            match change {
                PendingValue::Upsert(weight) => {
                    let id = self
                        .dict
                        .intern(&key)
                        .await
                        .context("intern ZSet key for flush")?;
                    let value = encode_weight(weight);
                    batch.put(self.encode_id(id), value);
                    self.cache.insert(key, weight);
                }
                PendingValue::Delete => {
                    if let Some(id) = self.dict.lookup(&key).await? {
                        batch.delete(self.encode_id(id));
                    }
                    self.cache.remove(&key);
                }
            }
            dirty = true;
        }

        if dirty {
            self.table.write_batch(batch).await?;
        }

        Ok(())
    }

    pub async fn items(&mut self) -> Result<Vec<(K, i64)>> {
        let mut entries = self.load_all().await?;
        self.apply_pending(&mut entries);
        Ok(entries.into_iter().collect())
    }

    pub async fn is_identity(&mut self) -> Result<bool> {
        if self
            .pending
            .values()
            .any(|value| matches!(value, PendingValue::Upsert(_)))
        {
            return Ok(false);
        }

        let entries = self
            .table
            .scan_range(prefix_bounds(&self.data_prefix), &ScanOptions::default())
            .await?;

        for (key_bytes, _) in entries {
            let id = self.decode_id(&key_bytes)?;
            let key = self
                .dict
                .resolve(id)
                .await
                .context("resolve ZSet key while checking identity")?;
            if let Some(PendingValue::Delete) = self.pending.get(&key) {
                continue;
            }

            return Ok(false);
        }

        Ok(true)
    }

    fn encode_id(&self, id: u64) -> Vec<u8> {
        let mut namespaced = self.data_prefix.clone();
        namespaced.extend_from_slice(&id.to_be_bytes());
        namespaced
    }

    fn decode_id(&self, key: &[u8]) -> Result<u64> {
        if key.len() != self.data_prefix.len() + 8 || !key.starts_with(&self.data_prefix) {
            return Err(anyhow!("unexpected key prefix while decoding ZSet entry"));
        }

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key[self.data_prefix.len()..]);
        Ok(u64::from_be_bytes(bytes))
    }

    async fn load_all(&self) -> Result<HashMap<K, i64>> {
        let entries = self
            .table
            .scan_range(prefix_bounds(&self.data_prefix), &ScanOptions::default())
            .await?;

        let mut map = HashMap::new();
        for (key_bytes, value_bytes) in entries {
            let id = self.decode_id(&key_bytes)?;
            let key = self
                .dict
                .resolve(id)
                .await
                .context("resolve ZSet key from dictionary")?;
            let weight = decode_weight(value_bytes.as_ref())?;
            map.insert(key, weight);
        }

        Ok(map)
    }

    fn apply_pending(&self, entries: &mut HashMap<K, i64>) {
        for (key, change) in &self.pending {
            match change {
                PendingValue::Upsert(weight) => {
                    if *weight == 0 {
                        entries.remove(key);
                    } else {
                        entries.insert(key.clone(), *weight);
                    }
                }
                PendingValue::Delete => {
                    entries.remove(key);
                }
            }
        }
    }
}

fn encode_weight(weight: i64) -> Vec<u8> {
    weight.to_be_bytes().to_vec()
}

fn decode_weight(bytes: &[u8]) -> Result<i64> {
    if bytes.len() != 8 {
        return Err(anyhow!(
            "expected 8 bytes for ZSet weight, found {}",
            bytes.len()
        ));
    }

    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    Ok(i64::from_be_bytes(array))
}

fn derived_namespace(prefix: &str, counter: &AtomicU64) -> String {
    let id = counter.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{id}")
}

fn encode_manifest(manifest: &ZSetVersionManifest) -> Result<Vec<u8>> {
    encoding::encode(manifest).context("encode ZSet manifest")
}

fn decode_manifest(bytes: &[u8]) -> Result<ZSetVersionManifest> {
    encoding::decode(bytes).context("decode ZSet manifest")
}

pub async fn select<K, P>(zset: &ZSet<K>, predicate: &P) -> Result<ZSet<K>>
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
    P: Fn(&K) -> bool + Send + Sync,
{
    let entries = collect_entries(zset).await?;
    let namespace = derived_namespace(SELECT_PREFIX, &SELECT_COUNTER);
    let mut result = ZSet::with_table(zset.table.clone(), namespace)
        .await
        .context("build derived ZSet for select")?;

    for (key, weight) in entries {
        if predicate(&key) {
            result.set_weight(key, weight);
        }
    }

    Ok(result)
}

pub async fn project<K, R, F>(zset: &ZSet<K>, projector: &F) -> Result<ZSet<R>>
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
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&K) -> R + Send + Sync,
{
    let mut aggregated: HashMap<R, i64> = HashMap::new();
    for (key, weight) in collect_entries(zset).await? {
        let projected = projector(&key);
        *aggregated.entry(projected).or_insert(0) += weight;
    }

    aggregated.retain(|_, weight| *weight != 0);

    let namespace = derived_namespace(PROJECT_PREFIX, &PROJECT_COUNTER);
    let mut result = ZSet::with_table(zset.table.clone(), namespace)
        .await
        .context("build derived ZSet for project")?;
    for (key, weight) in aggregated {
        result.set_weight(key, weight);
    }

    Ok(result)
}

pub async fn join<L, R, O, P, F>(
    left: &ZSet<L>,
    right: &ZSet<R>,
    predicate: &P,
    projector: &F,
) -> Result<ZSet<O>>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Fn(&L, &R) -> bool + Send + Sync,
    F: Fn(&L, &R) -> O + Send + Sync,
{
    let left_entries = collect_entries(left).await?;
    let right_entries = collect_entries(right).await?;

    let namespace = derived_namespace(JOIN_PREFIX, &JOIN_COUNTER);
    let mut result = ZSet::with_table(left.table.clone(), namespace)
        .await
        .context("build derived ZSet for join")?;

    for (left_key, left_weight) in left_entries {
        for (right_key, right_weight) in &right_entries {
            if predicate(&left_key, right_key) {
                let projected = projector(&left_key, right_key);
                let combined = left_weight * *right_weight;
                result.set_weight(projected, combined);
            }
        }
    }

    result.flush().await?;
    Ok(result)
}

pub async fn h<K>(diff: &ZSet<K>, integrated_state: &ZSet<K>) -> Result<ZSet<K>>
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
    let diff_entries = collect_entries(diff).await?;
    let integrated_entries = collect_entries(integrated_state).await?;

    let namespace = derived_namespace(H_PREFIX, &H_COUNTER);
    let mut result = ZSet::with_table(diff.table.clone(), namespace)
        .await
        .context("build derived ZSet for H operator")?;

    for (key, diff_weight) in diff_entries {
        let state_weight = integrated_entries.get(&key).copied().unwrap_or(0);
        let coalesced = diff_weight + state_weight;

        if state_weight > 0 && coalesced <= 0 {
            result.set_weight(key, -1);
        } else if state_weight <= 0 && coalesced > 0 {
            result.set_weight(key, 1);
        }
    }

    Ok(result)
}

async fn collect_entries<K>(zset: &ZSet<K>) -> Result<HashMap<K, i64>>
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
    let mut clone = zset.clone();
    let items = clone.items().await?;

    let mut map = HashMap::new();
    for (key, weight) in items {
        if weight != 0 {
            map.insert(key, weight);
        }
    }
    Ok(map)
}

fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::WriteBatch;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("test", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn creates_and_persists_weights() {
        let db = build_db().await;
        let mut zset = ZSet::new(db.clone(), "weights").await.expect("create zset");

        zset.set_weight("a".to_string(), 1);
        zset.set_weight("b".to_string(), 2);
        zset.flush().await.unwrap();

        let mut reload = ZSet::new(db, "weights").await.expect("reload zset");
        assert_eq!(reload.get_weight(&"a".to_string()).await.unwrap(), 1);
        assert_eq!(reload.get_weight(&"b".to_string()).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn logical_merge_of_pending_before_flush() {
        let db = build_db().await;
        let mut zset = ZSet::new(db, "merge").await.expect("create zset");

        zset.set_weight("item".to_string(), 3);
        assert_eq!(zset.contains(&"item".to_string()).await.unwrap(), true);
        zset.set_weight("item".to_string(), 0);
        assert_eq!(zset.contains(&"item".to_string()).await.unwrap(), false);
        zset.flush().await.unwrap();
        assert!(zset.items().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn add_weight_accumulates() {
        let db = build_db().await;
        let mut zset = ZSet::new(db.clone(), "acc").await.expect("create zset");

        zset.add_weight("key".to_string(), 3).await.unwrap();
        zset.flush().await.unwrap();

        let mut reload = ZSet::new(db, "acc").await.expect("reload zset");
        assert_eq!(reload.get_weight(&"key".to_string()).await.unwrap(), 3);

        reload.add_weight("key".to_string(), -3).await.unwrap();
        reload.flush().await.unwrap();
        assert!(reload.is_identity().await.unwrap());
    }

    #[tokio::test]
    async fn insert_then_negates_to_zero_removes_entry() {
        let db = build_db().await;
        let mut zset = ZSet::new(db, "zero_remove")
            .await
            .expect("create zset");

        zset.add_weight("gone".to_string(), 1).await.unwrap();
        zset.flush().await.unwrap();
        zset.add_weight("gone".to_string(), -1).await.unwrap();
        zset.flush().await.unwrap();

        assert_eq!(
            zset.contains(&"gone".to_string()).await.expect("contains check"),
            false
        );
        assert!(zset.items().await.expect("items after cancel").is_empty());
    }

    #[tokio::test]
    async fn sequential_deltas_equivalent_to_aggregated_delta() {
        let db = build_db().await;
        let mut seq = ZSet::new(db.clone(), "seq").await.expect("seq zset");

        let deltas = vec![
            vec![("a".to_string(), 1), ("b".to_string(), 2)],
            vec![("a".to_string(), -1), ("b".to_string(), 3)],
        ];

        for batch in &deltas {
            for (key, delta) in batch {
                seq.add_weight(key.clone(), *delta).await.unwrap();
            }
            seq.flush().await.unwrap();
        }
        let seq_items: HashMap<_, _> = seq
            .items()
            .await
            .expect("seq items")
            .into_iter()
            .collect();

        let mut aggregate_map: HashMap<String, i64> = HashMap::new();
        for batch in &deltas {
            for (key, delta) in batch {
                let entry = aggregate_map.entry(key.clone()).or_insert(0);
                *entry += *delta;
                if *entry == 0 {
                    aggregate_map.remove(key);
                }
            }
        }

        let mut agg = ZSet::new(db, "agg").await.expect("agg zset");
        for (key, weight) in &aggregate_map {
            agg.set_weight(key.clone(), *weight);
        }
        agg.flush().await.unwrap();
        let agg_items: HashMap<_, _> = agg.items().await.expect("agg items").into_iter().collect();

        assert_eq!(seq_items, agg_items);
        assert_eq!(agg_items.get("a"), None);
        assert_eq!(agg_items.get("b"), Some(&5));
    }

    #[tokio::test]
    async fn h_distincts_differences() {
        let db = build_db().await;
        let mut diff = ZSet::new(db.clone(), "h_diff")
            .await
            .expect("create diff zset");
        let mut state = ZSet::new(db.clone(), "h_state")
            .await
            .expect("create state zset");

        diff.set_weight("enter".to_string(), 2);
        diff.set_weight("leave".to_string(), -3);
        diff.set_weight("stay".to_string(), -1);

        state.set_weight("leave".to_string(), 3);
        state.set_weight("stay".to_string(), 1);

        let mut result = h(&diff, &state).await.expect("compute h");
        let mut entries = result.items().await.expect("materialize result");
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        assert_eq!(
            entries,
            vec![
                ("enter".to_string(), 1),
                ("leave".to_string(), -1),
                ("stay".to_string(), -1)
            ]
        );
    }

    #[tokio::test]
    async fn recovers_after_partial_flush() {
        let db = build_db().await;
        let mut zset = ZSet::new(db.clone(), "recover").await.expect("create zset");

        zset.set_weight("stay".to_string(), 5);
        zset.flush().await.expect("flush zset");

        let dict = zset.dict.clone();
        let stay_id = dict
            .intern(&"stay".to_string())
            .await
            .expect("intern stay key");
        let remove_id = stay_id + 1;

        let mut batch = WriteBatch::new();
        batch.put(zset.encode_id(remove_id), encode_weight(10));
        batch.put(zset.encode_id(stay_id), encode_weight(5));
        zset.table
            .write_batch(batch)
            .await
            .expect("write partial state");

        let mut reopened = ZSet::new(db, "recover").await.expect("reopen zset");
        assert_eq!(reopened.get_weight(&"stay".to_string()).await.unwrap(), 5);
        assert_eq!(reopened.get_weight(&"remove".to_string()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn reuses_interned_id_after_reopen() {
        let db = build_db().await;
        let mut zset = ZSet::new(db.clone(), "reuse").await.expect("create zset");

        zset.set_weight("shared".to_string(), 4);
        zset.flush().await.expect("flush zset");

        let id_before = zset
            .dict
            .lookup(&"shared".to_string())
            .await
            .expect("lookup shared key")
            .expect("id present");

        let mut reopen = ZSet::new(db, "reuse").await.expect("reopen zset");
        let id_after = reopen
            .dict
            .lookup(&"shared".to_string())
            .await
            .expect("lookup after reopen")
            .expect("id present after reopen");

        assert_eq!(id_before, id_after);
        assert_eq!(reopen.get_weight(&"shared".to_string()).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn versioned_zset_materializes_view() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "vz", None)
                .await
                .expect("build dictionary"),
        );

        let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), "vz".to_string())
            .await
            .expect("create versioned zset");

        let key_id = dict
            .intern(&"item".to_string())
            .await
            .expect("intern item key");
        let segment = SegmentRecord {
            id: 1,
            bucket: 0,
            deltas: vec![(key_id, 7)],
        };
        versioned
            .create_version(vec![segment])
            .await
            .expect("create version");

        let view = versioned.materialize().await.expect("materialize view");
        assert_eq!(view.get("item"), Some(&7));

        let reopened = VersionedZSet::new(dict.clone(), table.clone(), "vz".to_string())
            .await
            .expect("reopen versioned zset");
        let view = reopened
            .materialize()
            .await
            .expect("materialize reopened view");
        assert_eq!(view.get("item"), Some(&7));
    }

    #[tokio::test]
    async fn compacts_versioned_zset() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "vz_compact", None)
                .await
                .expect("build dictionary"),
        );

        let mut versioned =
            VersionedZSet::new(dict.clone(), table.clone(), "vz_compact".to_string())
                .await
                .expect("create versioned zset");

        let id_a = dict.intern(&"a".to_string()).await.expect("intern a");
        let id_b = dict.intern(&"b".to_string()).await.expect("intern b");

        let segments = vec![
            SegmentRecord {
                id: 1,
                bucket: 0,
                deltas: vec![(id_a, 4)],
            },
            SegmentRecord {
                id: 2,
                bucket: 1,
                deltas: vec![(id_b, 6)],
            },
        ];
        versioned
            .create_version(segments)
            .await
            .expect("create multi-segment version");

        let view_before = versioned.materialize().await.expect("materialize");
        assert_eq!(view_before.get("a"), Some(&4));
        assert_eq!(view_before.get("b"), Some(&6));

        versioned.compact_current().await.expect("compact version");

        let manifests_after = table
            .scan_range(
                prefix_bounds(versioned.manifest_prefix_bytes()),
                &ScanOptions::default(),
            )
            .await
            .expect("scan manifests after compaction");
        assert_eq!(manifests_after.len(), 1);

        let segments_after = table
            .scan_range(
                prefix_bounds(versioned.segment_prefix_bytes()),
                &ScanOptions::default(),
            )
            .await
            .expect("scan segments after compaction");
        assert_eq!(segments_after.len(), 1);

        let view_after = versioned
            .materialize()
            .await
            .expect("materialize after compact");
        assert_eq!(view_after.get("a"), Some(&4));
        assert_eq!(view_after.get("b"), Some(&6));

        let reopened = VersionedZSet::new(dict.clone(), table.clone(), "vz_compact".to_string())
            .await
            .expect("reopen");
        let manifest = reopened.manifest().expect("manifest present");
        assert_eq!(manifest.buckets.len(), 1);
        let total_segments: usize = manifest.buckets.values().map(|v| v.len()).sum();
        assert_eq!(total_segments, 1);
        let view_reopen = reopened.materialize().await.expect("materialize reopened");
        assert_eq!(view_reopen.get("a"), Some(&4));
        assert_eq!(view_reopen.get("b"), Some(&6));
    }

    #[tokio::test]
    async fn release_version_removes_segments() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "vz_release", None)
                .await
                .expect("build dictionary"),
        );

        let mut versioned =
            VersionedZSet::new(dict.clone(), table.clone(), "vz_release".to_string())
                .await
                .expect("create versioned zset");

        let id = dict.intern(&"x".to_string()).await.expect("intern key");
        let version = versioned
            .create_version(vec![SegmentRecord {
                id: 1,
                bucket: 0,
                deltas: vec![(id, 9)],
            }])
            .await
            .expect("create version");

        versioned
            .release_version(version)
            .await
            .expect("release version");

        let segments = table
            .scan_range(
                prefix_bounds(versioned.segment_prefix_bytes()),
                &ScanOptions::default(),
            )
            .await
            .expect("scan segments");
        assert!(segments.is_empty());

        let manifests = table
            .scan_range(
                prefix_bounds(versioned.manifest_prefix_bytes()),
                &ScanOptions::default(),
            )
            .await
            .expect("scan manifests");
        assert!(manifests.is_empty());

        let view = versioned.materialize().await.expect("materialize");
        assert!(view.is_empty());
    }

    #[tokio::test]
    async fn release_base_keeps_manifest_while_referenced() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "vz_refs", None)
                .await
                .expect("build dictionary"),
        );

        let mut versioned = VersionedZSet::new(dict.clone(), table.clone(), "vz_refs".to_string())
            .await
            .expect("create versioned zset");

        let base_id = dict
            .intern(&"base".to_string())
            .await
            .expect("intern base key");
        let v1 = versioned
            .create_version(vec![SegmentRecord {
                id: 1,
                bucket: 0,
                deltas: vec![(base_id, 2)],
            }])
            .await
            .expect("create base version");

        let child_id = dict
            .intern(&"child".to_string())
            .await
            .expect("intern child key");
        let v2 = versioned
            .create_version(vec![SegmentRecord {
                id: 2,
                bucket: 1,
                deltas: vec![(child_id, 3)],
            }])
            .await
            .expect("create child version");
        assert_eq!(v2, v1 + 1);

        versioned
            .release_version(v1)
            .await
            .expect("release base while child exists");

        let manifests = table
            .scan_range(
                prefix_bounds(versioned.manifest_prefix_bytes()),
                &ScanOptions::default(),
            )
            .await
            .expect("scan manifests after base release");
        assert_eq!(manifests.len(), 2);

        versioned
            .release_version(v2)
            .await
            .expect("release child version");

        let manifests = table
            .scan_range(
                prefix_bounds(versioned.manifest_prefix_bytes()),
                &ScanOptions::default(),
            )
            .await
            .expect("scan manifests after releasing child");
        assert!(manifests.is_empty());
    }

    #[tokio::test]
    async fn recovers_version_intent_on_reopen() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "vz_intent", None)
                .await
                .expect("build dictionary"),
        );

        let mut versioned =
            VersionedZSet::new(dict.clone(), table.clone(), "vz_intent".to_string())
                .await
                .expect("create versioned zset");

        let id = dict.intern(&"y".to_string()).await.expect("intern key");
        versioned
            .create_version(vec![SegmentRecord {
                id: 1,
                bucket: 0,
                deltas: vec![(id, 5)],
            }])
            .await
            .expect("create version");

        let mut batch = WriteBatch::new();
        batch.put(versioned.intent_key_bytes().to_vec(), vec![1]);
        table
            .write_batch(batch)
            .await
            .expect("write lingering intent");

        let reopened = VersionedZSet::new(dict, table, "vz_intent".to_string())
            .await
            .expect("reopen versioned zset");
        assert!(reopened.manifest().is_some());
    }
}
