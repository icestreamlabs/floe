use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::storage::dictionary::KeyIntern;
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::{SegmentId, SegmentRecord, VersionChainStats, VersionWritePlan, VersionedZSet, ZSetVersionManifest};
use super::super::prefix_bounds;

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

    pub async fn chain_stats(&self) -> Result<VersionChainStats> {
        if self.current_version == 0 {
            return Ok(VersionChainStats::default());
        }

        let mut version_count = 0;
        let mut segment_count = 0;
        let mut current = self.manifest.clone();

        while let Some(manifest) = current {
            version_count += 1;
            segment_count += manifest.buckets.values().map(|segments| segments.len()).sum::<usize>();
            if let Some(base_version) = manifest.base {
                current = Some(self.load_manifest_record(base_version).await?);
            } else {
                break;
            }
        }

        Ok(VersionChainStats {
            version_count,
            segment_count,
        })
    }

    pub async fn materialize(&self) -> Result<HashMap<K, i64>> {
        let span = tracing::debug_span!(
            "materialize",
            namespace = %self.namespace,
            version = self.current_version
        );
        let _enter = span.enter();
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

    pub async fn delta_iter(&self, version: u64) -> Result<Vec<(u64, i64)>> {
        if version == 0 {
            return Ok(Vec::new());
        }

        let manifest = self.load_manifest_record(version).await?;
        let mut deltas = Vec::new();

        for (bucket, segments) in manifest.buckets {
            for segment_id in segments {
                let record = self.load_segment(bucket, segment_id).await?;
                deltas.extend(record.deltas);
            }
        }

        Ok(deltas)
    }

    pub async fn delta_iter_with_dict(&self, version: u64) -> Result<Vec<(K, i64)>> {
        const LOCAL_LRU_CAPACITY: usize = 256;

        struct LocalLru<K> {
            capacity: usize,
            order: VecDeque<u64>,
            values: HashMap<u64, K>,
        }

        impl<K: Clone> LocalLru<K> {
            fn new(capacity: usize) -> Self {
                Self {
                    capacity,
                    order: VecDeque::new(),
                    values: HashMap::new(),
                }
            }

            fn get(&mut self, key: u64) -> Option<K> {
                let value = self.values.get(&key)?.clone();
                if let Some(pos) = self.order.iter().position(|id| *id == key) {
                    self.order.remove(pos);
                }
                self.order.push_back(key);
                Some(value)
            }

            fn insert(&mut self, key: u64, value: K) {
                if self.capacity == 0 {
                    return;
                }
                if let std::collections::hash_map::Entry::Occupied(mut entry) =
                    self.values.entry(key)
                {
                    entry.insert(value);
                    if let Some(pos) = self.order.iter().position(|id| *id == key) {
                        self.order.remove(pos);
                    }
                    self.order.push_back(key);
                    return;
                }
                if self.values.len() == self.capacity && let Some(evicted) = self.order.pop_front()
                {
                    self.values.remove(&evicted);
                }
                self.order.push_back(key);
                self.values.insert(key, value);
            }
        }

        if version == 0 {
            return Ok(Vec::new());
        }

        let manifest = self.load_manifest_record(version).await?;
        let mut deltas = Vec::new();
        let mut cache = LocalLru::new(LOCAL_LRU_CAPACITY);

        for (bucket, segments) in manifest.buckets {
            for segment_id in segments {
                let record = self.load_segment(bucket, segment_id).await?;
                for (key_id, delta) in record.deltas {
                    let key = if let Some(value) = cache.get(key_id) {
                        value
                    } else {
                        let resolved = self
                            .dict
                            .resolve(key_id)
                            .await
                            .context("resolve key while iterating delta layer")?;
                        cache.insert(key_id, resolved.clone());
                        resolved
                    };
                    deltas.push((key, delta));
                }
            }
        }

        Ok(deltas)
    }

    pub async fn load_existing_version(&self, version: u64) -> Result<HashMap<K, i64>> {
        if version == 0 {
            return Ok(HashMap::new());
        }
        self.load_version_chain(version).await
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

    pub(super) async fn refresh_state(&mut self) -> Result<()> {
        if let Some(intent_bytes) = self.table.get(&self.intent_key).await?
            && !intent_bytes.is_empty()
        {
            self.table
                .delete(&self.intent_key)
                .await
                .context("clear stale versioned intent")?;
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

    pub(super) async fn load_manifest_record(&self, version: u64) -> Result<ZSetVersionManifest> {
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
}

fn encode_manifest(manifest: &ZSetVersionManifest) -> Result<Vec<u8>> {
    encoding::encode(manifest).context("encode ZSet manifest")
}

fn decode_manifest(bytes: &[u8]) -> Result<ZSetVersionManifest> {
    encoding::decode(bytes).context("decode ZSet manifest")
}
