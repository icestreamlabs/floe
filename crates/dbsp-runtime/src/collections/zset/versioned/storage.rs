use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::handles::ZSetHandle;
use crate::storage::dictionary::KeyIntern;
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::util::{publish_transient_zset_batch, transient_zset_batch};

use super::super::prefix_bounds;
use super::{
    SegmentId, SegmentRecord, VersionChainStats, VersionWritePlan, VersionedZSet,
    ZSetVersionManifest,
};

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
        let base = self.manifest.as_ref().map(|_| self.persisted_version);
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

        let mut buckets = BTreeMap::new();
        for record in &processed {
            let key = self.segment_key(record.bucket, record.id);
            let encoded =
                encode_segment_record(record).context("encode versioned segment as Arrow IPC")?;
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
        self.persisted_version = plan.version;
        self.manifest = Some(plan.manifest.clone());
    }

    pub fn publish_replayable_batch(&mut self, deltas: Arc<Vec<(K, i64)>>) -> ZSetHandle {
        let version = self.current_version.saturating_add(1);
        self.current_version = version;
        let handle = self.handle_for_version(version);
        publish_transient_zset_batch(&handle, deltas);
        handle
    }

    pub async fn chain_stats(&self) -> Result<VersionChainStats> {
        if self.persisted_version == 0 {
            return Ok(VersionChainStats::default());
        }

        let mut version_count = 0;
        let mut segment_count = 0;
        let mut bucket_segment_counts: BTreeMap<u16, usize> = BTreeMap::new();
        let mut current = self.manifest.clone();

        while let Some(manifest) = current {
            version_count += 1;
            for (bucket, segments) in &manifest.buckets {
                let count = segments.len();
                segment_count += count;
                *bucket_segment_counts.entry(*bucket).or_insert(0) += count;
            }
            if let Some(base_version) = manifest.base {
                current = Some(self.load_manifest_record(base_version).await?);
            } else {
                break;
            }
        }

        let max_bucket_segment_count = bucket_segment_counts.values().copied().max().unwrap_or(0);
        Ok(VersionChainStats {
            version_count,
            segment_count,
            max_bucket_segment_count,
        })
    }

    pub async fn materialize(&self) -> Result<HashMap<K, i64>> {
        if self.current_version != 0
            && self.current_version != self.persisted_version
            && let Some(batch) =
                transient_zset_batch::<K>(&self.handle_for_version(self.current_version))
        {
            let mut aggregate: HashMap<K, i64> = HashMap::with_capacity(batch.len());
            for (key, delta) in batch.as_ref() {
                let next = aggregate
                    .get(key)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*delta);
                if next == 0 {
                    aggregate.remove(key);
                } else {
                    aggregate.insert(key.clone(), next);
                }
            }
            return Ok(aggregate);
        }

        let span = tracing::debug_span!(
            "materialize",
            namespace = %self.namespace,
            version = self.current_version
        );
        let _enter = span.enter();

        let total_start = Instant::now();
        let base_load_start = Instant::now();
        let mut aggregate = if let Some(base_version) = self.manifest.as_ref().and_then(|m| m.base)
        {
            self.load_version_chain(base_version).await?
        } else {
            HashMap::new()
        };
        let base_load_ms = base_load_start.elapsed().as_millis() as u64;

        let mut current_segment_count = 0usize;
        let mut current_segment_load_ms = 0u64;
        let mut current_delta_rows = 0usize;
        let mut current_resolve_calls = 0usize;
        let current_resolve_start = Instant::now();
        if let Some(current) = &self.manifest {
            for (bucket, segments) in &current.buckets {
                for segment_id in segments {
                    current_segment_count += 1;
                    let segment_start = Instant::now();
                    let record = self.load_segment(*bucket, *segment_id).await?;
                    current_segment_load_ms += segment_start.elapsed().as_millis() as u64;
                    current_delta_rows += record.deltas.len();
                    for (key_id, delta) in record.deltas {
                        current_resolve_calls += 1;
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
        let current_resolve_ms = current_resolve_start.elapsed().as_millis() as u64;

        let rows_before_retain = aggregate.len();
        aggregate.retain(|_, weight| *weight != 0);
        tracing::debug!(
            namespace = %self.namespace,
            version = self.current_version,
            base_load_ms,
            current_segment_count,
            current_segment_load_ms,
            current_delta_rows,
            current_resolve_calls,
            current_resolve_ms,
            rows_before_retain,
            rows_after_retain = aggregate.len(),
            total_ms = total_start.elapsed().as_millis() as u64,
            "versioned zset materialize breakdown"
        );
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
        if version == 0 {
            return Ok(Vec::new());
        }

        if version == self.current_version
            && version != self.persisted_version
            && let Some(batch) = transient_zset_batch::<K>(&self.handle_for_version(version))
        {
            return Ok(batch.as_ref().clone());
        }

        let total_start = Instant::now();
        let manifest_start = Instant::now();
        let manifest = self.load_manifest_record(version).await?;
        let manifest_load_ms = manifest_start.elapsed().as_millis() as u64;
        let bucket_count = manifest.buckets.len();

        let mut entries = Vec::new();
        let mut resolved_by_id: HashMap<u64, K> = HashMap::new();
        let mut segment_count = 0usize;
        let mut segment_load_ms = 0u64;

        for (bucket, segments) in manifest.buckets {
            for segment_id in segments {
                segment_count += 1;
                let segment_start = Instant::now();
                let record = self.load_segment(bucket, segment_id).await?;
                segment_load_ms += segment_start.elapsed().as_millis() as u64;
                entries.extend(record.deltas);
            }
        }

        if entries.is_empty() {
            tracing::debug!(
                namespace = %self.namespace,
                version,
                bucket_count,
                segment_count,
                manifest_load_ms,
                segment_load_ms,
                total_ms = total_start.elapsed().as_millis() as u64,
                "versioned zset delta_iter_with_dict breakdown"
            );
            return Ok(Vec::new());
        }

        let mut missing_ids = Vec::new();
        let mut seen = HashSet::new();
        for (key_id, _) in &entries {
            if resolved_by_id.contains_key(key_id) {
                continue;
            }
            if seen.insert(*key_id) {
                missing_ids.push(*key_id);
            }
        }

        let resolve_many_start = Instant::now();
        if !missing_ids.is_empty() {
            let resolved = self
                .dict
                .resolve_many(&missing_ids)
                .await
                .context("resolve keys while iterating delta layer")?;
            for (key_id, key) in missing_ids.iter().copied().zip(resolved) {
                resolved_by_id.insert(key_id, key);
            }
        }
        let resolve_many_ms = resolve_many_start.elapsed().as_millis() as u64;

        let remap_start = Instant::now();
        let mut deltas = Vec::with_capacity(entries.len());
        for (key_id, delta) in entries {
            let key = resolved_by_id
                .get(&key_id)
                .cloned()
                .ok_or_else(|| anyhow!("resolved key {key_id} missing from local cache"))?;
            deltas.push((key, delta));
        }
        let remap_ms = remap_start.elapsed().as_millis() as u64;

        tracing::debug!(
            namespace = %self.namespace,
            version,
            bucket_count,
            segment_count,
            manifest_load_ms,
            segment_load_ms,
            delta_rows = deltas.len(),
            unique_key_ids = resolved_by_id.len(),
            resolve_many_ms,
            remap_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "versioned zset delta_iter_with_dict breakdown"
        );

        Ok(deltas)
    }

    pub async fn load_existing_version(&self, version: u64) -> Result<HashMap<K, i64>> {
        if version == 0 {
            return Ok(HashMap::new());
        }
        if version == self.current_version
            && version != self.persisted_version
            && let Some(batch) = transient_zset_batch::<K>(&self.handle_for_version(version))
        {
            let mut aggregate: HashMap<K, i64> = HashMap::with_capacity(batch.len());
            for (key, delta) in batch.as_ref() {
                let next = aggregate
                    .get(key)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*delta);
                if next == 0 {
                    aggregate.remove(key);
                } else {
                    aggregate.insert(key.clone(), next);
                }
            }
            return Ok(aggregate);
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
        if let Some(intent_bytes) = self.table.get_bytes(&self.intent_key).await?
            && !intent_bytes.is_empty()
        {
            self.table
                .delete(&self.intent_key)
                .await
                .context("clear stale versioned intent")?;
        }

        let entries = self
            .table
            .scan_range_bytes(
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
        self.persisted_version = max_version;
        self.manifest = current;
        self.next_segment_id = max_segment_id.saturating_add(1).max(1);
        Ok(())
    }

    async fn load_version_chain(&self, version: u64) -> Result<HashMap<K, i64>> {
        let total_start = Instant::now();
        let manifest_load_start = Instant::now();
        let mut chain = Vec::new();
        let mut manifests = Vec::new();
        let mut current = Some(version);

        while let Some(v) = current {
            let key = self.manifest_key(v);
            let bytes = self
                .table
                .get_bytes(&key)
                .await?
                .ok_or_else(|| anyhow!("manifest version {v} not found"))?;
            let manifest = decode_manifest(bytes.as_ref())?;
            chain.push(v);
            manifests.push(manifest.clone());
            current = manifest.base;
        }
        let manifest_load_ms = manifest_load_start.elapsed().as_millis() as u64;

        let mut aggregate = HashMap::new();
        let mut segment_count = 0usize;
        let mut segment_load_ms = 0u64;
        let mut delta_rows = 0usize;
        let mut resolve_calls = 0usize;
        let resolve_start = Instant::now();
        for manifest in manifests.into_iter().rev() {
            for (bucket, segments) in manifest.buckets {
                for segment_id in segments {
                    segment_count += 1;
                    let segment_start = Instant::now();
                    let record = self.load_segment(bucket, segment_id).await?;
                    segment_load_ms += segment_start.elapsed().as_millis() as u64;
                    delta_rows += record.deltas.len();
                    for (key_id, delta) in record.deltas {
                        resolve_calls += 1;
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
        let resolve_ms = resolve_start.elapsed().as_millis() as u64;
        let rows_before_retain = aggregate.len();
        aggregate.retain(|_, weight| *weight != 0);

        tracing::debug!(
            namespace = %self.namespace,
            chain_head = version,
            chain_versions = chain.len(),
            manifest_load_ms,
            segment_count,
            segment_load_ms,
            delta_rows,
            resolve_calls,
            resolve_ms,
            rows_before_retain,
            rows = aggregate.len(),
            total_ms = total_start.elapsed().as_millis() as u64,
            "versioned zset load_version_chain breakdown"
        );

        Ok(aggregate)
    }

    pub(super) async fn load_segment(
        &self,
        bucket: u16,
        segment: SegmentId,
    ) -> Result<SegmentRecord> {
        let key = self.segment_key(bucket, segment);
        let bytes =
            self.table.get_bytes(&key).await?.ok_or_else(|| {
                anyhow!("segment not found for bucket {bucket} segment {segment}")
            })?;
        decode_segment_record(bucket, segment, bytes.as_ref())
            .context("decode versioned segment from Arrow IPC")
    }

    pub(super) async fn load_manifest_record(&self, version: u64) -> Result<ZSetVersionManifest> {
        let key = self.manifest_key(version);
        let bytes = self
            .table
            .get_bytes(&key)
            .await?
            .ok_or_else(|| anyhow!("manifest version {version} not found"))?;
        decode_manifest(bytes.as_ref())
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

fn segment_delta_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key_id", DataType::UInt64, false),
        Field::new("delta", DataType::Int64, false),
    ]))
}

fn encode_segment_record(record: &SegmentRecord) -> Result<Vec<u8>> {
    let schema = segment_delta_schema();
    let key_ids: Vec<u64> = record.deltas.iter().map(|(key_id, _)| *key_id).collect();
    let deltas: Vec<i64> = record.deltas.iter().map(|(_, delta)| *delta).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(key_ids)) as ArrayRef,
            Arc::new(Int64Array::from(deltas)) as ArrayRef,
        ],
    )
    .context("build versioned segment Arrow batch")?;

    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, schema.as_ref())
            .context("create versioned segment Arrow writer")?;
        writer
            .write(&batch)
            .context("write versioned segment Arrow batch")?;
        writer
            .finish()
            .context("finalize versioned segment Arrow writer")?;
    }

    Ok(payload)
}

fn decode_segment_record(
    bucket: u16,
    segment_id: SegmentId,
    bytes: &[u8],
) -> Result<SegmentRecord> {
    let cursor = Cursor::new(bytes);
    let mut reader =
        StreamReader::try_new(cursor, None).context("create versioned segment Arrow reader")?;

    let mut deltas = Vec::new();
    for batch in &mut reader {
        let batch = batch.context("read versioned segment Arrow batch")?;
        let key_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("invalid versioned segment key_id column type"))?;
        let weights = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("invalid versioned segment delta column type"))?;

        for idx in 0..batch.num_rows() {
            deltas.push((key_ids.value(idx), weights.value(idx)));
        }
    }

    Ok(SegmentRecord {
        id: segment_id,
        bucket,
        deltas,
    })
}
