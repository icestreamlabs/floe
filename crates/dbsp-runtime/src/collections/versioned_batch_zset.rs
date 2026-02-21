use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{BinaryBuilder, Int64Builder};
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use rkyv::{Deserialize, Serialize};
use slatedb::WriteBatch;
use tokio::sync::Mutex as AsyncMutex;

use crate::storage::KeyValueTable;
use crate::storage::encoding::{
    self, RkyvDeserializer, RkyvSerializer, RkyvValidator, decode, encode,
};
use crate::storage::segment::{ArrowSegmentStore, SegmentWriteStats};

#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
struct VersionManifest {
    base: Option<u64>,
    segments: Vec<u64>,
}

pub struct VersionedBatchZSet<K>
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
    segment_store: ArrowSegmentStore,
    schema: SchemaRef,
    manifest_prefix: Vec<u8>,
    current_version_key: Vec<u8>,
    next_version_key: Vec<u8>,
    next_segment_id_key: Vec<u8>,
    sequence_lock: AsyncMutex<()>,
    _marker: std::marker::PhantomData<K>,
}

impl<K> VersionedBatchZSet<K>
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
    pub fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let mut base = b"versioned_batch_arrow/".to_vec();
        base.extend_from_slice(namespace.as_bytes());
        base.push(b'/');

        let mut manifest_prefix = base.clone();
        manifest_prefix.extend_from_slice(b"manifest/");

        let mut current_version_key = base.clone();
        current_version_key.extend_from_slice(b"current_version");

        let mut next_version_key = base.clone();
        next_version_key.extend_from_slice(b"next_version");

        let mut next_segment_id_key = base;
        next_segment_id_key.extend_from_slice(b"next_segment_id");

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("delta", DataType::Int64, false),
        ]));

        Self {
            table: table.clone(),
            segment_store: ArrowSegmentStore::new(
                table,
                format!("versioned_batch_arrow/{namespace}"),
            ),
            schema,
            manifest_prefix,
            current_version_key,
            next_version_key,
            next_segment_id_key,
            sequence_lock: AsyncMutex::new(()),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn apply_deltas<I>(&self, deltas: I) -> Result<u64>
    where
        I: IntoIterator<Item = (K, i64)>,
    {
        self.create_version(deltas).await
    }

    pub async fn create_version<I>(&self, deltas: I) -> Result<u64>
    where
        I: IntoIterator<Item = (K, i64)>,
    {
        let mut encoded_rows = Vec::new();
        let mut min_hash = u64::MAX;
        let mut max_hash = 0_u64;
        let mut tombstones = 0_usize;

        for (key, delta) in deltas {
            if delta == 0 {
                continue;
            }
            let key_bytes = encode(&key).context("encode versioned Arrow key")?;
            let key_hash = hash_bytes(&key_bytes);
            min_hash = min_hash.min(key_hash);
            max_hash = max_hash.max(key_hash);
            if delta < 0 {
                tombstones = tombstones.saturating_add(1);
            }
            encoded_rows.push((key_bytes, delta));
        }

        if encoded_rows.is_empty() {
            return self.current_version().await;
        }

        let _guard = self.sequence_lock.lock().await;
        let current_version = self
            .read_u64_or_default(&self.current_version_key, 0)
            .await?;
        let version = self.read_u64_or_default(&self.next_version_key, 1).await?;
        let segment_id = self
            .read_u64_or_default(&self.next_segment_id_key, 1)
            .await?;

        let batch = self.record_batch_from_rows(&encoded_rows)?;
        let tombstone_ratio = tombstones as f64 / encoded_rows.len() as f64;
        self.segment_store
            .write_segment(
                segment_id,
                Arc::clone(&self.schema),
                &[batch],
                SegmentWriteStats::new(min_hash, max_hash, tombstone_ratio)
                    .context("build versioned Arrow segment stats")?,
            )
            .await
            .with_context(|| format!("write versioned Arrow segment {segment_id}"))?;

        let manifest = VersionManifest {
            base: (current_version != 0).then_some(current_version),
            segments: vec![segment_id],
        };

        let mut write_batch = WriteBatch::new();
        write_batch.put(
            self.manifest_key(version),
            encoding::encode(&manifest).context("encode versioned Arrow manifest")?,
        );
        write_batch.put(
            self.current_version_key.clone(),
            version.to_be_bytes().to_vec(),
        );
        write_batch.put(
            self.next_version_key.clone(),
            version.saturating_add(1).to_be_bytes().to_vec(),
        );
        write_batch.put(
            self.next_segment_id_key.clone(),
            segment_id.saturating_add(1).to_be_bytes().to_vec(),
        );
        self.table
            .write_batch(write_batch)
            .await
            .context("persist versioned Arrow manifest")?;

        Ok(version)
    }

    pub async fn current_version(&self) -> Result<u64> {
        self.read_u64_or_default(&self.current_version_key, 0).await
    }

    pub async fn materialize(&self) -> Result<HashMap<K, i64>> {
        let current = self.current_version().await?;
        self.materialize_version(current).await
    }

    pub async fn materialize_version(&self, version: u64) -> Result<HashMap<K, i64>> {
        if version == 0 {
            return Ok(HashMap::new());
        }

        let mut chain = Vec::new();
        let mut cursor = Some(version);
        while let Some(current) = cursor {
            let manifest = self
                .load_manifest(current)
                .await
                .with_context(|| format!("load versioned Arrow manifest {current}"))?;
            cursor = manifest.base;
            chain.push(manifest);
        }

        chain.reverse();

        let mut aggregate = HashMap::<Vec<u8>, i64>::new();
        for manifest in chain {
            for segment_id in manifest.segments {
                let Some(segment) = self
                    .segment_store
                    .read_segment(segment_id)
                    .await
                    .with_context(|| format!("read versioned Arrow segment {segment_id}"))?
                else {
                    return Err(anyhow!("missing versioned Arrow segment {segment_id}"));
                };

                for batch in &segment.batches {
                    let key_col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| anyhow!("invalid versioned Arrow key column type"))?;
                    let delta_col = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow!("invalid versioned Arrow delta column type"))?;

                    for idx in 0..batch.num_rows() {
                        let key_bytes = key_col.value(idx).to_vec();
                        let delta = delta_col.value(idx);
                        *aggregate.entry(key_bytes).or_insert(0) += delta;
                    }
                }
            }
        }

        let mut out = HashMap::new();
        for (key_bytes, weight) in aggregate {
            if weight == 0 {
                continue;
            }
            let key = decode::<K>(&key_bytes)
                .context("decode key bytes while materializing versioned Arrow state")?;
            out.insert(key, weight);
        }

        Ok(out)
    }

    async fn load_manifest(&self, version: u64) -> Result<VersionManifest> {
        let bytes = self
            .table
            .get(&self.manifest_key(version))
            .await
            .context("read versioned Arrow manifest")?
            .ok_or_else(|| anyhow!("missing versioned Arrow manifest {version}"))?;
        encoding::decode(&bytes).context("decode versioned Arrow manifest")
    }

    async fn read_u64_or_default(&self, key: &[u8], default: u64) -> Result<u64> {
        match self
            .table
            .get(key)
            .await
            .with_context(|| format!("read versioned Arrow meta key len={}", key.len()))?
        {
            Some(bytes) => decode_u64_payload(&bytes),
            None => Ok(default),
        }
    }

    fn record_batch_from_rows(&self, rows: &[(Vec<u8>, i64)]) -> Result<RecordBatch> {
        let mut key_builder = BinaryBuilder::new();
        let mut delta_builder = Int64Builder::new();

        for (key, delta) in rows {
            key_builder.append_value(key);
            delta_builder.append_value(*delta);
        }

        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(key_builder.finish()) as ArrayRef,
                Arc::new(delta_builder.finish()) as ArrayRef,
            ],
        )
        .context("build versioned Arrow record batch")
    }

    fn manifest_key(&self, version: u64) -> Vec<u8> {
        let mut key = self.manifest_prefix.clone();
        key.extend_from_slice(&version.to_be_bytes());
        key
    }
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

fn decode_u64_payload(bytes: &[u8]) -> Result<u64> {
    let chunk = bytes
        .get(0..8)
        .ok_or_else(|| anyhow!("expected 8 bytes for versioned Arrow u64 payload"))?;
    Ok(u64::from_be_bytes(chunk.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::Db;

    use crate::storage::SlateTable;

    use super::VersionedBatchZSet;

    async fn build_table(namespace: &str) -> Arc<dyn crate::storage::KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(namespace, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    #[tokio::test]
    async fn versioned_batch_zset_materializes_current_version() {
        let table = build_table("arrow-versioned-current").await;
        let versioned = VersionedBatchZSet::<i64>::new(table, "arrow_versioned_current");
        let v1 = versioned
            .apply_deltas(vec![(1, 2), (2, 3)])
            .await
            .expect("apply version 1");
        let v2 = versioned
            .apply_deltas(vec![(1, -2), (3, 5)])
            .await
            .expect("apply version 2");
        assert!(v2 > v1, "versions should advance");

        let materialized = versioned.materialize().await.expect("materialize current");
        assert_eq!(materialized.get(&1), None);
        assert_eq!(materialized.get(&2).copied(), Some(3));
        assert_eq!(materialized.get(&3).copied(), Some(5));
    }

    #[tokio::test]
    async fn versioned_batch_zset_materializes_historical_version() {
        let table = build_table("arrow-versioned-history").await;
        let versioned = VersionedBatchZSet::<i64>::new(table, "arrow_versioned_history");
        let v1 = versioned
            .apply_deltas(vec![(7, 1), (8, 2)])
            .await
            .expect("apply version 1");
        versioned
            .apply_deltas(vec![(7, -1), (9, 4)])
            .await
            .expect("apply version 2");

        let historical = versioned
            .materialize_version(v1)
            .await
            .expect("materialize version 1");
        assert_eq!(historical.get(&7).copied(), Some(1));
        assert_eq!(historical.get(&8).copied(), Some(2));
        assert_eq!(historical.get(&9), None);
    }
}
