use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::WriteBatch;

use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator, decode, encode};
use crate::storage::keyspace::{namespace_prefix, prefix};

pub struct IndexedZSet<K, V>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    data_prefix: Vec<u8>,
    marker: PhantomData<(K, V)>,
}

impl<K, V> IndexedZSet<K, V>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let mut data_prefix = namespace_prefix(prefix::INDEX, &namespace);
        data_prefix.extend_from_slice(b"data/");
        Self {
            table,
            namespace,
            data_prefix,
            marker: PhantomData,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub async fn values_for_key(&self, key: &K) -> Result<Vec<(V, i64)>> {
        let key_bytes = encode(key).context("encode join key")?;
        let prefix = self.data_prefix_for_key(&key_bytes)?;
        let entries = self
            .table
            .scan_prefix(&prefix, &ScanOptions::default())
            .await
            .context("scan join index prefix")?;

        let mut values = Vec::with_capacity(entries.len());
        for (entry_key, entry_value) in entries {
            let value_bytes = self
                .value_bytes_from_key(&entry_key)
                .context("decode indexed value bytes")?;
            let value = decode::<V>(value_bytes).context("decode indexed value")?;
            let weight = decode_weight(&entry_value)?;
            if weight != 0 {
                values.push((value, weight));
            }
        }
        Ok(values)
    }

    pub async fn apply_deltas<I>(&self, deltas: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        let mut batch = WriteBatch::new();
        let mut wrote = false;

        for (key, value, delta) in deltas {
            if delta == 0 {
                continue;
            }
            let key_bytes = encode(&key).context("encode join key")?;
            let value_bytes = encode(&value).context("encode join value")?;
            let composite_key = self
                .composite_key(&key_bytes, &value_bytes)
                .context("build join index key")?;
            let existing = self.table.get(&composite_key).await?;
            let current = match existing {
                Some(bytes) => decode_weight(&bytes)?,
                None => 0,
            };
            let next = current + delta;
            if next == 0 {
                batch.delete(composite_key);
            } else {
                batch.put(composite_key, encode_weight(next));
            }
            wrote = true;
        }

        if wrote {
            self.table
                .write_batch(batch)
                .await
                .context("persist join index updates")?;
        }

        Ok(())
    }

    fn data_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.data_prefix.clone();
        prefix.extend_from_slice(&encode_len(key_bytes.len())?);
        prefix.extend_from_slice(key_bytes);
        Ok(prefix)
    }

    fn composite_key(&self, key_bytes: &[u8], value_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut key = self.data_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&encode_len(value_bytes.len())?);
        key.extend_from_slice(value_bytes);
        Ok(key)
    }

    fn value_bytes_from_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if !key.starts_with(&self.data_prefix) {
            return Err(anyhow!("indexed key missing data prefix"));
        }
        let mut cursor = self.data_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read indexed key length")?;
        cursor = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("indexed key length overflow"))?;
        let value_len = read_len(key, &mut cursor).context("read indexed value length")?;
        let end = cursor
            .checked_add(value_len)
            .ok_or_else(|| anyhow!("indexed value length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("indexed value payload truncated"))
    }
}

fn encode_len(len: usize) -> Result<[u8; 4]> {
    let len = u32::try_from(len).map_err(|_| anyhow!("indexed key component too large"))?;
    Ok(len.to_be_bytes())
}

fn read_len(bytes: &[u8], cursor: &mut usize) -> Result<usize> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| anyhow!("indexed length overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("indexed length truncated"))?;
    *cursor = end;
    Ok(u32::from_be_bytes(chunk.try_into().unwrap()) as usize)
}

fn encode_weight(weight: i64) -> Vec<u8> {
    weight.to_be_bytes().to_vec()
}

fn decode_weight(bytes: &[u8]) -> Result<i64> {
    let chunk = bytes
        .get(0..8)
        .ok_or_else(|| anyhow!("expected 8 bytes for indexed weight"))?;
    Ok(i64::from_be_bytes(chunk.try_into().unwrap()))
}
