use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

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
    reverse_prefix: Option<Vec<u8>>,
    range_prefix: Option<Vec<u8>>,
    marker: PhantomData<(K, V)>,
}

/// Encode keys into an order-preserving byte representation for range scans.
pub trait RangeKey {
    fn encode_range_key(&self) -> Vec<u8>;
    fn encoded_len(encoded: &[u8]) -> Result<usize>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct OrderedBytes(pub Vec<u8>);

impl OrderedBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for OrderedBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for OrderedBytes {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<&str> for OrderedBytes {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

fn encode_memcomparable(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    for &b in bytes {
        if b == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0);
    out.push(0);
    out
}

fn memcomparable_len(encoded: &[u8]) -> Result<usize> {
    let mut idx = 0;
    while idx + 1 < encoded.len() {
        if encoded[idx] != 0 {
            idx += 1;
            continue;
        }
        match encoded[idx + 1] {
            0xFF => {
                idx += 2;
            }
            0x00 => {
                return Ok(idx + 2);
            }
            other => {
                return Err(anyhow!("invalid memcomparable escape byte: {other:#04x}"));
            }
        }
    }
    Err(anyhow!("truncated memcomparable encoding"))
}

impl RangeKey for i64 {
    fn encode_range_key(&self) -> Vec<u8> {
        let shifted = (*self as u64) ^ 0x8000_0000_0000_0000;
        shifted.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(8)
    }
}

impl RangeKey for u64 {
    fn encode_range_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(8)
    }
}

impl RangeKey for i32 {
    fn encode_range_key(&self) -> Vec<u8> {
        let shifted = (*self as u32) ^ 0x8000_0000;
        shifted.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(4)
    }
}

impl RangeKey for u32 {
    fn encode_range_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(4)
    }
}

impl RangeKey for OrderedBytes {
    fn encode_range_key(&self) -> Vec<u8> {
        encode_memcomparable(self.as_bytes())
    }

    fn encoded_len(encoded: &[u8]) -> Result<usize> {
        memcomparable_len(encoded)
    }
}

impl RangeKey for String {
    fn encode_range_key(&self) -> Vec<u8> {
        encode_memcomparable(self.as_bytes())
    }

    fn encoded_len(encoded: &[u8]) -> Result<usize> {
        memcomparable_len(encoded)
    }
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
            reverse_prefix: None,
            range_prefix: None,
            marker: PhantomData,
        }
    }

    pub fn with_reverse_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let mut data_prefix = namespace_prefix(prefix::INDEX, &namespace);
        data_prefix.extend_from_slice(b"data/");
        let mut reverse_prefix = namespace_prefix(prefix::INDEX, &namespace);
        reverse_prefix.extend_from_slice(b"data_by_value/");
        Self {
            table,
            namespace,
            data_prefix,
            reverse_prefix: Some(reverse_prefix),
            range_prefix: None,
            marker: PhantomData,
        }
    }

    pub fn with_range_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let mut data_prefix = namespace_prefix(prefix::INDEX, &namespace);
        data_prefix.extend_from_slice(b"data/");
        let mut range_prefix = namespace_prefix(prefix::INDEX, &namespace);
        range_prefix.extend_from_slice(b"range/");
        Self {
            table,
            namespace,
            data_prefix,
            reverse_prefix: None,
            range_prefix: Some(range_prefix),
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

    pub async fn values_for_key_range(&self, lower: &K, upper: &K) -> Result<Vec<(K, V, i64)>>
    where
        K: RangeKey,
    {
        let range_prefix = self
            .range_prefix
            .as_ref()
            .ok_or_else(|| anyhow!("range index not enabled"))?;
        let lower_bytes = lower.encode_range_key();
        let upper_bytes = upper.encode_range_key();
        if lower_bytes >= upper_bytes {
            return Ok(Vec::new());
        }

        let start = self.range_prefix_for_bound(range_prefix, &lower_bytes)?;
        let end = self.range_prefix_for_bound(range_prefix, &upper_bytes)?;
        let entries = self
            .table
            .scan_range(start..end, &ScanOptions::default())
            .await
            .context("scan range join index")?;

        let mut values = Vec::with_capacity(entries.len());
        for (entry_key, entry_value) in entries {
            let key_bytes = self
                .key_bytes_from_range_key::<K>(range_prefix, &entry_key)
                .context("decode indexed range key bytes")?;
            let value_bytes = self
                .value_bytes_from_range_key::<K>(range_prefix, &entry_key)
                .context("decode indexed range value bytes")?;
            let key = decode::<K>(key_bytes).context("decode indexed range key")?;
            let value = decode::<V>(value_bytes).context("decode indexed range value")?;
            let weight = decode_weight(&entry_value)?;
            if weight != 0 {
                values.push((key, value, weight));
            }
        }
        Ok(values)
    }

    pub async fn keys_for_value(&self, value: &V) -> Result<Vec<(K, i64)>> {
        let reverse_prefix = self
            .reverse_prefix
            .as_ref()
            .ok_or_else(|| anyhow!("reverse index not enabled"))?;
        let value_bytes = encode(value).context("encode join value")?;
        let prefix = self.data_prefix_for_value(reverse_prefix, &value_bytes)?;
        let entries = self
            .table
            .scan_prefix(&prefix, &ScanOptions::default())
            .await
            .context("scan reverse join index prefix")?;

        let mut keys = Vec::with_capacity(entries.len());
        for (entry_key, entry_value) in entries {
            let key_bytes = self
                .key_bytes_from_reverse_key(reverse_prefix, &entry_key)
                .context("decode indexed key bytes")?;
            let key = decode::<K>(key_bytes).context("decode indexed key")?;
            let weight = decode_weight(&entry_value)?;
            if weight != 0 {
                keys.push((key, weight));
            }
        }
        Ok(keys)
    }

    pub async fn apply_deltas<I>(&self, deltas: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if self.range_prefix.is_some() {
            return Err(anyhow!(
                "range index enabled: use apply_deltas_with_range to maintain range keys"
            ));
        }
        let mut batch = WriteBatch::new();
        let mut wrote = false;
        let reverse_prefix = self.reverse_prefix.as_ref();

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
            if let Some(prefix) = reverse_prefix {
                let reverse_key = self
                    .reverse_composite_key(prefix, &value_bytes, &key_bytes)
                    .context("build reverse join index key")?;
                if next == 0 {
                    batch.delete(reverse_key);
                } else {
                    batch.put(reverse_key, encode_weight(next));
                }
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

    pub async fn apply_deltas_with_range<I>(&self, deltas: I) -> Result<()>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        let range_prefix = self
            .range_prefix
            .as_ref()
            .ok_or_else(|| anyhow!("range index not enabled"))?;
        let mut batch = WriteBatch::new();
        let mut wrote = false;
        let reverse_prefix = self.reverse_prefix.as_ref();

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
            if let Some(prefix) = reverse_prefix {
                let reverse_key = self
                    .reverse_composite_key(prefix, &value_bytes, &key_bytes)
                    .context("build reverse join index key")?;
                if next == 0 {
                    batch.delete(reverse_key);
                } else {
                    batch.put(reverse_key, encode_weight(next));
                }
            }
            let range_key = key.encode_range_key();
            let range_composite_key = self
                .range_composite_key(range_prefix, &range_key, &key_bytes, &value_bytes)
                .context("build range index key")?;
            if next == 0 {
                batch.delete(range_composite_key);
            } else {
                batch.put(range_composite_key, encode_weight(next));
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

    pub async fn entries(&self) -> Result<Vec<(K, V, i64)>> {
        let entries = self
            .table
            .scan_prefix(&self.data_prefix, &ScanOptions::default())
            .await
            .context("scan index entries")?;

        let mut values = Vec::with_capacity(entries.len());
        for (entry_key, entry_value) in entries {
            let key_bytes = self
                .key_bytes_from_key(&entry_key)
                .context("decode indexed key bytes")?;
            let value_bytes = self
                .value_bytes_from_key(&entry_key)
                .context("decode indexed value bytes")?;
            let key = decode::<K>(key_bytes).context("decode indexed key")?;
            let value = decode::<V>(value_bytes).context("decode indexed value")?;
            let weight = decode_weight(&entry_value)?;
            if weight != 0 {
                values.push((key, value, weight));
            }
        }
        Ok(values)
    }

    fn data_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.data_prefix.clone();
        prefix.extend_from_slice(&encode_len(key_bytes.len())?);
        prefix.extend_from_slice(key_bytes);
        Ok(prefix)
    }

    fn data_prefix_for_value(&self, reverse_prefix: &[u8], value_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = reverse_prefix.to_vec();
        prefix.extend_from_slice(&encode_len(value_bytes.len())?);
        prefix.extend_from_slice(value_bytes);
        Ok(prefix)
    }

    fn composite_key(&self, key_bytes: &[u8], value_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut key = self.data_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&encode_len(value_bytes.len())?);
        key.extend_from_slice(value_bytes);
        Ok(key)
    }

    fn range_prefix_for_bound(&self, range_prefix: &[u8], range_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = range_prefix.to_vec();
        prefix.extend_from_slice(range_bytes);
        Ok(prefix)
    }

    fn range_composite_key(
        &self,
        range_prefix: &[u8],
        range_bytes: &[u8],
        key_bytes: &[u8],
        value_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        let mut key = self.range_prefix_for_bound(range_prefix, range_bytes)?;
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
        key.extend_from_slice(&encode_len(value_bytes.len())?);
        key.extend_from_slice(value_bytes);
        Ok(key)
    }

    fn reverse_composite_key(
        &self,
        reverse_prefix: &[u8],
        value_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        let mut key = self.data_prefix_for_value(reverse_prefix, value_bytes)?;
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
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

    fn key_bytes_from_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if !key.starts_with(&self.data_prefix) {
            return Err(anyhow!("indexed key missing data prefix"));
        }
        let mut cursor = self.data_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read indexed key length")?;
        let end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("indexed key length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("indexed key payload truncated"))
    }

    fn value_bytes_from_range_key<'a, RK: RangeKey>(
        &self,
        range_prefix: &[u8],
        key: &'a [u8],
    ) -> Result<&'a [u8]> {
        if !key.starts_with(range_prefix) {
            return Err(anyhow!("indexed range key missing prefix"));
        }
        let mut cursor = range_prefix.len();
        let range_len = RK::encoded_len(
            key.get(cursor..)
                .ok_or_else(|| anyhow!("indexed range key truncated"))?,
        )?;
        cursor = cursor
            .checked_add(range_len)
            .ok_or_else(|| anyhow!("indexed range key length overflow"))?;
        let key_len = read_len(key, &mut cursor).context("read indexed range key length")?;
        cursor = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("indexed range key length overflow"))?;
        let value_len = read_len(key, &mut cursor).context("read indexed range value length")?;
        let end = cursor
            .checked_add(value_len)
            .ok_or_else(|| anyhow!("indexed range value length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("indexed range value payload truncated"))
    }

    fn key_bytes_from_range_key<'a, RK: RangeKey>(
        &self,
        range_prefix: &[u8],
        key: &'a [u8],
    ) -> Result<&'a [u8]> {
        if !key.starts_with(range_prefix) {
            return Err(anyhow!("indexed range key missing prefix"));
        }
        let mut cursor = range_prefix.len();
        let range_len = RK::encoded_len(
            key.get(cursor..)
                .ok_or_else(|| anyhow!("indexed range key truncated"))?,
        )?;
        cursor = cursor
            .checked_add(range_len)
            .ok_or_else(|| anyhow!("indexed range key length overflow"))?;
        let key_len = read_len(key, &mut cursor).context("read indexed range key length")?;
        let end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("indexed range key length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("indexed range key payload truncated"))
    }

    fn key_bytes_from_reverse_key<'a>(
        &self,
        reverse_prefix: &[u8],
        key: &'a [u8],
    ) -> Result<&'a [u8]> {
        if !key.starts_with(reverse_prefix) {
            return Err(anyhow!("indexed reverse key missing data prefix"));
        }
        let mut cursor = reverse_prefix.len();
        let value_len = read_len(key, &mut cursor).context("read indexed value length")?;
        cursor = cursor
            .checked_add(value_len)
            .ok_or_else(|| anyhow!("indexed value length overflow"))?;
        let key_len = read_len(key, &mut cursor).context("read indexed key length")?;
        let end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("indexed key length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("indexed key payload truncated"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::sync::Arc;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open("indexed_zset_reverse", store)
                .await
                .expect("open SlateDB"),
        )
    }

    #[tokio::test]
    async fn reverse_index_supports_value_lookups() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));
        let index = IndexedZSet::<i64, i64>::with_reverse_index(table.clone(), "reverse_index");

        index
            .apply_deltas(vec![(1, 10, 2), (2, 10, 1), (1, 11, 3)])
            .await
            .expect("apply deltas");

        let mut values = index.values_for_key(&1).await.expect("values for key");
        values.sort_by_key(|(value, _)| *value);
        assert_eq!(values, vec![(10, 2), (11, 3)]);

        let mut keys = index.keys_for_value(&10).await.expect("keys for value");
        keys.sort_by_key(|(key, _)| *key);
        assert_eq!(keys, vec![(1, 2), (2, 1)]);

        index
            .apply_deltas(vec![(1, 10, -2), (2, 10, -1)])
            .await
            .expect("apply deletes");

        let keys_after = index.keys_for_value(&10).await.expect("keys after delete");
        assert!(keys_after.is_empty());
    }

    #[tokio::test]
    async fn range_index_supports_key_scans() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));
        let index = IndexedZSet::<i64, i64>::with_range_index(table.clone(), "range_index");

        index
            .apply_deltas_with_range(vec![(1, 10, 1), (3, 30, 2), (5, 50, 1)])
            .await
            .expect("apply deltas");

        let mut entries = index
            .values_for_key_range(&2, &6)
            .await
            .expect("range scan");
        entries.sort_by_key(|(key, value, _)| (*key, *value));

        assert_eq!(entries, vec![(3, 30, 2), (5, 50, 1)]);
    }

    #[tokio::test]
    async fn range_index_orders_bytes_lexicographically() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db));
        let index =
            IndexedZSet::<OrderedBytes, i64>::with_range_index(table.clone(), "range_index_bytes");

        index
            .apply_deltas_with_range(vec![
                (OrderedBytes::from("b"), 10, 1),
                (OrderedBytes::from("aa"), 20, 1),
                (OrderedBytes::from("c"), 30, 1),
            ])
            .await
            .expect("apply deltas");

        let mut entries = index
            .values_for_key_range(&OrderedBytes::from("b"), &OrderedBytes::from("d"))
            .await
            .expect("range scan");
        entries.sort_by(|(ka, va, _), (kb, vb, _)| {
            ka.as_bytes().cmp(kb.as_bytes()).then_with(|| va.cmp(vb))
        });

        assert_eq!(
            entries,
            vec![
                (OrderedBytes::from("b"), 10, 1),
                (OrderedBytes::from("c"), 30, 1),
            ]
        );
    }
}
