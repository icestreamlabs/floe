use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};

use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::{KeyValueTable, SlateTable};

const ZSET_PREFIX: &str = "zset/";

pub struct ZSet<K>
where
    K: Archive + Clone + Eq + Hash + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    prefix: Vec<u8>,
    cache: HashMap<K, i64>,
    pending: HashMap<K, PendingValue>,
}

enum PendingValue {
    Upsert(i64),
    Delete,
}

impl<K> ZSet<K>
where
    K: Archive + Clone + Eq + Hash + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(db: Arc<Db>, namespace: impl Into<String>) -> Self {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::with_table(table, namespace)
    }

    pub fn with_table(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let mut prefix = ZSET_PREFIX.as_bytes().to_vec();
        prefix.extend_from_slice(namespace.as_bytes());
        prefix.push(b'/');

        Self {
            table,
            prefix,
            cache: HashMap::new(),
            pending: HashMap::new(),
        }
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

        let encoded_key = self.encode_key(key)?;
        if let Some(bytes) = self.table.get(&encoded_key).await? {
            let weight = decode_weight(bytes.as_ref())?;
            self.cache.insert(key.clone(), weight);
            Ok(weight)
        } else {
            Ok(0)
        }
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
            let encoded_key = self.encode_key(&key)?;
            match change {
                PendingValue::Upsert(weight) => {
                    let value = encode_weight(weight);
                    batch.put(encoded_key, value);
                }
                PendingValue::Delete => {
                    batch.delete(encoded_key);
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
            .scan_range(prefix_bounds(&self.prefix), &ScanOptions::default())
            .await?;

        for (key_bytes, _) in entries {
            let key = self.decode_key(&key_bytes)?;
            if let Some(PendingValue::Delete) = self.pending.get(&key) {
                continue;
            }

            return Ok(false);
        }

        Ok(true)
    }

    fn encode_key(&self, key: &K) -> Result<Vec<u8>> {
        let mut namespaced = self.prefix.clone();
        let encoded = encoding::encode(key).context("unable to serialize ZSet key")?;
        namespaced.extend_from_slice(&encoded);
        Ok(namespaced)
    }

    fn decode_key(&self, key: &[u8]) -> Result<K> {
        if !key.starts_with(&self.prefix) {
            return Err(anyhow!("unexpected key prefix while decoding ZSet entry"));
        }

        let encoded = &key[self.prefix.len()..];
        encoding::decode(encoded).context("unable to deserialize ZSet key")
    }

    async fn load_all(&self) -> Result<HashMap<K, i64>> {
        let entries = self
            .table
            .scan_range(prefix_bounds(&self.prefix), &ScanOptions::default())
            .await?;

        let mut map = HashMap::new();
        for (key_bytes, value_bytes) in entries {
            let key = self.decode_key(&key_bytes)?;
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

fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use object_store::memory::InMemory;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("test", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn creates_and_persists_weights() {
        let db = build_db().await;
        let mut zset = ZSet::new(db.clone(), "weights");

        zset.set_weight("a".to_string(), 1);
        zset.set_weight("b".to_string(), 2);
        zset.flush().await.unwrap();

        let mut reload = ZSet::new(db, "weights");
        assert_eq!(reload.get_weight(&"a".to_string()).await.unwrap(), 1);
        assert_eq!(reload.get_weight(&"b".to_string()).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn logical_merge_of_pending_before_flush() {
        let db = build_db().await;
        let mut zset = ZSet::new(db, "merge");

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
        let mut zset = ZSet::new(db.clone(), "acc");

        zset.add_weight("key".to_string(), 3).await.unwrap();
        zset.flush().await.unwrap();

        let mut reload = ZSet::new(db, "acc");
        assert_eq!(reload.get_weight(&"key".to_string()).await.unwrap(), 3);

        reload.add_weight("key".to_string(), -3).await.unwrap();
        reload.flush().await.unwrap();
        assert!(reload.is_identity().await.unwrap());
    }
}
