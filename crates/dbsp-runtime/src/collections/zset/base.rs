use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};

use crate::storage::dictionary::{Dictionary, KeyIntern};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::{KeyValueTable, SlateTable};

use super::{ZSET_PREFIX, prefix_bounds};

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
            if let Some(bytes) = self.table.get_bytes(&encoded_key).await? {
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
                    dirty = true;
                }
                PendingValue::Delete => {
                    if let Some(id) = self.dict.lookup(&key).await? {
                        batch.delete(self.encode_id(id));
                        dirty = true;
                    }
                    self.cache.remove(&key);
                }
            }
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

#[cfg(test)]
mod tests;
