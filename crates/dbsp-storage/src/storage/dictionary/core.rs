use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use futures::future::try_join_all;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};
use xxhash_rust::xxh3::xxh3_64;

use super::super::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use super::super::keyspace::{self, namespace_prefix};
use super::super::{KeyValueTable, SlateTable};
use super::batch::DictionaryBatch;
use super::cache::{BatchOverlay, Cache, SharedKey};
use super::codec::{compress_value, decode_id, decompress_value, encode_id};
use super::{HashFn, KeyIntern};

const K2ID_PREFIX: &[u8] = b"k2id/";
const ID2K_PREFIX: &[u8] = b"id2k/";
const META_NEXT_ID: &[u8] = b"meta/next_id";
const RESOLVE_MANY_FETCH_CHUNK: usize = 256;
const RESOLVE_MANY_RANGE_SCAN_MIN_IDS: usize = 2;

enum LookupExistingResult {
    Existing(u64),
    Missing { first_free_slot: u16 },
}

pub struct Dictionary<K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    table: Arc<dyn KeyValueTable>,
    k2id_prefix: Vec<u8>,
    id2k_prefix: Vec<u8>,
    meta_key: Vec<u8>,
    next_id: AtomicU64,
    cache: Mutex<Cache>,
    fresh_next_slot_by_hash: Mutex<AHashMap<u64, u16>>,
    fast_path_fresh: bool,
    hash_fn: HashFn,
    _marker: std::marker::PhantomData<K>,
}

impl<K> Dictionary<K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    pub async fn new(db: Arc<Db>, namespace: impl Into<String>) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::with_table(table, namespace, None).await
    }

    pub async fn with_table(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        hash_fn: Option<HashFn>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let base = namespace_prefix(keyspace::prefix::DICT, &namespace);

        let mut k2id_prefix = base.clone();
        k2id_prefix.extend_from_slice(K2ID_PREFIX);

        let mut id2k_prefix = base.clone();
        id2k_prefix.extend_from_slice(ID2K_PREFIX);

        let mut meta_key = base;
        meta_key.extend_from_slice(META_NEXT_ID);

        let next_value = table
            .get_bytes(&meta_key)
            .await?
            .map(|bytes| decode_id(bytes.as_ref()))
            .transpose()?;

        let fast_path_fresh = next_value.is_none();
        let next_id = next_value.unwrap_or(1);

        Ok(Self {
            table,
            k2id_prefix,
            id2k_prefix,
            meta_key,
            next_id: AtomicU64::new(next_id),
            cache: Mutex::new(Cache::new()),
            fresh_next_slot_by_hash: Mutex::new(AHashMap::new()),
            fast_path_fresh,
            hash_fn: hash_fn.unwrap_or_else(|| Arc::new(xxh3_64)),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn batch(&self) -> DictionaryBatch<'_, K> {
        DictionaryBatch {
            dict: self,
            overlay: BatchOverlay::new(),
            _marker: std::marker::PhantomData,
        }
    }

    #[cfg(test)]
    pub(super) fn table(&self) -> Arc<dyn KeyValueTable> {
        Arc::clone(&self.table)
    }

    #[cfg(test)]
    pub(super) fn meta_key(&self) -> Vec<u8> {
        self.meta_key.clone()
    }

    #[cfg(test)]
    pub(super) fn next_id_value(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    pub(super) fn hash(&self, key: &[u8]) -> u64 {
        (self.hash_fn)(key)
    }

    pub(super) fn cache_guard(&self) -> MutexGuard<'_, Cache> {
        match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!(
                    "dictionary cache lock was poisoned; continuing with recovered cache"
                );
                poisoned.into_inner()
            }
        }
    }

    fn encode_k2id_key_into(&self, out: &mut Vec<u8>, hash: u64, slot: u16) {
        out.clear();
        out.reserve(self.k2id_prefix.len() + 10);
        out.extend_from_slice(&self.k2id_prefix);
        out.extend_from_slice(&hash.to_be_bytes());
        out.extend_from_slice(&slot.to_be_bytes());
    }

    pub(super) fn k2id_key(&self, hash: u64, slot: u16) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.k2id_prefix.len() + 10);
        self.encode_k2id_key_into(&mut key, hash, slot);
        key
    }

    fn encode_id2k_key_into(&self, out: &mut Vec<u8>, id: u64) {
        out.clear();
        out.reserve(self.id2k_prefix.len() + 8);
        out.extend_from_slice(&self.id2k_prefix);
        out.extend_from_slice(&id.to_be_bytes());
    }

    pub(super) fn id2k_key(&self, id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.id2k_prefix.len() + 8);
        self.encode_id2k_key_into(&mut key, id);
        key
    }

    fn decode_id2k_key_id(&self, key: &[u8]) -> Result<u64> {
        let suffix = key
            .strip_prefix(self.id2k_prefix.as_slice())
            .ok_or_else(|| anyhow!("dictionary reverse key missing id2k prefix"))?;
        if suffix.len() != 8 {
            return Err(anyhow!(
                "dictionary reverse key has unexpected suffix length {}",
                suffix.len()
            ));
        }
        Ok(u64::from_be_bytes(suffix.try_into().unwrap()))
    }

    fn id2k_range_end_exclusive(&self, end_id_inclusive: u64) -> Vec<u8> {
        if let Some(next_id) = end_id_inclusive.checked_add(1) {
            self.id2k_key(next_id)
        } else {
            let mut upper = self.id2k_prefix.clone();
            upper.push(0xFF);
            upper
        }
    }

    fn next_probe_slot(slot: u16) -> Option<u16> {
        (slot != u16::MAX).then_some(slot + 1)
    }

    fn reserve_fresh_slot(&self, hash: u64) -> Result<u16> {
        let mut next_slot_by_hash = match self.fresh_next_slot_by_hash.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!(
                    "dictionary fresh-slot lock was poisoned; continuing with recovered slots"
                );
                poisoned.into_inner()
            }
        };
        let next_slot = next_slot_by_hash.entry(hash).or_insert(0);
        let slot = *next_slot;
        *next_slot = Self::next_probe_slot(slot)
            .ok_or_else(|| anyhow!("dictionary full: all probe slots occupied for hash"))?;
        Ok(slot)
    }

    async fn first_free_slot(&self, hash: u64) -> Result<u16> {
        let mut slot = 0u16;
        let mut key_buf = Vec::with_capacity(self.k2id_prefix.len() + 10);
        loop {
            self.encode_k2id_key_into(&mut key_buf, hash, slot);
            if self.table.get_bytes(&key_buf).await?.is_none() {
                return Ok(slot);
            }

            slot = Self::next_probe_slot(slot)
                .ok_or_else(|| anyhow!("dictionary full: all probe slots occupied for hash"))?;
        }
    }

    async fn lookup_existing_id(&self, encoded_key: &[u8]) -> Result<Option<u64>> {
        match self.lookup_existing_or_first_free_slot(encoded_key).await? {
            LookupExistingResult::Existing(id) => Ok(Some(id)),
            LookupExistingResult::Missing { .. } => Ok(None),
        }
    }

    async fn lookup_existing_or_first_free_slot(
        &self,
        encoded_key: &[u8],
    ) -> Result<LookupExistingResult> {
        let hash = self.hash(encoded_key);
        let mut slot = 0u16;
        let mut k2id_key_buf = Vec::with_capacity(self.k2id_prefix.len() + 10);
        let mut id2k_key_buf = Vec::with_capacity(self.id2k_prefix.len() + 8);
        loop {
            self.encode_k2id_key_into(&mut k2id_key_buf, hash, slot);
            let Some(id_bytes) = self.table.get_bytes(&k2id_key_buf).await? else {
                return Ok(LookupExistingResult::Missing {
                    first_free_slot: slot,
                });
            };
            let id = decode_id(id_bytes.as_ref())?;

            self.encode_id2k_key_into(&mut id2k_key_buf, id);
            if let Some(stored) = self.table.get_bytes(&id2k_key_buf).await? {
                let decoded = decompress_value(stored.as_ref())?;
                let matches = decoded.as_slice() == encoded_key;

                let mut cache = self.cache_guard();
                cache.remember(decoded, id);
                drop(cache);

                if matches {
                    return Ok(LookupExistingResult::Existing(id));
                }
            } else {
                // If the reverse mapping is missing, clear the stale forward pointer.
                self.table.delete(&k2id_key_buf).await?;
                return Ok(LookupExistingResult::Missing {
                    first_free_slot: slot,
                });
            }

            slot = match Self::next_probe_slot(slot) {
                Some(next) => next,
                None => break,
            };
        }

        Err(anyhow!(
            "dictionary full: all probe slots occupied for hash"
        ))
    }

    async fn persist_mapping(
        &self,
        encoded_key: Vec<u8>,
        id: u64,
        hash: u64,
        slot: u16,
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(self.k2id_key(hash, slot), encode_id(id));
        batch.put(self.id2k_key(id), compress_value(encoded_key.as_slice())?);
        batch.put(
            self.meta_key.clone(),
            encode_id(self.next_id.load(Ordering::SeqCst)),
        );
        self.table.write_batch(batch).await?;

        let mut cache = self.cache_guard();
        cache.remember(encoded_key, id);
        Ok(())
    }

    fn reserve_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(super) async fn intern_key(
        &self,
        encoded_key: Vec<u8>,
        mut overlay: Option<&mut BatchOverlay>,
    ) -> Result<u64> {
        if let Some(overlay_ref) = overlay.as_deref_mut() {
            if let Some(id) = overlay_ref.lookup(&encoded_key) {
                return Ok(id);
            }
            if overlay_ref.is_negative(&encoded_key) {
                return self.allocate_new(encoded_key, overlay, None).await;
            }
        }

        if let Some(existing) = self.lookup_existing_in_cache(&encoded_key) {
            if let Some(overlay_ref) = overlay.as_deref_mut() {
                overlay_ref.remember_positive(encoded_key.clone(), existing);
            }
            return Ok(existing);
        }

        let should_allocate = {
            let mut cache = self.cache_guard();
            cache.is_negative(&encoded_key)
        };
        if should_allocate {
            return self.allocate_new(encoded_key, overlay, None).await;
        }

        match self
            .lookup_existing_or_first_free_slot(&encoded_key)
            .await?
        {
            LookupExistingResult::Existing(id) => {
                if let Some(overlay_ref) = overlay.as_deref_mut() {
                    overlay_ref.remember_positive(encoded_key.clone(), id);
                }
                let mut cache = self.cache_guard();
                cache.remember(encoded_key, id);
                Ok(id)
            }
            LookupExistingResult::Missing { first_free_slot } => {
                {
                    let mut cache = self.cache_guard();
                    cache.remember_negative(&encoded_key);
                }
                if let Some(overlay_ref) = overlay.as_deref_mut() {
                    overlay_ref.remember_negative(encoded_key.clone());
                }

                return self
                    .allocate_new(encoded_key, overlay, Some(first_free_slot))
                    .await;
            }
        }
    }

    fn lookup_existing_in_cache(&self, encoded_key: &[u8]) -> Option<u64> {
        let mut cache = self.cache_guard();
        cache.lookup_id(encoded_key)
    }

    async fn allocate_new(
        &self,
        encoded_key: Vec<u8>,
        overlay: Option<&mut BatchOverlay>,
        known_first_free_slot: Option<u16>,
    ) -> Result<u64> {
        let hash = self.hash(&encoded_key);
        if self.fast_path_fresh {
            let slot = self.reserve_fresh_slot(hash)?;
            let id = self.reserve_id();
            self.persist_mapping(encoded_key.clone(), id, hash, slot)
                .await?;
            {
                let mut cache = self.cache_guard();
                cache.clear_negative(&encoded_key);
            }
            if let Some(overlay) = overlay {
                overlay.clear_negative(&encoded_key);
                overlay.remember_positive(encoded_key.clone(), id);
            }
            return Ok(id);
        }

        let first_free_slot = match known_first_free_slot {
            Some(slot) => slot,
            None => self.first_free_slot(hash).await?,
        };
        let id = self.reserve_id();
        self.persist_mapping(encoded_key.clone(), id, hash, first_free_slot)
            .await?;
        {
            let mut cache = self.cache_guard();
            cache.clear_negative(&encoded_key);
        }
        if let Some(overlay) = overlay {
            overlay.clear_negative(&encoded_key);
            overlay.remember_positive(encoded_key.clone(), id);
        }
        Ok(id)
    }
}

mod intern_many;
mod resolve_many;
mod trait_impl;
