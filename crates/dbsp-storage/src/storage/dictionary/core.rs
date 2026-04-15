use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
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
const RESOLVE_MANY_RANGE_SCAN_MIN_IDS: usize = 512;

enum LookupExistingResult {
    Existing(u64),
    Missing { first_free_slot: u16 },
}

#[allow(dead_code)]
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
    #[allow(dead_code)]
    pub async fn new(db: Arc<Db>, namespace: impl Into<String>) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::with_table(table, namespace, None).await
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
        let mut next_slot_by_hash = self.fresh_next_slot_by_hash.lock().unwrap();
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

                let mut cache = self.cache.lock().unwrap();
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

        let mut cache = self.cache.lock().unwrap();
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
            let mut cache = self.cache.lock().unwrap();
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
                let mut cache = self.cache.lock().unwrap();
                cache.remember(encoded_key, id);
                return Ok(id);
            }
            LookupExistingResult::Missing { first_free_slot } => {
                {
                    let mut cache = self.cache.lock().unwrap();
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
        let mut cache = self.cache.lock().unwrap();
        cache.lookup_id(encoded_key)
    }

    pub async fn intern_many_values<'a, I>(&self, keys: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let total_start = Instant::now();
        let mut encoded_keys = Vec::new();
        for key in keys {
            encoded_keys
                .push(encoding::encode(key).context("unable to encode dictionary key in batch")?);
        }
        let encode_ms = total_start.elapsed().as_millis() as u64;
        let output = self.intern_many_encoded(encoded_keys).await?;
        let total_ms = total_start.elapsed().as_millis() as u64;
        if total_ms >= 10 {
            tracing::info!(
                keys = output.len(),
                encode_ms,
                intern_ms = total_ms.saturating_sub(encode_ms),
                total_ms,
                "dictionary intern_many_values latency"
            );
        }
        Ok(output)
    }

    /// Intern a batch of keys that are already unique within the batch.
    ///
    /// This skips duplicate-detection bookkeeping in `intern_many_values` and is
    /// intended for callers that already consolidated their delta by key.
    pub async fn intern_many_values_unique<'a, I>(&self, keys: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let total_start = Instant::now();
        let mut encoded_keys = Vec::new();
        for key in keys {
            encoded_keys
                .push(encoding::encode(key).context("unable to encode dictionary key in batch")?);
        }
        let encode_ms = total_start.elapsed().as_millis() as u64;
        let output = self.intern_many_encoded_unique(encoded_keys).await?;
        let total_ms = total_start.elapsed().as_millis() as u64;
        if total_ms >= 10 {
            tracing::info!(
                keys = output.len(),
                encode_ms,
                intern_ms = total_ms.saturating_sub(encode_ms),
                total_ms,
                "dictionary intern_many_values_unique latency"
            );
        }
        Ok(output)
    }

    /// Intern a batch of owned keys that are already unique within the batch.
    ///
    /// This avoids cloning key payloads while staging batch inserts.
    pub async fn intern_many_values_unique_owned(&self, keys: Vec<K>) -> Result<Vec<u64>> {
        let total_start = Instant::now();
        let mut encoded_keys = Vec::with_capacity(keys.len());
        for key in keys {
            encoded_keys
                .push(encoding::encode(&key).context("unable to encode dictionary key in batch")?);
        }
        let encode_ms = total_start.elapsed().as_millis() as u64;
        let output = self.intern_many_encoded_unique(encoded_keys).await?;
        let total_ms = total_start.elapsed().as_millis() as u64;
        if total_ms >= 10 {
            tracing::info!(
                keys = output.len(),
                encode_ms,
                intern_ms = total_ms.saturating_sub(encode_ms),
                total_ms,
                "dictionary intern_many_values_unique_owned latency"
            );
        }
        Ok(output)
    }

    async fn intern_many_encoded(&self, encoded_keys: Vec<Vec<u8>>) -> Result<Vec<u64>> {
        if encoded_keys.is_empty() {
            return Ok(Vec::new());
        }

        let total_start = Instant::now();
        let mut resolved = AHashMap::<u64, Vec<(usize, u64)>>::with_capacity(encoded_keys.len());
        let mut pending = Vec::<(Vec<u8>, u64, u64, u16)>::with_capacity(encoded_keys.len());
        let mut next_slot_by_hash = AHashMap::<u64, u16>::new();
        let mut output = Vec::with_capacity(encoded_keys.len());
        let mut duplicate_reuse_hits = 0usize;
        let mut cache_hits = 0usize;
        let mut negative_cache_hits = 0usize;
        let mut lookup_existing_calls = 0usize;
        let mut lookup_existing_hits = 0usize;
        let mut lookup_existing_ms = 0u64;
        let mut reserve_calls = 0usize;
        let mut reserve_ms = 0u64;

        for (index, encoded) in encoded_keys.iter().enumerate() {
            let resolved_hash = self.hash(encoded);
            if let Some(entries) = resolved.get(&resolved_hash)
                && let Some((_, id)) = entries
                    .iter()
                    .find(|(resolved_index, _)| encoded_keys[*resolved_index].as_slice() == encoded)
            {
                duplicate_reuse_hits += 1;
                output.push(*id);
                continue;
            }

            let id = if let Some(existing) = self.lookup_existing_in_cache(encoded) {
                cache_hits += 1;
                existing
            } else {
                let should_allocate = {
                    let mut cache = self.cache.lock().unwrap();
                    cache.is_negative(encoded)
                };
                if should_allocate || self.fast_path_fresh {
                    if should_allocate {
                        negative_cache_hits += 1;
                    }
                    reserve_calls += 1;
                    let reserve_start = Instant::now();
                    let id = self
                        .reserve_in_batch(
                            resolved_hash,
                            encoded,
                            &mut pending,
                            &mut next_slot_by_hash,
                            None,
                        )
                        .await?;
                    reserve_ms += reserve_start.elapsed().as_millis() as u64;
                    id
                } else {
                    lookup_existing_calls += 1;
                    let lookup_start = Instant::now();
                    let existing = self.lookup_existing_or_first_free_slot(encoded).await?;
                    lookup_existing_ms += lookup_start.elapsed().as_millis() as u64;
                    match existing {
                        LookupExistingResult::Existing(existing) => {
                            lookup_existing_hits += 1;
                            let mut cache = self.cache.lock().unwrap();
                            cache.remember(encoded.clone(), existing);
                            existing
                        }
                        LookupExistingResult::Missing { first_free_slot } => {
                            {
                                let mut cache = self.cache.lock().unwrap();
                                cache.remember_negative(encoded);
                            }
                            reserve_calls += 1;
                            let reserve_start = Instant::now();
                            let id = self
                                .reserve_in_batch(
                                    resolved_hash,
                                    encoded,
                                    &mut pending,
                                    &mut next_slot_by_hash,
                                    Some(first_free_slot),
                                )
                                .await?;
                            reserve_ms += reserve_start.elapsed().as_millis() as u64;
                            id
                        }
                    }
                }
            };

            resolved.entry(resolved_hash).or_default().push((index, id));
            output.push(id);
        }

        let pending_writes = pending.len();
        let flush_start = Instant::now();
        self.flush_pending_batch(pending).await?;
        let flush_ms = flush_start.elapsed().as_millis() as u64;

        tracing::debug!(
            batch_keys = encoded_keys.len(),
            output_ids = output.len(),
            pending_writes,
            duplicate_reuse_hits,
            cache_hits,
            negative_cache_hits,
            lookup_existing_calls,
            lookup_existing_hits,
            lookup_existing_ms,
            reserve_calls,
            reserve_ms,
            flush_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "dictionary intern_many breakdown"
        );

        Ok(output)
    }

    async fn intern_many_encoded_unique(&self, encoded_keys: Vec<Vec<u8>>) -> Result<Vec<u64>> {
        self.intern_many_encoded_unique_owned(encoded_keys).await
    }

    async fn intern_many_encoded_unique_owned(
        &self,
        encoded_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<u64>> {
        if encoded_keys.is_empty() {
            return Ok(Vec::new());
        }

        let total_start = Instant::now();
        let mut pending = Vec::<(Vec<u8>, u64, u64, u16)>::with_capacity(encoded_keys.len());
        let mut next_slot_by_hash = AHashMap::<u64, u16>::new();
        let mut output = Vec::with_capacity(encoded_keys.len());
        let mut cache_hits = 0usize;
        let mut negative_cache_hits = 0usize;
        let mut lookup_existing_calls = 0usize;
        let mut lookup_existing_hits = 0usize;
        let mut lookup_existing_ms = 0u64;
        let mut reserve_calls = 0usize;
        let mut reserve_ms = 0u64;

        for encoded in encoded_keys {
            let id = if let Some(existing) = self.lookup_existing_in_cache(encoded.as_slice()) {
                cache_hits += 1;
                existing
            } else {
                let hash = self.hash(encoded.as_slice());
                let should_allocate = {
                    let mut cache = self.cache.lock().unwrap();
                    cache.is_negative(encoded.as_slice())
                };
                if should_allocate || self.fast_path_fresh {
                    if should_allocate {
                        negative_cache_hits += 1;
                    }
                    reserve_calls += 1;
                    let reserve_start = Instant::now();
                    let id = self
                        .reserve_in_batch_owned(
                            hash,
                            encoded,
                            &mut pending,
                            &mut next_slot_by_hash,
                            None,
                        )
                        .await?;
                    reserve_ms += reserve_start.elapsed().as_millis() as u64;
                    id
                } else {
                    lookup_existing_calls += 1;
                    let lookup_start = Instant::now();
                    let existing = self
                        .lookup_existing_or_first_free_slot(encoded.as_slice())
                        .await?;
                    lookup_existing_ms += lookup_start.elapsed().as_millis() as u64;
                    match existing {
                        LookupExistingResult::Existing(existing) => {
                            lookup_existing_hits += 1;
                            let mut cache = self.cache.lock().unwrap();
                            cache.remember(encoded, existing);
                            existing
                        }
                        LookupExistingResult::Missing { first_free_slot } => {
                            {
                                let mut cache = self.cache.lock().unwrap();
                                cache.remember_negative(encoded.as_slice());
                            }
                            reserve_calls += 1;
                            let reserve_start = Instant::now();
                            let id = self
                                .reserve_in_batch_owned(
                                    hash,
                                    encoded,
                                    &mut pending,
                                    &mut next_slot_by_hash,
                                    Some(first_free_slot),
                                )
                                .await?;
                            reserve_ms += reserve_start.elapsed().as_millis() as u64;
                            id
                        }
                    }
                }
            };
            output.push(id);
        }

        let pending_writes = pending.len();
        let flush_start = Instant::now();
        self.flush_pending_batch(pending).await?;
        let flush_ms = flush_start.elapsed().as_millis() as u64;

        tracing::debug!(
            batch_keys = output.len(),
            pending_writes,
            cache_hits,
            negative_cache_hits,
            lookup_existing_calls,
            lookup_existing_hits,
            lookup_existing_ms,
            reserve_calls,
            reserve_ms,
            flush_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "dictionary intern_many_unique breakdown"
        );

        Ok(output)
    }

    async fn flush_pending_batch(&self, pending: Vec<(Vec<u8>, u64, u64, u16)>) -> Result<()> {
        if !pending.is_empty() {
            let total_start = Instant::now();
            let mut batch = WriteBatch::new();
            let build_batch_start = Instant::now();
            let mut raw_values = 0usize;
            let mut compressed_values = 0usize;
            let mut input_bytes = 0usize;
            let mut stored_bytes = 0usize;
            for (encoded, id, hash, slot) in &pending {
                batch.put(self.k2id_key(*hash, *slot), encode_id(*id));
                let compressed = compress_value(encoded.as_slice())?;
                input_bytes += encoded.len();
                stored_bytes += compressed.len();
                if compressed.first().copied() == Some(0x00) {
                    raw_values += 1;
                } else {
                    compressed_values += 1;
                }
                batch.put(self.id2k_key(*id), compressed);
            }
            batch.put(
                self.meta_key.clone(),
                encode_id(self.next_id.load(Ordering::SeqCst)),
            );
            let build_batch_ms = build_batch_start.elapsed().as_millis() as u64;

            let write_start = Instant::now();
            self.table.write_batch(batch).await?;
            let write_batch_ms = write_start.elapsed().as_millis() as u64;

            let cache_update_start = Instant::now();
            let mut cache = self.cache.lock().unwrap();
            for (encoded, id, _, _) in pending {
                cache.clear_negative(&encoded);
                cache.remember(encoded, id);
            }
            let cache_update_ms = cache_update_start.elapsed().as_millis() as u64;

            tracing::debug!(
                pending_writes = raw_values + compressed_values,
                raw_values,
                compressed_values,
                input_bytes,
                stored_bytes,
                build_batch_ms,
                write_batch_ms,
                cache_update_ms,
                total_ms = total_start.elapsed().as_millis() as u64,
                "dictionary flush_pending_batch breakdown"
            );
        }

        Ok(())
    }

    async fn reserve_in_batch(
        &self,
        hash: u64,
        encoded_key: &[u8],
        pending: &mut Vec<(Vec<u8>, u64, u64, u16)>,
        next_slot_by_hash: &mut AHashMap<u64, u16>,
        known_first_free_slot: Option<u16>,
    ) -> Result<u64> {
        if self.fast_path_fresh {
            let slot = self.reserve_fresh_slot(hash)?;
            let id = self.reserve_id();
            pending.push((encoded_key.to_vec(), id, hash, slot));
            return Ok(id);
        }

        let next_slot = match next_slot_by_hash.entry(hash) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let loaded = match known_first_free_slot {
                    Some(slot) => slot,
                    None => self.first_free_slot(hash).await?,
                };
                entry.insert(loaded)
            }
        };

        let slot = *next_slot;
        let id = self.reserve_id();
        *next_slot = Self::next_probe_slot(slot)
            .ok_or_else(|| anyhow!("dictionary full: all probe slots occupied for hash"))?;
        pending.push((encoded_key.to_vec(), id, hash, slot));
        Ok(id)
    }

    async fn reserve_in_batch_owned(
        &self,
        hash: u64,
        encoded_key: Vec<u8>,
        pending: &mut Vec<(Vec<u8>, u64, u64, u16)>,
        next_slot_by_hash: &mut AHashMap<u64, u16>,
        known_first_free_slot: Option<u16>,
    ) -> Result<u64> {
        if self.fast_path_fresh {
            let slot = self.reserve_fresh_slot(hash)?;
            let id = self.reserve_id();
            pending.push((encoded_key, id, hash, slot));
            return Ok(id);
        }

        let next_slot = match next_slot_by_hash.entry(hash) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let loaded = match known_first_free_slot {
                    Some(slot) => slot,
                    None => self.first_free_slot(hash).await?,
                };
                entry.insert(loaded)
            }
        };

        let slot = *next_slot;
        let id = self.reserve_id();
        *next_slot = Self::next_probe_slot(slot)
            .ok_or_else(|| anyhow!("dictionary full: all probe slots occupied for hash"))?;
        pending.push((encoded_key, id, hash, slot));
        Ok(id)
    }

    pub async fn resolve_many_ids(&self, ids: &[u64]) -> Result<Vec<K>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let total_start = Instant::now();
        let cache_scan_start = Instant::now();
        let mut encoded_by_id: AHashMap<u64, SharedKey> = AHashMap::with_capacity(ids.len());
        let mut missing_ids = Vec::new();
        let mut seen_missing = AHashSet::with_capacity(ids.len());
        let mut cache_hit_refs = 0usize;

        {
            let mut cache = self.cache.lock().unwrap();
            for id in ids {
                if *id == 0 {
                    return Err(anyhow!("id 0 is not valid"));
                }
                if let Some(key) = cache.lookup_key(id) {
                    cache_hit_refs += 1;
                    encoded_by_id.entry(*id).or_insert(key);
                } else if seen_missing.insert(*id) {
                    missing_ids.push(*id);
                }
            }
        }
        let cache_scan_ms = cache_scan_start.elapsed().as_millis() as u64;

        let fetch_start = Instant::now();
        let mut range_scan_spans = 0usize;
        let mut range_scan_ids = 0usize;
        let mut point_fetch_chunks = 0usize;
        if !missing_ids.is_empty() {
            let mut sorted_missing_ids = missing_ids.clone();
            sorted_missing_ids.sort_unstable();
            let mut point_fetch_ids = Vec::with_capacity(sorted_missing_ids.len());

            let mut span_start = 0usize;
            while span_start < sorted_missing_ids.len() {
                let mut span_end = span_start + 1;
                while span_end < sorted_missing_ids.len()
                    && sorted_missing_ids[span_end] == sorted_missing_ids[span_end - 1] + 1
                {
                    span_end += 1;
                }

                let span_ids = &sorted_missing_ids[span_start..span_end];
                if span_ids.len() >= RESOLVE_MANY_RANGE_SCAN_MIN_IDS {
                    range_scan_spans += 1;
                    range_scan_ids += span_ids.len();
                    let start_key = self.id2k_key(span_ids[0]);
                    let end_key = self.id2k_range_end_exclusive(*span_ids.last().unwrap());
                    let scanned = self
                        .table
                        .scan_range_bytes(start_key..end_key, &ScanOptions::default())
                        .await?;
                    for (key, bytes) in scanned {
                        let id = self.decode_id2k_key_id(key.as_ref())?;
                        let decoded = decompress_value(bytes.as_ref())?;
                        let shared = {
                            let mut cache = self.cache.lock().unwrap();
                            cache.remember(decoded, id)
                        };
                        encoded_by_id.insert(id, shared);
                    }
                } else {
                    point_fetch_ids.extend_from_slice(span_ids);
                }

                span_start = span_end;
            }

            for chunk in point_fetch_ids.chunks(RESOLVE_MANY_FETCH_CHUNK) {
                point_fetch_chunks += 1;
                let mut id2k_keys = Vec::with_capacity(chunk.len());
                for &id in chunk {
                    let mut key = Vec::with_capacity(self.id2k_prefix.len() + 8);
                    self.encode_id2k_key_into(&mut key, id);
                    id2k_keys.push((id, key));
                }
                let fetched = try_join_all(id2k_keys.into_iter().map(|(id, key)| async move {
                    let bytes = self.table.get_bytes(&key).await?;
                    Ok::<_, anyhow::Error>((id, bytes))
                }))
                .await?;

                for (id, bytes) in fetched {
                    let bytes = bytes.ok_or_else(|| anyhow!("no key found for id {id}"))?;
                    let decoded = decompress_value(bytes.as_ref())?;
                    let shared = {
                        let mut cache = self.cache.lock().unwrap();
                        cache.remember(decoded, id)
                    };
                    encoded_by_id.insert(id, shared);
                }
            }
        }
        let fetch_ms = fetch_start.elapsed().as_millis() as u64;

        let decode_start = Instant::now();
        let mut decoded_by_id = AHashMap::with_capacity(encoded_by_id.len());
        for id in ids {
            if decoded_by_id.contains_key(id) {
                continue;
            }
            let encoded = encoded_by_id
                .get(id)
                .ok_or_else(|| anyhow!("no key found for id {id}"))?;
            let decoded = encoding::decode(encoded.as_ref())
                .context("unable to decode dictionary value in batch")?;
            decoded_by_id.insert(*id, decoded);
        }
        let decode_ms = decode_start.elapsed().as_millis() as u64;

        let output_start = Instant::now();
        let mut resolved = Vec::with_capacity(ids.len());
        for id in ids {
            let value = decoded_by_id
                .get(id)
                .cloned()
                .ok_or_else(|| anyhow!("no key found for id {id}"))?;
            resolved.push(value);
        }
        let output_ms = output_start.elapsed().as_millis() as u64;

        tracing::debug!(
            ids = ids.len(),
            unique_ids = decoded_by_id.len(),
            cache_hit_refs,
            cache_miss_unique = missing_ids.len(),
            cache_scan_ms,
            range_scan_spans,
            range_scan_ids,
            point_fetch_chunks,
            fetch_ms,
            decode_ms,
            output_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "dictionary resolve_many breakdown"
        );

        Ok(resolved)
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
                let mut cache = self.cache.lock().unwrap();
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
            let mut cache = self.cache.lock().unwrap();
            cache.clear_negative(&encoded_key);
        }
        if let Some(overlay) = overlay {
            overlay.clear_negative(&encoded_key);
            overlay.remember_positive(encoded_key.clone(), id);
        }
        Ok(id)
    }
}

#[async_trait]
impl<K> KeyIntern<K> for Dictionary<K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    async fn intern(&self, key: &K) -> Result<u64> {
        let encoded = encoding::encode(key).context("unable to encode dictionary key")?;
        self.intern_key(encoded, None).await
    }

    async fn resolve(&self, id: u64) -> Result<K> {
        if id == 0 {
            return Err(anyhow!("id 0 is not valid"));
        }

        let encoded = {
            if let Some(bytes) = {
                let mut cache = self.cache.lock().unwrap();
                cache.lookup_key(&id)
            } {
                bytes
            } else {
                let mut key = Vec::with_capacity(self.id2k_prefix.len() + 8);
                self.encode_id2k_key_into(&mut key, id);
                let bytes = self
                    .table
                    .get_bytes(&key)
                    .await?
                    .ok_or_else(|| anyhow!("no key found for id {id}"))?;
                let decoded = decompress_value(bytes.as_ref())?;
                let mut cache = self.cache.lock().unwrap();
                cache.remember(decoded, id)
            }
        };

        encoding::decode(encoded.as_ref()).context("unable to decode dictionary value")
    }

    async fn resolve_many(&self, ids: &[u64]) -> Result<Vec<K>> {
        self.resolve_many_ids(ids).await
    }

    async fn lookup(&self, key: &K) -> Result<Option<u64>> {
        let encoded = encoding::encode(key).context("unable to encode dictionary key")?;
        if let Some(id) = self.lookup_existing_in_cache(&encoded) {
            return Ok(Some(id));
        }
        self.lookup_existing_id(&encoded).await
    }
}
