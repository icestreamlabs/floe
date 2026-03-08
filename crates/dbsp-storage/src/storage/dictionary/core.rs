use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use futures::future::try_join_all;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::{Db, WriteBatch};
use xxhash_rust::xxh3::xxh3_64;

use super::super::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use super::super::keyspace::{self, namespace_prefix};
use super::super::{KeyValueTable, SlateTable};
use super::batch::DictionaryBatch;
use super::cache::{BatchOverlay, Cache};
use super::codec::{compress_value, decode_id, decompress_value, encode_id};
use super::{HashFn, KeyIntern};

const K2ID_PREFIX: &[u8] = b"k2id/";
const ID2K_PREFIX: &[u8] = b"id2k/";
const META_NEXT_ID: &[u8] = b"meta/next_id";
const RESOLVE_MANY_FETCH_CHUNK: usize = 256;

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
    seen_hashes: Mutex<std::collections::HashSet<u64>>,
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
            .get(&meta_key)
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
            seen_hashes: Mutex::new(std::collections::HashSet::new()),
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

    pub(super) fn k2id_key(&self, hash: u64, slot: u16) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.k2id_prefix.len() + 10);
        key.extend_from_slice(&self.k2id_prefix);
        key.extend_from_slice(&hash.to_be_bytes());
        key.extend_from_slice(&slot.to_be_bytes());
        key
    }

    pub(super) fn id2k_key(&self, id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.id2k_prefix.len() + 8);
        key.extend_from_slice(&self.id2k_prefix);
        key.extend_from_slice(&id.to_be_bytes());
        key
    }

    async fn lookup_existing_id(&self, encoded_key: &[u8]) -> Result<Option<u64>> {
        let hash = self.hash(encoded_key);

        for slot in 0..=u16::MAX {
            let k2id_key = self.k2id_key(hash, slot);
            match self.table.get(&k2id_key).await? {
                Some(bytes) => {
                    let id = decode_id(&bytes)?;

                    if let Some(stored) = self.table.get(&self.id2k_key(id)).await? {
                        let decoded = decompress_value(&stored)?;
                        let matches = decoded.as_slice() == encoded_key;

                        let mut cache = self.cache.lock().unwrap();
                        cache.remember(decoded, id);
                        drop(cache);

                        if matches {
                            let mut cache = self.cache.lock().unwrap();
                            cache.remember(encoded_key.to_vec(), id);
                            return Ok(Some(id));
                        }
                    } else {
                        self.table.delete(&k2id_key).await?;
                    }
                }
                None => return Ok(None),
            }
        }

        Err(anyhow!(
            "exhausted dictionary slots while probing for existing key"
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
                return self.allocate_new(encoded_key, overlay).await;
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
            return self.allocate_new(encoded_key, overlay).await;
        }

        if let Some(id) = self.lookup_existing_id(&encoded_key).await? {
            if let Some(overlay_ref) = overlay.as_deref_mut() {
                overlay_ref.remember_positive(encoded_key.clone(), id);
            }
            let mut cache = self.cache.lock().unwrap();
            cache.remember(encoded_key, id);
            return Ok(id);
        }

        {
            let mut cache = self.cache.lock().unwrap();
            cache.remember_negative(&encoded_key);
        }
        if let Some(overlay_ref) = overlay.as_deref_mut() {
            overlay_ref.remember_negative(encoded_key.clone());
        }

        self.allocate_new(encoded_key, overlay).await
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
        let mut encoded_keys = Vec::new();
        for key in keys {
            encoded_keys
                .push(encoding::encode(key).context("unable to encode dictionary key in batch")?);
        }
        self.intern_many_encoded(encoded_keys).await
    }

    async fn intern_many_encoded(&self, encoded_keys: Vec<Vec<u8>>) -> Result<Vec<u64>> {
        if encoded_keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut resolved = std::collections::HashMap::<Vec<u8>, u64>::new();
        let mut pending = Vec::<(Vec<u8>, u64, u64, u16)>::new();
        let mut pending_slots = std::collections::HashSet::<(u64, u16)>::new();
        let mut output = Vec::with_capacity(encoded_keys.len());

        for encoded in &encoded_keys {
            if let Some(id) = resolved.get(encoded).copied() {
                output.push(id);
                continue;
            }

            let id = if let Some(existing) = self.lookup_existing_in_cache(encoded) {
                existing
            } else {
                let should_allocate = {
                    let mut cache = self.cache.lock().unwrap();
                    cache.is_negative(encoded)
                };
                if should_allocate || self.fast_path_fresh {
                    self.reserve_in_batch(encoded, &mut pending, &mut pending_slots)
                        .await?
                } else if let Some(existing) = self.lookup_existing_id(encoded).await? {
                    let mut cache = self.cache.lock().unwrap();
                    cache.remember(encoded.clone(), existing);
                    existing
                } else {
                    {
                        let mut cache = self.cache.lock().unwrap();
                        cache.remember_negative(encoded);
                    }
                    self.reserve_in_batch(encoded, &mut pending, &mut pending_slots)
                        .await?
                }
            };

            resolved.insert(encoded.clone(), id);
            output.push(id);
        }

        if !pending.is_empty() {
            let mut batch = WriteBatch::new();
            for (encoded, id, hash, slot) in &pending {
                batch.put(self.k2id_key(*hash, *slot), encode_id(*id));
                batch.put(self.id2k_key(*id), compress_value(encoded.as_slice())?);
            }
            batch.put(
                self.meta_key.clone(),
                encode_id(self.next_id.load(Ordering::SeqCst)),
            );
            self.table.write_batch(batch).await?;

            let mut cache = self.cache.lock().unwrap();
            for (encoded, id, _, _) in pending {
                cache.clear_negative(&encoded);
                cache.remember(encoded, id);
            }
        }

        Ok(output)
    }

    async fn reserve_in_batch(
        &self,
        encoded_key: &[u8],
        pending: &mut Vec<(Vec<u8>, u64, u64, u16)>,
        pending_slots: &mut std::collections::HashSet<(u64, u16)>,
    ) -> Result<u64> {
        let hash = self.hash(encoded_key);
        if self.fast_path_fresh && !pending_slots.contains(&(hash, 0)) {
            let can_use_slot_zero = {
                let mut seen = self.seen_hashes.lock().unwrap();
                seen.insert(hash)
            };
            if can_use_slot_zero {
                let id = self.reserve_id();
                pending_slots.insert((hash, 0));
                pending.push((encoded_key.to_vec(), id, hash, 0));
                return Ok(id);
            }
        }
        for slot in 0..=u16::MAX {
            if pending_slots.contains(&(hash, slot)) {
                continue;
            }
            let k2id_key = self.k2id_key(hash, slot);
            if self.table.get(&k2id_key).await?.is_none() {
                let id = self.reserve_id();
                pending_slots.insert((hash, slot));
                pending.push((encoded_key.to_vec(), id, hash, slot));
                return Ok(id);
            }
        }

        Err(anyhow!(
            "dictionary full: all probe slots occupied for hash"
        ))
    }

    pub async fn resolve_many_ids(&self, ids: &[u64]) -> Result<Vec<K>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut encoded_by_id: std::collections::HashMap<u64, Vec<u8>> =
            std::collections::HashMap::new();
        let mut missing_ids = Vec::new();
        let mut seen_missing = std::collections::HashSet::new();

        {
            let mut cache = self.cache.lock().unwrap();
            for id in ids {
                if *id == 0 {
                    return Err(anyhow!("id 0 is not valid"));
                }
                if let Some(key) = cache.lookup_key(id) {
                    encoded_by_id.entry(*id).or_insert(key);
                } else if seen_missing.insert(*id) {
                    missing_ids.push(*id);
                }
            }
        }

        if !missing_ids.is_empty() {
            let mut freshly_resolved = Vec::with_capacity(missing_ids.len());
            for chunk in missing_ids.chunks(RESOLVE_MANY_FETCH_CHUNK) {
                let fetched = try_join_all(chunk.iter().copied().map(|id| async move {
                    let key = self.id2k_key(id);
                    let bytes = self.table.get(&key).await?;
                    Ok::<_, anyhow::Error>((id, bytes))
                }))
                .await?;

                for (id, bytes) in fetched {
                    let bytes = bytes.ok_or_else(|| anyhow!("no key found for id {id}"))?;
                    let decoded = decompress_value(&bytes)?;
                    freshly_resolved.push((id, decoded.clone()));
                    encoded_by_id.insert(id, decoded);
                }
            }

            let mut cache = self.cache.lock().unwrap();
            for (id, key) in freshly_resolved {
                cache.remember(key, id);
            }
        }

        let mut decoded_by_id = std::collections::HashMap::new();
        for id in ids {
            if decoded_by_id.contains_key(id) {
                continue;
            }
            let encoded = encoded_by_id
                .get(id)
                .ok_or_else(|| anyhow!("no key found for id {id}"))?;
            let decoded =
                encoding::decode(encoded).context("unable to decode dictionary value in batch")?;
            decoded_by_id.insert(*id, decoded);
        }

        let mut resolved = Vec::with_capacity(ids.len());
        for id in ids {
            let value = decoded_by_id
                .get(id)
                .cloned()
                .ok_or_else(|| anyhow!("no key found for id {id}"))?;
            resolved.push(value);
        }
        Ok(resolved)
    }

    async fn allocate_new(
        &self,
        encoded_key: Vec<u8>,
        overlay: Option<&mut BatchOverlay>,
    ) -> Result<u64> {
        let hash = self.hash(&encoded_key);
        if self.fast_path_fresh {
            let can_use_slot_zero = {
                let mut seen = self.seen_hashes.lock().unwrap();
                seen.insert(hash)
            };
            if can_use_slot_zero {
                let id = self.reserve_id();
                self.persist_mapping(encoded_key.clone(), id, hash, 0)
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
        }

        for slot in 0..=u16::MAX {
            let k2id_key = self.k2id_key(hash, slot);
            if self.table.get(&k2id_key).await?.is_none() {
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
        }

        Err(anyhow!(
            "dictionary full: all probe slots occupied for hash"
        ))
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
                let key = self.id2k_key(id);
                let bytes = self
                    .table
                    .get(&key)
                    .await?
                    .ok_or_else(|| anyhow!("no key found for id {id}"))?;
                let decoded = decompress_value(&bytes)?;
                let mut cache = self.cache.lock().unwrap();
                cache.remember(decoded.clone(), id);
                decoded
            }
        };

        encoding::decode(&encoded).context("unable to decode dictionary value")
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
