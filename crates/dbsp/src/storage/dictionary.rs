use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use lru::LruCache;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::{Db, WriteBatch};
use snap::raw::{Decoder, Encoder};
use xxhash_rust::xxh3::xxh3_64;

use super::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use super::keyspace::{self, namespace_prefix};
use super::{KeyValueTable, SlateTable};

type HashFn = Arc<dyn Fn(&[u8]) -> u64 + Send + Sync + 'static>;

const K2ID_PREFIX: &[u8] = b"k2id/";
const ID2K_PREFIX: &[u8] = b"id2k/";
const META_NEXT_ID: &[u8] = b"meta/next_id";

#[allow(dead_code)]
#[async_trait]
pub trait KeyIntern<K>: Send + Sync
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    async fn intern(&self, key: &K) -> Result<u64>;
    async fn resolve(&self, id: u64) -> Result<K>;
    async fn lookup(&self, key: &K) -> Result<Option<u64>>;
}

const CACHE_CAPACITY: usize = 1024;

struct Cache {
    key_to_id: LruCache<Vec<u8>, u64>,
    id_to_key: LruCache<u64, Vec<u8>>,
    negatives: LruCache<Vec<u8>, ()>,
    max_key_len: usize,
}

impl Cache {
    #[allow(dead_code)]
    fn new() -> Self {
        let capacity = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero cache size");
        Self {
            key_to_id: LruCache::new(capacity),
            id_to_key: LruCache::new(capacity),
            negatives: LruCache::new(capacity),
            max_key_len: 0,
        }
    }

    fn remember(&mut self, key: Vec<u8>, id: u64) {
        self.key_to_id.put(key.clone(), id);
        self.id_to_key.put(id, key.clone());
        self.negatives.pop(&key);
        self.max_key_len = self.max_key_len.max(key.len());
    }

    fn lookup_id(&mut self, key: &[u8]) -> Option<u64> {
        self.key_to_id.get(key).copied()
    }

    fn lookup_key(&mut self, id: &u64) -> Option<Vec<u8>> {
        self.id_to_key.get(id).cloned()
    }

    fn remember_negative(&mut self, key: &[u8]) {
        self.negatives.put(key.to_vec(), ());
    }

    fn clear_negative(&mut self, key: &[u8]) {
        self.negatives.pop(key);
    }

    fn is_negative(&mut self, key: &[u8]) -> bool {
        self.negatives.contains(key)
    }
}

struct BatchOverlay {
    positives: HashMap<Vec<u8>, u64>,
    negatives: HashSet<Vec<u8>>,
}

impl BatchOverlay {
    fn new() -> Self {
        Self {
            positives: HashMap::new(),
            negatives: HashSet::new(),
        }
    }

    fn lookup(&self, key: &[u8]) -> Option<u64> {
        self.positives.get(key).copied()
    }

    fn remember_positive(&mut self, key: Vec<u8>, id: u64) {
        self.positives.insert(key, id);
    }

    fn remember_negative(&mut self, key: Vec<u8>) {
        self.negatives.insert(key);
    }

    fn clear_negative(&mut self, key: &[u8]) {
        self.negatives.remove(key);
    }

    fn is_negative(&self, key: &[u8]) -> bool {
        self.negatives.contains(key)
    }
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

        let next_id = next_value.unwrap_or(1);

        Ok(Self {
            table,
            k2id_prefix,
            id2k_prefix,
            meta_key,
            next_id: AtomicU64::new(next_id),
            cache: Mutex::new(Cache::new()),
            hash_fn: hash_fn.unwrap_or_else(|| Arc::new(|bytes| xxh3_64(bytes))),
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

    fn hash(&self, key: &[u8]) -> u64 {
        (self.hash_fn)(key)
    }

    fn k2id_key(&self, hash: u64, slot: u16) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.k2id_prefix.len() + 10);
        key.extend_from_slice(&self.k2id_prefix);
        key.extend_from_slice(&hash.to_be_bytes());
        key.extend_from_slice(&slot.to_be_bytes());
        key
    }

    fn id2k_key(&self, id: u64) -> Vec<u8> {
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

    async fn intern_key(
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

    async fn allocate_new(
        &self,
        encoded_key: Vec<u8>,
        overlay: Option<&mut BatchOverlay>,
    ) -> Result<u64> {
        let hash = self.hash(&encoded_key);

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

fn encode_id(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn decode_id(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(anyhow!("expected 8 bytes for dictionary id"));
    }
    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(array))
}

fn compress_value(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder
        .compress_vec(bytes)
        .map_err(|err| anyhow!("failed to compress dictionary value: {err}"))
}

fn decompress_value(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(bytes)
        .map_err(|err| anyhow!("failed to decompress dictionary value: {err}"))
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

    async fn lookup(&self, key: &K) -> Result<Option<u64>> {
        let encoded = encoding::encode(key).context("unable to encode dictionary key")?;
        if let Some(id) = self.lookup_existing_in_cache(&encoded) {
            return Ok(Some(id));
        }
        self.lookup_existing_id(&encoded).await
    }
}

#[allow(dead_code)]
pub struct DictionaryBatch<'a, K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    dict: &'a Dictionary<K>,
    overlay: BatchOverlay,
    _marker: std::marker::PhantomData<K>,
}

impl<'a, K> DictionaryBatch<'a, K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    #[allow(dead_code)]
    pub async fn intern(&mut self, key: &K) -> Result<u64> {
        let encoded = encoding::encode(key).context("unable to encode dictionary key")?;
        self.dict.intern_key(encoded, Some(&mut self.overlay)).await
    }

    #[allow(dead_code)]
    pub async fn resolve(&mut self, id: u64) -> Result<K> {
        self.dict.resolve(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use slatedb::WriteBatch;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    #[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
    struct TestKey {
        value: String,
    }

    async fn build_table() -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("dictionary-test", store)
                .await
                .expect("open SlateDB"),
        );
        Arc::new(SlateTable::new(db))
    }

    #[tokio::test]
    async fn interning_returns_stable_id() {
        let table = build_table().await;
        let dict = Dictionary::<TestKey>::with_table(table, "stable", None)
            .await
            .expect("build dictionary");

        let id1 = dict
            .intern(&TestKey {
                value: "hello".into(),
            })
            .await
            .expect("intern first");
        let id2 = dict
            .intern(&TestKey {
                value: "hello".into(),
            })
            .await
            .expect("intern second");

        assert_eq!(id1, id2);

        let resolved = dict.resolve(id1).await.expect("resolve id");
        assert_eq!(resolved.value, "hello");
    }

    #[tokio::test]
    async fn handles_hash_collisions() {
        let table = build_table().await;
        let forced_hash: HashFn = Arc::new(|_| 42);
        let dict = Dictionary::<TestKey>::with_table(table, "collide", Some(forced_hash))
            .await
            .expect("build dictionary");

        let keys = vec!["a", "b", "c"];
        let mut ids = Vec::new();
        for value in &keys {
            let id = dict
                .intern(&TestKey {
                    value: value.to_string(),
                })
                .await
                .expect("intern key");
            ids.push(id);
        }

        assert_eq!(ids.len(), 3);
        assert!(
            ids.iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                == 3
        );

        for (value, id) in keys.into_iter().zip(ids.into_iter()) {
            let resolved = dict.resolve(id).await.expect("resolve id");
            assert_eq!(resolved.value, value);
        }
    }

    #[tokio::test]
    async fn batch_overlay_reuses_lookup() {
        let table = build_table().await;
        let dict = Dictionary::<TestKey>::with_table(table, "batch", None)
            .await
            .expect("build dictionary");

        let mut batch = dict.batch();
        let key = TestKey {
            value: "overlay".into(),
        };

        let first = batch.intern(&key).await.expect("first intern");
        let second = batch.intern(&key).await.expect("second intern");
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn interning_long_key_preserves_short_slate_keys() {
        let table = build_table().await;
        let dict = Dictionary::<TestKey>::with_table(table, "length", None)
            .await
            .expect("build dictionary");

        let long_key = TestKey {
            value: "a".repeat(u16::MAX as usize + 1),
        };

        let id = dict
            .intern(&long_key)
            .await
            .expect("intern long key into dictionary");

        let encoded = encoding::encode(&long_key).expect("encode key");
        let hash = dict.hash(&encoded);
        let k2id_key = dict.k2id_key(hash, 0);
        assert!(k2id_key.len() <= u16::MAX as usize);

        let id2k_key = dict.id2k_key(id);
        assert!(id2k_key.len() <= u16::MAX as usize);

        let resolved = dict.resolve(id).await.expect("resolve long key");
        assert_eq!(resolved.value, long_key.value);
    }

    #[tokio::test]
    async fn recovers_when_k2id_missing() {
        let table = build_table().await;
        let dict = Dictionary::<TestKey>::with_table(table.clone(), "recover_k2id", None)
            .await
            .expect("build dictionary");

        let key = TestKey {
            value: "lost".into(),
        };
        let encoded = encoding::encode(&key).expect("encode key");
        let id = dict.next_id.load(Ordering::SeqCst);

        let mut batch = WriteBatch::new();
        batch.put(
            dict.id2k_key(id),
            compress_value(&encoded).expect("compress value"),
        );
        batch.put(dict.meta_key.clone(), encode_id(id));
        dict.table
            .write_batch(batch)
            .await
            .expect("write partial state");

        let recovered = Dictionary::<TestKey>::with_table(table.clone(), "recover_k2id", None)
            .await
            .expect("reopen dictionary");

        let assigned = recovered.intern(&key).await.expect("intern after recovery");
        assert_ne!(assigned, 0);

        let hash = recovered.hash(&encoding::encode(&key).expect("encode key"));
        let k2id_key = recovered.k2id_key(hash, 0);
        let stored_id = recovered
            .table
            .get(&k2id_key)
            .await
            .expect("fetch k2id")
            .map(|bytes| decode_id(&bytes).expect("decode id"))
            .expect("id present");
        assert_eq!(stored_id, assigned);
    }

    #[tokio::test]
    async fn recovers_when_id2k_missing() {
        let table = build_table().await;
        let dict = Dictionary::<TestKey>::with_table(table.clone(), "recover_id2k", None)
            .await
            .expect("build dictionary");

        let key = TestKey {
            value: "stale".into(),
        };
        let encoded = encoding::encode(&key).expect("encode key");
        let hash = dict.hash(&encoded);
        let k2id_key = dict.k2id_key(hash, 0);
        let id = dict.next_id.load(Ordering::SeqCst);

        let mut batch = WriteBatch::new();
        batch.put(k2id_key.clone(), encode_id(id));
        batch.put(dict.meta_key.clone(), encode_id(id + 1));
        dict.table
            .write_batch(batch)
            .await
            .expect("write partial state");

        let recovered = Dictionary::<TestKey>::with_table(table.clone(), "recover_id2k", None)
            .await
            .expect("reopen dictionary");

        let assigned = recovered.intern(&key).await.expect("intern after recovery");
        assert_ne!(assigned, 0);

        let stored_id = recovered
            .table
            .get(&k2id_key)
            .await
            .expect("fetch repaired k2id")
            .map(|bytes| decode_id(&bytes).expect("decode id"))
            .expect("id present");
        assert_eq!(stored_id, assigned);

        let resolved = recovered.resolve(assigned).await.expect("resolve key");
        assert_eq!(resolved.value, key.value);
    }

    #[tokio::test]
    async fn recovers_mixed_partial_batch() {
        let table = build_table().await;
        let dict = Dictionary::<TestKey>::with_table(table.clone(), "recover_mixed", None)
            .await
            .expect("build dictionary");

        let key_id2k_only = TestKey {
            value: "id2k_only".into(),
        };
        let key_k2id_only = TestKey {
            value: "k2id_only".into(),
        };

        let encoded_a = encoding::encode(&key_id2k_only).expect("encode key a");
        let encoded_b = encoding::encode(&key_k2id_only).expect("encode key b");

        let id_a = dict.next_id.load(Ordering::SeqCst);
        let id_b = id_a + 1;
        let hash_b = dict.hash(&encoded_b);
        let k2id_b = dict.k2id_key(hash_b, 0);

        let mut batch = WriteBatch::new();
        batch.put(
            dict.id2k_key(id_a),
            compress_value(&encoded_a).expect("compress value a"),
        );
        batch.put(k2id_b.clone(), encode_id(id_b));
        batch.put(dict.meta_key.clone(), encode_id(id_b + 1));
        dict.table
            .write_batch(batch)
            .await
            .expect("write partial state");

        let recovered = Dictionary::<TestKey>::with_table(table.clone(), "recover_mixed", None)
            .await
            .expect("reopen dictionary");

        let assigned_a = recovered
            .intern(&key_id2k_only)
            .await
            .expect("intern key a after recovery");
        let assigned_b = recovered
            .intern(&key_k2id_only)
            .await
            .expect("intern key b after recovery");

        assert_ne!(assigned_a, 0);
        assert_ne!(assigned_b, 0);

        let stored_a = recovered
            .table
            .get(&recovered.id2k_key(assigned_a))
            .await
            .expect("fetch id2k a")
            .expect("id2k a present");
        let decoded_a = decompress_value(&stored_a).expect("decompress a");
        assert_eq!(decoded_a, encoded_a);
        let resolved_a = recovered.resolve(assigned_a).await.expect("resolve a");
        assert_eq!(resolved_a.value, key_id2k_only.value);

        let stored_b = recovered
            .table
            .get(&k2id_b)
            .await
            .expect("fetch repaired k2id b")
            .map(|bytes| decode_id(&bytes).expect("decode id"))
            .expect("id present for b");
        assert_eq!(stored_b, assigned_b);
        let stored_b_value = recovered
            .table
            .get(&recovered.id2k_key(assigned_b))
            .await
            .expect("fetch id2k b")
            .expect("id2k b present");
        let decoded_b = decompress_value(&stored_b_value).expect("decompress b");
        assert_eq!(decoded_b, encoded_b);
    }

    #[tokio::test]
    async fn randomized_dictionary_harness() {
        let table = build_table().await;
        let dict = Dictionary::<TestKey>::with_table(table.clone(), "random", None)
            .await
            .expect("build dictionary");

        let mut rng = StdRng::seed_from_u64(0x5A17);
        let mut mapping: HashMap<String, u64> = HashMap::new();
        let mut ids: Vec<u64> = Vec::new();

        for _ in 0..200 {
            if rng.gen_bool(0.3) && !ids.is_empty() {
                let id = ids[rng.gen_range(0..ids.len())];
                let resolved = dict.resolve(id).await.expect("resolve id");
                let expected_key = mapping
                    .iter()
                    .find(|(_, stored_id)| **stored_id == id)
                    .map(|(key, _)| key.clone())
                    .expect("id present in mapping");
                assert_eq!(resolved.value, expected_key);
            } else {
                let key = format!("key-{}", rng.gen_range(0..150));
                let tk = TestKey { value: key.clone() };
                let assigned = dict.intern(&tk).await.expect("intern key");
                if let Some(existing) = mapping.get(&key) {
                    assert_eq!(*existing, assigned);
                } else {
                    mapping.insert(key.clone(), assigned);
                    ids.push(assigned);
                }
            }
        }

        drop(dict);

        let recovered = Dictionary::<TestKey>::with_table(table, "random", None)
            .await
            .expect("reopen dictionary");

        for (key, id) in &mapping {
            let tk = TestKey { value: key.clone() };
            let reassigned = recovered.intern(&tk).await.expect("intern after reopen");
            assert_eq!(*id, reassigned);
            let resolved = recovered.resolve(*id).await.expect("resolve after reopen");
            assert_eq!(resolved.value, *key);
        }
    }
}
