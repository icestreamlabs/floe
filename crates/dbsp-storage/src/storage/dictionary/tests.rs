use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::memory::InMemory;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};

use super::super::encoding;
use super::super::{KeyValueTable, SlateTable};
use super::codec::{compress_value, decode_id, decompress_value, encode_id};
use super::{Dictionary, HashFn, KeyIntern};

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct TestKey {
    value: String,
}

struct CountingTable {
    inner: Arc<dyn KeyValueTable>,
    get_bytes_calls: AtomicUsize,
}

impl CountingTable {
    fn new(inner: Arc<dyn KeyValueTable>) -> Self {
        Self {
            inner,
            get_bytes_calls: AtomicUsize::new(0),
        }
    }

    fn reset_get_bytes_calls(&self) {
        self.get_bytes_calls.store(0, Ordering::Relaxed);
    }

    fn get_bytes_calls(&self) -> usize {
        self.get_bytes_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl KeyValueTable for CountingTable {
    async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_bytes_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_bytes(key).await
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.inner.write_batch(batch).await
    }

    async fn scan_range_bytes(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.inner.scan_range_bytes(range, options).await
    }
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
async fn fresh_collision_slots_are_reserved_across_batch_boundaries() {
    let table = build_table().await;
    let forced_hash: HashFn = Arc::new(|_| 7);
    let dict =
        Dictionary::<TestKey>::with_table(table, "fresh_collision_batches", Some(forced_hash))
            .await
            .expect("build dictionary");

    let id_a = dict
        .intern(&TestKey {
            value: "a".to_string(),
        })
        .await
        .expect("intern key a");
    let batch_ids = dict
        .intern_many_values_unique_owned(vec![
            TestKey {
                value: "b".to_string(),
            },
            TestKey {
                value: "c".to_string(),
            },
        ])
        .await
        .expect("intern batch");
    let id_d = dict
        .intern(&TestKey {
            value: "d".to_string(),
        })
        .await
        .expect("intern key d");

    let ids = vec![id_a, batch_ids[0], batch_ids[1], id_d];
    assert_eq!(ids.iter().copied().collect::<HashSet<_>>().len(), 4);

    for (slot, id) in ids.iter().enumerate() {
        let stored = dict
            .table()
            .get(&dict.k2id_key(7, slot as u16))
            .await
            .expect("fetch collision slot")
            .map(|bytes| decode_id(&bytes).expect("decode id"))
            .expect("collision slot present");
        assert_eq!(stored, *id);
    }

    let resolved = dict.resolve_many(&ids).await.expect("resolve ids");
    let values = resolved
        .into_iter()
        .map(|entry| entry.value)
        .collect::<Vec<_>>();
    assert_eq!(values, vec!["a", "b", "c", "d"]);
}

#[tokio::test]
async fn batch_miss_reuses_discovered_collision_slot() {
    let table = build_table().await;
    let forced_hash: HashFn = Arc::new(|_| 11);
    let seed = Dictionary::<TestKey>::with_table(
        table.clone(),
        "reuse_collision_slot",
        Some(forced_hash.clone()),
    )
    .await
    .expect("build seed dictionary");
    seed.intern(&TestKey {
        value: "existing".to_string(),
    })
    .await
    .expect("seed existing key");
    drop(seed);

    let counting = Arc::new(CountingTable::new(table));
    let dict = Dictionary::<TestKey>::with_table(
        counting.clone() as Arc<dyn KeyValueTable>,
        "reuse_collision_slot",
        Some(forced_hash),
    )
    .await
    .expect("reopen dictionary");

    counting.reset_get_bytes_calls();
    let assigned = dict
        .intern(&TestKey {
            value: "new".to_string(),
        })
        .await
        .expect("intern colliding miss");

    assert_ne!(assigned, 0);
    assert_eq!(
        counting.get_bytes_calls(),
        3,
        "lookup should reuse the first free slot discovered during the miss probe",
    );
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
async fn interns_owned_unique_batch_without_cloning_keys() {
    let table = build_table().await;
    let dict = Dictionary::<TestKey>::with_table(table, "owned_unique", None)
        .await
        .expect("build dictionary");

    let ids = dict
        .intern_many_values_unique_owned(vec![
            TestKey {
                value: "alpha".to_string(),
            },
            TestKey {
                value: "beta".to_string(),
            },
            TestKey {
                value: "gamma".to_string(),
            },
        ])
        .await
        .expect("intern owned unique batch");

    assert_eq!(ids.len(), 3);

    let resolved = dict
        .resolve_many(&ids)
        .await
        .expect("resolve owned unique ids");
    let resolved_values = resolved
        .into_iter()
        .map(|entry| entry.value)
        .collect::<Vec<_>>();
    assert_eq!(resolved_values, vec!["alpha", "beta", "gamma"]);
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
    let id = dict.next_id_value();

    let mut batch = WriteBatch::new();
    batch.put(
        dict.id2k_key(id),
        compress_value(&encoded).expect("compress value"),
    );
    batch.put(dict.meta_key(), encode_id(id));
    dict.table()
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
        .table()
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
    let id = dict.next_id_value();

    let mut batch = WriteBatch::new();
    batch.put(k2id_key.clone(), encode_id(id));
    batch.put(dict.meta_key(), encode_id(id + 1));
    dict.table()
        .write_batch(batch)
        .await
        .expect("write partial state");

    let recovered = Dictionary::<TestKey>::with_table(table.clone(), "recover_id2k", None)
        .await
        .expect("reopen dictionary");

    let assigned = recovered.intern(&key).await.expect("intern after recovery");
    assert_ne!(assigned, 0);

    let stored_id = recovered
        .table()
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

    let id_a = dict.next_id_value();
    let id_b = id_a + 1;
    let hash_b = dict.hash(&encoded_b);
    let k2id_b = dict.k2id_key(hash_b, 0);

    let mut batch = WriteBatch::new();
    batch.put(
        dict.id2k_key(id_a),
        compress_value(&encoded_a).expect("compress value a"),
    );
    batch.put(k2id_b.clone(), encode_id(id_b));
    batch.put(dict.meta_key(), encode_id(id_b + 1));
    dict.table()
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
        .table()
        .get(&recovered.id2k_key(assigned_a))
        .await
        .expect("fetch id2k a")
        .expect("id2k a present");
    let decoded_a = decompress_value(&stored_a).expect("decompress a");
    assert_eq!(decoded_a, encoded_a);
    let resolved_a = recovered.resolve(assigned_a).await.expect("resolve a");
    assert_eq!(resolved_a.value, key_id2k_only.value);

    let stored_b = recovered
        .table()
        .get(&k2id_b)
        .await
        .expect("fetch repaired k2id b")
        .map(|bytes| decode_id(&bytes).expect("decode id"))
        .expect("id present for b");
    assert_eq!(stored_b, assigned_b);
    let stored_b_value = recovered
        .table()
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
