use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;

const KEY_TO_ID_CACHE_CAPACITY: usize = 4_096;
const ID_TO_KEY_CACHE_CAPACITY: usize = 32_768;
const NEGATIVE_CACHE_CAPACITY: usize = 4_096;
pub(super) type SharedKey = Arc<[u8]>;

pub(super) struct Cache {
    key_to_id: LruCache<SharedKey, u64>,
    id_to_key: LruCache<u64, SharedKey>,
    negatives: LruCache<SharedKey, ()>,
}

impl Cache {
    #[allow(dead_code)]
    pub(super) fn new() -> Self {
        let key_to_id_capacity =
            NonZeroUsize::new(KEY_TO_ID_CACHE_CAPACITY).expect("non-zero cache size");
        let id_to_key_capacity =
            NonZeroUsize::new(ID_TO_KEY_CACHE_CAPACITY).expect("non-zero cache size");
        let negative_capacity =
            NonZeroUsize::new(NEGATIVE_CACHE_CAPACITY).expect("non-zero cache size");
        Self {
            key_to_id: LruCache::new(key_to_id_capacity),
            id_to_key: LruCache::new(id_to_key_capacity),
            negatives: LruCache::new(negative_capacity),
        }
    }

    pub(super) fn remember(&mut self, key: impl Into<SharedKey>, id: u64) -> SharedKey {
        let key = key.into();
        self.key_to_id.put(key.clone(), id);
        self.id_to_key.put(id, key.clone());
        self.negatives.pop(key.as_ref());
        key
    }

    pub(super) fn lookup_id(&mut self, key: &[u8]) -> Option<u64> {
        self.key_to_id.get(key).copied()
    }

    pub(super) fn lookup_key(&mut self, id: &u64) -> Option<SharedKey> {
        self.id_to_key.get(id).cloned()
    }

    pub(super) fn remember_negative(&mut self, key: &[u8]) {
        self.negatives.put(SharedKey::from(key), ());
    }

    pub(super) fn clear_negative(&mut self, key: &[u8]) {
        self.negatives.pop(key);
    }

    pub(super) fn is_negative(&mut self, key: &[u8]) -> bool {
        self.negatives.contains(key)
    }
}

pub(super) struct BatchOverlay {
    positives: HashMap<SharedKey, u64>,
    negatives: HashSet<SharedKey>,
}

impl BatchOverlay {
    pub(super) fn new() -> Self {
        Self {
            positives: HashMap::new(),
            negatives: HashSet::new(),
        }
    }

    pub(super) fn lookup(&self, key: &[u8]) -> Option<u64> {
        self.positives.get(key).copied()
    }

    pub(super) fn remember_positive(&mut self, key: impl Into<SharedKey>, id: u64) {
        self.positives.insert(key.into(), id);
    }

    pub(super) fn remember_negative(&mut self, key: impl Into<SharedKey>) {
        self.negatives.insert(key.into());
    }

    pub(super) fn clear_negative(&mut self, key: &[u8]) {
        self.negatives.remove(key);
    }

    pub(super) fn is_negative(&self, key: &[u8]) -> bool {
        self.negatives.contains(key)
    }
}
