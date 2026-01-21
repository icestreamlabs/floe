use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

use lru::LruCache;

const CACHE_CAPACITY: usize = 1024;

pub(super) struct Cache {
    key_to_id: LruCache<Vec<u8>, u64>,
    id_to_key: LruCache<u64, Vec<u8>>,
    negatives: LruCache<Vec<u8>, ()>,
    max_key_len: usize,
}

impl Cache {
    #[allow(dead_code)]
    pub(super) fn new() -> Self {
        let capacity = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero cache size");
        Self {
            key_to_id: LruCache::new(capacity),
            id_to_key: LruCache::new(capacity),
            negatives: LruCache::new(capacity),
            max_key_len: 0,
        }
    }

    pub(super) fn remember(&mut self, key: Vec<u8>, id: u64) {
        self.key_to_id.put(key.clone(), id);
        self.id_to_key.put(id, key.clone());
        self.negatives.pop(&key);
        self.max_key_len = self.max_key_len.max(key.len());
    }

    pub(super) fn lookup_id(&mut self, key: &[u8]) -> Option<u64> {
        self.key_to_id.get(key).copied()
    }

    pub(super) fn lookup_key(&mut self, id: &u64) -> Option<Vec<u8>> {
        self.id_to_key.get(id).cloned()
    }

    pub(super) fn remember_negative(&mut self, key: &[u8]) {
        self.negatives.put(key.to_vec(), ());
    }

    pub(super) fn clear_negative(&mut self, key: &[u8]) {
        self.negatives.pop(key);
    }

    pub(super) fn is_negative(&mut self, key: &[u8]) -> bool {
        self.negatives.contains(key)
    }
}

pub(super) struct BatchOverlay {
    positives: HashMap<Vec<u8>, u64>,
    negatives: HashSet<Vec<u8>>,
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

    pub(super) fn remember_positive(&mut self, key: Vec<u8>, id: u64) {
        self.positives.insert(key, id);
    }

    pub(super) fn remember_negative(&mut self, key: Vec<u8>) {
        self.negatives.insert(key);
    }

    pub(super) fn clear_negative(&mut self, key: &[u8]) {
        self.negatives.remove(key);
    }

    pub(super) fn is_negative(&self, key: &[u8]) -> bool {
        self.negatives.contains(key)
    }
}
