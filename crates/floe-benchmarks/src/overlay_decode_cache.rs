use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    Hit,
    Miss,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DecodeCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
    pub invalidations: usize,
}

#[derive(Debug, Clone)]
struct CacheEntry<T> {
    value: Arc<T>,
    last_touch: u64,
}

/// Deterministic, bounded LRU cache keyed by overlay segment id.
pub struct OverlayDecodeCache<T> {
    capacity: usize,
    touch_clock: u64,
    entries: HashMap<u64, CacheEntry<T>>,
    stats: DecodeCacheStats,
}

impl<T> OverlayDecodeCache<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            touch_clock: 0,
            entries: HashMap::new(),
            stats: DecodeCacheStats::default(),
        }
    }

    pub fn invalidate(&mut self, segment_id: u64) {
        if self.entries.remove(&segment_id).is_some() {
            self.stats.invalidations = self.stats.invalidations.saturating_add(1);
        }
    }

    pub fn stats(&self) -> DecodeCacheStats {
        self.stats
    }

    pub fn get(&mut self, segment_id: u64) -> Option<Arc<T>> {
        self.touch_clock = self.touch_clock.saturating_add(1);
        let entry = self.entries.get_mut(&segment_id)?;
        entry.last_touch = self.touch_clock;
        self.stats.hits = self.stats.hits.saturating_add(1);
        Some(Arc::clone(&entry.value))
    }

    pub fn insert_miss(&mut self, segment_id: u64, value: T) -> Arc<T> {
        self.stats.misses = self.stats.misses.saturating_add(1);
        self.insert_value(segment_id, value)
    }

    pub fn get_or_insert_with<F>(&mut self, segment_id: u64, decode: F) -> (Arc<T>, CacheStatus)
    where
        F: FnOnce() -> T,
    {
        if let Some(value) = self.get(segment_id) {
            return (value, CacheStatus::Hit);
        }

        let value = self.insert_miss(segment_id, decode());
        (value, CacheStatus::Miss)
    }

    fn insert_value(&mut self, segment_id: u64, value: T) -> Arc<T> {
        self.touch_clock = self.touch_clock.saturating_add(1);
        if self.capacity > 0 && self.entries.len() >= self.capacity {
            if let Some(evicted) = self.select_lru_segment_id() {
                self.entries.remove(&evicted);
                self.stats.evictions = self.stats.evictions.saturating_add(1);
            }
        }

        let value = Arc::new(value);
        if self.capacity > 0 {
            self.entries.insert(
                segment_id,
                CacheEntry {
                    value: Arc::clone(&value),
                    last_touch: self.touch_clock,
                },
            );
        }
        value
    }

    fn select_lru_segment_id(&self) -> Option<u64> {
        self.entries
            .iter()
            .min_by_key(|(segment_id, entry)| (entry.last_touch, *segment_id))
            .map(|(segment_id, _)| *segment_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheStatus, OverlayDecodeCache};

    #[test]
    fn cache_hits_misses_and_evictions_are_deterministic() {
        let mut cache = OverlayDecodeCache::new(2);

        let (one, one_status) = cache.get_or_insert_with(1, || vec![1_u8]);
        let (_, one_hit_status) = cache.get_or_insert_with(1, || vec![99_u8]);
        let (two, two_status) = cache.get_or_insert_with(2, || vec![2_u8]);
        let (_, three_status) = cache.get_or_insert_with(3, || vec![3_u8]);
        let (_, one_reloaded_status) = cache.get_or_insert_with(1, || vec![10_u8]);

        assert_eq!(one_status, CacheStatus::Miss);
        assert_eq!(one_hit_status, CacheStatus::Hit);
        assert_eq!(two_status, CacheStatus::Miss);
        assert_eq!(three_status, CacheStatus::Miss);
        assert_eq!(one_reloaded_status, CacheStatus::Miss);
        assert_eq!(*one, vec![1_u8]);
        assert_eq!(*two, vec![2_u8]);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 4);
        assert_eq!(stats.evictions, 2);
        assert_eq!(stats.invalidations, 0);
    }

    #[test]
    fn invalidation_forces_redecode_after_segment_rewrite() {
        let mut cache = OverlayDecodeCache::new(2);

        let (first, first_status) = cache.get_or_insert_with(7, || vec![1_u8, 2_u8]);
        assert_eq!(first_status, CacheStatus::Miss);
        assert_eq!(*first, vec![1_u8, 2_u8]);

        cache.invalidate(7);

        let (second, second_status) = cache.get_or_insert_with(7, || vec![9_u8, 9_u8]);
        assert_eq!(second_status, CacheStatus::Miss);
        assert_eq!(*second, vec![9_u8, 9_u8]);

        let stats = cache.stats();
        assert_eq!(stats.invalidations, 1);
    }
}
