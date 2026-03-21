use std::collections::BTreeMap;

use crate::stream::{Stream, add, zip_with};
use crate::values::{IndexedZSet, Set, Window, ZSet};

pub fn map_set<T, U>(
    input: &Stream<Set<T>>,
    function: impl Fn(&T) -> U + Send + Sync + 'static,
) -> Stream<Set<U>>
where
    T: Clone + Ord + Send + Sync + 'static,
    U: Clone + Ord + Send + Sync + 'static,
{
    input.lift("set_map", move |value| value.map(|row| function(row)))
}

pub fn filter_set<T>(
    input: &Stream<Set<T>>,
    predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
) -> Stream<Set<T>>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    input.lift("set_filter", move |value| {
        value.filter(|row| predicate(row))
    })
}

pub fn union_set<T>(left: &Stream<Set<T>>, right: &Stream<Set<T>>) -> Stream<Set<T>>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    zip_with("set_union", left, right, |l, r| l.union(r))
}

pub fn join_set<L, R, K, O>(
    left: &Stream<Set<L>>,
    right: &Stream<Set<R>>,
    left_key: impl Fn(&L) -> Option<K> + Send + Sync + 'static,
    right_key: impl Fn(&R) -> Option<K> + Send + Sync + 'static,
    predicate: impl Fn(&L, &R) -> bool + Send + Sync + 'static,
    projector: impl Fn(&L, &R) -> O + Send + Sync + 'static,
) -> Stream<Set<O>>
where
    L: Clone + Ord + Send + Sync + 'static,
    R: Clone + Ord + Send + Sync + 'static,
    K: Clone + Ord + Send + Sync + 'static,
    O: Clone + Ord + Send + Sync + 'static,
{
    zip_with("set_join", left, right, move |l, r| {
        let mut out = Vec::new();
        for left_value in l.iter() {
            let Some(left_key_value) = left_key(left_value) else {
                continue;
            };
            for right_value in r.iter() {
                let Some(right_key_value) = right_key(right_value) else {
                    continue;
                };
                if left_key_value == right_key_value && predicate(left_value, right_value) {
                    out.push(projector(left_value, right_value));
                }
            }
        }
        Set::new(out)
    })
}

pub fn map_zset<T, U>(
    input: &Stream<ZSet<T>>,
    function: impl Fn(&T) -> U + Send + Sync + 'static,
) -> Stream<ZSet<U>>
where
    T: Clone + Ord + Send + Sync + 'static,
    U: Clone + Ord + Send + Sync + 'static,
{
    input.lift("zset_map", move |value| value.map(|row| function(row)))
}

pub fn filter_zset<T>(
    input: &Stream<ZSet<T>>,
    predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
) -> Stream<ZSet<T>>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    input.lift("zset_filter", move |value| {
        value.filter(|row| predicate(row))
    })
}

pub fn flat_map_zset<T, U, I>(
    input: &Stream<ZSet<T>>,
    function: impl Fn(&T) -> I + Send + Sync + 'static,
) -> Stream<ZSet<U>>
where
    T: Clone + Ord + Send + Sync + 'static,
    U: Clone + Ord + Send + Sync + 'static,
    I: IntoIterator<Item = (U, i64)>,
{
    input.lift("zset_flat_map", move |value| {
        value.flat_map(|row| function(row))
    })
}

pub fn union_zset<T>(left: &Stream<ZSet<T>>, right: &Stream<ZSet<T>>) -> Stream<ZSet<T>>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    add(left, right)
}

pub fn join_zset<L, R, K, O>(
    left: &Stream<ZSet<L>>,
    right: &Stream<ZSet<R>>,
    left_key: impl Fn(&L) -> Option<K> + Send + Sync + 'static,
    right_key: impl Fn(&R) -> Option<K> + Send + Sync + 'static,
    predicate: impl Fn(&L, &R) -> bool + Send + Sync + 'static,
    projector: impl Fn(&L, &R) -> O + Send + Sync + 'static,
) -> Stream<ZSet<O>>
where
    L: Clone + Ord + Send + Sync + 'static,
    R: Clone + Ord + Send + Sync + 'static,
    K: Clone + Ord + Send + Sync + 'static,
    O: Clone + Ord + Send + Sync + 'static,
{
    zip_with("zset_join", left, right, move |l, r| {
        let mut out = Vec::new();
        for (left_value, left_weight) in l.iter() {
            let Some(left_key_value) = left_key(left_value) else {
                continue;
            };
            for (right_value, right_weight) in r.iter() {
                let Some(right_key_value) = right_key(right_value) else {
                    continue;
                };
                if left_key_value == right_key_value && predicate(left_value, right_value) {
                    out.push((
                        projector(left_value, right_value),
                        left_weight * right_weight,
                    ));
                }
            }
        }
        ZSet::from_weights(out)
    })
}

pub fn distinct_zset<T>(input: &Stream<ZSet<T>>) -> Stream<Set<T>>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    input.lift("zset_distinct", |value| value.distinct())
}

pub fn arrange_by<T, K>(
    input: &Stream<ZSet<T>>,
    key_extractor: impl Fn(&T) -> Option<K> + Send + Sync + 'static,
) -> Stream<IndexedZSet<K, T>>
where
    T: Clone + Ord + Send + Sync + 'static,
    K: Clone + Ord + Send + Sync + 'static,
{
    input.lift("zset_arrange", move |value| {
        value.index_by(|row| key_extractor(row))
    })
}

pub fn lookup_index<K, V>(input: &Stream<IndexedZSet<K, V>>, key: K) -> Stream<ZSet<V>>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync + 'static,
{
    input.lift("index_lookup", move |value| value.lookup(&key))
}

pub fn join_indexed<K, L, R, O>(
    left: &Stream<IndexedZSet<K, L>>,
    right: &Stream<IndexedZSet<K, R>>,
    projector: impl Fn(&K, &L, &R) -> O + Send + Sync + 'static,
) -> Stream<ZSet<O>>
where
    K: Clone + Ord + Send + Sync + 'static,
    L: Clone + Ord + Send + Sync + 'static,
    R: Clone + Ord + Send + Sync + 'static,
    O: Clone + Ord + Send + Sync + 'static,
{
    zip_with("indexed_join", left, right, move |l, r| {
        l.join(r, |k, lv, rv| projector(k, lv, rv))
    })
}

pub fn aggregate_zset<K, V, A>(
    input: &Stream<ZSet<V>>,
    key_extractor: impl Fn(&V) -> Option<K> + Send + Sync + 'static,
    aggregator: impl Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync + 'static,
) -> Stream<ZSet<(K, A)>>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync + 'static,
    A: Clone + Ord + Send + Sync + 'static,
{
    input.lift("zset_aggregate", move |value| {
        let mut grouped: BTreeMap<K, Vec<(V, i64)>> = BTreeMap::new();
        for (row, weight) in value.iter() {
            if let Some(key) = key_extractor(row) {
                grouped.entry(key).or_default().push((row.clone(), *weight));
            }
        }

        let mut out = Vec::new();
        for (key, rows) in grouped {
            if let Some(result) = aggregator(&key, &rows) {
                out.push(((key, result), 1));
            }
        }
        ZSet::from_weights(out)
    })
}

pub fn count_by_zset<K, V>(
    input: &Stream<ZSet<V>>,
    key_extractor: impl Fn(&V) -> Option<K> + Send + Sync + 'static,
) -> Stream<ZSet<(K, i64)>>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync + 'static,
{
    aggregate_zset(input, key_extractor, |_key, rows| {
        Some((rows.iter().map(|(_, weight)| *weight).sum::<i64>()).max(0))
    })
}

pub fn unnest_zset<L, R>(input: &Stream<ZSet<(L, ZSet<R>)>>) -> Stream<ZSet<(L, R)>>
where
    L: Clone + Ord + Send + Sync + 'static,
    R: Clone + Ord + Send + Sync + 'static,
{
    input.lift("zset_unnest", |value| {
        let mut out = Vec::new();
        for ((outer, inner), outer_weight) in value.iter() {
            for (inner_value, inner_weight) in inner.iter() {
                out.push((
                    (outer.clone(), inner_value.clone()),
                    outer_weight * inner_weight,
                ));
            }
        }
        ZSet::from_weights(out)
    })
}

pub fn sliding_window_aggregate<K, V, A>(
    input: &Stream<ZSet<V>>,
    key_extractor: impl Fn(&V) -> Option<K> + Send + Sync + 'static,
    time_extractor: impl Fn(&V) -> Option<i64> + Send + Sync + 'static,
    aggregator: impl Fn(&Window<K>, &[(V, i64)]) -> Option<A> + Send + Sync + 'static,
    window_size: i64,
    window_slide: i64,
) -> Stream<ZSet<(Window<K>, A)>>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync + 'static,
    A: Clone + Ord + Send + Sync + 'static,
{
    assert!(window_size > 0, "window_size must be positive");
    assert!(window_slide > 0, "window_slide must be positive");

    input.lift("sliding_window", move |value| {
        let mut grouped: BTreeMap<Window<K>, Vec<(V, i64)>> = BTreeMap::new();
        for (row, weight) in value.iter() {
            let Some(key) = key_extractor(row) else {
                continue;
            };
            let Some(ts) = time_extractor(row) else {
                continue;
            };
            for (start, end) in window_assignments(ts, window_size, window_slide) {
                grouped
                    .entry(Window {
                        key: key.clone(),
                        start,
                        end,
                    })
                    .or_default()
                    .push((row.clone(), *weight));
            }
        }

        let mut out = Vec::new();
        for (window, rows) in grouped {
            if let Some(result) = aggregator(&window, &rows) {
                out.push(((window, result), 1));
            }
        }
        ZSet::from_weights(out)
    })
}

pub fn tumbling_window_aggregate<K, V, A>(
    input: &Stream<ZSet<V>>,
    key_extractor: impl Fn(&V) -> Option<K> + Send + Sync + 'static,
    time_extractor: impl Fn(&V) -> Option<i64> + Send + Sync + 'static,
    aggregator: impl Fn(&Window<K>, &[(V, i64)]) -> Option<A> + Send + Sync + 'static,
    window_size: i64,
) -> Stream<ZSet<(Window<K>, A)>>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync + 'static,
    A: Clone + Ord + Send + Sync + 'static,
{
    sliding_window_aggregate(
        input,
        key_extractor,
        time_extractor,
        aggregator,
        window_size,
        window_size,
    )
}

fn window_assignments(timestamp: i64, window_size: i64, window_slide: i64) -> Vec<(i64, i64)> {
    if timestamp < 0 {
        return Vec::new();
    }

    let last_start = (timestamp / window_slide) * window_slide;
    let mut start = last_start;
    let mut assignments = Vec::new();
    loop {
        let end = start + window_size;
        if timestamp >= start && timestamp < end {
            assignments.push((start, end));
        }
        if start < window_slide {
            break;
        }
        if timestamp >= start + window_size {
            break;
        }
        start -= window_slide;
    }
    assignments.sort();
    assignments.dedup();
    assignments
}
