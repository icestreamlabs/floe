use std::collections::BTreeMap;
use std::hash::Hash;

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct Set<T> {
    elements: Vec<T>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct ZSet<T> {
    entries: Vec<(T, i64)>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct IndexedZSet<K, V> {
    entries: Vec<(K, ZSet<V>)>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct Window<K> {
    pub key: K,
    pub start: i64,
    pub end: i64,
}

pub trait ZeroValue: Clone + PartialEq + Send + Sync + 'static {
    fn zero() -> Self;
}

pub trait GroupValue: ZeroValue {
    fn add(&self, rhs: &Self) -> Self;
    fn neg(&self) -> Self;

    fn sub(&self, rhs: &Self) -> Self {
        self.add(&rhs.neg())
    }
}

impl ZeroValue for i64 {
    fn zero() -> Self {
        0
    }
}

impl GroupValue for i64 {
    fn add(&self, rhs: &Self) -> Self {
        self + rhs
    }

    fn neg(&self) -> Self {
        -self
    }
}

impl<A, B> ZeroValue for (A, B)
where
    A: ZeroValue,
    B: ZeroValue,
{
    fn zero() -> Self {
        (A::zero(), B::zero())
    }
}

impl<A, B> GroupValue for (A, B)
where
    A: GroupValue,
    B: GroupValue,
{
    fn add(&self, rhs: &Self) -> Self {
        (self.0.add(&rhs.0), self.1.add(&rhs.1))
    }

    fn neg(&self) -> Self {
        (self.0.neg(), self.1.neg())
    }
}

impl<T> Set<T>
where
    T: Clone + Ord,
{
    pub fn empty() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    pub fn new<I>(elements: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let mut elements: Vec<_> = elements.into_iter().collect();
        elements.sort();
        elements.dedup();
        Self { elements }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements.iter()
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn contains(&self, value: &T) -> bool {
        self.elements.binary_search(value).is_ok()
    }

    pub fn to_zset(&self) -> ZSet<T> {
        ZSet::from_weights(self.elements.iter().cloned().map(|value| (value, 1)))
    }

    pub fn map<U, F>(&self, function: F) -> Set<U>
    where
        U: Clone + Ord,
        F: Fn(&T) -> U,
    {
        Set::new(self.elements.iter().map(function))
    }

    pub fn filter<F>(&self, predicate: F) -> Set<T>
    where
        F: Fn(&T) -> bool,
    {
        Set::new(
            self.elements
                .iter()
                .filter(|value| predicate(value))
                .cloned(),
        )
    }

    pub fn union(&self, rhs: &Set<T>) -> Set<T> {
        Set::new(self.iter().cloned().chain(rhs.iter().cloned()))
    }
}

impl<T> ZeroValue for Set<T>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    fn zero() -> Self {
        Set::empty()
    }
}

impl<T> ZSet<T>
where
    T: Clone + Ord,
{
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn singleton(value: T, weight: i64) -> Self {
        Self::from_weights([(value, weight)])
    }

    pub fn from_weights<I>(entries: I) -> Self
    where
        I: IntoIterator<Item = (T, i64)>,
    {
        let mut combined = BTreeMap::new();
        for (value, weight) in entries {
            if weight == 0 {
                continue;
            }
            *combined.entry(value).or_insert(0) += weight;
        }
        combined.retain(|_, weight| *weight != 0);
        Self {
            entries: combined.into_iter().collect(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &(T, i64)> {
        self.entries.iter()
    }

    pub fn entries(&self) -> &[(T, i64)] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn weight(&self, value: &T) -> i64 {
        self.entries
            .binary_search_by(|(candidate, _)| candidate.cmp(value))
            .map(|index| self.entries[index].1)
            .unwrap_or(0)
    }

    pub fn map<U, F>(&self, function: F) -> ZSet<U>
    where
        U: Clone + Ord,
        F: Fn(&T) -> U,
    {
        ZSet::from_weights(
            self.entries
                .iter()
                .map(|(value, weight)| (function(value), *weight)),
        )
    }

    pub fn filter<F>(&self, predicate: F) -> ZSet<T>
    where
        F: Fn(&T) -> bool,
    {
        ZSet::from_weights(
            self.entries
                .iter()
                .filter(|(value, _)| predicate(value))
                .cloned(),
        )
    }

    pub fn flat_map<U, F, I>(&self, function: F) -> ZSet<U>
    where
        U: Clone + Ord,
        F: Fn(&T) -> I,
        I: IntoIterator<Item = (U, i64)>,
    {
        let mut out = Vec::new();
        for (value, weight) in &self.entries {
            for (mapped, mapped_weight) in function(value) {
                out.push((mapped, weight * mapped_weight));
            }
        }
        ZSet::from_weights(out)
    }

    pub fn distinct(&self) -> Set<T> {
        Set::new(
            self.entries
                .iter()
                .filter(|(_, weight)| *weight > 0)
                .map(|(value, _)| value.clone()),
        )
    }

    pub fn distinct_zset(&self) -> ZSet<T> {
        self.distinct().to_zset()
    }

    pub fn to_btree_map(&self) -> BTreeMap<T, i64> {
        self.entries.iter().cloned().collect()
    }

    pub fn index_by<K, F>(&self, key_extractor: F) -> IndexedZSet<K, T>
    where
        K: Clone + Ord,
        F: Fn(&T) -> Option<K>,
    {
        let mut grouped: BTreeMap<K, Vec<(T, i64)>> = BTreeMap::new();
        for (value, weight) in &self.entries {
            if let Some(key) = key_extractor(value) {
                grouped
                    .entry(key)
                    .or_default()
                    .push((value.clone(), *weight));
            }
        }
        IndexedZSet::from_grouped(grouped)
    }
}

impl<T> ZeroValue for ZSet<T>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    fn zero() -> Self {
        ZSet::empty()
    }
}

impl<T> GroupValue for ZSet<T>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    fn add(&self, rhs: &Self) -> Self {
        ZSet::from_weights(self.iter().cloned().chain(rhs.iter().cloned()))
    }

    fn neg(&self) -> Self {
        ZSet::from_weights(self.iter().map(|(value, weight)| (value.clone(), -*weight)))
    }
}

impl<K, V> IndexedZSet<K, V>
where
    K: Clone + Ord,
    V: Clone + Ord,
{
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn from_grouped(grouped: BTreeMap<K, Vec<(V, i64)>>) -> Self {
        Self {
            entries: grouped
                .into_iter()
                .map(|(key, values)| (key, ZSet::from_weights(values)))
                .filter(|(_, values)| !values.is_empty())
                .collect(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &(K, ZSet<V>)> {
        self.entries.iter()
    }

    pub fn entries(&self) -> &[(K, ZSet<V>)] {
        &self.entries
    }

    pub fn lookup(&self, key: &K) -> ZSet<V> {
        self.entries
            .binary_search_by(|(candidate, _)| candidate.cmp(key))
            .map(|index| self.entries[index].1.clone())
            .unwrap_or_else(|_| ZSet::empty())
    }

    pub fn as_pairs(&self) -> ZSet<(K, V)> {
        let mut out = Vec::new();
        for (key, values) in &self.entries {
            for (value, weight) in values.iter() {
                out.push(((key.clone(), value.clone()), *weight));
            }
        }
        ZSet::from_weights(out)
    }

    pub fn join<R, O, F>(&self, rhs: &IndexedZSet<K, R>, projector: F) -> ZSet<O>
    where
        R: Clone + Ord,
        O: Clone + Ord,
        F: Fn(&K, &V, &R) -> O,
    {
        let mut out = Vec::new();
        for (key, left_values) in &self.entries {
            let right_values = rhs.lookup(key);
            for (left_value, left_weight) in left_values.iter() {
                for (right_value, right_weight) in right_values.iter() {
                    out.push((
                        projector(key, left_value, right_value),
                        left_weight * right_weight,
                    ));
                }
            }
        }
        ZSet::from_weights(out)
    }
}

impl<K, V> ZeroValue for IndexedZSet<K, V>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync + 'static,
{
    fn zero() -> Self {
        IndexedZSet::empty()
    }
}

impl<K, V> GroupValue for IndexedZSet<K, V>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Ord + Send + Sync + 'static,
{
    fn add(&self, rhs: &Self) -> Self {
        let mut grouped: BTreeMap<K, Vec<(V, i64)>> = BTreeMap::new();
        for (key, values) in self.iter().chain(rhs.iter()) {
            grouped
                .entry(key.clone())
                .or_default()
                .extend(values.iter().cloned());
        }
        IndexedZSet::from_grouped(grouped)
    }

    fn neg(&self) -> Self {
        let mut grouped = BTreeMap::new();
        for (key, values) in &self.entries {
            grouped.insert(key.clone(), values.neg().entries().to_vec());
        }
        IndexedZSet::from_grouped(grouped)
    }
}

pub trait RuntimeKeyBounds:
    Clone
    + Eq
    + Hash
    + Ord
    + Send
    + Sync
    + 'static
    + for<'a> rkyv::Serialize<dbsp_runtime::storage::encoding::RkyvSerializer<'a>>
    + rkyv::Archive
where
    Self::Archived: rkyv::Deserialize<Self, dbsp_runtime::storage::encoding::RkyvDeserializer>
        + for<'a> rkyv::bytecheck::CheckBytes<dbsp_runtime::storage::encoding::RkyvValidator<'a>>,
{
}

impl<T> RuntimeKeyBounds for T
where
    T: Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> rkyv::Serialize<dbsp_runtime::storage::encoding::RkyvSerializer<'a>>
        + rkyv::Archive,
    T::Archived: rkyv::Deserialize<Self, dbsp_runtime::storage::encoding::RkyvDeserializer>
        + for<'a> rkyv::bytecheck::CheckBytes<dbsp_runtime::storage::encoding::RkyvValidator<'a>>,
{
}
