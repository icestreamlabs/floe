use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::values::{GroupValue, ZeroValue};

trait Evaluate<T>: Send + Sync {
    fn at(&self, t: usize) -> T;
}

#[derive(Clone)]
pub struct Stream<T> {
    inner: Arc<dyn Evaluate<T>>,
}

enum CacheEntry<T> {
    Computing,
    Ready(T),
}

struct MemoizedEval<T, F>
where
    F: Fn(usize) -> T + Send + Sync + 'static,
{
    name: &'static str,
    cache: Mutex<BTreeMap<usize, CacheEntry<T>>>,
    function: F,
}

impl<T, F> Evaluate<T> for MemoizedEval<T, F>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(usize) -> T + Send + Sync + 'static,
{
    fn at(&self, t: usize) -> T {
        {
            let cache = self.cache.lock().expect("semantic stream cache poisoned");
            if let Some(CacheEntry::Ready(value)) = cache.get(&t) {
                return value.clone();
            }
            if matches!(cache.get(&t), Some(CacheEntry::Computing)) {
                panic!(
                    "unguarded semantic feedback detected while evaluating '{}' at logical time {}",
                    self.name, t
                );
            }
        }

        {
            let mut cache = self.cache.lock().expect("semantic stream cache poisoned");
            if let Some(CacheEntry::Ready(value)) = cache.get(&t) {
                return value.clone();
            }
            if matches!(cache.get(&t), Some(CacheEntry::Computing)) {
                panic!(
                    "unguarded semantic feedback detected while evaluating '{}' at logical time {}",
                    self.name, t
                );
            }
            cache.insert(t, CacheEntry::Computing);
        }

        let value = (self.function)(t);

        let mut cache = self.cache.lock().expect("semantic stream cache poisoned");
        cache.insert(t, CacheEntry::Ready(value.clone()));
        value
    }
}

struct RecursiveEval<T> {
    name: &'static str,
    target: OnceLock<Stream<T>>,
    cache: Mutex<BTreeMap<usize, CacheEntry<T>>>,
}

impl<T> RecursiveEval<T> {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            target: OnceLock::new(),
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    fn set_target(&self, target: Stream<T>) {
        assert!(
            self.target.set(target).is_ok(),
            "semantic feedback target already initialized"
        );
    }
}

impl<T> Evaluate<T> for RecursiveEval<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn at(&self, t: usize) -> T {
        {
            let cache = self.cache.lock().expect("semantic stream cache poisoned");
            if let Some(CacheEntry::Ready(value)) = cache.get(&t) {
                return value.clone();
            }
            if matches!(cache.get(&t), Some(CacheEntry::Computing)) {
                panic!(
                    "unguarded semantic feedback detected while evaluating '{}' at logical time {}",
                    self.name, t
                );
            }
        }

        {
            let mut cache = self.cache.lock().expect("semantic stream cache poisoned");
            if let Some(CacheEntry::Ready(value)) = cache.get(&t) {
                return value.clone();
            }
            if matches!(cache.get(&t), Some(CacheEntry::Computing)) {
                panic!(
                    "unguarded semantic feedback detected while evaluating '{}' at logical time {}",
                    self.name, t
                );
            }
            cache.insert(t, CacheEntry::Computing);
        }

        let target = self
            .target
            .get()
            .expect("semantic feedback target must be initialized before evaluation");
        let value = target.at(t);

        let mut cache = self.cache.lock().expect("semantic stream cache poisoned");
        cache.insert(t, CacheEntry::Ready(value.clone()));
        value
    }
}

impl<T> Stream<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn from_evaluator<E>(evaluator: E) -> Self
    where
        E: Evaluate<T> + 'static,
    {
        Self {
            inner: Arc::new(evaluator),
        }
    }

    pub fn from_fn(
        name: &'static str,
        function: impl Fn(usize) -> T + Send + Sync + 'static,
    ) -> Self {
        Self::from_evaluator(MemoizedEval {
            name,
            cache: Mutex::new(BTreeMap::new()),
            function,
        })
    }

    pub fn constant(value: T) -> Self {
        Self::from_fn("constant", move |_| value.clone())
    }

    pub fn from_prefix(prefix: impl Into<Vec<T>>, tail: T) -> Self {
        let prefix = prefix.into();
        Self::from_fn("prefix", move |t| {
            prefix.get(t).cloned().unwrap_or_else(|| tail.clone())
        })
    }

    pub fn at(&self, t: usize) -> T {
        self.inner.at(t)
    }

    pub fn prefix(&self, len: usize) -> Vec<T> {
        (0..len).map(|t| self.at(t)).collect()
    }

    pub fn lift<U>(
        &self,
        name: &'static str,
        function: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> Stream<U>
    where
        U: Clone + Send + Sync + 'static,
    {
        let input = self.clone();
        Stream::from_fn(name, move |t| {
            let value = input.at(t);
            function(&value)
        })
    }
}

pub fn zip_with<L, R, O>(
    name: &'static str,
    left: &Stream<L>,
    right: &Stream<R>,
    function: impl Fn(&L, &R) -> O + Send + Sync + 'static,
) -> Stream<O>
where
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    let left = left.clone();
    let right = right.clone();
    Stream::from_fn(name, move |t| {
        let left_value = left.at(t);
        let right_value = right.at(t);
        function(&left_value, &right_value)
    })
}

pub fn pair<L, R>(left: &Stream<L>, right: &Stream<R>) -> Stream<(L, R)>
where
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    zip_with("pair", left, right, |l, r| (l.clone(), r.clone()))
}

pub fn add<T>(left: &Stream<T>, right: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    zip_with("add", left, right, |l, r| l.add(r))
}

pub fn negate<T>(input: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    input.lift("negate", |value| value.neg())
}

pub fn subtract<T>(left: &Stream<T>, right: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    zip_with("subtract", left, right, |l, r| l.sub(r))
}

pub fn delay<T>(input: &Stream<T>) -> Stream<T>
where
    T: ZeroValue,
{
    let input = input.clone();
    Stream::from_fn(
        "delay",
        move |t| {
            if t == 0 { T::zero() } else { input.at(t - 1) }
        },
    )
}

pub fn differentiate<T>(input: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    subtract(input, &delay(input))
}

pub fn feedback<T>(
    name: &'static str,
    builder: impl Fn(Stream<T>) -> Stream<T> + Send + Sync + 'static,
) -> Stream<T>
where
    T: Clone + Send + Sync + 'static,
{
    let recursive = Arc::new(RecursiveEval::new(name));
    let stream = Stream {
        inner: recursive.clone(),
    };
    let body = builder(stream.clone());
    recursive.set_target(body);
    stream
}

pub fn integrate<T>(input: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    let input = input.clone();
    feedback("integrate", move |state| add(&input, &delay(&state)))
}

pub struct ReferenceEvaluator;

impl ReferenceEvaluator {
    pub fn observe_prefix<T>(stream: &Stream<T>, len: usize) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        stream.prefix(len)
    }
}
