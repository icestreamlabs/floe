use std::hash::Hash;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::zset::{self, ZSet};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

pub struct LiftedSelect<P> {
    predicate: P,
}

impl<P> LiftedSelect<P> {
    pub fn new(predicate: P) -> Self {
        Self { predicate }
    }

    pub async fn apply<K>(&self, zset: &ZSet<K>) -> Result<ZSet<K>>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        P: Fn(&K) -> bool + Send + Sync,
    {
        zset::select(zset, &self.predicate).await
    }
}

pub struct LiftedProject<F> {
    projector: F,
}

impl<F> LiftedProject<F> {
    pub fn new(projector: F) -> Self {
        Self { projector }
    }

    pub async fn apply<K, R>(&self, zset: &ZSet<K>) -> Result<ZSet<R>>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        F: Fn(&K) -> R + Send + Sync,
    {
        zset::project(zset, &self.projector).await
    }
}

pub struct LiftedJoin<P, F> {
    predicate: P,
    projector: F,
}

impl<P, F> LiftedJoin<P, F> {
    pub fn new(predicate: P, projector: F) -> Self {
        Self {
            predicate,
            projector,
        }
    }

    pub async fn apply<L, R, O>(&self, left: &ZSet<L>, right: &ZSet<R>) -> Result<ZSet<O>>
    where
        L: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        P: Fn(&L, &R) -> bool + Send + Sync,
        F: Fn(&L, &R) -> O + Send + Sync,
    {
        zset::join(left, right, &self.predicate, &self.projector).await
    }
}
