use std::hash::Hash;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::zset::{self, ZSet};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

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
