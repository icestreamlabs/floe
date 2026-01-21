use std::hash::Hash;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::zset::{self, ZSet};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

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
