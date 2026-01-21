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
