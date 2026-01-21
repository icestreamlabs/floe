mod batch;
mod cache;
mod codec;
mod core;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use super::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

pub use batch::DictionaryBatch;
pub use core::Dictionary;

type HashFn = Arc<dyn Fn(&[u8]) -> u64 + Send + Sync + 'static>;

#[allow(dead_code)]
#[async_trait]
pub trait KeyIntern<K>: Send + Sync
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    async fn intern(&self, key: &K) -> Result<u64>;
    async fn resolve(&self, id: u64) -> Result<K>;
    async fn lookup(&self, key: &K) -> Result<Option<u64>>;
}

#[cfg(test)]
mod tests;
