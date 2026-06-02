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

#[async_trait]
pub trait KeyIntern<K>: Send + Sync
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    async fn intern(&self, key: &K) -> Result<u64>;
    async fn resolve(&self, id: u64) -> Result<K>;
    async fn resolve_many(&self, ids: &[u64]) -> Result<Vec<K>> {
        let mut resolved = Vec::with_capacity(ids.len());
        for id in ids {
            resolved.push(self.resolve(*id).await?);
        }
        Ok(resolved)
    }
    async fn lookup(&self, key: &K) -> Result<Option<u64>>;
}

#[cfg(test)]
mod tests;
