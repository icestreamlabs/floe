use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use super::cache::BatchOverlay;
use super::core::Dictionary;
use super::KeyIntern;
use super::super::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};

#[allow(dead_code)]
pub struct DictionaryBatch<'a, K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    pub(super) dict: &'a Dictionary<K>,
    pub(super) overlay: BatchOverlay,
    pub(super) _marker: std::marker::PhantomData<K>,
}

impl<'a, K> DictionaryBatch<'a, K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    #[allow(dead_code)]
    pub async fn intern(&mut self, key: &K) -> Result<u64> {
        let encoded = encoding::encode(key).context("unable to encode dictionary key")?;
        self.dict.intern_key(encoded, Some(&mut self.overlay)).await
    }

    #[allow(dead_code)]
    pub async fn resolve(&mut self, id: u64) -> Result<K> {
        self.dict.resolve(id).await
    }
}
