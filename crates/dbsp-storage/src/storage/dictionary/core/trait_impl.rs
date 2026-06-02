use super::*;

#[async_trait]
impl<K> KeyIntern<K> for Dictionary<K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    async fn intern(&self, key: &K) -> Result<u64> {
        let encoded = encoding::encode(key).context("unable to encode dictionary key")?;
        self.intern_key(encoded, None).await
    }

    async fn resolve(&self, id: u64) -> Result<K> {
        if id == 0 {
            return Err(anyhow!("id 0 is not valid"));
        }

        let encoded = {
            if let Some(bytes) = {
                let mut cache = self.cache.lock().unwrap();
                cache.lookup_key(&id)
            } {
                bytes
            } else {
                let mut key = Vec::with_capacity(self.id2k_prefix.len() + 8);
                self.encode_id2k_key_into(&mut key, id);
                let bytes = self
                    .table
                    .get_bytes(&key)
                    .await?
                    .ok_or_else(|| anyhow!("no key found for id {id}"))?;
                let decoded = decompress_value(bytes.as_ref())?;
                let mut cache = self.cache.lock().unwrap();
                cache.remember(decoded, id)
            }
        };

        encoding::decode(encoded.as_ref()).context("unable to decode dictionary value")
    }

    async fn resolve_many(&self, ids: &[u64]) -> Result<Vec<K>> {
        self.resolve_many_ids(ids).await
    }

    async fn lookup(&self, key: &K) -> Result<Option<u64>> {
        let encoded = encoding::encode(key).context("unable to encode dictionary key")?;
        if let Some(id) = self.lookup_existing_in_cache(&encoded) {
            return Ok(Some(id));
        }
        self.lookup_existing_id(&encoded).await
    }
}
