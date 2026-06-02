use super::*;

impl<K> Dictionary<K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    pub async fn intern_many_values<'a, I>(&self, keys: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let total_start = Instant::now();
        let mut encoded_keys = Vec::new();
        for key in keys {
            encoded_keys
                .push(encoding::encode(key).context("unable to encode dictionary key in batch")?);
        }
        let encode_ms = total_start.elapsed().as_millis() as u64;
        let output = self.intern_many_encoded(encoded_keys).await?;
        let total_ms = total_start.elapsed().as_millis() as u64;
        if total_ms >= 10 {
            tracing::info!(
                keys = output.len(),
                encode_ms,
                intern_ms = total_ms.saturating_sub(encode_ms),
                total_ms,
                "dictionary intern_many_values latency"
            );
        }
        Ok(output)
    }

    /// Intern a batch of keys that are already unique within the batch.
    ///
    /// This skips duplicate-detection bookkeeping in `intern_many_values` and is
    /// intended for callers that already consolidated their delta by key.
    pub async fn intern_many_values_unique<'a, I>(&self, keys: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let total_start = Instant::now();
        let mut encoded_keys = Vec::new();
        for key in keys {
            encoded_keys
                .push(encoding::encode(key).context("unable to encode dictionary key in batch")?);
        }
        let encode_ms = total_start.elapsed().as_millis() as u64;
        let output = self.intern_many_encoded_unique(encoded_keys).await?;
        let total_ms = total_start.elapsed().as_millis() as u64;
        if total_ms >= 10 {
            tracing::info!(
                keys = output.len(),
                encode_ms,
                intern_ms = total_ms.saturating_sub(encode_ms),
                total_ms,
                "dictionary intern_many_values_unique latency"
            );
        }
        Ok(output)
    }

    /// Intern a batch of owned keys that are already unique within the batch.
    ///
    /// This avoids cloning key payloads while staging batch inserts.
    pub async fn intern_many_values_unique_owned(&self, keys: Vec<K>) -> Result<Vec<u64>> {
        let total_start = Instant::now();
        let mut encoded_keys = Vec::with_capacity(keys.len());
        for key in keys {
            encoded_keys
                .push(encoding::encode(&key).context("unable to encode dictionary key in batch")?);
        }
        let encode_ms = total_start.elapsed().as_millis() as u64;
        let output = self.intern_many_encoded_unique(encoded_keys).await?;
        let total_ms = total_start.elapsed().as_millis() as u64;
        if total_ms >= 10 {
            tracing::info!(
                keys = output.len(),
                encode_ms,
                intern_ms = total_ms.saturating_sub(encode_ms),
                total_ms,
                "dictionary intern_many_values_unique_owned latency"
            );
        }
        Ok(output)
    }

    async fn intern_many_encoded(&self, encoded_keys: Vec<Vec<u8>>) -> Result<Vec<u64>> {
        if encoded_keys.is_empty() {
            return Ok(Vec::new());
        }

        let total_start = Instant::now();
        let mut resolved = AHashMap::<u64, Vec<(usize, u64)>>::with_capacity(encoded_keys.len());
        let mut pending = Vec::<(Vec<u8>, u64, u64, u16)>::with_capacity(encoded_keys.len());
        let mut next_slot_by_hash = AHashMap::<u64, u16>::new();
        let mut output = Vec::with_capacity(encoded_keys.len());
        let mut duplicate_reuse_hits = 0usize;
        let mut cache_hits = 0usize;
        let mut negative_cache_hits = 0usize;
        let mut lookup_existing_calls = 0usize;
        let mut lookup_existing_hits = 0usize;
        let mut lookup_existing_ms = 0u64;
        let mut reserve_calls = 0usize;
        let mut reserve_ms = 0u64;

        for (index, encoded) in encoded_keys.iter().enumerate() {
            let resolved_hash = self.hash(encoded);
            if let Some(entries) = resolved.get(&resolved_hash)
                && let Some((_, id)) = entries
                    .iter()
                    .find(|(resolved_index, _)| encoded_keys[*resolved_index].as_slice() == encoded)
            {
                duplicate_reuse_hits += 1;
                output.push(*id);
                continue;
            }

            let id = if let Some(existing) = self.lookup_existing_in_cache(encoded) {
                cache_hits += 1;
                existing
            } else {
                let should_allocate = {
                    let mut cache = self.cache.lock().unwrap();
                    cache.is_negative(encoded)
                };
                if should_allocate || self.fast_path_fresh {
                    if should_allocate {
                        negative_cache_hits += 1;
                    }
                    reserve_calls += 1;
                    let reserve_start = Instant::now();
                    let id = self
                        .reserve_in_batch(
                            resolved_hash,
                            encoded,
                            &mut pending,
                            &mut next_slot_by_hash,
                            None,
                        )
                        .await?;
                    reserve_ms += reserve_start.elapsed().as_millis() as u64;
                    id
                } else {
                    lookup_existing_calls += 1;
                    let lookup_start = Instant::now();
                    let existing = self.lookup_existing_or_first_free_slot(encoded).await?;
                    lookup_existing_ms += lookup_start.elapsed().as_millis() as u64;
                    match existing {
                        LookupExistingResult::Existing(existing) => {
                            lookup_existing_hits += 1;
                            let mut cache = self.cache.lock().unwrap();
                            cache.remember(encoded.clone(), existing);
                            existing
                        }
                        LookupExistingResult::Missing { first_free_slot } => {
                            {
                                let mut cache = self.cache.lock().unwrap();
                                cache.remember_negative(encoded);
                            }
                            reserve_calls += 1;
                            let reserve_start = Instant::now();
                            let id = self
                                .reserve_in_batch(
                                    resolved_hash,
                                    encoded,
                                    &mut pending,
                                    &mut next_slot_by_hash,
                                    Some(first_free_slot),
                                )
                                .await?;
                            reserve_ms += reserve_start.elapsed().as_millis() as u64;
                            id
                        }
                    }
                }
            };

            resolved.entry(resolved_hash).or_default().push((index, id));
            output.push(id);
        }

        let pending_writes = pending.len();
        let flush_start = Instant::now();
        self.flush_pending_batch(pending).await?;
        let flush_ms = flush_start.elapsed().as_millis() as u64;

        tracing::debug!(
            batch_keys = encoded_keys.len(),
            output_ids = output.len(),
            pending_writes,
            duplicate_reuse_hits,
            cache_hits,
            negative_cache_hits,
            lookup_existing_calls,
            lookup_existing_hits,
            lookup_existing_ms,
            reserve_calls,
            reserve_ms,
            flush_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "dictionary intern_many breakdown"
        );

        Ok(output)
    }

    async fn intern_many_encoded_unique(&self, encoded_keys: Vec<Vec<u8>>) -> Result<Vec<u64>> {
        self.intern_many_encoded_unique_owned(encoded_keys).await
    }

    async fn intern_many_encoded_unique_owned(
        &self,
        encoded_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<u64>> {
        if encoded_keys.is_empty() {
            return Ok(Vec::new());
        }

        let total_start = Instant::now();
        let mut pending = Vec::<(Vec<u8>, u64, u64, u16)>::with_capacity(encoded_keys.len());
        let mut next_slot_by_hash = AHashMap::<u64, u16>::new();
        let mut output = Vec::with_capacity(encoded_keys.len());
        let mut cache_hits = 0usize;
        let mut negative_cache_hits = 0usize;
        let mut lookup_existing_calls = 0usize;
        let mut lookup_existing_hits = 0usize;
        let mut lookup_existing_ms = 0u64;
        let mut reserve_calls = 0usize;
        let mut reserve_ms = 0u64;

        for encoded in encoded_keys {
            let id = if let Some(existing) = self.lookup_existing_in_cache(encoded.as_slice()) {
                cache_hits += 1;
                existing
            } else {
                let hash = self.hash(encoded.as_slice());
                let should_allocate = {
                    let mut cache = self.cache.lock().unwrap();
                    cache.is_negative(encoded.as_slice())
                };
                if should_allocate || self.fast_path_fresh {
                    if should_allocate {
                        negative_cache_hits += 1;
                    }
                    reserve_calls += 1;
                    let reserve_start = Instant::now();
                    let id = self
                        .reserve_in_batch_owned(
                            hash,
                            encoded,
                            &mut pending,
                            &mut next_slot_by_hash,
                            None,
                        )
                        .await?;
                    reserve_ms += reserve_start.elapsed().as_millis() as u64;
                    id
                } else {
                    lookup_existing_calls += 1;
                    let lookup_start = Instant::now();
                    let existing = self
                        .lookup_existing_or_first_free_slot(encoded.as_slice())
                        .await?;
                    lookup_existing_ms += lookup_start.elapsed().as_millis() as u64;
                    match existing {
                        LookupExistingResult::Existing(existing) => {
                            lookup_existing_hits += 1;
                            let mut cache = self.cache.lock().unwrap();
                            cache.remember(encoded, existing);
                            existing
                        }
                        LookupExistingResult::Missing { first_free_slot } => {
                            {
                                let mut cache = self.cache.lock().unwrap();
                                cache.remember_negative(encoded.as_slice());
                            }
                            reserve_calls += 1;
                            let reserve_start = Instant::now();
                            let id = self
                                .reserve_in_batch_owned(
                                    hash,
                                    encoded,
                                    &mut pending,
                                    &mut next_slot_by_hash,
                                    Some(first_free_slot),
                                )
                                .await?;
                            reserve_ms += reserve_start.elapsed().as_millis() as u64;
                            id
                        }
                    }
                }
            };
            output.push(id);
        }

        let pending_writes = pending.len();
        let flush_start = Instant::now();
        self.flush_pending_batch(pending).await?;
        let flush_ms = flush_start.elapsed().as_millis() as u64;

        tracing::debug!(
            batch_keys = output.len(),
            pending_writes,
            cache_hits,
            negative_cache_hits,
            lookup_existing_calls,
            lookup_existing_hits,
            lookup_existing_ms,
            reserve_calls,
            reserve_ms,
            flush_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "dictionary intern_many_unique breakdown"
        );

        Ok(output)
    }

    async fn flush_pending_batch(&self, pending: Vec<(Vec<u8>, u64, u64, u16)>) -> Result<()> {
        if !pending.is_empty() {
            let total_start = Instant::now();
            let mut batch = WriteBatch::new();
            let build_batch_start = Instant::now();
            let mut raw_values = 0usize;
            let mut compressed_values = 0usize;
            let mut input_bytes = 0usize;
            let mut stored_bytes = 0usize;
            for (encoded, id, hash, slot) in &pending {
                batch.put(self.k2id_key(*hash, *slot), encode_id(*id));
                let compressed = compress_value(encoded.as_slice())?;
                input_bytes += encoded.len();
                stored_bytes += compressed.len();
                if compressed.first().copied() == Some(0x00) {
                    raw_values += 1;
                } else {
                    compressed_values += 1;
                }
                batch.put(self.id2k_key(*id), compressed);
            }
            batch.put(
                self.meta_key.clone(),
                encode_id(self.next_id.load(Ordering::SeqCst)),
            );
            let build_batch_ms = build_batch_start.elapsed().as_millis() as u64;

            let write_start = Instant::now();
            self.table.write_batch(batch).await?;
            let write_batch_ms = write_start.elapsed().as_millis() as u64;

            let cache_update_start = Instant::now();
            let mut cache = self.cache.lock().unwrap();
            for (encoded, id, _, _) in pending {
                cache.clear_negative(&encoded);
                cache.remember(encoded, id);
            }
            let cache_update_ms = cache_update_start.elapsed().as_millis() as u64;

            tracing::debug!(
                pending_writes = raw_values + compressed_values,
                raw_values,
                compressed_values,
                input_bytes,
                stored_bytes,
                build_batch_ms,
                write_batch_ms,
                cache_update_ms,
                total_ms = total_start.elapsed().as_millis() as u64,
                "dictionary flush_pending_batch breakdown"
            );
        }

        Ok(())
    }

    async fn reserve_in_batch(
        &self,
        hash: u64,
        encoded_key: &[u8],
        pending: &mut Vec<(Vec<u8>, u64, u64, u16)>,
        next_slot_by_hash: &mut AHashMap<u64, u16>,
        known_first_free_slot: Option<u16>,
    ) -> Result<u64> {
        if self.fast_path_fresh {
            let slot = self.reserve_fresh_slot(hash)?;
            let id = self.reserve_id();
            pending.push((encoded_key.to_vec(), id, hash, slot));
            return Ok(id);
        }

        let next_slot = match next_slot_by_hash.entry(hash) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let loaded = match known_first_free_slot {
                    Some(slot) => slot,
                    None => self.first_free_slot(hash).await?,
                };
                entry.insert(loaded)
            }
        };

        let slot = *next_slot;
        let id = self.reserve_id();
        *next_slot = Self::next_probe_slot(slot)
            .ok_or_else(|| anyhow!("dictionary full: all probe slots occupied for hash"))?;
        pending.push((encoded_key.to_vec(), id, hash, slot));
        Ok(id)
    }

    async fn reserve_in_batch_owned(
        &self,
        hash: u64,
        encoded_key: Vec<u8>,
        pending: &mut Vec<(Vec<u8>, u64, u64, u16)>,
        next_slot_by_hash: &mut AHashMap<u64, u16>,
        known_first_free_slot: Option<u16>,
    ) -> Result<u64> {
        if self.fast_path_fresh {
            let slot = self.reserve_fresh_slot(hash)?;
            let id = self.reserve_id();
            pending.push((encoded_key, id, hash, slot));
            return Ok(id);
        }

        let next_slot = match next_slot_by_hash.entry(hash) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let loaded = match known_first_free_slot {
                    Some(slot) => slot,
                    None => self.first_free_slot(hash).await?,
                };
                entry.insert(loaded)
            }
        };

        let slot = *next_slot;
        let id = self.reserve_id();
        *next_slot = Self::next_probe_slot(slot)
            .ok_or_else(|| anyhow!("dictionary full: all probe slots occupied for hash"))?;
        pending.push((encoded_key, id, hash, slot));
        Ok(id)
    }
}
