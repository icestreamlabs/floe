use super::*;

impl<K> Dictionary<K>
where
    K: Archive + Clone + Send + Sync + 'static + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'rk> CheckBytes<RkyvValidator<'rk>>,
{
    pub async fn resolve_many_ids(&self, ids: &[u64]) -> Result<Vec<K>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let total_start = Instant::now();
        let cache_scan_start = Instant::now();
        let mut encoded_by_id: AHashMap<u64, SharedKey> = AHashMap::with_capacity(ids.len());
        let mut missing_ids = Vec::new();
        let mut seen_missing = AHashSet::with_capacity(ids.len());
        let mut cache_hit_refs = 0usize;

        {
            let mut cache = self.cache_guard();
            for id in ids {
                if *id == 0 {
                    return Err(anyhow!("id 0 is not valid"));
                }
                if let Some(key) = cache.lookup_key(id) {
                    cache_hit_refs += 1;
                    encoded_by_id.entry(*id).or_insert(key);
                } else if seen_missing.insert(*id) {
                    missing_ids.push(*id);
                }
            }
        }
        let cache_scan_ms = cache_scan_start.elapsed().as_millis() as u64;

        let fetch_start = Instant::now();
        let mut range_scan_spans = 0usize;
        let mut range_scan_ids = 0usize;
        let mut point_fetch_chunks = 0usize;
        if !missing_ids.is_empty() {
            let mut sorted_missing_ids = missing_ids.clone();
            sorted_missing_ids.sort_unstable();
            let mut point_fetch_ids = Vec::with_capacity(sorted_missing_ids.len());

            let mut span_start = 0usize;
            while span_start < sorted_missing_ids.len() {
                let mut span_end = span_start + 1;
                while span_end < sorted_missing_ids.len()
                    && sorted_missing_ids[span_end] == sorted_missing_ids[span_end - 1] + 1
                {
                    span_end += 1;
                }

                let span_ids = &sorted_missing_ids[span_start..span_end];
                if span_ids.len() >= RESOLVE_MANY_RANGE_SCAN_MIN_IDS {
                    range_scan_spans += 1;
                    range_scan_ids += span_ids.len();
                    let start_key = self.id2k_key(span_ids[0]);
                    let end_key = self.id2k_range_end_exclusive(*span_ids.last().unwrap());
                    let scanned = self
                        .table
                        .scan_range_bytes(start_key..end_key, &ScanOptions::default())
                        .await?;
                    for (key, bytes) in scanned {
                        let id = self.decode_id2k_key_id(key.as_ref())?;
                        let decoded = decompress_value(bytes.as_ref())?;
                        let shared = {
                            let mut cache = self.cache_guard();
                            cache.remember(decoded, id)
                        };
                        encoded_by_id.insert(id, shared);
                    }
                } else {
                    point_fetch_ids.extend_from_slice(span_ids);
                }

                span_start = span_end;
            }

            for chunk in point_fetch_ids.chunks(RESOLVE_MANY_FETCH_CHUNK) {
                point_fetch_chunks += 1;
                let mut id2k_keys = Vec::with_capacity(chunk.len());
                for &id in chunk {
                    let mut key = Vec::with_capacity(self.id2k_prefix.len() + 8);
                    self.encode_id2k_key_into(&mut key, id);
                    id2k_keys.push((id, key));
                }
                let fetched = try_join_all(id2k_keys.into_iter().map(|(id, key)| async move {
                    let bytes = self.table.get_bytes(&key).await?;
                    Ok::<_, anyhow::Error>((id, bytes))
                }))
                .await?;

                for (id, bytes) in fetched {
                    let bytes = bytes.ok_or_else(|| anyhow!("no key found for id {id}"))?;
                    let decoded = decompress_value(bytes.as_ref())?;
                    let shared = {
                        let mut cache = self.cache_guard();
                        cache.remember(decoded, id)
                    };
                    encoded_by_id.insert(id, shared);
                }
            }
        }
        let fetch_ms = fetch_start.elapsed().as_millis() as u64;

        let decode_start = Instant::now();
        let mut decoded_by_id = AHashMap::with_capacity(encoded_by_id.len());
        for id in ids {
            if decoded_by_id.contains_key(id) {
                continue;
            }
            let encoded = encoded_by_id
                .get(id)
                .ok_or_else(|| anyhow!("no key found for id {id}"))?;
            let decoded = encoding::decode(encoded.as_ref())
                .context("unable to decode dictionary value in batch")?;
            decoded_by_id.insert(*id, decoded);
        }
        let decode_ms = decode_start.elapsed().as_millis() as u64;

        let output_start = Instant::now();
        let mut resolved = Vec::with_capacity(ids.len());
        for id in ids {
            let value = decoded_by_id
                .get(id)
                .cloned()
                .ok_or_else(|| anyhow!("no key found for id {id}"))?;
            resolved.push(value);
        }
        let output_ms = output_start.elapsed().as_millis() as u64;

        tracing::debug!(
            ids = ids.len(),
            unique_ids = decoded_by_id.len(),
            cache_hit_refs,
            cache_miss_unique = missing_ids.len(),
            cache_scan_ms,
            range_scan_spans,
            range_scan_ids,
            point_fetch_chunks,
            fetch_ms,
            decode_ms,
            output_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "dictionary resolve_many breakdown"
        );

        Ok(resolved)
    }
}
