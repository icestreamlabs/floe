use super::*;

impl<K, V> IncrementalAggregateOp<K, V>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(super) fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64> {
        let mut merged = HashMap::new();
        for (row, weight) in deltas {
            let entry = merged.entry(row.clone()).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                merged.remove(&row);
            }
        }
        merged
    }

    pub(super) async fn apply_deltas_to_versioned<T>(
        versioned: &mut VersionedZSet<T>,
        deltas: &HashMap<T, i64>,
        base: Option<u64>,
        state_label: &'static str,
    ) -> Result<ZSetHandle>
    where
        T: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        let mut keyed_deltas: Vec<(&T, i64)> = Vec::new();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            keyed_deltas.push((key, *delta));
        }
        if keyed_deltas.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        if versioned.uses_replayable_persistence() {
            anyhow::ensure!(
                base.is_none(),
                "replayable versioned ZSet does not support persisted base chaining"
            );
            let batch = Arc::new(
                keyed_deltas
                    .iter()
                    .map(|(key, delta)| ((*key).clone(), *delta))
                    .collect(),
            );
            return Ok(versioned.publish_replayable_batch(batch));
        }

        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let intern_start = Instant::now();
        let ids = dict
            .intern_many_values_unique(keyed_deltas.iter().map(|(key, _)| *key))
            .await
            .context("batch intern keys while staging incremental aggregate delta")?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            state_label,
            "intern_keys",
            intern_start.elapsed().as_millis() as u64,
        );

        let bucketize_start = Instant::now();
        for ((_, delta), id) in keyed_deltas.iter().zip(ids.into_iter()) {
            buckets
                .entry(bucket_for(id))
                .or_default()
                .push((id, *delta));
        }
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            state_label,
            "bucketize_deltas",
            bucketize_start.elapsed().as_millis() as u64,
        );

        let build_segments_start = Instant::now();
        let mut segments = Vec::new();
        for (bucket, mut bucket_deltas) in buckets {
            bucket_deltas.retain(|(_, delta)| *delta != 0);
            if bucket_deltas.is_empty() {
                continue;
            }
            bucket_deltas.sort_by_key(|(id, _)| *id);
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas: bucket_deltas,
            });
        }
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            state_label,
            "build_segments",
            build_segments_start.elapsed().as_millis() as u64,
        );

        let persist_start = Instant::now();
        let mut batch = WriteBatch::new();
        let enqueue_start = Instant::now();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule incremental aggregate version update")?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            state_label,
            "enqueue_version",
            enqueue_start.elapsed().as_millis() as u64,
        );

        let write_start = Instant::now();
        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write incremental aggregate version update")?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            state_label,
            "write_batch",
            write_start.elapsed().as_millis() as u64,
        );

        let apply_plan_start = Instant::now();
        versioned.apply_version_plan(&plan);
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            state_label,
            "apply_version_plan",
            apply_plan_start.elapsed().as_millis() as u64,
        );
        metrics::observe_operator_persistence_latency_ms(
            "incremental_aggregate",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }
}

pub(super) fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
