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
    pub(crate) fn new_batch(
        state: RelationState<(K, GroupedIncrementalAggregateState)>,
        table: Arc<dyn KeyValueTable>,
        row_evaluator: BatchRowEvaluator<V, K>,
        output: VersionedZSet<(K, Vec<AggregateValue>)>,
        slot_kinds: Vec<IncrementalAggregateSlotKind>,
        indexes: IncrementalAggregateIndexes<K, V>,
    ) -> Self {
        Self {
            state,
            table,
            row_evaluator,
            output,
            dict_cache: HashMap::new(),
            state_cache: None,
            slot_kinds,
            distinct_index: indexes.distinct,
            input_index: indexes.input,
            extrema_index: indexes.extrema,
            append_only_input: false,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    pub fn enable_live_output_replayable(&mut self) {
        self.output.enable_replayable_persistence();
    }

    pub fn enable_append_only_input(&mut self) {
        self.append_only_input = true;
    }

    pub(super) fn has_extrema(&self) -> bool {
        self.slot_kinds.iter().any(|kind| {
            matches!(
                kind,
                IncrementalAggregateSlotKind::Min(_) | IncrementalAggregateSlotKind::Max(_)
            )
        })
    }

    pub(super) async fn ensure_state_cache(&mut self) -> Result<usize> {
        if self.state_cache.is_some() {
            return Ok(0);
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize incremental aggregate integrated state")?;
        let mut cache = HashMap::new();
        let rebuild_rows = materialized.len();
        for ((key, aggregate), weight) in materialized {
            if weight != 0 {
                cache.insert(key, aggregate);
            }
        }
        self.state_cache = Some(cache);
        Ok(rebuild_rows)
    }

    pub(crate) async fn snapshot_grouped_state(
        &mut self,
    ) -> Result<Vec<(K, GroupedIncrementalAggregateState)>> {
        self.ensure_state_cache().await?;
        Ok(self
            .state_cache
            .as_ref()
            .map(|cache| {
                cache
                    .iter()
                    .filter(|(_, state)| state.is_present())
                    .map(|(key, state)| (key.clone(), state.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }

    pub(crate) fn restore_grouped_state(
        &mut self,
        grouped: Vec<(K, GroupedIncrementalAggregateState)>,
    ) {
        self.state_cache = Some(
            grouped
                .into_iter()
                .filter(|(_, state)| state.is_present())
                .collect(),
        );
    }

    pub(crate) fn snapshot_distinct_index(
        &self,
    ) -> Result<Vec<(DistinctGroupKey<K>, AggregateValue, i64)>> {
        match self.distinct_index.as_ref() {
            Some(index) => index.replayable_snapshot_entries(),
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn restore_distinct_index(
        &mut self,
        entries: Vec<(DistinctGroupKey<K>, AggregateValue, i64)>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let distinct_index = self
            .distinct_index
            .as_ref()
            .context("incremental aggregate distinct index missing during restore")?;
        distinct_index
            .apply_deltas(entries)
            .await
            .context("restore incremental aggregate distinct index")
    }

    pub(crate) fn snapshot_input_index(&self) -> Result<Vec<(K, V, i64)>> {
        match self.input_index.as_ref() {
            Some(index) => index.replayable_snapshot_entries(),
            None => Ok(Vec::new()),
        }
    }

    pub(crate) async fn restore_input_index(&mut self, entries: Vec<(K, V, i64)>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let input_index = self
            .input_index
            .as_ref()
            .context("incremental aggregate input index missing during restore")?;
        input_index
            .apply_deltas(entries)
            .await
            .context("restore incremental aggregate input index")
    }
}
