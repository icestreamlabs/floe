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
    pub fn new_batch(
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

    #[cfg(test)]
    pub(crate) fn enable_append_only_input(&mut self) {
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
}
