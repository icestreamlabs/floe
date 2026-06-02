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
    fn apply_slot_update(
        &self,
        state: &mut GroupedIncrementalAggregateState,
        slot_idx: usize,
        slot: &IncrementalAggregateSlotUpdate,
        weight: i64,
    ) -> Result<()> {
        match (&self.slot_kinds[slot_idx], &mut state.slots[slot_idx], slot) {
            (
                IncrementalAggregateSlotKind::Count,
                IncrementalAggregateSlotState::Count { count },
                IncrementalAggregateSlotUpdate::Count(value),
            ) => {
                *count += value * weight;
            }
            (
                IncrementalAggregateSlotKind::CountDistinct,
                IncrementalAggregateSlotState::CountDistinct { .. },
                IncrementalAggregateSlotUpdate::Value(_),
            ) => {}
            (
                IncrementalAggregateSlotKind::Sum(_),
                IncrementalAggregateSlotState::Sum {
                    sum,
                    non_null_count,
                },
                IncrementalAggregateSlotUpdate::Value(Some(value)),
            ) => {
                if let Some(number) = value.as_sum_numeric() {
                    *sum = checked_add_i64_sum(*sum, checked_weighted_sum_delta(number, weight)?)?;
                    *non_null_count += weight;
                }
            }
            (
                IncrementalAggregateSlotKind::Sum(_),
                IncrementalAggregateSlotState::DecimalSum {
                    sum,
                    non_null_count,
                },
                IncrementalAggregateSlotUpdate::Value(Some(value)),
            ) => {
                if let Some(number) = value.as_sum_numeric() {
                    *sum = checked_add_sum(*sum, checked_weighted_sum_delta(number, weight)?)?;
                    *non_null_count += weight;
                }
            }
            (
                IncrementalAggregateSlotKind::Avg,
                IncrementalAggregateSlotState::Avg { sum, count },
                IncrementalAggregateSlotUpdate::Value(Some(value)),
            ) => {
                if let Some(number) = value.as_i64_numeric() {
                    *sum += number * weight;
                    *count += weight;
                }
            }
            (
                IncrementalAggregateSlotKind::Min(_),
                IncrementalAggregateSlotState::Min { current },
                IncrementalAggregateSlotUpdate::Value(Some(value)),
            ) if weight > 0 => {
                let next = match current.take() {
                    Some(existing) => match value.cmp_non_null(&existing) {
                        Some(std::cmp::Ordering::Less) => value.clone(),
                        Some(_) | None => existing,
                    },
                    None => value.clone(),
                };
                *current = Some(next);
            }
            (
                IncrementalAggregateSlotKind::Max(_),
                IncrementalAggregateSlotState::Max { current },
                IncrementalAggregateSlotUpdate::Value(Some(value)),
            ) if weight > 0 => {
                let next = match current.take() {
                    Some(existing) => match value.cmp_non_null(&existing) {
                        Some(std::cmp::Ordering::Greater) => value.clone(),
                        Some(_) | None => existing,
                    },
                    None => value.clone(),
                };
                *current = Some(next);
            }
            (
                IncrementalAggregateSlotKind::Sum(_)
                | IncrementalAggregateSlotKind::Avg
                | IncrementalAggregateSlotKind::Min(_)
                | IncrementalAggregateSlotKind::Max(_),
                _,
                IncrementalAggregateSlotUpdate::Value(None),
            ) => {}
            (expected_kind, actual_state, actual_input) => {
                tracing::warn!(
                    ?expected_kind,
                    ?actual_state,
                    ?actual_input,
                    slot_idx,
                    "incremental aggregate row evaluator returned mismatched slot kind"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn recompute_group_state(
        &self,
        key: &K,
        logical_work: Option<&mut metrics::LogicalWorkSnapshot>,
    ) -> Result<Option<GroupedIncrementalAggregateState>> {
        let input_index = self
            .input_index
            .as_ref()
            .context("incremental aggregate input index missing during recompute")?;
        let (values, lookup_metrics) = input_index
            .values_for_key_with_metrics(key)
            .await
            .context("load incremental aggregate input values for recompute")?;
        if let Some(work) = logical_work {
            work.add_lookup_metrics(lookup_metrics);
            work.extrema_rebuild_rows = work
                .extrema_rebuild_rows
                .saturating_add(values.len() as u64);
        }

        if values.is_empty() {
            return Ok(None);
        }

        let mut state = GroupedIncrementalAggregateState::zero(&self.slot_kinds);
        let mut distinct_weights: Vec<HashMap<AggregateValue, i64>> =
            self.slot_kinds.iter().map(|_| HashMap::new()).collect();

        let row_updates = (self.row_evaluator)(&values);
        for (_value, row_update, weight) in row_updates {
            if weight == 0 {
                continue;
            }
            state.total_rows += weight;
            for (slot_idx, slot) in row_update.slots.iter().enumerate() {
                match (&self.slot_kinds[slot_idx], slot) {
                    (
                        IncrementalAggregateSlotKind::CountDistinct,
                        IncrementalAggregateSlotUpdate::Value(Some(value)),
                    ) => {
                        let entry = distinct_weights[slot_idx].entry(value.clone()).or_insert(0);
                        *entry += weight;
                        if *entry == 0 {
                            distinct_weights[slot_idx].remove(value);
                        }
                    }
                    _ => self.apply_slot_update(&mut state, slot_idx, slot, weight)?,
                }
            }
        }

        for (slot_idx, slot_kind) in self.slot_kinds.iter().enumerate() {
            if !matches!(slot_kind, IncrementalAggregateSlotKind::CountDistinct) {
                continue;
            }
            let count = distinct_weights[slot_idx]
                .values()
                .filter(|weight| **weight > 0)
                .count() as i64;
            state.slots[slot_idx] = IncrementalAggregateSlotState::CountDistinct { count };
        }

        if state.is_present() {
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn evict_keys_where<F>(
        &mut self,
        predicate: F,
    ) -> Result<HashMap<(K, Vec<AggregateValue>), i64>>
    where
        F: Fn(&K) -> bool,
    {
        self.ensure_state_cache()
            .await
            .context("load incremental aggregate cache for eviction")?;

        let keys_to_evict = self
            .state_cache
            .as_ref()
            .context("incremental aggregate cache missing during eviction")?
            .keys()
            .filter(|key| predicate(key))
            .cloned()
            .collect::<Vec<_>>();
        if keys_to_evict.is_empty() {
            return Ok(HashMap::new());
        }

        if let Some(distinct_index) = self.distinct_index.as_ref() {
            let distinct_slots = self
                .slot_kinds
                .iter()
                .enumerate()
                .filter_map(|(slot_idx, kind)| {
                    matches!(kind, IncrementalAggregateSlotKind::CountDistinct)
                        .then_some(slot_idx as u32)
                })
                .collect::<Vec<_>>();
            let mut distinct_updates = Vec::new();
            for key in &keys_to_evict {
                for slot in &distinct_slots {
                    let distinct_key = DistinctGroupKey {
                        group_key: key.clone(),
                        slot: *slot,
                    };
                    let values = distinct_index
                        .values_for_key(&distinct_key)
                        .await
                        .context("load incremental aggregate distinct values for eviction")?;
                    for (value, weight) in values {
                        if weight != 0 {
                            distinct_updates.push((distinct_key.clone(), value, -weight));
                        }
                    }
                }
            }

            if !distinct_updates.is_empty() {
                distinct_index
                    .apply_deltas(distinct_updates)
                    .await
                    .context("evict incremental aggregate distinct index entries")?;
            }
        }

        if let Some(input_index) = self.input_index.as_ref() {
            let mut input_updates = Vec::new();
            for key in &keys_to_evict {
                let values = input_index
                    .values_for_key(key)
                    .await
                    .context("load incremental aggregate input values for eviction")?;
                for (value, weight) in values {
                    if weight != 0 {
                        input_updates.push((key.clone(), value, -weight));
                    }
                }
            }

            if !input_updates.is_empty() {
                input_index
                    .apply_deltas(input_updates)
                    .await
                    .context("evict incremental aggregate input index entries")?;
            }
        }

        let mut state_deltas: HashMap<(K, GroupedIncrementalAggregateState), i64> = HashMap::new();
        let mut output_deltas: HashMap<(K, Vec<AggregateValue>), i64> = HashMap::new();
        {
            let state_cache = self
                .state_cache
                .as_ref()
                .context("incremental aggregate cache missing during eviction")?;
            for key in &keys_to_evict {
                let Some(old_state) = state_cache.get(key).cloned() else {
                    continue;
                };
                state_deltas.insert((key.clone(), old_state.clone()), -1);
                output_deltas.insert(
                    (key.clone(), old_state.output_values(&self.slot_kinds)?),
                    -1,
                );
            }
        }

        if state_deltas.is_empty() {
            return Ok(HashMap::new());
        }

        let base_version = self.state.base_version_for_update();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &state_deltas,
            base_version,
            "integrated",
        )
        .await
        .context("evict incremental aggregate integrated state")?;
        self.state.update_handle(new_integrated_handle);

        if let Some(state_cache) = self.state_cache.as_mut() {
            for key in keys_to_evict {
                state_cache.remove(&key);
            }
        }

        Ok(output_deltas)
    }

    pub(crate) async fn persist_output_deltas(
        &mut self,
        output_deltas: &HashMap<(K, Vec<AggregateValue>), i64>,
    ) -> Result<ZSetHandle> {
        Self::apply_deltas_to_versioned(&mut self.output, output_deltas, None, "output")
            .await
            .context("persist incremental aggregate output delta")
    }

    pub(crate) fn empty_output_handle(&self) -> ZSetHandle {
        self.output.handle_for_version(0)
    }

    pub(crate) async fn state_entry_count(&mut self) -> Result<usize> {
        self.ensure_state_cache()
            .await
            .context("load incremental aggregate cache for state size")?;
        Ok(self.state_cache.as_ref().map_or(0, HashMap::len))
    }
}
