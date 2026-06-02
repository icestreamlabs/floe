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
    pub async fn apply_delta_values(
        &mut self,
        delta_values: &[(V, i64)],
    ) -> Result<HashMap<(K, Vec<AggregateValue>), i64>> {
        self.apply_delta_values_with_work(delta_values, None).await
    }

    pub(super) async fn apply_delta_values_with_work(
        &mut self,
        delta_values: &[(V, i64)],
        mut logical_work: Option<&mut metrics::LogicalWorkSnapshot>,
    ) -> Result<HashMap<(K, Vec<AggregateValue>), i64>> {
        let total_start = Instant::now();
        if delta_values.is_empty() {
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "apply_delta_values_total",
                total_start.elapsed().as_millis() as u64,
            );
            return Ok(HashMap::new());
        }

        let all_nonnegative = delta_values.iter().all(|(_, weight)| *weight >= 0);
        let coalesced = if self.append_only_input || all_nonnegative {
            if self.append_only_input && !all_nonnegative {
                anyhow::bail!("append-only incremental aggregate received negative input weight");
            }
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "coalesce_input",
                0,
            );
            None
        } else {
            let coalesce_start = Instant::now();
            let coalesced = self.coalesce_deltas(delta_values.to_vec());
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "coalesce_input",
                coalesce_start.elapsed().as_millis() as u64,
            );
            if coalesced.is_empty() {
                metrics::observe_operator_phase_latency_ms(
                    "incremental_aggregate",
                    "step",
                    "apply_delta_values_total",
                    total_start.elapsed().as_millis() as u64,
                );
                return Ok(HashMap::new());
            }
            Some(coalesced)
        };

        #[derive(Clone, Debug)]
        enum AggregatedSlotDelta {
            Count {
                delta: i64,
            },
            CountDistinct,
            Sum {
                sum_delta: i128,
                non_null_delta: i64,
            },
            Avg {
                sum_delta: i64,
                count_delta: i64,
            },
            Min {
                candidate: Option<AggregateValue>,
            },
            Max {
                candidate: Option<AggregateValue>,
            },
        }

        #[derive(Clone, Debug)]
        struct AggregatedKeyUpdates {
            total_rows_delta: i64,
            slot_deltas: Vec<AggregatedSlotDelta>,
        }

        let mut affected_keys = HashSet::new();
        let mut recompute_keys = HashSet::new();
        let mut distinct_deltas: HashMap<(DistinctGroupKey<K>, AggregateValue), i64> =
            HashMap::new();
        let mut index_updates = Vec::new();
        let mut extrema_index_updates = Vec::new();
        let mut extrema_refresh_keys = HashSet::new();
        let slot_kinds = &self.slot_kinds;
        let mut aggregated_updates_by_key: HashMap<K, AggregatedKeyUpdates> = HashMap::new();

        let has_extrema = self.has_extrema();
        let use_ordered_extrema = self.extrema_index.is_some();
        let mut apply_value = |value: V,
                               row_update: IncrementalAggregateRow<K>,
                               weight: i64|
         -> Result<()> {
            if weight == 0 {
                return Ok(());
            }
            if row_update.slots.len() != self.slot_kinds.len() {
                tracing::warn!(
                    expected = self.slot_kinds.len(),
                    actual = row_update.slots.len(),
                    "incremental aggregate row evaluator returned unexpected slot vector width"
                );
                return Ok(());
            }
            let key = row_update.key;
            let slots = row_update.slots;
            if has_extrema && weight < 0 && !use_ordered_extrema {
                recompute_keys.insert(key.clone());
                aggregated_updates_by_key.remove(&key);
            }
            if use_ordered_extrema {
                if self.input_index.is_some() {
                    index_updates.push((key.clone(), value.clone(), weight));
                }
                for (slot_idx, slot) in slots.iter().enumerate() {
                    let descending = match self.slot_kinds[slot_idx] {
                        IncrementalAggregateSlotKind::Min(_) => false,
                        IncrementalAggregateSlotKind::Max(_) => true,
                        _ => continue,
                    };
                    let IncrementalAggregateSlotUpdate::Value(Some(aggregate_value)) = slot else {
                        continue;
                    };
                    if let Some(index_key) =
                        self.extrema_index_key(&key, slot_idx, aggregate_value, descending)?
                    {
                        extrema_index_updates.push((index_key, value.clone(), weight));
                        extrema_refresh_keys.insert(key.clone());
                    }
                }
            } else if self.input_index.is_some() {
                index_updates.push((key.clone(), value, weight));
            }
            for (slot_idx, slot) in slots.iter().enumerate() {
                if matches!(
                    self.slot_kinds[slot_idx],
                    IncrementalAggregateSlotKind::CountDistinct
                ) && let IncrementalAggregateSlotUpdate::Value(Some(distinct_value)) = slot
                {
                    let distinct_key = DistinctGroupKey {
                        group_key: key.clone(),
                        slot: slot_idx as u32,
                    };
                    let entry = distinct_deltas
                        .entry((distinct_key, distinct_value.clone()))
                        .or_insert(0);
                    *entry += weight;
                }
            }
            affected_keys.insert(key.clone());
            if recompute_keys.contains(&key) {
                return Ok(());
            }

            let updates =
                aggregated_updates_by_key
                    .entry(key)
                    .or_insert_with(|| AggregatedKeyUpdates {
                        total_rows_delta: 0,
                        slot_deltas: slot_kinds
                            .iter()
                            .map(|kind| match kind {
                                IncrementalAggregateSlotKind::Count => {
                                    AggregatedSlotDelta::Count { delta: 0 }
                                }
                                IncrementalAggregateSlotKind::CountDistinct => {
                                    AggregatedSlotDelta::CountDistinct
                                }
                                IncrementalAggregateSlotKind::Sum(_) => AggregatedSlotDelta::Sum {
                                    sum_delta: 0,
                                    non_null_delta: 0,
                                },
                                IncrementalAggregateSlotKind::Avg => AggregatedSlotDelta::Avg {
                                    sum_delta: 0,
                                    count_delta: 0,
                                },
                                IncrementalAggregateSlotKind::Min(_) => {
                                    AggregatedSlotDelta::Min { candidate: None }
                                }
                                IncrementalAggregateSlotKind::Max(_) => {
                                    AggregatedSlotDelta::Max { candidate: None }
                                }
                            })
                            .collect(),
                    });
            updates.total_rows_delta += weight;
            for (slot_idx, slot) in slots.iter().enumerate() {
                match (&mut updates.slot_deltas[slot_idx], slot) {
                    (
                        AggregatedSlotDelta::Count { delta },
                        IncrementalAggregateSlotUpdate::Count(value),
                    ) => {
                        *delta += value * weight;
                    }
                    (
                        AggregatedSlotDelta::Sum {
                            sum_delta,
                            non_null_delta,
                        },
                        IncrementalAggregateSlotUpdate::Value(Some(value)),
                    ) => {
                        if let Some(number) = value.as_sum_numeric() {
                            *sum_delta = checked_add_sum(
                                *sum_delta,
                                checked_weighted_sum_delta(number, weight)?,
                            )?;
                            *non_null_delta += weight;
                        }
                    }
                    (
                        AggregatedSlotDelta::Avg {
                            sum_delta,
                            count_delta,
                        },
                        IncrementalAggregateSlotUpdate::Value(Some(value)),
                    ) => {
                        if let Some(number) = value.as_i64_numeric() {
                            *sum_delta += number * weight;
                            *count_delta += weight;
                        }
                    }
                    (
                        AggregatedSlotDelta::Min { candidate },
                        IncrementalAggregateSlotUpdate::Value(Some(value)),
                    ) if weight > 0 => match candidate.take() {
                        Some(existing) => {
                            *candidate = Some(match value.cmp_non_null(&existing) {
                                Some(std::cmp::Ordering::Less) => value.clone(),
                                Some(_) | None => existing,
                            });
                        }
                        None => {
                            *candidate = Some(value.clone());
                        }
                    },
                    (
                        AggregatedSlotDelta::Max { candidate },
                        IncrementalAggregateSlotUpdate::Value(Some(value)),
                    ) if weight > 0 => match candidate.take() {
                        Some(existing) => {
                            *candidate = Some(match value.cmp_non_null(&existing) {
                                Some(std::cmp::Ordering::Greater) => value.clone(),
                                Some(_) | None => existing,
                            });
                        }
                        None => {
                            *candidate = Some(value.clone());
                        }
                    },
                    (
                        AggregatedSlotDelta::CountDistinct,
                        IncrementalAggregateSlotUpdate::Value(_),
                    )
                    | (_, IncrementalAggregateSlotUpdate::Value(None)) => {}
                    (aggregated, slot) => {
                        tracing::warn!(
                            slot_idx,
                            ?aggregated,
                            ?slot,
                            "incremental aggregate slot update shape mismatch during aggregation"
                        );
                    }
                }
            }
            Ok(())
        };

        let aggregate_updates_start = Instant::now();
        let row_updates = if let Some(coalesced) = coalesced {
            (self.row_evaluator)(
                &coalesced
                    .into_iter()
                    .filter(|(_, weight)| *weight != 0)
                    .collect::<Vec<_>>(),
            )
        } else {
            (self.row_evaluator)(delta_values)
        };
        for (value, row_update, weight) in row_updates {
            apply_value(value, row_update, weight)?;
        }
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "aggregate_updates",
            aggregate_updates_start.elapsed().as_millis() as u64,
        );

        if affected_keys.is_empty() {
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "apply_delta_values_total",
                total_start.elapsed().as_millis() as u64,
            );
            return Ok(HashMap::new());
        }
        if let Some(work) = logical_work.as_deref_mut() {
            work.changed_groups = affected_keys.len() as u64;
            work.distinct_aux_rows_examined = distinct_deltas.len() as u64;
        }

        if let Some(input_index) = self.input_index.as_ref()
            && !index_updates.is_empty()
        {
            let input_index_start = Instant::now();
            if let Some(work) = logical_work.as_deref_mut() {
                work.record_persisted_rows(index_updates.len());
            }
            input_index
                .apply_deltas(index_updates)
                .await
                .context("update incremental aggregate input index")?;
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "update_input_index",
                input_index_start.elapsed().as_millis() as u64,
            );
        }
        if let Some(extrema_index) = self.extrema_index.as_ref()
            && !extrema_index_updates.is_empty()
        {
            let extrema_index_start = Instant::now();
            if let Some(work) = logical_work.as_deref_mut() {
                work.record_persisted_rows(extrema_index_updates.len());
            }
            extrema_index
                .apply_deltas_with_range_only(extrema_index_updates)
                .await
                .context("update incremental aggregate extrema index")?;
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "update_extrema_index",
                extrema_index_start.elapsed().as_millis() as u64,
            );
        }

        let mut distinct_count_adjustments: HashMap<K, Vec<i64>> = HashMap::new();
        if !distinct_deltas.is_empty() {
            let distinct_index_start = Instant::now();
            let distinct_index = self
                .distinct_index
                .as_ref()
                .context("incremental aggregate distinct index missing")?;
            let mut distinct_updates = Vec::with_capacity(distinct_deltas.len());
            for ((distinct_key, distinct_value), delta) in distinct_deltas {
                if delta == 0 {
                    continue;
                }
                if self.append_only_input && delta < 0 {
                    anyhow::bail!(
                        "append-only incremental aggregate received negative distinct delta"
                    );
                }
                let old_weight = distinct_index
                    .value_weight_for_key_value(&distinct_key, &distinct_value)
                    .await
                    .context("load incremental aggregate distinct multiplicity")?;
                if let Some(work) = logical_work.as_deref_mut() {
                    work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
                    work.state_lookup_rows = work
                        .state_lookup_rows
                        .saturating_add((old_weight != 0) as u64);
                }
                let index_delta = if self.append_only_input {
                    if old_weight > 0 { 0 } else { 1 }
                } else {
                    delta
                };
                let new_weight = old_weight + index_delta;
                let adjustments = distinct_count_adjustments
                    .entry(distinct_key.group_key.clone())
                    .or_insert_with(|| vec![0; self.slot_kinds.len()]);
                if old_weight > 0 && new_weight <= 0 {
                    adjustments[distinct_key.slot as usize] -= 1;
                } else if old_weight <= 0 && new_weight > 0 {
                    adjustments[distinct_key.slot as usize] += 1;
                }
                if index_delta != 0 {
                    distinct_updates.push((distinct_key, distinct_value, index_delta));
                }
            }
            if !distinct_updates.is_empty() {
                if let Some(work) = logical_work.as_deref_mut() {
                    work.record_persisted_rows(distinct_updates.len());
                }
                distinct_index
                    .apply_deltas(distinct_updates)
                    .await
                    .context("update incremental aggregate distinct index")?;
            }
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "update_distinct_index",
                distinct_index_start.elapsed().as_millis() as u64,
            );
        }

        let ensure_cache_start = Instant::now();
        let cache_rebuild_rows = self
            .ensure_state_cache()
            .await
            .context("load incremental aggregate cache")?;
        if cache_rebuild_rows != 0
            && let Some(work) = logical_work.as_deref_mut()
        {
            work.cache_rebuild_rows = cache_rebuild_rows as u64;
            work.state_full_scan_count = 1;
            work.state_scan_rows = work
                .state_scan_rows
                .saturating_add(cache_rebuild_rows as u64);
        }
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "ensure_state_cache",
            ensure_cache_start.elapsed().as_millis() as u64,
        );

        let zero_state = GroupedIncrementalAggregateState::zero(&self.slot_kinds);
        let mut state_deltas: HashMap<(K, GroupedIncrementalAggregateState), i64> = HashMap::new();
        let mut output_deltas: HashMap<(K, Vec<AggregateValue>), i64> = HashMap::new();
        let mut cache_updates = Vec::new();

        let compute_group_states_start = Instant::now();
        {
            let state_cache = self
                .state_cache
                .as_ref()
                .context("incremental aggregate cache missing")?;

            for key in affected_keys {
                let old_state = state_cache.get(&key).cloned();
                if let Some(work) = logical_work.as_deref_mut() {
                    work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
                    work.state_lookup_rows = work
                        .state_lookup_rows
                        .saturating_add(old_state.is_some() as u64);
                    work.group_state_rows_examined =
                        work.group_state_rows_examined.saturating_add(1);
                }
                let new_state = if recompute_keys.contains(&key) {
                    self.recompute_group_state(&key, logical_work.as_deref_mut())
                        .await
                        .context("recompute incremental aggregate state for key")?
                } else {
                    let mut next = old_state.clone().unwrap_or_else(|| zero_state.clone());
                    if let Some(updates) = aggregated_updates_by_key.get(&key) {
                        next.total_rows += updates.total_rows_delta;
                        for (slot_idx, slot_delta) in updates.slot_deltas.iter().enumerate() {
                            match (&mut next.slots[slot_idx], slot_delta) {
                                (
                                    IncrementalAggregateSlotState::Count { count },
                                    AggregatedSlotDelta::Count { delta },
                                ) => {
                                    *count += *delta;
                                }
                                (
                                    IncrementalAggregateSlotState::CountDistinct { .. },
                                    AggregatedSlotDelta::CountDistinct,
                                ) => {}
                                (
                                    IncrementalAggregateSlotState::Sum {
                                        sum,
                                        non_null_count,
                                    },
                                    AggregatedSlotDelta::Sum {
                                        sum_delta,
                                        non_null_delta,
                                    },
                                ) => {
                                    *sum = checked_add_i64_sum(*sum, *sum_delta)?;
                                    *non_null_count += *non_null_delta;
                                }
                                (
                                    IncrementalAggregateSlotState::DecimalSum {
                                        sum,
                                        non_null_count,
                                    },
                                    AggregatedSlotDelta::Sum {
                                        sum_delta,
                                        non_null_delta,
                                    },
                                ) => {
                                    *sum = checked_add_sum(*sum, *sum_delta)?;
                                    *non_null_count += *non_null_delta;
                                }
                                (
                                    IncrementalAggregateSlotState::Avg { sum, count },
                                    AggregatedSlotDelta::Avg {
                                        sum_delta,
                                        count_delta,
                                    },
                                ) => {
                                    *sum += *sum_delta;
                                    *count += *count_delta;
                                }
                                (
                                    IncrementalAggregateSlotState::Min { current },
                                    AggregatedSlotDelta::Min {
                                        candidate: Some(candidate),
                                    },
                                ) => {
                                    let next_value = match current.take() {
                                        Some(existing) => match candidate.cmp_non_null(&existing) {
                                            Some(std::cmp::Ordering::Less) => candidate.clone(),
                                            Some(_) | None => existing,
                                        },
                                        None => candidate.clone(),
                                    };
                                    *current = Some(next_value);
                                }
                                (
                                    IncrementalAggregateSlotState::Max { current },
                                    AggregatedSlotDelta::Max {
                                        candidate: Some(candidate),
                                    },
                                ) => {
                                    let next_value = match current.take() {
                                        Some(existing) => match candidate.cmp_non_null(&existing) {
                                            Some(std::cmp::Ordering::Greater) => candidate.clone(),
                                            Some(_) | None => existing,
                                        },
                                        None => candidate.clone(),
                                    };
                                    *current = Some(next_value);
                                }
                                (
                                    IncrementalAggregateSlotState::Min { .. },
                                    AggregatedSlotDelta::Min { candidate: None },
                                )
                                | (
                                    IncrementalAggregateSlotState::Max { .. },
                                    AggregatedSlotDelta::Max { candidate: None },
                                ) => {}
                                (state_slot, aggregate_slot) => {
                                    tracing::warn!(
                                        slot_idx,
                                        ?state_slot,
                                        ?aggregate_slot,
                                        "incremental aggregate slot state/aggregate mismatch"
                                    );
                                }
                            }
                        }
                    }
                    if let Some(adjustments) = distinct_count_adjustments.get(&key) {
                        for (slot_idx, adjustment) in adjustments.iter().enumerate() {
                            if *adjustment == 0 {
                                continue;
                            }
                            if let IncrementalAggregateSlotState::CountDistinct { count } =
                                &mut next.slots[slot_idx]
                            {
                                *count += *adjustment;
                            }
                        }
                    }
                    if use_ordered_extrema && extrema_refresh_keys.contains(&key) {
                        self.refresh_extrema_slots_from_index(
                            &key,
                            &mut next,
                            logical_work.as_deref_mut(),
                        )
                        .await
                        .context(
                            "refresh incremental aggregate extrema slots from ordered index",
                        )?;
                    }
                    if next.is_present() { Some(next) } else { None }
                };

                if old_state == new_state {
                    continue;
                }

                match (&old_state, &new_state) {
                    (Some(old), Some(new)) => {
                        state_deltas.insert((key.clone(), old.clone()), -1);
                        state_deltas.insert((key.clone(), new.clone()), 1);
                    }
                    (Some(old), None) => {
                        state_deltas.insert((key.clone(), old.clone()), -1);
                    }
                    (None, Some(new)) => {
                        state_deltas.insert((key.clone(), new.clone()), 1);
                    }
                    (None, None) => {}
                }

                let old_output = old_state
                    .as_ref()
                    .map(|state| state.output_values(&self.slot_kinds))
                    .transpose()?;
                let new_output = new_state
                    .as_ref()
                    .map(|state| state.output_values(&self.slot_kinds))
                    .transpose()?;
                match (old_output, new_output) {
                    (Some(old), Some(new)) if old == new => {}
                    (Some(old), Some(new)) => {
                        output_deltas.insert((key.clone(), old), -1);
                        output_deltas.insert((key.clone(), new), 1);
                    }
                    (Some(old), None) => {
                        output_deltas.insert((key.clone(), old), -1);
                    }
                    (None, Some(new)) => {
                        output_deltas.insert((key.clone(), new), 1);
                    }
                    (None, None) => {}
                }

                cache_updates.push((key, new_state));
            }
        }
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "compute_group_states",
            compute_group_states_start.elapsed().as_millis() as u64,
        );

        if state_deltas.is_empty() {
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "apply_delta_values_total",
                total_start.elapsed().as_millis() as u64,
            );
            return Ok(HashMap::new());
        }

        let base_version = self.state.base_version_for_update();
        let persist_integrated_start = Instant::now();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &state_deltas,
            base_version,
            "integrated",
        )
        .await
        .context("update incremental aggregate integrated state")?;
        if let Some(work) = logical_work {
            work.record_persisted_rows(state_deltas.len());
            work.aggregate_state_rows_updated = cache_updates.len() as u64;
        }
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "persist_integrated",
            persist_integrated_start.elapsed().as_millis() as u64,
        );
        self.state.update_handle(new_integrated_handle);

        if let Some(state_cache) = self.state_cache.as_mut() {
            let cache_update_start = Instant::now();
            for (key, value) in cache_updates {
                if let Some(value) = value {
                    state_cache.insert(key, value);
                } else {
                    state_cache.remove(&key);
                }
            }
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "apply_cache_updates",
                cache_update_start.elapsed().as_millis() as u64,
            );
        }

        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "apply_delta_values_total",
            total_start.elapsed().as_millis() as u64,
        );
        Ok(output_deltas)
    }
}
