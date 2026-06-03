use super::key::{
    TransientTopNKey, TransientTopNKeyExtractor, TransientTopNKeyLayout, TransientTopNKeyedDelta,
    transient_topn_order_specs,
};
use super::shared::{accumulate_single_weight_delta, accumulate_weight_deltas};
use super::*;
use std::time::Instant;

type PartitionedTop1OrderIndex = HashMap<Vec<u8>, BTreeMap<(TransientTopNKey, Vec<u8>), i64>>;

pub(in crate::dbsp_graph_builder::builder) struct TransientTopNProcessor {
    graph_id: String,
    key_extractor: TransientTopNKeyExtractor,
    limit: usize,
    offset: usize,
    order_index: BTreeMap<Vec<u8>, BTreeMap<TransientTopNKey, i64>>,
    partition_output_cache: BTreeMap<Vec<u8>, HashMap<Vec<u8>, i64>>,
    profile_enabled: bool,
    profiled_batches: usize,
}

pub(in crate::dbsp_graph_builder::builder) struct TransientTop1Processor {
    key_extractor: TransientTopNKeyExtractor,
    order_index: PartitionedTop1OrderIndex,
    partition_output_cache: HashMap<Vec<u8>, Vec<u8>>,
}

impl TransientTopNProcessor {
    pub(in crate::dbsp_graph_builder::builder) fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
        _append_only_input: bool,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            graph_id,
            key_extractor,
            limit: topn.limit(),
            offset: topn.offset(),
            order_index: BTreeMap::new(),
            partition_output_cache: BTreeMap::new(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
        }
    }

    pub(in crate::dbsp_graph_builder::builder) fn apply_deltas(
        &mut self,
        deltas: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;
        let mut mutation_us = 0u128;

        let mut affected_partitions = BTreeSet::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_topn(&deltas)?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientTopNKeyedDelta {
                diff,
                partition_key,
                order_key,
                ..
            } = keyed;
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };
            affected_partitions.insert(partition_key.clone());

            let mutation_start = profile_this_batch.then(Instant::now);
            let partition_index = self.order_index.entry(partition_key.clone()).or_default();
            let previous_weight = partition_index.get(&order_key).copied().unwrap_or(0);
            let next_weight = previous_weight.saturating_add(diff);
            if next_weight <= 0 {
                partition_index.remove(&order_key);
                if partition_index.is_empty() {
                    self.order_index.remove(&partition_key);
                }
            } else {
                partition_index.insert(order_key, next_weight);
            }
            if let Some(mutation_start) = mutation_start {
                mutation_us += mutation_start.elapsed().as_micros();
            }
        }

        let recompute_start = profile_this_batch.then(Instant::now);
        let mut recompute_rows_scanned = 0usize;
        let mut affected_partition_count = 0usize;
        let mut output_deltas = HashMap::new();
        for partition_key in affected_partitions {
            affected_partition_count += 1;
            let previous_output = self
                .partition_output_cache
                .remove(&partition_key)
                .unwrap_or_default();
            let next_output = self
                .order_index
                .get(&partition_key)
                .map(|partition_index| {
                    if profile_this_batch {
                        recompute_rows_scanned += partition_index.len();
                    }
                    self.compute_partition_topn(partition_index)
                })
                .unwrap_or_default();
            accumulate_weight_deltas(&mut output_deltas, &previous_output, &next_output);
            if !next_output.is_empty() {
                self.partition_output_cache
                    .insert(partition_key, next_output);
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let recompute_us = recompute_start
                .expect("recompute start present")
                .elapsed()
                .as_micros();
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partition_output_cache.len(),
                recompute_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                mutation_us,
                recompute_us,
                total_us,
                "transient topn batch profile"
            );
        }

        Ok(output_deltas)
    }

    fn compute_partition_topn(
        &self,
        partition_index: &BTreeMap<TransientTopNKey, i64>,
    ) -> HashMap<Vec<u8>, i64> {
        if self.limit == 0 {
            return HashMap::new();
        }

        let mut remaining_skip = self.offset;
        let mut remaining_take = self.limit;
        let mut output = HashMap::new();

        for (order_key, weight) in partition_index {
            if remaining_take == 0 {
                break;
            }

            let mut remaining_weight = *weight;
            if remaining_skip > 0 {
                let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
                let skip = remaining_skip.min(available);
                remaining_skip -= skip;
                remaining_weight -= skip as i64;
            }

            if remaining_weight <= 0 {
                continue;
            }

            let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take > 0 {
                output.insert(order_key.tie_breaker.clone(), take as i64);
                remaining_take -= take;
            }
        }

        output
    }

    #[cfg(test)]
    pub(in crate::dbsp_graph_builder::builder) fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        let retain_count = self.offset.saturating_add(self.limit);
        if retain_count == 0 {
            return Vec::new();
        }

        self.order_index
            .values()
            .flat_map(|partition_index| {
                let mut remaining = retain_count;
                partition_index
                    .iter()
                    .filter_map(move |(order_key, weight)| {
                        if remaining == 0 || *weight <= 0 {
                            return None;
                        }
                        let retained = usize::try_from(*weight)
                            .unwrap_or(usize::MAX)
                            .min(remaining);
                        remaining -= retained;
                        Some((order_key.tie_breaker.clone(), retained as i64))
                    })
            })
            .collect()
    }
}

#[derive(Default)]
pub(super) struct TransientAppendOnlyTopNPartitionState {
    visible_rows: BTreeMap<TransientTopNKey, i64>,
    visible_count: usize,
}

pub(super) struct TransientAppendOnlyTopNProcessor {
    graph_id: String,
    key_extractor: TransientTopNKeyExtractor,
    limit: usize,
    profile_enabled: bool,
    profiled_batches: usize,
    partitions: HashMap<Vec<u8>, TransientAppendOnlyTopNPartitionState>,
}

impl TransientAppendOnlyTopNProcessor {
    pub(in crate::dbsp_graph_builder::builder) fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            graph_id,
            key_extractor,
            limit: topn.limit(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
            partitions: HashMap::new(),
        }
    }

    pub(in crate::dbsp_graph_builder::builder) fn apply_deltas(
        &mut self,
        deltas: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;
        let mut partition_apply_us = 0u128;
        let mut trimmed_rows = 0usize;
        let mut skipped_rows = 0usize;
        let mut affected_partitions = HashSet::new();
        let mut output_deltas = HashMap::new();

        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_topn(&deltas)?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientTopNKeyedDelta {
                diff,
                partition_key,
                order_key,
                ..
            } = keyed;
            if diff < 0 {
                bail!(
                    "append-only transient topn received negative diff for graph {}",
                    self.graph_id
                );
            }

            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };

            affected_partitions.insert(partition_key.clone());
            let apply_start = profile_this_batch.then(Instant::now);
            let state = self.partitions.entry(partition_key).or_default();
            Self::apply_positive_delta(
                state,
                order_key,
                diff,
                self.limit,
                &mut output_deltas,
                &mut trimmed_rows,
                &mut skipped_rows,
            );
            if let Some(apply_start) = apply_start {
                partition_apply_us += apply_start.elapsed().as_micros();
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count = affected_partitions.len(),
                retained_partitions = self.partitions.len(),
                trimmed_rows,
                skipped_rows,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                partition_apply_us,
                total_us,
                "transient append-only topn profile"
            );
        }

        Ok(output_deltas)
    }

    fn apply_positive_delta(
        state: &mut TransientAppendOnlyTopNPartitionState,
        order_key: TransientTopNKey,
        diff: i64,
        limit: usize,
        output_deltas: &mut HashMap<Vec<u8>, i64>,
        trimmed_rows: &mut usize,
        skipped_rows: &mut usize,
    ) {
        if limit == 0 {
            return;
        }

        if state.visible_count >= limit
            && let Some((worst_key, _)) = state.visible_rows.last_key_value()
            && order_key > *worst_key
        {
            *skipped_rows = skipped_rows.saturating_add(diff as usize);
            return;
        }

        let row_key = order_key.tie_breaker.clone();
        let entry = state.visible_rows.entry(order_key).or_insert(0);
        *entry = entry.saturating_add(diff);
        state.visible_count = state.visible_count.saturating_add(diff as usize);
        accumulate_single_weight_delta(output_deltas, row_key, diff);

        while state.visible_count > limit {
            let overflow = state.visible_count - limit;
            let Some((worst_key, worst_weight)) = state
                .visible_rows
                .last_key_value()
                .map(|(key, weight)| (key.clone(), *weight))
            else {
                break;
            };
            let removable = usize::try_from(worst_weight)
                .unwrap_or(usize::MAX)
                .min(overflow) as i64;
            if removable <= 0 {
                break;
            }
            if let Some(weight) = state.visible_rows.get_mut(&worst_key) {
                *weight -= removable;
                if *weight <= 0 {
                    state.visible_rows.remove(&worst_key);
                }
            }
            state.visible_count -= removable as usize;
            *trimmed_rows = trimmed_rows.saturating_add(removable as usize);
            accumulate_single_weight_delta(
                output_deltas,
                worst_key.tie_breaker.clone(),
                -removable,
            );
        }
    }
}

impl TransientTop1Processor {
    pub(in crate::dbsp_graph_builder::builder) fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            key_extractor,
            order_index: HashMap::new(),
            partition_output_cache: HashMap::new(),
        }
    }

    pub(in crate::dbsp_graph_builder::builder) fn apply_deltas(
        &mut self,
        deltas: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let mut output_deltas = HashMap::new();
        for keyed in self.key_extractor.extract_topn(&deltas)? {
            let TransientTopNKeyedDelta {
                row_key,
                diff,
                partition_key,
                order_key,
            } = keyed;
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };

            let previous_top = self.partition_output_cache.get(&partition_key).cloned();
            let partition_now_empty = {
                let partition_index = self.order_index.entry(partition_key.clone()).or_default();
                let index_key = (order_key, row_key.clone());
                let previous_weight = partition_index.get(&index_key).copied().unwrap_or(0);
                let next_weight = previous_weight.saturating_add(diff);
                if next_weight <= 0 {
                    partition_index.remove(&index_key);
                } else {
                    partition_index.insert(index_key, next_weight);
                }
                partition_index.is_empty()
            };

            let next_top = if partition_now_empty {
                self.order_index.remove(&partition_key);
                None
            } else {
                self.order_index
                    .get(&partition_key)
                    .and_then(|partition_index| {
                        partition_index
                            .first_key_value()
                            .map(|((_order_key, row_key), _)| row_key.clone())
                    })
            };

            if previous_top == next_top {
                continue;
            }
            if let Some(previous_top) = previous_top {
                let entry = output_deltas.entry(previous_top).or_insert(0);
                *entry -= 1;
            }
            match next_top {
                Some(next_top) => {
                    let entry = output_deltas.entry(next_top.clone()).or_insert(0);
                    *entry += 1;
                    self.partition_output_cache.insert(partition_key, next_top);
                }
                None => {
                    self.partition_output_cache.remove(&partition_key);
                }
            }
        }

        Ok(output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect())
    }

    #[cfg(test)]
    pub(in crate::dbsp_graph_builder::builder) fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        let mut snapshot = self
            .partition_output_cache
            .values()
            .map(|row_key| (row_key.clone(), 1))
            .collect::<Vec<_>>();
        snapshot.sort();
        snapshot
    }
}
