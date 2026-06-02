use super::key::{
    TransientDirectPartitionTopNKeyedDelta, TransientDirectTop1KeyedDelta,
    TransientDirectTop1PartitionKey, TransientDirectTop1PartitionLayout, TransientTopNKey,
    TransientTopNKeyExtractor, TransientTopNKeyLayout, transient_topn_order_specs,
};
use super::shared::accumulate_single_weight_delta;
use super::*;
use std::time::Instant;

#[derive(Clone)]
pub(in crate::dbsp_graph_builder::builder) struct TransientDirectTop1Config {
    pub(in crate::dbsp_graph_builder::builder) partition_layout: TransientDirectTop1PartitionLayout,
    pub(in crate::dbsp_graph_builder::builder) order_idx: usize,
    pub(in crate::dbsp_graph_builder::builder) ascending: bool,
}

#[derive(Clone)]
struct TransientDirectTop1PartitionUpdate {
    row_key: Vec<u8>,
    order_value: i64,
    diff: i64,
}

#[derive(Clone)]
pub(in crate::dbsp_graph_builder::builder) struct TransientDirectTop1LiveRow {
    order_value: i64,
    weight: i64,
}

#[derive(Default)]
pub(in crate::dbsp_graph_builder::builder) struct TransientDirectTop1PartitionState {
    pub(in crate::dbsp_graph_builder::builder) live_rows:
        HashMap<Vec<u8>, TransientDirectTop1LiveRow>,
    top_row: Option<Vec<u8>>,
}

#[derive(Clone)]
pub(in crate::dbsp_graph_builder::builder) struct TransientDirectPartitionTopNConfig {
    pub(in crate::dbsp_graph_builder::builder) partition_idx: usize,
}

#[derive(Clone, Default)]
struct TransientDirectPartitionTopNOutput {
    rows: BTreeMap<TransientTopNKey, i64>,
    visible_count: usize,
}

impl TransientDirectPartitionTopNOutput {
    fn is_empty(&self) -> bool {
        self.rows.is_empty() || self.visible_count == 0
    }

    fn row_weight(&self, row_key: &[u8]) -> i64 {
        self.rows
            .iter()
            .find_map(|(order_key, weight)| {
                (order_key.tie_breaker.as_slice() == row_key).then_some(*weight)
            })
            .unwrap_or(0)
    }
}

struct TransientDirectPartitionTopNPartitionUpdate {
    diff: i64,
    order_key: TransientTopNKey,
}

pub(in crate::dbsp_graph_builder::builder) struct TransientDirectPartitionTopNProcessor {
    graph_id: String,
    partition_idx: usize,
    key_extractor: TransientTopNKeyExtractor,
    limit: usize,
    offset: usize,
    order_index: HashMap<i64, BTreeMap<TransientTopNKey, i64>>,
    partition_output_cache: HashMap<i64, TransientDirectPartitionTopNOutput>,
    profile_enabled: bool,
    profiled_batches: usize,
}

pub(in crate::dbsp_graph_builder::builder) struct TransientDirectTop1Processor {
    graph_id: String,
    partition_layout: TransientDirectTop1PartitionLayout,
    order_idx: usize,
    ascending: bool,
    compact_append_only_state: bool,
    key_extractor: TransientTopNKeyExtractor,
    pub(in crate::dbsp_graph_builder::builder) partitions:
        HashMap<TransientDirectTop1PartitionKey, TransientDirectTop1PartitionState>,
    profile_enabled: bool,
    profiled_batches: usize,
}

impl TransientDirectPartitionTopNProcessor {
    pub(in crate::dbsp_graph_builder::builder) fn new(
        graph_id: impl Into<String>,
        config: TransientDirectPartitionTopNConfig,
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
            partition_idx: config.partition_idx,
            key_extractor,
            limit: topn.limit(),
            offset: topn.offset(),
            order_index: HashMap::new(),
            partition_output_cache: HashMap::new(),
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

        let mut partition_updates =
            HashMap::<i64, Vec<TransientDirectPartitionTopNPartitionUpdate>>::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self
            .key_extractor
            .extract_direct_partition_topn(&deltas, self.partition_idx)?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientDirectPartitionTopNKeyedDelta {
                diff,
                partition_value,
                order_key,
            } = keyed;
            partition_updates.entry(partition_value).or_default().push(
                TransientDirectPartitionTopNPartitionUpdate {
                    diff,
                    order_key: order_key.clone(),
                },
            );

            let mutation_start = profile_this_batch.then(Instant::now);
            let partition_index = self.order_index.entry(partition_value).or_default();
            let previous_weight = partition_index.get(&order_key).copied().unwrap_or(0);
            let next_weight = previous_weight.saturating_add(diff);
            if next_weight <= 0 {
                partition_index.remove(&order_key);
                if partition_index.is_empty() {
                    self.order_index.remove(&partition_value);
                }
            } else {
                partition_index.insert(order_key, next_weight);
            }
            if let Some(mutation_start) = mutation_start {
                mutation_us += mutation_start.elapsed().as_micros();
            }
        }

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut recompute_rows_scanned = 0usize;
        let mut positive_partition_count = 0usize;
        let mut exact_partition_count = 0usize;
        let mut positive_update_count = 0usize;
        let mut skipped_rows = 0usize;
        let mut trimmed_rows = 0usize;
        let mut output_deltas = HashMap::new();
        let affected_partition_count = partition_updates.len();
        for (partition_key, updates) in partition_updates {
            let previous_output = self
                .partition_output_cache
                .remove(&partition_key)
                .unwrap_or_default();
            if self.offset == 0 && updates.iter().all(|update| update.diff > 0) {
                positive_partition_count += 1;
                positive_update_count += updates.len();
                let mut next_output = previous_output;
                for update in updates {
                    Self::apply_positive_output_delta(
                        &mut next_output,
                        update.order_key,
                        update.diff,
                        self.limit,
                        &mut output_deltas,
                        &mut trimmed_rows,
                        &mut skipped_rows,
                    );
                }
                if !next_output.is_empty() {
                    self.partition_output_cache
                        .insert(partition_key, next_output);
                }
            } else {
                exact_partition_count += 1;
                let next_output = self
                    .order_index
                    .get(&partition_key)
                    .map(|partition_index| {
                        self.compute_partition_topn(partition_index, &mut recompute_rows_scanned)
                    })
                    .unwrap_or_default();
                Self::accumulate_output_deltas(&mut output_deltas, &previous_output, &next_output);
                if !next_output.is_empty() {
                    self.partition_output_cache
                        .insert(partition_key, next_output);
                }
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let partition_apply_us = partition_apply_start
                .expect("partition apply start present")
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
                positive_partition_count,
                exact_partition_count,
                positive_update_count,
                skipped_rows,
                trimmed_rows,
                recompute_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                mutation_us,
                partition_apply_us,
                total_us,
                "transient direct-partition topn batch profile"
            );
        }

        Ok(output_deltas)
    }

    fn apply_positive_output_delta(
        state: &mut TransientDirectPartitionTopNOutput,
        order_key: TransientTopNKey,
        diff: i64,
        limit: usize,
        output_deltas: &mut HashMap<Vec<u8>, i64>,
        trimmed_rows: &mut usize,
        skipped_rows: &mut usize,
    ) {
        if limit == 0 || diff <= 0 {
            return;
        }

        let diff_count = usize::try_from(diff).unwrap_or(usize::MAX);
        if state.visible_count >= limit
            && let Some((worst_key, _)) = state.rows.last_key_value()
            && order_key > *worst_key
        {
            *skipped_rows = skipped_rows.saturating_add(diff_count);
            return;
        }

        let row_key = order_key.tie_breaker.clone();
        let entry = state.rows.entry(order_key).or_insert(0);
        *entry = entry.saturating_add(diff);
        state.visible_count = state.visible_count.saturating_add(diff_count);
        accumulate_single_weight_delta(output_deltas, row_key, diff);

        while state.visible_count > limit {
            let overflow = state.visible_count - limit;
            let Some((worst_key, worst_weight)) = state
                .rows
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
            if let Some(weight) = state.rows.get_mut(&worst_key) {
                *weight -= removable;
                if *weight <= 0 {
                    state.rows.remove(&worst_key);
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

    fn compute_partition_topn(
        &self,
        partition_index: &BTreeMap<TransientTopNKey, i64>,
        rows_scanned: &mut usize,
    ) -> TransientDirectPartitionTopNOutput {
        if self.limit == 0 {
            return TransientDirectPartitionTopNOutput::default();
        }

        let mut remaining_skip = self.offset;
        let mut remaining_take = self.limit;
        let mut output = TransientDirectPartitionTopNOutput::default();
        for (order_key, weight) in partition_index {
            *rows_scanned = rows_scanned.saturating_add(1);
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
                output.rows.insert(order_key.clone(), take as i64);
                output.visible_count = output.visible_count.saturating_add(take);
                remaining_take -= take;
            }
        }
        output
    }

    fn accumulate_output_deltas(
        output_deltas: &mut HashMap<Vec<u8>, i64>,
        previous_output: &TransientDirectPartitionTopNOutput,
        next_output: &TransientDirectPartitionTopNOutput,
    ) {
        for (previous_key, previous_weight) in &previous_output.rows {
            let row_key = previous_key.tie_breaker.as_slice();
            let next_weight = next_output.row_weight(row_key);
            let delta = next_weight.saturating_sub(*previous_weight);
            if delta != 0 {
                accumulate_single_weight_delta(
                    output_deltas,
                    previous_key.tie_breaker.clone(),
                    delta,
                );
            }
        }
        for (next_key, next_weight) in &next_output.rows {
            if previous_output.row_weight(next_key.tie_breaker.as_slice()) != 0 {
                continue;
            }
            if *next_weight != 0 {
                accumulate_single_weight_delta(
                    output_deltas,
                    next_key.tie_breaker.clone(),
                    *next_weight,
                );
            }
        }
    }
}

impl TransientDirectTop1Processor {
    pub(in crate::dbsp_graph_builder::builder) fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        config: TransientDirectTop1Config,
        compact_append_only_state: bool,
    ) -> Self {
        let graph_id = graph_id.into();
        let partition_columns = match config.partition_layout {
            TransientDirectTop1PartitionLayout::One(partition_idx) => vec![partition_idx],
            TransientDirectTop1PartitionLayout::Two(partition_indices) => {
                partition_indices.to_vec()
            }
        };
        let order_specs = transient_topn_order_specs(topn);
        let order_type = topn
            .output_schema()
            .field(config.order_idx)
            .expect("direct top1 order index should be in bounds")
            .data_type
            .clone();
        let key_extractor = TransientTopNKeyExtractor::new(
            graph_id.clone(),
            Arc::clone(topn.output_schema()),
            Arc::new(partition_columns),
            Arc::new(vec![config.order_idx]),
            Arc::new(vec![order_type]),
            order_specs,
        )
        .expect("direct top1 transient topn key layout should be valid");
        Self {
            graph_id,
            partition_layout: config.partition_layout,
            order_idx: config.order_idx,
            ascending: config.ascending,
            compact_append_only_state,
            key_extractor,
            partitions: HashMap::new(),
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

        let grouping_start = profile_this_batch.then(Instant::now);
        let mut partition_updates = HashMap::<
            TransientDirectTop1PartitionKey,
            Vec<TransientDirectTop1PartitionUpdate>,
        >::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_direct_top1(
            &deltas,
            self.partition_layout,
            self.order_idx,
        )?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientDirectTop1KeyedDelta {
                row_key,
                diff,
                partition_key,
                order_value,
            } = keyed;
            partition_updates.entry(partition_key).or_default().push(
                TransientDirectTop1PartitionUpdate {
                    row_key,
                    order_value,
                    diff,
                },
            );
        }
        let grouping_us = grouping_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut output_deltas = HashMap::new();
        let mut affected_partition_count = 0usize;
        let mut exact_rows_scanned = 0usize;
        for (partition_key, updates) in partition_updates {
            affected_partition_count += 1;
            let mut state = self.partitions.remove(&partition_key).unwrap_or_default();
            let previous_top = state.top_row.clone();
            let next_top = if updates.iter().all(|update| update.diff > 0) {
                self.apply_partition_updates_append_only(&mut state, &updates)
            } else {
                self.apply_partition_updates_exact(&mut state, &updates, &mut exact_rows_scanned)
            };

            if previous_top != next_top {
                if let Some(previous_top) = previous_top {
                    let entry = output_deltas.entry(previous_top).or_insert(0);
                    *entry -= 1;
                }
                if let Some(next_top_row) = next_top.clone() {
                    let entry = output_deltas.entry(next_top_row).or_insert(0);
                    *entry += 1;
                }
            }

            state.top_row = next_top;
            if !state.live_rows.is_empty() {
                self.partitions.insert(partition_key, state);
            }
        }
        let partition_apply_us = partition_apply_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

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
                affected_partition_count,
                retained_partitions = self.partitions.len(),
                exact_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                grouping_us,
                partition_apply_us,
                total_us,
                "transient direct top1 profile"
            );
        }

        Ok(output_deltas)
    }

    #[cfg(test)]
    pub(in crate::dbsp_graph_builder::builder) fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.partitions
            .values()
            .filter_map(|state| {
                let row_key = state.top_row.as_ref()?;
                let weight = state.live_rows.get(row_key)?.weight;
                (weight > 0).then_some((row_key.clone(), weight))
            })
            .collect()
    }

    fn apply_partition_updates_append_only(
        &self,
        state: &mut TransientDirectTop1PartitionState,
        updates: &[TransientDirectTop1PartitionUpdate],
    ) -> Option<Vec<u8>> {
        let mut next_top = state.top_row.clone();
        for update in updates {
            let next_weight = Self::apply_live_row_update(state, update);
            if next_weight <= 0 {
                continue;
            }
            let previous_top = next_top.clone();
            match next_top.as_ref() {
                Some(current_top) => {
                    if self.compare_live_rows(state, &update.row_key, current_top)
                        == std::cmp::Ordering::Less
                    {
                        next_top = Some(update.row_key.clone());
                    }
                }
                None => {
                    next_top = Some(update.row_key.clone());
                }
            }
            if next_top.as_ref() == Some(&update.row_key)
                && previous_top.as_ref() != Some(&update.row_key)
                && self.compact_append_only_state
            {
                let retained = state
                    .live_rows
                    .get(&update.row_key)
                    .cloned()
                    .expect("winning append-only top1 row must be live");
                state.live_rows.clear();
                state.live_rows.insert(update.row_key.clone(), retained);
            } else if previous_top.as_ref() != Some(&update.row_key)
                && self.compact_append_only_state
            {
                state.live_rows.remove(&update.row_key);
            }
        }
        next_top
    }

    fn apply_partition_updates_exact(
        &self,
        state: &mut TransientDirectTop1PartitionState,
        updates: &[TransientDirectTop1PartitionUpdate],
        exact_rows_scanned: &mut usize,
    ) -> Option<Vec<u8>> {
        for update in updates {
            Self::apply_live_row_update(state, update);
        }

        *exact_rows_scanned += state.live_rows.len();
        let mut best_row_key: Option<&Vec<u8>> = None;
        let mut best_order_value = 0i64;
        for (row_key, live_row) in &state.live_rows {
            if live_row.weight <= 0 {
                continue;
            }
            match best_row_key {
                Some(current_best) => {
                    if self.compare_order_and_tie_breaker(
                        live_row.order_value,
                        row_key,
                        best_order_value,
                        current_best,
                    ) == std::cmp::Ordering::Less
                    {
                        best_row_key = Some(row_key);
                        best_order_value = live_row.order_value;
                    }
                }
                None => {
                    best_row_key = Some(row_key);
                    best_order_value = live_row.order_value;
                }
            }
        }
        best_row_key.cloned()
    }

    fn apply_live_row_update(
        state: &mut TransientDirectTop1PartitionState,
        update: &TransientDirectTop1PartitionUpdate,
    ) -> i64 {
        let next_weight = match state.live_rows.get(&update.row_key) {
            Some(live_row) => live_row.weight.saturating_add(update.diff),
            None => update.diff,
        };
        if next_weight <= 0 {
            state.live_rows.remove(&update.row_key);
            return 0;
        }
        state.live_rows.insert(
            update.row_key.clone(),
            TransientDirectTop1LiveRow {
                order_value: update.order_value,
                weight: next_weight,
            },
        );
        next_weight
    }

    fn compare_live_rows(
        &self,
        state: &TransientDirectTop1PartitionState,
        left: &Vec<u8>,
        right: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let left_live_row = state
            .live_rows
            .get(left)
            .expect("live row must exist for left comparison");
        let right_live_row = state
            .live_rows
            .get(right)
            .expect("live row must exist for right comparison");
        self.compare_order_and_tie_breaker(
            left_live_row.order_value,
            left,
            right_live_row.order_value,
            right,
        )
    }

    fn compare_order_and_tie_breaker(
        &self,
        left_order: i64,
        left_row_key: &Vec<u8>,
        right_order: i64,
        right_row_key: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let order_cmp = if self.ascending {
            left_order.cmp(&right_order)
        } else {
            right_order.cmp(&left_order)
        };
        if order_cmp != std::cmp::Ordering::Equal {
            return order_cmp;
        }
        left_row_key.cmp(right_row_key)
    }
}
