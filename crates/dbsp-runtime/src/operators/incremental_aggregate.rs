use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::IndexedBatchZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum AggregateValueType {
    Int64,
    TimestampMillis,
    Utf8,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum AggregateValue {
    Null(AggregateValueType),
    Int64(i64),
    TimestampMillis(i64),
    Utf8(String),
}

impl AggregateValue {
    pub(crate) fn cmp_non_null(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Int64(left), Self::Int64(right)) => Some(left.cmp(right)),
            (Self::TimestampMillis(left), Self::TimestampMillis(right)) => Some(left.cmp(right)),
            (Self::Utf8(left), Self::Utf8(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }

    pub(crate) fn as_numeric(&self) -> Option<i64> {
        match self {
            Self::Int64(value) | Self::TimestampMillis(value) => Some(*value),
            Self::Null(_) | Self::Utf8(_) => None,
        }
    }

    pub(crate) fn from_numeric(value: i64, value_type: &AggregateValueType) -> Self {
        match value_type {
            AggregateValueType::Int64 => Self::Int64(value),
            AggregateValueType::TimestampMillis => Self::TimestampMillis(value),
            AggregateValueType::Utf8 => Self::Utf8(value.to_string()),
        }
    }

    pub(crate) fn null(value_type: &AggregateValueType) -> Self {
        Self::Null(value_type.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalAggregateSlotKind {
    Count,
    CountDistinct,
    Sum(AggregateValueType),
    Avg,
    Min(AggregateValueType),
    Max(AggregateValueType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalAggregateSlotUpdate {
    Count(i64),
    Value(Option<AggregateValue>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAggregateRow<K> {
    pub key: K,
    pub slots: Vec<IncrementalAggregateSlotUpdate>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DistinctGroupKey<K> {
    group_key: K,
    slot: u32,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum IncrementalAggregateSlotState {
    Count { count: i64 },
    CountDistinct { count: i64 },
    Sum { sum: i64, non_null_count: i64 },
    Avg { sum: i64, count: i64 },
    Min { current: Option<AggregateValue> },
    Max { current: Option<AggregateValue> },
}

impl IncrementalAggregateSlotState {
    pub(crate) fn zero(kind: &IncrementalAggregateSlotKind) -> Self {
        match kind {
            IncrementalAggregateSlotKind::Count => Self::Count { count: 0 },
            IncrementalAggregateSlotKind::CountDistinct => Self::CountDistinct { count: 0 },
            IncrementalAggregateSlotKind::Sum(_) => Self::Sum {
                sum: 0,
                non_null_count: 0,
            },
            IncrementalAggregateSlotKind::Avg => Self::Avg { sum: 0, count: 0 },
            IncrementalAggregateSlotKind::Min(_) => Self::Min { current: None },
            IncrementalAggregateSlotKind::Max(_) => Self::Max { current: None },
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct GroupedIncrementalAggregateState {
    total_rows: i64,
    slots: Vec<IncrementalAggregateSlotState>,
}

impl GroupedIncrementalAggregateState {
    pub(crate) fn zero(slot_kinds: &[IncrementalAggregateSlotKind]) -> Self {
        Self {
            total_rows: 0,
            slots: slot_kinds
                .iter()
                .map(IncrementalAggregateSlotState::zero)
                .collect(),
        }
    }

    pub(crate) fn is_present(&self) -> bool {
        self.total_rows != 0
    }

    pub(crate) fn output_values(
        &self,
        slot_kinds: &[IncrementalAggregateSlotKind],
    ) -> Vec<AggregateValue> {
        self.slots
            .iter()
            .zip(slot_kinds.iter())
            .map(|(slot, kind)| match (slot, kind) {
                (IncrementalAggregateSlotState::Count { count }, _)
                | (IncrementalAggregateSlotState::CountDistinct { count }, _) => {
                    AggregateValue::Int64(*count)
                }
                (
                    IncrementalAggregateSlotState::Sum {
                        sum,
                        non_null_count,
                    },
                    IncrementalAggregateSlotKind::Sum(value_type),
                ) => {
                    if *non_null_count > 0 {
                        AggregateValue::from_numeric(*sum, value_type)
                    } else {
                        AggregateValue::null(value_type)
                    }
                }
                (
                    IncrementalAggregateSlotState::Avg { sum, count },
                    IncrementalAggregateSlotKind::Avg,
                ) => {
                    if *count != 0 {
                        AggregateValue::Int64(sum / count)
                    } else {
                        AggregateValue::Null(AggregateValueType::Int64)
                    }
                }
                (
                    IncrementalAggregateSlotState::Min { current },
                    IncrementalAggregateSlotKind::Min(value_type),
                )
                | (
                    IncrementalAggregateSlotState::Max { current },
                    IncrementalAggregateSlotKind::Max(value_type),
                ) => current
                    .clone()
                    .unwrap_or_else(|| AggregateValue::null(value_type)),
                (other, kind) => {
                    tracing::warn!(
                        ?other,
                        ?kind,
                        "incremental aggregate slot state/kind mismatch"
                    );
                    AggregateValue::Null(AggregateValueType::Int64)
                }
            })
            .collect()
    }
}

type RowEvaluator<V, K> = Arc<dyn Fn(&V) -> Option<IncrementalAggregateRow<K>> + Send + Sync>;

pub struct IncrementalAggregateOp<K, V>
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
    pub state: RelationState<(K, GroupedIncrementalAggregateState)>,
    pub table: Arc<dyn KeyValueTable>,
    pub row_evaluator: RowEvaluator<V, K>,
    output: VersionedZSet<(K, Vec<AggregateValue>)>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    state_cache: Option<HashMap<K, GroupedIncrementalAggregateState>>,
    slot_kinds: Vec<IncrementalAggregateSlotKind>,
    distinct_index: Option<IndexedBatchZSet<DistinctGroupKey<K>, AggregateValue>>,
    input_index: Option<IndexedBatchZSet<K, V>>,
    append_only_input: bool,
}

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
    pub(crate) fn new(
        state: RelationState<(K, GroupedIncrementalAggregateState)>,
        table: Arc<dyn KeyValueTable>,
        row_evaluator: RowEvaluator<V, K>,
        output: VersionedZSet<(K, Vec<AggregateValue>)>,
        slot_kinds: Vec<IncrementalAggregateSlotKind>,
        distinct_index: Option<IndexedBatchZSet<DistinctGroupKey<K>, AggregateValue>>,
        input_index: Option<IndexedBatchZSet<K, V>>,
    ) -> Self {
        Self {
            state,
            table,
            row_evaluator,
            output,
            dict_cache: HashMap::new(),
            state_cache: None,
            slot_kinds,
            distinct_index,
            input_index,
            append_only_input: false,
        }
    }

    pub fn enable_live_output_replayable(&mut self) {
        self.output.enable_replayable_persistence();
    }

    pub fn enable_append_only_input(&mut self) {
        self.append_only_input = true;
    }

    fn has_extrema(&self) -> bool {
        self.slot_kinds.iter().any(|kind| {
            matches!(
                kind,
                IncrementalAggregateSlotKind::Min(_) | IncrementalAggregateSlotKind::Max(_)
            )
        })
    }

    async fn ensure_state_cache(&mut self) -> Result<()> {
        if self.state_cache.is_some() {
            return Ok(());
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize incremental aggregate integrated state")?;
        let mut cache = HashMap::new();
        for ((key, aggregate), weight) in materialized {
            if weight != 0 {
                cache.insert(key, aggregate);
            }
        }
        self.state_cache = Some(cache);
        Ok(())
    }

    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64> {
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

    async fn apply_deltas_to_versioned<T>(
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

    pub async fn apply_delta_values(
        &mut self,
        delta_values: &[(V, i64)],
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

        let coalesced = if self.append_only_input {
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
            Count { delta: i64 },
            CountDistinct,
            Sum { sum_delta: i64, non_null_delta: i64 },
            Avg { sum_delta: i64, count_delta: i64 },
            Min { candidate: Option<AggregateValue> },
            Max { candidate: Option<AggregateValue> },
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
        let slot_kinds = &self.slot_kinds;
        let mut aggregated_updates_by_key: HashMap<K, AggregatedKeyUpdates> = HashMap::new();

        let has_extrema = self.has_extrema();
        let mut apply_value = |value: V, weight: i64| {
            if weight == 0 {
                return;
            }
            let Some(row_update) = (self.row_evaluator)(&value) else {
                return;
            };
            if row_update.slots.len() != self.slot_kinds.len() {
                tracing::warn!(
                    expected = self.slot_kinds.len(),
                    actual = row_update.slots.len(),
                    "incremental aggregate row evaluator returned unexpected slot vector width"
                );
                return;
            }
            let key = row_update.key;
            let slots = row_update.slots;
            if has_extrema && weight < 0 {
                recompute_keys.insert(key.clone());
                aggregated_updates_by_key.remove(&key);
            }
            if self.input_index.is_some() {
                index_updates.push((key.clone(), value.clone(), weight));
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
                return;
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
                        if let Some(number) = value.as_numeric() {
                            *sum_delta += number * weight;
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
                        if let Some(number) = value.as_numeric() {
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
        };

        let aggregate_updates_start = Instant::now();
        if let Some(coalesced) = coalesced {
            for (value, weight) in coalesced {
                apply_value(value, weight);
            }
        } else {
            for (value, weight) in delta_values.iter() {
                apply_value(value.clone(), *weight);
            }
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

        if let Some(input_index) = self.input_index.as_ref()
            && !index_updates.is_empty()
        {
            let input_index_start = Instant::now();
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
                let old_weight = distinct_index
                    .value_weight_for_key_value(&distinct_key, &distinct_value)
                    .await
                    .context("load incremental aggregate distinct multiplicity")?;
                let new_weight = old_weight + delta;
                let adjustments = distinct_count_adjustments
                    .entry(distinct_key.group_key.clone())
                    .or_insert_with(|| vec![0; self.slot_kinds.len()]);
                if old_weight > 0 && new_weight <= 0 {
                    adjustments[distinct_key.slot as usize] -= 1;
                } else if old_weight <= 0 && new_weight > 0 {
                    adjustments[distinct_key.slot as usize] += 1;
                }
                distinct_updates.push((distinct_key, distinct_value, delta));
            }
            if !distinct_updates.is_empty() {
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
        self.ensure_state_cache()
            .await
            .context("load incremental aggregate cache")?;
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
                let new_state = if recompute_keys.contains(&key) {
                    self.recompute_group_state(&key)
                        .await
                        .with_context(|| format!("recompute incremental aggregate state for key"))?
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
                                    *sum += *sum_delta;
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
                    .map(|state| state.output_values(&self.slot_kinds));
                let new_output = new_state
                    .as_ref()
                    .map(|state| state.output_values(&self.slot_kinds));
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

    fn apply_slot_update(
        &self,
        state: &mut GroupedIncrementalAggregateState,
        slot_idx: usize,
        slot: &IncrementalAggregateSlotUpdate,
        weight: i64,
    ) {
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
                if let Some(number) = value.as_numeric() {
                    *sum += number * weight;
                    *non_null_count += weight;
                }
            }
            (
                IncrementalAggregateSlotKind::Avg,
                IncrementalAggregateSlotState::Avg { sum, count },
                IncrementalAggregateSlotUpdate::Value(Some(value)),
            ) => {
                if let Some(number) = value.as_numeric() {
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
    }

    async fn recompute_group_state(
        &self,
        key: &K,
    ) -> Result<Option<GroupedIncrementalAggregateState>> {
        let input_index = self
            .input_index
            .as_ref()
            .context("incremental aggregate input index missing during recompute")?;
        let values = input_index
            .values_for_key(key)
            .await
            .context("load incremental aggregate input values for recompute")?;

        if values.is_empty() {
            return Ok(None);
        }

        let mut state = GroupedIncrementalAggregateState::zero(&self.slot_kinds);
        let mut distinct_weights: Vec<HashMap<AggregateValue, i64>> = self
            .slot_kinds
            .iter()
            .map(|kind| {
                if matches!(kind, IncrementalAggregateSlotKind::CountDistinct) {
                    HashMap::new()
                } else {
                    HashMap::new()
                }
            })
            .collect();

        for (value, weight) in values {
            if weight == 0 {
                continue;
            }
            let Some(row_update) = (self.row_evaluator)(&value) else {
                continue;
            };
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
                    _ => self.apply_slot_update(&mut state, slot_idx, slot, weight),
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
}

#[async_trait]
impl<K, V> DeltaOperator for IncrementalAggregateOp<K, V>
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
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let step_start = Instant::now();
        let delta_handle = inputs
            .first()
            .cloned()
            .context("incremental aggregate operator requires one input delta handle")?;

        let load_delta_start = Instant::now();
        let delta_values =
            delta_zset_handle_batch::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for incremental aggregate")?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "load_delta",
            load_delta_start.elapsed().as_millis() as u64,
        );

        let apply_values_start = Instant::now();
        let output_deltas = self.apply_delta_values(delta_values.as_ref()).await?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "apply_delta_values",
            apply_values_start.elapsed().as_millis() as u64,
        );
        if output_deltas.is_empty() {
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "on_step_total",
                step_start.elapsed().as_millis() as u64,
            );
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let persist_output_start = Instant::now();
        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &output_deltas, None, "output")
                .await
                .context("persist incremental aggregate output delta")?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "persist_output",
            persist_output_start.elapsed().as_millis() as u64,
        );
        publish_transient_zset_batch(
            &delta_handle,
            Arc::new(output_deltas.into_iter().collect::<Vec<_>>()),
        );
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "on_step_total",
            step_start.elapsed().as_millis() as u64,
        );
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use crate::storage::SlateTable;
    use crate::storage::dictionary::Dictionary;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;

    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
    struct AggregateRow {
        group_key: i64,
        price: Option<i64>,
        category: String,
    }

    async fn stage_version<T>(
        dict: Arc<Dictionary<T>>,
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        deltas: &[(T, i64)],
    ) -> ZSetHandle
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
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern test key for incremental aggregate");
            buckets
                .entry(bucket_for(id))
                .or_default()
                .push((id, *delta));
        }
        drop(dict_batch);

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

        let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
            .await
            .expect("build versioned");
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create version");
        versioned.handle_for_version(version)
    }

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    #[tokio::test]
    async fn incremental_aggregate_tracks_mixed_slots_and_delete_recompute() {
        let table = build_table("incremental-aggregate").await;
        let input_dict = Arc::new(
            Dictionary::<AggregateRow>::with_table(
                table.clone(),
                "incremental_aggregate_input".to_string(),
                None,
            )
            .await
            .expect("create input dictionary"),
        );
        let state = RelationState::<(i64, GroupedIncrementalAggregateState)>::empty(
            table.clone(),
            "incremental_aggregate_state".to_string(),
        )
        .await
        .expect("create incremental aggregate state");
        let output_dict = Arc::new(
            Dictionary::<(i64, Vec<AggregateValue>)>::with_table(
                table.clone(),
                "incremental_aggregate_output".to_string(),
                None,
            )
            .await
            .expect("create incremental aggregate output dictionary"),
        );
        let output = VersionedZSet::new(
            output_dict,
            table.clone(),
            "incremental_aggregate_output".to_string(),
        )
        .await
        .expect("create incremental aggregate output");

        let mut op = IncrementalAggregateOp::new(
            state,
            table.clone(),
            Arc::new(|row: &AggregateRow| {
                Some(IncrementalAggregateRow {
                    key: row.group_key,
                    slots: vec![
                        IncrementalAggregateSlotUpdate::Count(1),
                        IncrementalAggregateSlotUpdate::Value(row.price.map(AggregateValue::Int64)),
                        IncrementalAggregateSlotUpdate::Value(row.price.map(AggregateValue::Int64)),
                        IncrementalAggregateSlotUpdate::Value(row.price.map(AggregateValue::Int64)),
                        IncrementalAggregateSlotUpdate::Value(Some(AggregateValue::Utf8(
                            row.category.clone(),
                        ))),
                    ],
                })
            }),
            output,
            vec![
                IncrementalAggregateSlotKind::Count,
                IncrementalAggregateSlotKind::Sum(AggregateValueType::Int64),
                IncrementalAggregateSlotKind::Avg,
                IncrementalAggregateSlotKind::Min(AggregateValueType::Int64),
                IncrementalAggregateSlotKind::Max(AggregateValueType::Utf8),
            ],
            None,
            Some(IndexedBatchZSet::new(
                table.clone(),
                "incremental_aggregate_input_index".to_string(),
            )),
        );

        let batch_one = stage_version(
            input_dict.clone(),
            table.clone(),
            "incremental_aggregate_input",
            &[
                (
                    AggregateRow {
                        group_key: 1,
                        price: Some(10),
                        category: "b".to_string(),
                    },
                    1,
                ),
                (
                    AggregateRow {
                        group_key: 1,
                        price: Some(30),
                        category: "c".to_string(),
                    },
                    1,
                ),
            ],
        )
        .await;
        let out_one = op
            .on_step(0, std::slice::from_ref(&batch_one))
            .await
            .expect("run incremental aggregate t1")
            .expect("incremental aggregate t1 output");
        let mut cache = HashMap::new();
        let delta_one = materialize_zset_handle::<(i64, Vec<AggregateValue>)>(
            table.clone(),
            &mut cache,
            &out_one,
        )
        .await
        .expect("materialize incremental aggregate t1");
        assert_eq!(
            delta_one,
            HashMap::from([(
                (
                    1,
                    vec![
                        AggregateValue::Int64(2),
                        AggregateValue::Int64(40),
                        AggregateValue::Int64(20),
                        AggregateValue::Int64(10),
                        AggregateValue::Utf8("c".to_string()),
                    ],
                ),
                1,
            )])
        );

        let batch_two = stage_version(
            input_dict.clone(),
            table.clone(),
            "incremental_aggregate_input",
            &[(
                AggregateRow {
                    group_key: 1,
                    price: Some(30),
                    category: "c".to_string(),
                },
                -1,
            )],
        )
        .await;
        let out_two = op
            .on_step(1, std::slice::from_ref(&batch_two))
            .await
            .expect("run incremental aggregate t2")
            .expect("incremental aggregate t2 output");
        let delta_two = materialize_zset_handle::<(i64, Vec<AggregateValue>)>(
            table.clone(),
            &mut cache,
            &out_two,
        )
        .await
        .expect("materialize incremental aggregate t2");
        assert_eq!(
            delta_two,
            HashMap::from([
                (
                    (
                        1,
                        vec![
                            AggregateValue::Int64(2),
                            AggregateValue::Int64(40),
                            AggregateValue::Int64(20),
                            AggregateValue::Int64(10),
                            AggregateValue::Utf8("c".to_string()),
                        ],
                    ),
                    -1
                ),
                (
                    (
                        1,
                        vec![
                            AggregateValue::Int64(1),
                            AggregateValue::Int64(10),
                            AggregateValue::Int64(10),
                            AggregateValue::Int64(10),
                            AggregateValue::Utf8("b".to_string()),
                        ],
                    ),
                    1
                ),
            ])
        );
    }
}
