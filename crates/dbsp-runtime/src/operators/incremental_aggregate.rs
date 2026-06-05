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

use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::collections::{IndexedBatchZSet, OrderedBytes};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum AggregateValueType {
    Int64,
    TimestampMillis,
    Utf8,
    DateDays,
    Decimal128 { precision: u8, scale: i8 },
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum AggregateValue {
    Null(AggregateValueType),
    Int64(i64),
    TimestampMillis(i64),
    Utf8(String),
    DateDays(i32),
    Decimal128(i128),
}

impl AggregateValue {
    pub(crate) fn cmp_non_null(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Int64(left), Self::Int64(right)) => Some(left.cmp(right)),
            (Self::TimestampMillis(left), Self::TimestampMillis(right)) => Some(left.cmp(right)),
            (Self::Utf8(left), Self::Utf8(right)) => Some(left.cmp(right)),
            (Self::DateDays(left), Self::DateDays(right)) => Some(left.cmp(right)),
            (Self::Decimal128(left), Self::Decimal128(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }

    pub(crate) fn as_i64_numeric(&self) -> Option<i64> {
        match self {
            Self::Int64(value) | Self::TimestampMillis(value) => Some(*value),
            Self::Null(_) | Self::Utf8(_) | Self::DateDays(_) | Self::Decimal128(_) => None,
        }
    }

    pub(crate) fn as_sum_numeric(&self) -> Option<i128> {
        match self {
            Self::Int64(value) | Self::TimestampMillis(value) => Some(i128::from(*value)),
            Self::Decimal128(value) => Some(*value),
            Self::Null(_) | Self::Utf8(_) | Self::DateDays(_) => None,
        }
    }

    pub(crate) fn from_sum_numeric(value: i128, value_type: &AggregateValueType) -> Result<Self> {
        match value_type {
            AggregateValueType::Int64 => i64::try_from(value)
                .map(Self::Int64)
                .context("incremental Int64 SUM overflow"),
            AggregateValueType::TimestampMillis => i64::try_from(value)
                .map(Self::TimestampMillis)
                .context("incremental TimestampMillis SUM overflow"),
            AggregateValueType::Decimal128 { precision, .. } => {
                ensure_decimal_fits_precision(value, *precision)?;
                Ok(Self::Decimal128(value))
            }
            AggregateValueType::Utf8 | AggregateValueType::DateDays => {
                anyhow::bail!("unsupported numeric aggregate type {value_type:?}")
            }
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
pub struct DistinctGroupKey<K> {
    group_key: K,
    slot: u32,
}

impl<K> DistinctGroupKey<K> {
    pub fn new(group_key: K, slot: u32) -> Self {
        Self { group_key, slot }
    }

    pub fn group_key(&self) -> &K {
        &self.group_key
    }

    pub fn slot(&self) -> u32 {
        self.slot
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum IncrementalAggregateSlotState {
    Count { count: i64 },
    CountDistinct { count: i64 },
    Sum { sum: i64, non_null_count: i64 },
    Avg { sum: i64, count: i64 },
    Min { current: Option<AggregateValue> },
    Max { current: Option<AggregateValue> },
    DecimalSum { sum: i128, non_null_count: i64 },
}

impl IncrementalAggregateSlotState {
    pub(crate) fn zero(kind: &IncrementalAggregateSlotKind) -> Self {
        match kind {
            IncrementalAggregateSlotKind::Count => Self::Count { count: 0 },
            IncrementalAggregateSlotKind::CountDistinct => Self::CountDistinct { count: 0 },
            IncrementalAggregateSlotKind::Sum(AggregateValueType::Decimal128 { .. }) => {
                Self::DecimalSum {
                    sum: 0,
                    non_null_count: 0,
                }
            }
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
    pub fn from_parts(total_rows: i64, slots: Vec<IncrementalAggregateSlotState>) -> Self {
        Self { total_rows, slots }
    }

    pub fn total_rows(&self) -> i64 {
        self.total_rows
    }

    pub fn slots(&self) -> &[IncrementalAggregateSlotState] {
        &self.slots
    }

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
    ) -> Result<Vec<AggregateValue>> {
        self.slots
            .iter()
            .zip(slot_kinds.iter())
            .map(|(slot, kind)| match (slot, kind) {
                (IncrementalAggregateSlotState::Count { count }, _)
                | (IncrementalAggregateSlotState::CountDistinct { count }, _) => {
                    Ok(AggregateValue::Int64(*count))
                }
                (
                    IncrementalAggregateSlotState::Sum {
                        sum,
                        non_null_count,
                    },
                    IncrementalAggregateSlotKind::Sum(value_type),
                ) => {
                    if *non_null_count > 0 {
                        AggregateValue::from_sum_numeric(i128::from(*sum), value_type)
                    } else {
                        Ok(AggregateValue::null(value_type))
                    }
                }
                (
                    IncrementalAggregateSlotState::DecimalSum {
                        sum,
                        non_null_count,
                    },
                    IncrementalAggregateSlotKind::Sum(value_type),
                ) => {
                    if *non_null_count > 0 {
                        AggregateValue::from_sum_numeric(*sum, value_type)
                    } else {
                        Ok(AggregateValue::null(value_type))
                    }
                }
                (
                    IncrementalAggregateSlotState::Avg { sum, count },
                    IncrementalAggregateSlotKind::Avg,
                ) => {
                    if *count != 0 {
                        Ok(AggregateValue::Int64(sum / count))
                    } else {
                        Ok(AggregateValue::Null(AggregateValueType::Int64))
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
                    .map(Ok)
                    .unwrap_or_else(|| Ok(AggregateValue::null(value_type))),
                (other, kind) => {
                    tracing::warn!(
                        ?other,
                        ?kind,
                        "incremental aggregate slot state/kind mismatch"
                    );
                    Ok(AggregateValue::Null(AggregateValueType::Int64))
                }
            })
            .collect()
    }
}

fn ensure_decimal_fits_precision(value: i128, precision: u8) -> Result<()> {
    let max_abs = 10_i128
        .checked_pow(u32::from(precision))
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| anyhow::anyhow!("invalid Decimal128 precision {precision}"))?;
    let abs = value
        .checked_abs()
        .ok_or_else(|| anyhow::anyhow!("Decimal128 SUM overflow"))?;
    anyhow::ensure!(
        abs <= max_abs,
        "Decimal128 SUM overflow: value {value} exceeds precision {precision}"
    );
    Ok(())
}

fn checked_weighted_sum_delta(value: i128, weight: i64) -> Result<i128> {
    value
        .checked_mul(i128::from(weight))
        .ok_or_else(|| anyhow::anyhow!("incremental SUM overflow while applying input weight"))
}

fn checked_add_sum(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(|| anyhow::anyhow!("incremental SUM overflow"))
}

fn checked_add_i64_sum(left: i64, right: i128) -> Result<i64> {
    let right = i64::try_from(right).context("incremental Int64 SUM overflow")?;
    left.checked_add(right)
        .ok_or_else(|| anyhow::anyhow!("incremental Int64 SUM overflow"))
}

fn aggregate_value_order_bytes(value: &AggregateValue, descending: bool) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    match value {
        AggregateValue::Null(_) => return None,
        AggregateValue::Int64(value) | AggregateValue::TimestampMillis(value) => {
            let shifted = (*value as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&shifted.to_be_bytes());
        }
        AggregateValue::Utf8(value) => {
            append_memcomparable_bytes(value.as_bytes(), &mut out);
        }
        AggregateValue::DateDays(value) => {
            let shifted = (*value as u32) ^ 0x8000_0000;
            out.extend_from_slice(&shifted.to_be_bytes());
        }
        AggregateValue::Decimal128(value) => {
            let shifted = (*value as u128) ^ (1_u128 << 127);
            out.extend_from_slice(&shifted.to_be_bytes());
        }
    }
    if descending {
        for byte in &mut out {
            *byte = !*byte;
        }
    }
    Some(out)
}

fn append_memcomparable_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    for &byte in bytes {
        if byte == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(byte);
        }
    }
    out.push(0);
    out.push(0);
}

fn bytes_prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(byte) = next.last_mut() {
        if *byte != 0xFF {
            *byte += 1;
            return Some(next);
        }
        next.pop();
    }
    None
}

type BatchRowEvaluator<V, K> =
    Arc<dyn Fn(&[(V, i64)]) -> Vec<(V, IncrementalAggregateRow<K>, i64)> + Send + Sync>;

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
    pub(crate) state: RelationState<(K, GroupedIncrementalAggregateState)>,
    pub(crate) table: Arc<dyn KeyValueTable>,
    pub(crate) row_evaluator: BatchRowEvaluator<V, K>,
    output: VersionedZSet<(K, Vec<AggregateValue>)>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    state_cache: Option<HashMap<K, GroupedIncrementalAggregateState>>,
    slot_kinds: Vec<IncrementalAggregateSlotKind>,
    distinct_index: Option<IndexedBatchZSet<DistinctGroupKey<K>, AggregateValue>>,
    input_index: Option<IndexedBatchZSet<K, V>>,
    extrema_index: Option<IndexedBatchZSet<OrderedBytes, V>>,
    append_only_input: bool,
    logical_work: metrics::LogicalWorkCollector,
}

pub struct IncrementalAggregateIndexes<K, V>
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
    distinct: Option<IndexedBatchZSet<DistinctGroupKey<K>, AggregateValue>>,
    input: Option<IndexedBatchZSet<K, V>>,
    extrema: Option<IndexedBatchZSet<OrderedBytes, V>>,
}

impl<K, V> IncrementalAggregateIndexes<K, V>
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
    pub fn new(
        distinct: Option<IndexedBatchZSet<DistinctGroupKey<K>, AggregateValue>>,
        input: Option<IndexedBatchZSet<K, V>>,
        extrema: Option<IndexedBatchZSet<OrderedBytes, V>>,
    ) -> Self {
        Self {
            distinct,
            input,
            extrema,
        }
    }
}

mod apply;
mod extrema;
mod lifecycle;
mod operator;
mod persistence;
mod recompute_evict;

#[cfg(test)]
mod tests;
