use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::AHashMap;
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

type JoinPredicate<L, R> = Arc<dyn Fn(&L, &R) -> bool + Send + Sync>;
type JoinProjector<L, R, O> = Arc<dyn Fn(&L, &R) -> O + Send + Sync>;
type BatchJoinKeyExtractor<T, K> = Arc<dyn Fn(&[(T, i64)]) -> Vec<(K, T, i64)> + Send + Sync>;
type FastHashMap<K, V> = AHashMap<K, V>;
type KeyedRowDeltas<K, T> = FastHashMap<K, FastHashMap<T, i64>>;
static NEXT_JOIN_CLOSED_INDEX_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct JoinMapMetrics {
    left_rows_examined: u64,
    right_rows_examined: u64,
    candidate_pairs_examined: u64,
    output_rows: u64,
}

pub(crate) struct JoinStepResult<O> {
    #[cfg(test)]
    pub(crate) delta_batch: Arc<Vec<(O, i64)>>,
    pub(crate) _output: std::marker::PhantomData<O>,
    pub(crate) persisted_handle: Option<ZSetHandle>,
}

pub struct JoinTransientInputs<L, R, K> {
    pub(crate) left: Option<Arc<Vec<(L, i64)>>>,
    pub(crate) right: Option<Arc<Vec<(R, i64)>>>,
    pub(crate) left_closed_keys: Option<Arc<Vec<(K, i64)>>>,
    pub(crate) right_closed_keys: Option<Arc<Vec<(K, i64)>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinInputRetention {
    RetainAll,
    DropMatchedAppendOnly,
}

pub struct JoinOp<L, R, O, K>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    #[cfg(test)]
    pub(crate) left_state: RelationState<L>,
    #[cfg(test)]
    pub(crate) right_state: RelationState<R>,
    pub(crate) left_index: IndexedBatchZSet<K, L>,
    pub(crate) right_index: IndexedBatchZSet<K, R>,
    pub(crate) left_closed_index: IndexedBatchZSet<K, ()>,
    pub(crate) right_closed_index: IndexedBatchZSet<K, ()>,
    pub(crate) left_key: BatchJoinKeyExtractor<L, K>,
    pub(crate) right_key: BatchJoinKeyExtractor<R, K>,
    pub(crate) predicate: JoinPredicate<L, R>,
    pub(crate) projector: JoinProjector<L, R, O>,
    pub(crate) table: Arc<dyn KeyValueTable>,
    pub(crate) integrated: Option<RelationState<O>>,
    output: Option<VersionedZSet<O>>,
    dict_cache_left: HashMap<String, Arc<Dictionary<L>>>,
    dict_cache_right: HashMap<String, Arc<Dictionary<R>>>,
    left_memory_index: FastHashMap<K, FastHashMap<L, i64>>,
    right_memory_index: FastHashMap<K, FastHashMap<R, i64>>,
    left_closed_memory_index: FastHashMap<K, i64>,
    right_closed_memory_index: FastHashMap<K, i64>,
    persist_indexes: bool,
    left_retention: JoinInputRetention,
    right_retention: JoinInputRetention,
    logical_work: metrics::LogicalWorkCollector,
}

impl<L, R, O, K> JoinOp<L, R, O, K>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new_batch(
        _left_state: RelationState<L>,
        _right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_key: BatchJoinKeyExtractor<L, K>,
        right_key: BatchJoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<O>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        let closed_id = NEXT_JOIN_CLOSED_INDEX_ID.fetch_add(1, Ordering::Relaxed);
        Self::new_batch_with_closed_indexes(
            _left_state,
            _right_state,
            left_index,
            right_index,
            IndexedBatchZSet::new(table.clone(), format!("join_left_closed_index_{closed_id}")),
            IndexedBatchZSet::new(
                table.clone(),
                format!("join_right_closed_index_{closed_id}"),
            ),
            left_key,
            right_key,
            predicate,
            projector,
            table,
            output,
            integrated,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_batch_with_closed_indexes(
        _left_state: RelationState<L>,
        _right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_closed_index: IndexedBatchZSet<K, ()>,
        right_closed_index: IndexedBatchZSet<K, ()>,
        left_key: BatchJoinKeyExtractor<L, K>,
        right_key: BatchJoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<O>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        debug_assert_eq!(left_index.engine_kind(), "indexed_batch");
        debug_assert_eq!(right_index.engine_kind(), "indexed_batch");
        Self {
            #[cfg(test)]
            left_state: _left_state,
            #[cfg(test)]
            right_state: _right_state,
            left_index,
            right_index,
            left_closed_index,
            right_closed_index,
            left_key,
            right_key,
            predicate,
            projector,
            table,
            integrated,
            output: Some(output),
            dict_cache_left: HashMap::new(),
            dict_cache_right: HashMap::new(),
            left_memory_index: FastHashMap::new(),
            right_memory_index: FastHashMap::new(),
            left_closed_memory_index: FastHashMap::new(),
            right_closed_memory_index: FastHashMap::new(),
            persist_indexes: true,
            left_retention: JoinInputRetention::RetainAll,
            right_retention: JoinInputRetention::RetainAll,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    pub fn enable_live_output_replayable(&mut self) {
        if let Some(output) = self.output.as_mut() {
            output.enable_replayable_persistence();
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_without_output_batch(
        _left_state: RelationState<L>,
        _right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_key: BatchJoinKeyExtractor<L, K>,
        right_key: BatchJoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        let closed_id = NEXT_JOIN_CLOSED_INDEX_ID.fetch_add(1, Ordering::Relaxed);
        Self::new_without_output_batch_with_closed_indexes(
            _left_state,
            _right_state,
            left_index,
            right_index,
            IndexedBatchZSet::new(table.clone(), format!("join_left_closed_index_{closed_id}")),
            IndexedBatchZSet::new(
                table.clone(),
                format!("join_right_closed_index_{closed_id}"),
            ),
            left_key,
            right_key,
            predicate,
            projector,
            table,
            integrated,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_without_output_batch_with_closed_indexes(
        _left_state: RelationState<L>,
        _right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_closed_index: IndexedBatchZSet<K, ()>,
        right_closed_index: IndexedBatchZSet<K, ()>,
        left_key: BatchJoinKeyExtractor<L, K>,
        right_key: BatchJoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        debug_assert_eq!(left_index.engine_kind(), "indexed_batch");
        debug_assert_eq!(right_index.engine_kind(), "indexed_batch");
        Self {
            #[cfg(test)]
            left_state: _left_state,
            #[cfg(test)]
            right_state: _right_state,
            left_index,
            right_index,
            left_closed_index,
            right_closed_index,
            left_key,
            right_key,
            predicate,
            projector,
            table,
            integrated,
            output: None,
            dict_cache_left: HashMap::new(),
            dict_cache_right: HashMap::new(),
            left_memory_index: FastHashMap::new(),
            right_memory_index: FastHashMap::new(),
            left_closed_memory_index: FastHashMap::new(),
            right_closed_memory_index: FastHashMap::new(),
            persist_indexes: true,
            left_retention: JoinInputRetention::RetainAll,
            right_retention: JoinInputRetention::RetainAll,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    pub fn with_persist_indexes(mut self, persist_indexes: bool) -> Self {
        self.persist_indexes = persist_indexes;
        self
    }

    pub fn with_input_retention(
        mut self,
        left_retention: JoinInputRetention,
        right_retention: JoinInputRetention,
    ) -> Self {
        self.left_retention = left_retention;
        self.right_retention = right_retention;
        self
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }
}

mod helpers;
mod operator;
mod step;

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
