use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use object_store::memory::InMemory;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::Db;

use super::{JoinBatchConfig, JoinInputRetention, JoinOp, JoinTransientInputs};
use crate::collections::IndexedBatchZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{compute_delta, materialize_zset_handle};

async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("joinop", store).await.expect("open SlateDB"))
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

async fn stage_version<K>(
    dict: Arc<Dictionary<K>>,
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
    deltas: &[(K, i64)],
) -> ZSetHandle
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
    let mut dict_batch = dict.batch();
    for (key, delta) in deltas {
        let id = dict_batch.intern(key).await.expect("intern key for join");
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

fn project_sum(l: &i64, r: &i64) -> i64 {
    l + r
}

fn empty_handle(namespace: &str) -> ZSetHandle {
    ZSetHandle {
        ns: namespace.to_string(),
        version: 0,
    }
}

type RowKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;
type BatchJoinKeyExtractor<T, K> = Arc<dyn Fn(&[(T, i64)]) -> Vec<(K, T, i64)> + Send + Sync>;

fn batch_join_key<T, K>(key_extractor: RowKeyExtractor<T, K>) -> BatchJoinKeyExtractor<T, K>
where
    T: Clone + 'static,
    K: 'static,
{
    Arc::new(move |deltas: &[(T, i64)]| {
        deltas
            .iter()
            .filter_map(|(row, weight)| key_extractor(row).map(|key| (key, row.clone(), *weight)))
            .collect()
    })
}

fn apply_deltas(state: &mut HashMap<i64, i64>, deltas: &[(i64, i64)]) {
    for (key, delta) in deltas {
        let entry = state.entry(*key).or_insert(0);
        *entry += *delta;
        if *entry == 0 {
            state.remove(key);
        }
    }
}

fn recompute_join(left: &HashMap<i64, i64>, right: &HashMap<i64, i64>) -> HashMap<i64, i64> {
    let mut out = HashMap::new();
    for (lk, lw) in left {
        for (rk, rw) in right {
            if lk == rk {
                *out.entry(lk + rk).or_insert(0) += lw * rw;
            }
        }
    }
    out.retain(|_, weight| *weight != 0);
    out
}

fn batch_to_map(batch: &Arc<Vec<(i64, i64)>>) -> HashMap<i64, i64> {
    let mut out = HashMap::new();
    for (key, weight) in batch.iter() {
        let next = out.get(key).copied().unwrap_or(0) + *weight;
        if next == 0 {
            out.remove(key);
        } else {
            out.insert(*key, next);
        }
    }
    out
}

#[path = "tests/history.rs"]
mod history;
#[path = "tests/incremental.rs"]
mod incremental;
#[path = "tests/retention.rs"]
mod retention;
#[path = "tests/transient.rs"]
mod transient;
