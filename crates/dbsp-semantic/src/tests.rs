use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use async_trait::async_trait;
use dbsp_runtime::algebra::AbelianGroup;
use dbsp_runtime::handles::ZSetHandle;
use dbsp_runtime::storage::{KeyValueTable, SlateTable};
use dbsp_runtime::stream::Stream as RuntimeStream;
use object_store::memory::InMemory;
use slatedb::Db;

use crate::circuit::{
    Circuit, add_circuit, circuit_d, circuit_i, identity, incrementalize, pointwise, strict_delay,
};
use crate::lowering::{
    collect_runtime_scalar_prefix, collect_runtime_zset_prefix, lower_indexed, lower_scalar,
    lower_set, lower_zset,
};
use crate::operators::{
    aggregate_zset, arrange_by, count_by_zset, distinct_zset, filter_set, filter_zset,
    flat_map_zset, join_indexed, join_set, join_zset, lookup_index, map_set, map_zset,
    sliding_window_aggregate, tumbling_window_aggregate, union_set, union_zset, unnest_zset,
};
use crate::stream::{
    ReferenceEvaluator, Stream, StreamNodeKind, delay, differentiate, feedback, integrate, subtract,
};
use crate::values::{Set, Window, ZSet};

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
struct Event {
    user: i64,
    ts: i64,
    value: i64,
}

struct IntGroup;

#[async_trait]
impl AbelianGroup<i64> for IntGroup {
    async fn add(&self, a: &i64, b: &i64) -> i64 {
        a + b
    }

    async fn neg(&self, a: &i64) -> i64 {
        -a
    }

    async fn identity(&self) -> i64 {
        0
    }
}

#[derive(Clone)]
struct HandleGroup {
    default: ZSetHandle,
}

#[async_trait]
impl AbelianGroup<ZSetHandle> for HandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("semantic-test", store).await.expect("open db"))
}

fn build_table(db: Arc<Db>) -> Arc<dyn KeyValueTable> {
    Arc::new(SlateTable::new(db))
}

fn event(user: i64, ts: i64, value: i64) -> Event {
    Event { user, ts, value }
}

fn zset<T>(entries: impl IntoIterator<Item = (T, i64)>) -> ZSet<T>
where
    T: Clone + Ord,
{
    ZSet::from_weights(entries)
}

fn set<T>(entries: impl IntoIterator<Item = T>) -> Set<T>
where
    T: Clone + Ord,
{
    Set::new(entries)
}

fn observe<T>(stream: &Stream<T>, len: usize) -> Vec<T>
where
    T: Clone + Send + Sync + 'static,
{
    ReferenceEvaluator::observe_prefix(stream, len)
}

fn window_count(window: Window<i64>, count: i64) -> ((Window<i64>, i64), i64) {
    ((window, count), 1)
}

fn reachability_stream() -> Stream<ZSet<(i64, i64)>> {
    let edges = Stream::constant(zset([
        ((1_i64, 2_i64), 1),
        ((2_i64, 3_i64), 1),
        ((3_i64, 4_i64), 1),
    ]));
    feedback("reachability", {
        let edges = edges.clone();
        move |paths| {
            let delayed = delay(&paths);
            let extended = join_zset(
                &delayed,
                &edges,
                |(_, mid)| Some(*mid),
                |(mid, _)| Some(*mid),
                |_, _| true,
                |(src, _), (_, dst)| (*src, *dst),
            );
            union_zset(&edges, &extended)
        }
    })
}

fn oscillating_collection_stream() -> Stream<ZSet<String>> {
    let base = Stream::constant(zset([("x".to_string(), 1)]));
    feedback("oscillating", {
        let base = base.clone();
        move |state| subtract(&base, &delay(&state))
    })
}

#[path = "tests/lowered_execution.rs"]
mod lowered_execution;
#[path = "tests/lowered_recursion.rs"]
mod lowered_recursion;
#[path = "tests/semantic_laws.rs"]
mod semantic_laws;
#[path = "tests/window_semantics.rs"]
mod window_semantics;
