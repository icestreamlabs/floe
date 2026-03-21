use std::sync::Arc;

use async_trait::async_trait;
use dbsp_runtime::algebra::AbelianGroup;
use dbsp_runtime::handles::ZSetHandle;
use dbsp_runtime::storage::{KeyValueTable, SlateTable};
use dbsp_runtime::stream::Stream as RuntimeStream;
use object_store::memory::InMemory;
use slatedb::Db;

use crate::circuit::{Circuit, incrementalize};
use crate::lowering::{
    collect_runtime_scalar_prefix, collect_runtime_zset_prefix, lower_indexed_prefix,
    lower_scalar_prefix, lower_set_prefix, lower_zset_prefix,
};
use crate::operators::{
    aggregate_zset, arrange_by, count_by_zset, distinct_zset, filter_set, filter_zset,
    join_indexed, join_set, join_zset, map_set, map_zset, sliding_window_aggregate,
    tumbling_window_aggregate, union_set, union_zset, unnest_zset,
};
use crate::stream::{
    ReferenceEvaluator, Stream, delay, differentiate, feedback, integrate, subtract,
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

#[test]
fn paper_operators_are_total_on_infinite_scalar_streams() {
    let ones = Stream::constant(1_i64);

    assert_eq!(
        ReferenceEvaluator::observe_prefix(&delay(&ones), 6),
        vec![0, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        ReferenceEvaluator::observe_prefix(&integrate(&ones), 6),
        vec![1, 2, 3, 4, 5, 6]
    );
    assert_eq!(
        ReferenceEvaluator::observe_prefix(&differentiate(&integrate(&ones)), 6),
        vec![1, 1, 1, 1, 1, 1]
    );

    let alternating = Stream::from_fn("alternating", |t| if t % 2 == 0 { 1 } else { -1 });
    assert_eq!(
        ReferenceEvaluator::observe_prefix(&integrate(&alternating), 6),
        vec![1, 0, 1, 0, 1, 0]
    );
}

#[test]
fn semantic_queries_cover_sets_bags_and_indexes() {
    let bag = Stream::constant(zset([(1_i64, 2), (2, 1), (3, 1)]));
    let mapped = map_zset(&bag, |value| value * 10);
    let filtered = filter_zset(&bag, |value| *value >= 2);
    let distinct = distinct_zset(&bag);
    let arranged = arrange_by(&bag, |value| Some(value % 2));
    let looked_up = crate::lookup_index(&arranged, 1_i64);

    assert_eq!(mapped.at(0), zset([(10_i64, 2), (20, 1), (30, 1)]));
    assert_eq!(filtered.at(0), zset([(2_i64, 1), (3, 1)]));
    assert_eq!(distinct.at(0), set([1_i64, 2, 3]));
    assert_eq!(looked_up.at(0), zset([(1_i64, 2), (3, 1)]));

    let left_set = Stream::constant(set([1_i64, 2, 3]));
    let right_set = Stream::constant(set([2_i64, 3, 4]));
    assert_eq!(
        map_set(&left_set, |value| value * 2).at(0),
        set([2_i64, 4, 6])
    );
    assert_eq!(
        filter_set(&left_set, |value| *value >= 2).at(0),
        set([2_i64, 3])
    );
    assert_eq!(
        union_set(&left_set, &right_set).at(0),
        set([1_i64, 2, 3, 4])
    );
    assert_eq!(
        join_set(
            &left_set,
            &right_set,
            |value| Some(*value),
            |value| Some(*value),
            |_, _| true,
            |left, right| (*left, *right),
        )
        .at(0),
        set([(2_i64, 2_i64), (3_i64, 3_i64)])
    );

    let indexed_left = Stream::constant(zset([(10_i64, 1), (11_i64, 1), (20_i64, 1)]))
        .lift("arrange-left", |value| value.index_by(|row| Some(row / 10)));
    let indexed_right = Stream::constant(zset([(100_i64, 1), (101_i64, 1), (200_i64, 1)]))
        .lift("arrange-right", |value| {
            value.index_by(|row| Some(row / 100))
        });
    assert_eq!(
        join_indexed(&indexed_left, &indexed_right, |key, left, right| (
            *key, *left, *right
        ))
        .at(0),
        zset([
            ((1_i64, 10_i64, 100_i64), 1),
            ((1_i64, 10_i64, 101_i64), 1),
            ((1_i64, 11_i64, 100_i64), 1),
            ((1_i64, 11_i64, 101_i64), 1),
            ((2_i64, 20_i64, 200_i64), 1),
        ])
    );
}

#[test]
fn semantic_aggregation_nesting_and_windows_match_expected_values() {
    let bag = Stream::constant(zset([(1_i64, 2), (2_i64, 1), (3_i64, 1), (4_i64, 1)]));
    let counts = count_by_zset(&bag, |value| Some(value % 2));
    assert_eq!(
        counts.at(0),
        zset([((0_i64, 2_i64), 1), ((1_i64, 3_i64), 1)])
    );

    let nested = Stream::constant(zset([
        ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 1),
        ((2_i64, zset([(30_i64, 2)])), 1),
    ]));
    assert_eq!(
        unnest_zset(&nested).at(0),
        zset([
            ((1_i64, 10_i64), 1),
            ((1_i64, 20_i64), 1),
            ((2_i64, 30_i64), 2)
        ])
    );

    let events = Stream::constant(zset([
        (
            Event {
                user: 1,
                ts: 2,
                value: 5,
            },
            1,
        ),
        (
            Event {
                user: 1,
                ts: 6,
                value: 7,
            },
            1,
        ),
        (
            Event {
                user: 2,
                ts: 11,
                value: 9,
            },
            1,
        ),
    ]));
    let sliding = sliding_window_aggregate(
        &events,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
        5,
    );
    assert_eq!(
        sliding.at(0),
        zset([
            (
                (
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10
                    },
                    2_i64
                ),
                1
            ),
            (
                (
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 15
                    },
                    1_i64
                ),
                1
            ),
            (
                (
                    Window {
                        key: 2_i64,
                        start: 5,
                        end: 15
                    },
                    1_i64
                ),
                1
            ),
            (
                (
                    Window {
                        key: 2_i64,
                        start: 10,
                        end: 20
                    },
                    1_i64
                ),
                1
            ),
        ])
    );

    let tumbling = tumbling_window_aggregate(
        &events,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
    );
    assert_eq!(
        tumbling.at(0),
        zset([
            (
                (
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10
                    },
                    2_i64
                ),
                1
            ),
            (
                (
                    Window {
                        key: 2_i64,
                        start: 10,
                        end: 20
                    },
                    1_i64
                ),
                1
            ),
        ])
    );
}

#[test]
fn feedback_supports_monotonic_and_non_monotonic_recursion() {
    let edges = Stream::constant(zset([
        ((1_i64, 2_i64), 1),
        ((2_i64, 3_i64), 1),
        ((3_i64, 4_i64), 1),
    ]));
    let reachability = feedback("reachability", {
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
    });
    assert_eq!(
        ReferenceEvaluator::observe_prefix(&reachability, 3),
        vec![
            zset([
                ((1_i64, 2_i64), 1),
                ((2_i64, 3_i64), 1),
                ((3_i64, 4_i64), 1)
            ]),
            zset([
                ((1_i64, 2_i64), 1),
                ((1_i64, 3_i64), 1),
                ((2_i64, 3_i64), 1),
                ((2_i64, 4_i64), 1),
                ((3_i64, 4_i64), 1),
            ]),
            zset([
                ((1_i64, 2_i64), 1),
                ((1_i64, 3_i64), 1),
                ((1_i64, 4_i64), 1),
                ((2_i64, 3_i64), 1),
                ((2_i64, 4_i64), 1),
                ((3_i64, 4_i64), 1),
            ]),
        ]
    );

    let base = Stream::constant(zset([("x".to_string(), 1)]));
    let oscillating = feedback("oscillating", {
        let base = base.clone();
        move |state| subtract(&base, &delay(&state))
    });
    assert_eq!(
        ReferenceEvaluator::observe_prefix(&oscillating, 5),
        vec![
            zset([("x".to_string(), 1)]),
            zset::<String>([]),
            zset([("x".to_string(), 1)]),
            zset::<String>([]),
            zset([("x".to_string(), 1)]),
        ]
    );
}

#[test]
fn incrementalization_matches_reference_for_collection_circuit() {
    let query = Circuit::new("parity-count", |input: Stream<ZSet<i64>>| {
        let filtered = filter_zset(&input, |value| *value >= 0);
        let projected = map_zset(&filtered, |value| value % 2);
        count_by_zset(&projected, |value| Some(*value))
    });

    let deltas = Stream::from_prefix(
        vec![
            zset([(1_i64, 1)]),
            zset([(2_i64, 1)]),
            zset([(1_i64, -1)]),
            zset::<i64>([]),
        ],
        zset::<i64>([]),
    );

    let incremental = incrementalize(query.clone()).apply(deltas.clone());
    let reference = differentiate(&query.apply(integrate(&deltas)));
    assert_eq!(
        ReferenceEvaluator::observe_prefix(&incremental, 4),
        ReferenceEvaluator::observe_prefix(&reference, 4)
    );
}

#[tokio::test]
async fn lowered_scalar_prefix_matches_reference_and_survives_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let semantic = integrate(&Stream::constant(1_i64));

    let mut lowered = lower_scalar_prefix(table.clone(), "semantic_scalar", &semantic, 6)
        .await
        .expect("lower scalar prefix");
    assert_eq!(
        collect_runtime_scalar_prefix(&mut lowered, 6)
            .await
            .expect("read lowered scalar prefix"),
        ReferenceEvaluator::observe_prefix(&semantic, 6)
    );

    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntGroup);
    let mut reopened = RuntimeStream::with_table(table, lowered.namespace().to_string(), group)
        .await
        .expect("reopen scalar stream");
    assert_eq!(
        collect_runtime_scalar_prefix(&mut reopened, 6)
            .await
            .expect("read reopened scalar prefix"),
        ReferenceEvaluator::observe_prefix(&semantic, 6)
    );
}

#[tokio::test]
async fn lowered_zset_prefix_matches_reference_delta_and_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let snapshots = Stream::from_prefix(
        vec![
            zset([(1_i64, 1)]),
            zset([(1_i64, 1)]),
            zset([(1_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(2_i64, 1)]),
        ],
        zset([(2_i64, 1)]),
    );
    let output = aggregate_zset(
        &snapshots,
        |value| Some(value % 2),
        |_key, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
    );
    let delta_output = differentiate(&output);

    let lowered = lower_zset_prefix(table.clone(), "semantic_zset", &output, 6)
        .await
        .expect("lower zset prefix");
    let mut snapshot_stream = lowered.snapshot_stream().stream();
    let mut delta_stream = lowered.delta_stream().stream();

    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(table.clone(), &mut snapshot_stream, 6)
            .await
            .expect("read lowered snapshot prefix"),
        ReferenceEvaluator::observe_prefix(&output, 6)
    );
    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(table.clone(), &mut delta_stream, 6)
            .await
            .expect("read lowered delta prefix"),
        ReferenceEvaluator::observe_prefix(&delta_output, 6)
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_zset".to_string(),
            version: 0,
        },
    });
    let mut reopened =
        RuntimeStream::with_table(table, "semantic_zset".to_string(), snapshot_group)
            .await
            .expect("reopen zset snapshot stream");
    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(build_table(db), &mut reopened, 6)
            .await
            .expect("read reopened snapshot prefix"),
        ReferenceEvaluator::observe_prefix(&output, 6)
    );
}

#[tokio::test]
async fn lowered_set_and_indexed_prefixes_match_reference() {
    let db = build_db().await;
    let table = build_table(db);
    let bag = Stream::from_prefix(
        vec![
            zset([(1_i64, 2), (2_i64, 1)]),
            zset([(1_i64, 2), (2_i64, 1)]),
            zset([(2_i64, 1), (3_i64, 1)]),
        ],
        zset([(2_i64, 1), (3_i64, 1)]),
    );
    let set_stream = distinct_zset(&bag);
    let indexed_stream = arrange_by(&bag, |value| Some(value % 2));

    let lowered_set = lower_set_prefix(table.clone(), "semantic_set", &set_stream, 3)
        .await
        .expect("lower set prefix");
    let lowered_indexed =
        lower_indexed_prefix(table.clone(), "semantic_indexed", &indexed_stream, 3)
            .await
            .expect("lower indexed prefix");

    let mut set_snapshot = lowered_set.snapshot_stream().stream();
    let mut indexed_snapshot = lowered_indexed.snapshot_stream().stream();

    assert_eq!(
        collect_runtime_zset_prefix::<i64>(table.clone(), &mut set_snapshot, 3)
            .await
            .expect("read lowered set prefix"),
        ReferenceEvaluator::observe_prefix(
            &set_stream.lift("set_to_zset", |value| value.to_zset()),
            3
        )
    );
    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(table, &mut indexed_snapshot, 3)
            .await
            .expect("read lowered indexed prefix"),
        ReferenceEvaluator::observe_prefix(
            &indexed_stream.lift("indexed_to_pairs", |value| value.as_pairs()),
            3
        )
    );
}
