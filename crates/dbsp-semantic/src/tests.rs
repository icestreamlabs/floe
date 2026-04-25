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

#[test]
fn semantic_ir_is_explicit_and_normalized() {
    let left = Circuit::new("inc-delay", |input: Stream<i64>| {
        delay(&input.lift("inc", |value| value + 1))
    });
    let right = Circuit::new("inc-delay", |input: Stream<i64>| {
        delay(&input.lift("inc", |value| value + 1))
    });
    assert_eq!(left.plan(), right.plan());

    let transformed = incrementalize(circuit_d(
        pointwise("double", |value: &i64| value * 2).fanout(strict_delay::<i64>()),
    ));
    let plan = transformed.plan();
    assert!(plan.contains_kind(|kind| matches!(kind, StreamNodeKind::Input { .. })));
    assert!(plan.contains_kind(
        |kind| matches!(kind, StreamNodeKind::Lift { name } if name.as_ref() == "double")
    ));
    assert!(plan.contains_kind(|kind| matches!(kind, StreamNodeKind::Delay)));
    assert!(plan.contains_kind(|kind| matches!(kind, StreamNodeKind::BindInput { .. })));
}

#[test]
fn scalar_paper_operator_laws_hold() {
    let stream = Stream::from_fn("quadratic", |t| {
        let t = t as i64;
        t * t + t
    });
    let delayed = delay(&stream);
    let diff = differentiate(&stream);
    let integrated = integrate(&stream);

    assert_eq!(delayed.at(0), 0);
    for t in 0..7 {
        assert_eq!(delayed.at(t + 1), stream.at(t));
        assert_eq!(diff.at(t), stream.at(t) - delayed.at(t));
        assert_eq!(
            integrated.at(t),
            stream.at(t) + if t == 0 { 0 } else { integrated.at(t - 1) }
        );
    }

    assert_eq!(
        observe(&differentiate(&integrate(&stream)), 8),
        observe(&stream, 8)
    );

    let zero_initial = Stream::from_fn("zero-initial", |t| {
        let t = t as i64;
        if t == 0 { 0 } else { t * 3 - 1 }
    });
    assert_eq!(
        observe(&integrate(&differentiate(&zero_initial)), 8),
        observe(&zero_initial, 8)
    );
}

#[test]
fn reference_evaluator_handles_non_eventually_constant_infinite_streams() {
    let alternating = Stream::from_fn("alternating", |t| if t % 2 == 0 { 1_i64 } else { -1 });
    let cubic = Stream::from_fn("cubic", |t| {
        let t = t as i64;
        t * t * t - 2 * t
    });

    assert_eq!(observe(&integrate(&alternating), 6), vec![1, 0, 1, 0, 1, 0]);
    assert_eq!(
        observe(&differentiate(&integrate(&cubic)), 7),
        observe(&cubic, 7)
    );
}

#[test]
fn collection_domains_are_total_on_infinite_streams() {
    let bags = Stream::from_fn("bags", |t| {
        let t = t as i64;
        zset([(t % 3, 1), ((t + 1) % 3, if t % 2 == 0 { 1 } else { -1 })])
    });
    let sets = distinct_zset(&bags);
    let indexed = arrange_by(&bags, |value| Some(value % 2));
    let nested = bags.lift("nest", |value| zset([((0_i64, value.clone()), 1)]));

    assert_eq!(
        observe(&differentiate(&integrate(&bags)), 6),
        observe(&bags, 6)
    );
    assert_eq!(
        observe(&differentiate(&integrate(&indexed)), 6),
        observe(&indexed, 6)
    );
    assert_eq!(
        observe(&delay(&nested), 1),
        vec![zset::<(i64, ZSet<i64>)>([])]
    );
    assert_eq!(observe(&union_set(&sets, &sets), 4), observe(&sets, 4));
}

#[test]
fn circuit_transforms_match_paper_equations() {
    let input = Stream::from_fn("input", |t| t as i64 - 2);
    let query = pointwise("double", |value: &i64| value * 2)
        .compose(pointwise("shift", |value: &i64| value + 3));

    assert_eq!(
        observe(&strict_delay::<i64>().apply(input.clone()), 6),
        observe(&delay(&input), 6)
    );
    assert_eq!(
        observe(&circuit_d(query.clone()).apply(input.clone()), 6),
        observe(&differentiate(&query.apply(input.clone())), 6)
    );
    assert_eq!(
        observe(&circuit_i(query.clone()).apply(input.clone()), 6),
        observe(&integrate(&query.apply(input.clone())), 6)
    );

    let deltas = Stream::from_fn("delta-input", |t| if t % 3 == 0 { 2_i64 } else { -1 });
    assert_eq!(
        observe(&incrementalize(query.clone()).apply(deltas.clone()), 8),
        observe(&differentiate(&query.apply(integrate(&deltas))), 8)
    );

    let fanout = identity::<i64>().fanout(pointwise("negate", |value: &i64| -value));
    assert_eq!(
        observe(&fanout.apply(input.clone()), 5),
        observe(&input.lift("fanout", |value| (*value, -*value)), 5)
    );

    let add = add_circuit::<i64>();
    let pairs = Stream::from_fn("pairs", |t| (t as i64, (t as i64) * 10));
    assert_eq!(observe(&add.apply(pairs), 5), vec![0, 11, 22, 33, 44]);
}

#[test]
fn incremental_composition_matches_paper_equation() {
    let q1 = pointwise("double", |value: &i64| value * 2);
    let q2 = pointwise("square", |value: &i64| value * value);
    let composed = q1.compose(q2.clone());
    let deltas = Stream::from_prefix(vec![0_i64, 2, -1, 3, -2, 0], 0);

    assert_eq!(
        observe(&incrementalize(composed.clone()).apply(deltas.clone()), 7),
        observe(
            &incrementalize(q1)
                .compose(incrementalize(q2))
                .apply(deltas.clone()),
            7,
        ),
        "DBSP requires (Q2 o Q1)Delta = Q2Delta o Q1Delta"
    );
    assert_eq!(
        observe(&incrementalize(composed.clone()).apply(deltas.clone()), 7),
        observe(&differentiate(&composed.apply(integrate(&deltas))), 7,),
        "incrementalization must denote D o up-arrow(Q) o I"
    );
}

#[test]
fn semantic_queries_cover_sets_bags_and_indexes() {
    let bag = Stream::constant(zset([(1_i64, 2), (2, 1), (3, 1)]));
    let mapped = map_zset(&bag, |value| value * 10);
    let filtered = filter_zset(&bag, |value| *value >= 2);
    let distinct = distinct_zset(&bag);
    let arranged = arrange_by(&bag, |value| Some(value % 2));
    let looked_up = lookup_index(&arranged, 1_i64);

    assert_eq!(mapped.at(0), zset([(10_i64, 2), (20, 1), (30, 1)]));
    assert_eq!(filtered.at(0), zset([(2_i64, 1), (3_i64, 1)]));
    assert_eq!(distinct.at(0), set([1_i64, 2, 3]));
    assert_eq!(looked_up.at(0), zset([(1_i64, 2), (3_i64, 1)]));

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
fn relational_collection_laws_cover_negative_weights() {
    let set_stream = Stream::constant(set([1_i64, 2, 3]));
    let composed_map = map_set(&map_set(&set_stream, |value| value + 1), |value| value * 10);
    let direct_map = map_set(&set_stream, |value| (value + 1) * 10);
    assert_eq!(composed_map.at(0), direct_map.at(0));
    assert_eq!(
        filter_set(&set_stream, |value| value % 2 == 1).at(0),
        set([1_i64, 3])
    );

    let left = Stream::constant(zset([(1_i64, 2), (2_i64, -1), (3_i64, 1)]));
    let right = Stream::constant(zset([(1_i64, -1), (2_i64, 3), (3_i64, 1)]));
    assert_eq!(
        union_zset(&left, &right).at(0),
        zset([(1_i64, 1), (2_i64, 2), (3_i64, 2)])
    );
    assert_eq!(
        join_zset(
            &left,
            &right,
            |value| Some(*value),
            |value| Some(*value),
            |_, _| true,
            |left, right| (*left, *right),
        )
        .at(0),
        zset([
            ((1_i64, 1_i64), -2),
            ((2_i64, 2_i64), -3),
            ((3_i64, 3_i64), 1)
        ])
    );
    assert_eq!(
        distinct_zset(&Stream::constant(zset([
            (1_i64, -2),
            (2_i64, 0),
            (3_i64, 2)
        ])))
        .at(0),
        set([3_i64])
    );
}

#[test]
fn indexed_semantics_match_non_indexed_reference_formulations() {
    let left = Stream::constant(zset([(10_i64, 1), (11_i64, 2), (20_i64, 1), (30_i64, -1)]));
    let right = Stream::constant(zset([(100_i64, 1), (101_i64, 1), (200_i64, 2)]));

    let arranged = arrange_by(&left, |value| Some(value / 10));
    assert_eq!(
        lookup_index(&arranged, 1_i64).at(0),
        zset([(10_i64, 1), (11_i64, 2)])
    );
    assert_eq!(
        lookup_index(&arranged, 2_i64).at(0),
        filter_zset(&left, |value| value / 10 == 2).at(0)
    );

    let indexed_join = join_indexed(
        &arrange_by(&left, |value| Some(value / 10)),
        &arrange_by(&right, |value| Some(value / 100)),
        |key, left, right| (*key, *left, *right),
    )
    .at(0);
    let plain_join = join_zset(
        &left,
        &right,
        |value| Some(value / 10),
        |value| Some(value / 100),
        |_, _| true,
        |left, right| (left / 10, *left, *right),
    )
    .at(0);
    assert_eq!(indexed_join, plain_join);
}

#[test]
fn aggregation_semantics_cover_additive_and_non_additive_behaviors() {
    let bag = Stream::constant(zset([(1_i64, 2), (2_i64, 1), (3_i64, -1), (4_i64, 1)]));
    let counts = count_by_zset(&bag, |value| Some(value % 2));
    let maxima = aggregate_zset(
        &bag,
        |value| Some(value % 2),
        |_key, rows| {
            rows.iter()
                .filter(|(_, weight)| *weight > 0)
                .map(|(value, _)| *value)
                .max()
        },
    );

    assert_eq!(
        counts.at(0),
        zset([((0_i64, 2_i64), 1), ((1_i64, 1_i64), 1)])
    );
    assert_eq!(
        maxima.at(0),
        zset([((0_i64, 4_i64), 1), ((1_i64, 1_i64), 1)])
    );
}

#[test]
fn incrementalization_matches_reference_for_scalar_and_collection_circuits() {
    let scalar_query = pointwise("square", |value: &i64| value * value)
        .compose(pointwise("shift", |value: &i64| value + 1));
    let scalar_deltas = Stream::from_prefix(vec![1_i64, 0, -1, 2, -2], 1_i64);
    assert_eq!(
        observe(
            &incrementalize(scalar_query.clone()).apply(scalar_deltas.clone()),
            6
        ),
        observe(
            &differentiate(&scalar_query.apply(integrate(&scalar_deltas))),
            6,
        )
    );

    let count_query = Circuit::new("parity-count", |input: Stream<ZSet<i64>>| {
        let filtered = filter_zset(&input, |value| *value >= 0);
        let projected = map_zset(&filtered, |value| value % 2);
        count_by_zset(&projected, |value| Some(*value))
    });
    let count_deltas = Stream::from_prefix(
        vec![
            zset([(1_i64, 1)]),
            zset([(2_i64, 1)]),
            zset([(1_i64, -1)]),
            zset::<i64>([]),
        ],
        zset::<i64>([]),
    );
    assert_eq!(
        observe(
            &incrementalize(count_query.clone()).apply(count_deltas.clone()),
            4,
        ),
        observe(
            &differentiate(&count_query.apply(integrate(&count_deltas))),
            4,
        )
    );

    let max_query = Circuit::new("parity-max", |input: Stream<ZSet<i64>>| {
        aggregate_zset(
            &input,
            |value| Some(value % 2),
            |_key, rows| {
                rows.iter()
                    .filter(|(_, weight)| *weight > 0)
                    .map(|(value, _)| *value)
                    .max()
            },
        )
    });
    let max_deltas = Stream::from_prefix(
        vec![
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1), (5_i64, 1)]),
            zset([(1_i64, 1), (2_i64, -1), (5_i64, 1)]),
            zset([(1_i64, 1), (5_i64, 1), (8_i64, 1)]),
        ],
        zset([(1_i64, 1), (5_i64, 1), (8_i64, 1)]),
    );
    assert_eq!(
        observe(
            &incrementalize(max_query.clone()).apply(max_deltas.clone()),
            4
        ),
        observe(&differentiate(&max_query.apply(integrate(&max_deltas))), 4)
    );
}

#[test]
fn nested_relations_support_flat_map_and_unnest_laws() {
    let bag = Stream::constant(zset([(1_i64, 2), (2_i64, 1)]));
    assert_eq!(
        flat_map_zset(&bag, |value| [(*value, 1), (value * 10, 1)]).at(0),
        zset([(1_i64, 2), (2_i64, 1), (10_i64, 2), (20_i64, 1)])
    );
    assert_eq!(
        flat_map_zset(&bag, |value| [(value * 10, 1)]).at(0),
        map_zset(&bag, |value| value * 10).at(0)
    );

    let nested = Stream::constant(zset([
        ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 2),
        ((2_i64, zset([(30_i64, 2)])), 1),
    ]));
    assert_eq!(
        unnest_zset(&nested).at(0),
        zset([
            ((1_i64, 10_i64), 2),
            ((1_i64, 20_i64), 2),
            ((2_i64, 30_i64), 2),
        ])
    );
}

#[test]
fn recursive_semantics_require_guarded_feedback() {
    let reachability = reachability_stream();
    assert_eq!(
        observe(&reachability, 3),
        vec![
            zset([
                ((1_i64, 2_i64), 1),
                ((2_i64, 3_i64), 1),
                ((3_i64, 4_i64), 1),
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

    let oscillating = oscillating_collection_stream();
    assert_eq!(
        observe(&oscillating, 5),
        vec![
            zset([("x".to_string(), 1)]),
            zset::<String>([]),
            zset([("x".to_string(), 1)]),
            zset::<String>([]),
            zset([("x".to_string(), 1)]),
        ]
    );

    let invalid = catch_unwind(AssertUnwindSafe(|| {
        let unguarded = feedback("unguarded", |state: Stream<i64>| state);
        unguarded.at(0)
    }));
    let panic = invalid.expect_err("unguarded recursion should panic");
    let panic_message = panic
        .downcast_ref::<String>()
        .map(|message| message.as_str())
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .expect("panic payload should contain a message");
    assert!(panic_message.contains("unguarded semantic feedback"));
}

#[tokio::test]
async fn lowered_guarded_monotonic_collection_recursion_matches_reference_and_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let reachability = reachability_stream();
    let delta_reachability = differentiate(&reachability);

    let lowered = lower_zset(table.clone(), "semantic_reachability", &reachability)
        .await
        .expect("lower guarded monotonic collection recursion");
    assert_eq!(
        lowered
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered monotonic recursion snapshot prefix"),
        observe(&reachability, 4)
    );
    assert_eq!(
        lowered
            .collect_delta_prefix(4)
            .await
            .expect("read lowered monotonic recursion delta prefix"),
        observe(&delta_reachability, 4)
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_reachability".to_string(),
            version: 0,
        },
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_reachability/delta".to_string(),
            version: 0,
        },
    });
    let mut reopened_snapshot = RuntimeStream::with_table(
        table.clone(),
        "semantic_reachability".to_string(),
        snapshot_group,
    )
    .await
    .expect("reopen monotonic recursion snapshot stream");
    let mut reopened_delta = RuntimeStream::with_table(
        table,
        "semantic_reachability/delta".to_string(),
        delta_group,
    )
    .await
    .expect("reopen monotonic recursion delta stream");

    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(
            build_table(db.clone()),
            &mut reopened_snapshot,
            4,
        )
        .await
        .expect("read reopened monotonic recursion snapshot prefix"),
        observe(&reachability, 4)
    );
    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(build_table(db), &mut reopened_delta, 4)
            .await
            .expect("read reopened monotonic recursion delta prefix"),
        observe(&delta_reachability, 4)
    );
}

#[tokio::test]
async fn lowered_guarded_non_monotonic_collection_recursion_matches_reference_and_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let oscillating = oscillating_collection_stream();
    let delta_oscillating = differentiate(&oscillating);

    let lowered = lower_zset(table.clone(), "semantic_oscillating", &oscillating)
        .await
        .expect("lower guarded non-monotonic collection recursion");
    assert_eq!(
        lowered
            .collect_snapshot_prefix(6)
            .await
            .expect("read lowered non-monotonic recursion snapshot prefix"),
        observe(&oscillating, 6)
    );
    assert_eq!(
        lowered
            .collect_delta_prefix(6)
            .await
            .expect("read lowered non-monotonic recursion delta prefix"),
        observe(&delta_oscillating, 6)
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_oscillating".to_string(),
            version: 0,
        },
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_oscillating/delta".to_string(),
            version: 0,
        },
    });
    let mut reopened_snapshot = RuntimeStream::with_table(
        table.clone(),
        "semantic_oscillating".to_string(),
        snapshot_group,
    )
    .await
    .expect("reopen non-monotonic recursion snapshot stream");
    let mut reopened_delta =
        RuntimeStream::with_table(table, "semantic_oscillating/delta".to_string(), delta_group)
            .await
            .expect("reopen non-monotonic recursion delta stream");

    assert_eq!(
        collect_runtime_zset_prefix::<String>(build_table(db.clone()), &mut reopened_snapshot, 6)
            .await
            .expect("read reopened non-monotonic recursion snapshot prefix"),
        observe(&oscillating, 6)
    );
    assert_eq!(
        collect_runtime_zset_prefix::<String>(build_table(db), &mut reopened_delta, 6)
            .await
            .expect("read reopened non-monotonic recursion delta prefix"),
        observe(&delta_oscillating, 6)
    );
}

#[test]
fn semantic_windows_cover_overlaps_empty_windows_and_changing_prefixes() {
    let snapshots = Stream::from_prefix(
        vec![
            zset::<Event>([]),
            zset([(event(1, 0, 5), 1)]),
            zset([(event(1, 0, 5), 1), (event(1, 4, 7), 1)]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
                (event(1, -1, 99), 1),
            ]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
                (event(1, -1, 99), 1),
            ]),
        ],
        zset([
            (event(1, 0, 5), 1),
            (event(1, 4, 7), 1),
            (event(1, 10, 9), 1),
            (event(1, -1, 99), 1),
        ]),
    );
    let sliding = sliding_window_aggregate(
        &snapshots,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
        5,
    );
    let tumbling = tumbling_window_aggregate(
        &snapshots,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
    );

    assert_eq!(
        observe(&sliding, 5),
        vec![
            zset::<(Window<i64>, i64)>([]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                1,
            )]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                2,
            )]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 15,
                    },
                    1,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 15,
                    },
                    1,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
        ]
    );
    assert_eq!(
        observe(&tumbling, 5),
        vec![
            zset::<(Window<i64>, i64)>([]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                1,
            )]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                2,
            )]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
        ]
    );
}

async fn handle_versions(stream: &mut RuntimeStream<ZSetHandle>, len: usize) -> Vec<u64> {
    let mut versions = Vec::with_capacity(len);
    for t in 0..len {
        versions.push(
            stream
                .get(t as i64)
                .await
                .expect("read handle version")
                .version,
        );
    }
    versions
}

#[tokio::test]
async fn lowered_scalar_and_circuit_execution_match_reference_and_survive_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let input = Stream::from_prefix(vec![0_i64, 1, 3, 3, 6, 6, 6, 7], 7);
    let query = pointwise("double", |value: &i64| value * 2)
        .fanout(strict_delay::<i64>())
        .compose(add_circuit());
    let output = query.apply(input.clone());
    let delta_output = circuit_d(query.clone()).apply(input.clone());
    let reintegrated_output = circuit_i(circuit_d(query.clone())).apply(input.clone());
    let incremental_output = incrementalize(query.clone()).apply(differentiate(&input));

    let lowered_output = lower_scalar(table.clone(), "semantic_scalar_query", &output)
        .await
        .expect("lower scalar query");
    let lowered_delta = lower_scalar(table.clone(), "semantic_scalar_delta", &delta_output)
        .await
        .expect("lower scalar delta query");
    let lowered_reintegrated = lower_scalar(
        table.clone(),
        "semantic_scalar_reintegrated",
        &reintegrated_output,
    )
    .await
    .expect("lower scalar reintegrated query");
    let lowered_incremental = lower_scalar(
        table.clone(),
        "semantic_scalar_incremental",
        &incremental_output,
    )
    .await
    .expect("lower scalar incremental query");

    assert_eq!(
        lowered_output
            .collect_prefix(8)
            .await
            .expect("read lowered scalar query"),
        observe(&output, 8)
    );
    assert_eq!(
        lowered_delta
            .collect_prefix(8)
            .await
            .expect("read lowered scalar delta query"),
        observe(&delta_output, 8)
    );
    assert_eq!(
        lowered_reintegrated
            .collect_prefix(8)
            .await
            .expect("read lowered scalar reintegrated query"),
        observe(&reintegrated_output, 8)
    );
    assert_eq!(
        lowered_incremental
            .collect_prefix(8)
            .await
            .expect("read lowered scalar incremental query"),
        observe(&incremental_output, 8)
    );

    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntGroup);
    let mut reopened =
        RuntimeStream::with_table(table, lowered_output.namespace().to_string(), group)
            .await
            .expect("reopen scalar query stream");
    assert_eq!(
        collect_runtime_scalar_prefix(&mut reopened, 8)
            .await
            .expect("read reopened scalar query"),
        observe(&output, 8)
    );
}

#[tokio::test]
async fn lowered_guarded_feedback_execution_matches_reference_and_survives_reopen() {
    let db = build_db().await;
    let table = build_table(db.clone());
    let recursive = feedback("guarded_toggle", |_state| Stream::constant(1_i64));
    let guarded = feedback("guarded_feedback", move |state| {
        subtract(&recursive, &delay(&state))
    });

    let lowered = lower_scalar(table.clone(), "semantic_guarded_feedback", &guarded)
        .await
        .expect("lower guarded feedback");
    assert_eq!(
        lowered
            .collect_prefix(8)
            .await
            .expect("read lowered guarded feedback"),
        observe(&guarded, 8)
    );

    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntGroup);
    let mut reopened = RuntimeStream::with_table(table, lowered.namespace().to_string(), group)
        .await
        .expect("reopen guarded feedback stream");
    assert_eq!(
        collect_runtime_scalar_prefix(&mut reopened, 8)
            .await
            .expect("read reopened guarded feedback"),
        observe(&guarded, 8)
    );
}

#[tokio::test]
async fn lowered_zset_execution_matches_reference_delta_versions_and_reopen() {
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

    let lowered = lower_zset(table.clone(), "semantic_zset", &output)
        .await
        .expect("lower zset execution");
    assert_eq!(
        lowered
            .collect_snapshot_prefix(6)
            .await
            .expect("read lowered zset snapshot prefix"),
        observe(&output, 6)
    );
    assert_eq!(
        lowered
            .collect_delta_prefix(6)
            .await
            .expect("read lowered zset delta prefix"),
        observe(&delta_output, 6)
    );

    let mut snapshot_stream = lowered.snapshot_stream().stream();
    let mut delta_stream = lowered.delta_stream().stream();
    assert_eq!(
        handle_versions(&mut snapshot_stream, 6).await,
        vec![1, 1, 1, 2, 2, 3]
    );
    assert_eq!(
        handle_versions(&mut delta_stream, 6).await,
        vec![1, 0, 0, 2, 0, 3]
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_zset".to_string(),
            version: 0,
        },
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_zset/delta".to_string(),
            version: 0,
        },
    });
    let mut reopened_snapshot =
        RuntimeStream::with_table(table.clone(), "semantic_zset".to_string(), snapshot_group)
            .await
            .expect("reopen zset snapshot stream");
    let mut reopened_delta =
        RuntimeStream::with_table(table, "semantic_zset/delta".to_string(), delta_group)
            .await
            .expect("reopen zset delta stream");

    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(
            build_table(db.clone()),
            &mut reopened_snapshot,
            6
        )
        .await
        .expect("read reopened zset snapshot prefix"),
        observe(&output, 6)
    );
    assert_eq!(
        collect_runtime_zset_prefix::<(i64, i64)>(build_table(db), &mut reopened_delta, 6)
            .await
            .expect("read reopened zset delta prefix"),
        observe(&delta_output, 6)
    );
}

#[tokio::test]
async fn lowered_relational_execution_matches_reference() {
    let db = build_db().await;
    let table = build_table(db);
    let left = Stream::from_prefix(
        vec![
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 2), (2_i64, 1), (3_i64, -1)]),
            zset([(1_i64, 2), (2_i64, 1), (3_i64, -1)]),
            zset([(2_i64, 1), (3_i64, -1), (4_i64, 1)]),
            zset([(2_i64, 1), (3_i64, -1), (4_i64, 1)]),
            zset([(4_i64, 1)]),
            zset([(4_i64, 1)]),
        ],
        zset([(4_i64, 1)]),
    );
    let right = Stream::from_prefix(
        vec![
            zset([(1_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1), (3_i64, 1)]),
            zset([(2_i64, 1), (4_i64, 1)]),
            zset([(2_i64, 1), (4_i64, 1)]),
            zset([(4_i64, 1)]),
            zset([(4_i64, 1)]),
        ],
        zset([(4_i64, 1)]),
    );

    let bag_union = union_zset(&left, &right);
    let bag_map_filter = map_zset(&filter_zset(&left, |value| *value >= 2), |value| value * 10);
    let bag_join = join_zset(
        &left,
        &right,
        |left| Some(left % 2),
        |right| Some(right % 2),
        |left, right| left <= right,
        |left, right| (*left, *right),
    );

    let left_set = distinct_zset(&left);
    let right_set = distinct_zset(&right);
    let set_union = union_set(&left_set, &right_set);
    let set_map_filter = map_set(&filter_set(&left_set, |value| *value >= 2), |value| {
        value * 100
    });
    let set_joined = join_set(
        &left_set,
        &right_set,
        |left| Some(left % 2),
        |right| Some(right % 2),
        |left, right| left <= right,
        |left, right| (*left, *right),
    );

    let lowered_bag_union = lower_zset(table.clone(), "semantic_bag_union", &bag_union)
        .await
        .expect("lower bag union");
    let lowered_bag_map_filter =
        lower_zset(table.clone(), "semantic_bag_map_filter", &bag_map_filter)
            .await
            .expect("lower bag map/filter");
    let lowered_bag_join = lower_zset(table.clone(), "semantic_bag_join", &bag_join)
        .await
        .expect("lower bag join");
    let lowered_set_union = lower_set(table.clone(), "semantic_set_union", &set_union)
        .await
        .expect("lower set union");
    let lowered_set_map_filter =
        lower_set(table.clone(), "semantic_set_map_filter", &set_map_filter)
            .await
            .expect("lower set map/filter");
    let lowered_set_join = lower_set(table, "semantic_set_join", &set_joined)
        .await
        .expect("lower set join");

    assert_eq!(
        lowered_bag_union
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered bag union"),
        observe(&bag_union, 8)
    );
    assert_eq!(
        lowered_bag_map_filter
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered bag map/filter"),
        observe(&bag_map_filter, 8)
    );
    assert_eq!(
        lowered_bag_join
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered bag join"),
        observe(&bag_join, 8)
    );
    assert_eq!(
        lowered_set_union
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered set union"),
        observe(
            &set_union.lift("set_union_to_zset", |value| value.to_zset()),
            8
        )
    );
    assert_eq!(
        lowered_set_map_filter
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered set map/filter"),
        observe(
            &set_map_filter.lift("set_map_filter_to_zset", |value| value.to_zset()),
            8,
        )
    );
    assert_eq!(
        lowered_set_join
            .collect_snapshot_prefix(8)
            .await
            .expect("read lowered set join"),
        observe(
            &set_joined.lift("set_join_to_zset", |value| value.to_zset()),
            8
        )
    );
}

#[tokio::test]
async fn lowered_set_and_indexed_execution_matches_reference() {
    let db = build_db().await;
    let table = build_table(db);
    let bag = Stream::from_prefix(
        vec![
            zset([(1_i64, 2), (2_i64, 1)]),
            zset([(1_i64, 2), (2_i64, 1)]),
            zset([(2_i64, 1), (3_i64, 1)]),
            zset([(2_i64, 1), (3_i64, 1)]),
        ],
        zset([(2_i64, 1), (3_i64, 1)]),
    );
    let other = Stream::from_prefix(
        vec![
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1)]),
            zset([(1_i64, 1), (3_i64, 1)]),
            zset([(1_i64, 1), (3_i64, 1)]),
        ],
        zset([(1_i64, 1), (3_i64, 1)]),
    );
    let set_stream = distinct_zset(&bag);
    let indexed_stream = arrange_by(&bag, |value| Some(value % 2));
    let lookup_stream = lookup_index(&indexed_stream, 1_i64);
    let indexed_joined = join_indexed(
        &indexed_stream,
        &arrange_by(&other, |value| Some(value % 2)),
        |key, left, right| (*key, *left, *right),
    );

    let lowered_set = lower_set(table.clone(), "semantic_set", &set_stream)
        .await
        .expect("lower set execution");
    let lowered_indexed = lower_indexed(table.clone(), "semantic_indexed", &indexed_stream)
        .await
        .expect("lower indexed execution");
    let lowered_lookup = lower_zset(table.clone(), "semantic_lookup", &lookup_stream)
        .await
        .expect("lower indexed lookup");
    let lowered_indexed_join = lower_zset(table, "semantic_indexed_join", &indexed_joined)
        .await
        .expect("lower indexed join");

    assert_eq!(
        lowered_set
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered set execution"),
        observe(&set_stream.lift("set_to_zset", |value| value.to_zset()), 4)
    );
    assert_eq!(
        lowered_indexed
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered indexed execution"),
        observe(
            &indexed_stream.lift("indexed_to_pairs", |value| value.as_pairs()),
            4,
        )
    );
    assert_eq!(
        lowered_lookup
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered indexed lookup"),
        observe(&lookup_stream, 4)
    );
    assert_eq!(
        lowered_indexed_join
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered indexed join"),
        observe(&indexed_joined, 4)
    );
}

#[tokio::test]
async fn lowered_nested_flat_map_unnest_and_window_execution_match_reference() {
    let db = build_db().await;
    let table = build_table(db.clone());

    let nested = Stream::from_prefix(
        vec![
            zset::<(i64, ZSet<i64>)>([]),
            zset([((1_i64, zset([(10_i64, 1)])), 1)]),
            zset([
                ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 1),
                ((2_i64, zset([(30_i64, 2)])), 1),
            ]),
            zset([
                ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 1),
                ((2_i64, zset([(30_i64, 2)])), 1),
            ]),
        ],
        zset([
            ((1_i64, zset([(10_i64, 1), (20_i64, 1)])), 1),
            ((2_i64, zset([(30_i64, 2)])), 1),
        ]),
    );
    let flat_mapped = flat_map_zset(
        &Stream::from_prefix(
            vec![
                zset([(1_i64, 1)]),
                zset([(1_i64, 1), (2_i64, 1)]),
                zset([(2_i64, 1)]),
            ],
            zset([(2_i64, 1)]),
        ),
        |value| [(*value * 10, 1), (*value * 10 + 1, -1)],
    );
    let unnested = unnest_zset(&nested);

    let lowered_nested = lower_zset(table.clone(), "semantic_nested", &nested)
        .await
        .expect("lower nested execution");
    let lowered_flat_map = lower_zset(table.clone(), "semantic_flat_map", &flat_mapped)
        .await
        .expect("lower flat_map execution");
    let lowered_unnest = lower_zset(table.clone(), "semantic_unnest", &unnested)
        .await
        .expect("lower unnest execution");

    assert_eq!(
        lowered_nested
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered nested execution"),
        observe(&nested, 4)
    );
    assert_eq!(
        lowered_flat_map
            .collect_snapshot_prefix(3)
            .await
            .expect("read lowered flat_map execution"),
        observe(&flat_mapped, 3)
    );
    assert_eq!(
        lowered_unnest
            .collect_snapshot_prefix(4)
            .await
            .expect("read lowered unnest execution"),
        observe(&unnested, 4)
    );

    let window_input = Stream::from_prefix(
        vec![
            zset::<Event>([]),
            zset([(event(1, 0, 5), 1)]),
            zset([(event(1, 0, 5), 1), (event(1, 4, 7), 1)]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
            ]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
            ]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
            ]),
        ],
        zset([
            (event(1, 0, 5), 1),
            (event(1, 4, 7), 1),
            (event(1, 10, 9), 1),
        ]),
    );
    let windows = sliding_window_aggregate(
        &window_input,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
        5,
    );
    let delta_windows = differentiate(&windows);
    let lowered_windows = lower_zset(table.clone(), "semantic_windows", &windows)
        .await
        .expect("lower window execution");

    assert_eq!(
        lowered_windows
            .collect_snapshot_prefix(6)
            .await
            .expect("read lowered window snapshot execution"),
        observe(&windows, 6)
    );
    assert_eq!(
        lowered_windows
            .collect_delta_prefix(6)
            .await
            .expect("read lowered window delta execution"),
        observe(&delta_windows, 6)
    );

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup {
        default: ZSetHandle {
            ns: "semantic_windows".to_string(),
            version: 0,
        },
    });
    let mut reopened =
        RuntimeStream::with_table(table, "semantic_windows".to_string(), snapshot_group)
            .await
            .expect("reopen window snapshot stream");
    assert_eq!(
        collect_runtime_zset_prefix::<(Window<i64>, i64)>(build_table(db), &mut reopened, 6)
            .await
            .expect("read reopened window snapshot execution"),
        observe(&windows, 6)
    );
}
