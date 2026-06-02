use super::*;

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
fn incremental_composition_covers_non_zero_preserving_queries() {
    let q1 = pointwise("plus-one", |value: &i64| value + 1);
    let q2 = pointwise("square", |value: &i64| value * value);
    let composed = q1.compose(q2.clone());
    let deltas = Stream::from_prefix(vec![0_i64, 2, -1, 3, -2, 0], 0);

    let direct_incremental = observe(&incrementalize(composed.clone()).apply(deltas.clone()), 7);
    let definition = observe(&differentiate(&composed.apply(integrate(&deltas))), 7);
    assert_eq!(
        direct_incremental, definition,
        "incrementalization still denotes D o up-arrow(Q) o I"
    );

    let composed_incrementals = observe(
        &incrementalize(q1).compose(incrementalize(q2)).apply(deltas),
        7,
    );
    assert_eq!(
        direct_incremental, composed_incrementals,
        "DBSP composition must also hold for non-zero-preserving queries"
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
fn aggregation_semantics_cover_duplicates_retractions_and_empty_groups() {
    let snapshots = Stream::from_prefix(
        vec![
            zset([(1_i64, 2), (3_i64, 1)]),
            zset([(1_i64, 1), (2_i64, 1), (3_i64, 1)]),
            zset([(1_i64, -1), (2_i64, 1), (4_i64, 2)]),
            zset([(1_i64, -1), (2_i64, -1)]),
        ],
        zset([(1_i64, -1), (2_i64, -1)]),
    );
    let counts = count_by_zset(&snapshots, |value| Some(value % 2));
    let positive_max = aggregate_zset(
        &snapshots,
        |value| Some(value % 2),
        |_key, rows| {
            rows.iter()
                .filter(|(_, weight)| *weight > 0)
                .map(|(value, _)| *value)
                .max()
        },
    );

    assert_eq!(
        observe(&counts, 4),
        vec![
            zset([((1_i64, 3_i64), 1)]),
            zset([((0_i64, 1_i64), 1), ((1_i64, 2_i64), 1)]),
            zset([((0_i64, 3_i64), 1), ((1_i64, 0_i64), 1)]),
            zset([((0_i64, 0_i64), 1), ((1_i64, 0_i64), 1)]),
        ]
    );
    assert_eq!(
        observe(&positive_max, 4),
        vec![
            zset([((1_i64, 3_i64), 1)]),
            zset([((0_i64, 2_i64), 1), ((1_i64, 3_i64), 1)]),
            zset([((0_i64, 4_i64), 1)]),
            zset::<(i64, i64)>([]),
        ]
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
