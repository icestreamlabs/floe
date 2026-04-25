use std::sync::Arc;

use crate::algebra::AbelianGroup;
use crate::stream::addition::StreamAddition;
use crate::stream::core::stream::{Stream, unregister_stream_evaluator_for_test};
use crate::stream::operations::basic::{
    delay, differentiate, incrementalize2, integrate, lift1, lift2, stream_elimination,
    stream_elimination_prefix, stream_elimination_range,
};
use crate::stream::tests::common::{IntegerGroup, build_db};

#[tokio::test]
async fn stream_addition_and_negation() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut left_source = Stream::new(db.clone(), "left_source", group.clone())
        .await
        .expect("create left stream");
    left_source.send(1).await.expect("send left t1");
    left_source.send(4).await.expect("send left t2");

    let left = delay(&left_source).await.expect("delay left stream");

    let mut right = Stream::new(db.clone(), "right", group.clone())
        .await
        .expect("create right stream");
    right.set_default(2).await.expect("set right default");

    let addition = StreamAddition::from_stream(&left);
    let mut sum = addition.add(&left, &right).await;
    assert_eq!(sum.current_time(), 2);
    assert_eq!(sum.semantic_horizon(), 3);
    assert_eq!(sum.get(0).await.expect("sum t0"), 2);
    assert_eq!(sum.get(1).await.expect("sum t1"), 2);
    assert_eq!(sum.get(2).await.expect("sum t2"), 3);
    assert_eq!(sum.get(3).await.expect("sum t3"), 6);
    assert_eq!(sum.get(4).await.expect("sum t4"), 2);

    let mut neg = addition.neg(&left).await;
    assert_eq!(neg.current_time(), 2);
    assert_eq!(neg.semantic_horizon(), 4);
    assert_eq!(neg.get(0).await.expect("neg t0"), 0);
    assert_eq!(neg.get(1).await.expect("neg t1"), 0);
    assert_eq!(neg.get(2).await.expect("neg t2"), -1);
    assert_eq!(neg.get(3).await.expect("neg t3"), -4);
    assert_eq!(neg.get(4).await.expect("neg t4"), 0);
}

#[tokio::test]
async fn delay_shifts_stream_values() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "delay_input", group.clone())
        .await
        .expect("create stream");
    source.send(5).await.expect("send t1");
    source.send(10).await.expect("send t2");
    source.send(15).await.expect("send t3");

    let mut delayed = delay(&source).await.expect("apply delay");
    assert_eq!(delayed.get(0).await.expect("t0"), 0);
    assert_eq!(delayed.get(1).await.expect("t1"), 0);
    assert_eq!(delayed.get(2).await.expect("t2"), 5);
    assert_eq!(delayed.get(3).await.expect("t3"), 10);
    assert_eq!(delayed.get(4).await.expect("t4"), 15);
    assert_eq!(delayed.get(5).await.expect("t5"), 0);
}

#[tokio::test]
async fn delay_emits_identity_at_t0_for_non_identity_initial_value() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "delay_non_identity_t0", group.clone())
        .await
        .expect("create stream");
    source.set_default(7).await.expect("set non-identity t0");
    source.send(9).await.expect("send t1");

    let mut delayed = delay(&source).await.expect("apply delay");
    assert_eq!(delayed.get(0).await.expect("t0"), 0);
    assert_eq!(delayed.get(1).await.expect("t1"), 7);
    assert_eq!(delayed.get(2).await.expect("t2"), 9);
}

#[tokio::test]
async fn differentiate_computes_deltas() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "differentiate_input", group.clone())
        .await
        .expect("create stream");
    source.send(2).await.expect("send t1");
    source.send(6).await.expect("send t2");
    source.send(9).await.expect("send t3");

    let mut diff = differentiate(&source).await.expect("apply diff");
    assert_eq!(diff.get(0).await.expect("t0"), 0);
    assert_eq!(diff.get(1).await.expect("t1"), 2);
    assert_eq!(diff.get(2).await.expect("t2"), 4);
    assert_eq!(diff.get(3).await.expect("t3"), 3);
    assert_eq!(diff.get(4).await.expect("t4"), -9);
    assert_eq!(diff.get(5).await.expect("t5"), 0);
}

#[tokio::test]
async fn integrate_accumulates_stream() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "integrate_input", group.clone())
        .await
        .expect("create stream");
    source.send(1).await.expect("send t1");
    source.send(2).await.expect("send t2");
    source.send(3).await.expect("send t3");

    let mut integrated = integrate(&source).await.expect("apply integrate");
    assert_eq!(integrated.get(0).await.expect("t0"), 0);
    assert_eq!(integrated.get(1).await.expect("t1"), 1);
    assert_eq!(integrated.get(2).await.expect("t2"), 3);
    assert_eq!(integrated.get(3).await.expect("t3"), 6);
    assert_eq!(integrated.get(4).await.expect("t4"), 6);
    assert_eq!(integrated.get(5).await.expect("t5"), 6);
}

#[tokio::test]
async fn integrate_advances_non_identity_tail_exactly() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "integrate_non_identity_tail", group.clone())
        .await
        .expect("create stream");
    source
        .set_default(1)
        .await
        .expect("set non-identity default");

    let mut integrated = integrate(&source).await.expect("integrate stream");
    assert_eq!(integrated.get(0).await.expect("t0"), 1);
    assert_eq!(integrated.get(1).await.expect("t1"), 2);
    assert_eq!(integrated.get(2).await.expect("t2"), 3);

    integrated.advance_to(5).await.expect("advance derived");
    assert_eq!(integrated.get(5).await.expect("t5"), 6);
}

#[tokio::test]
async fn differentiate_integrate_roundtrip_with_non_identity_tail() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "diff_integrate_roundtrip", group.clone())
        .await
        .expect("create stream");
    source
        .set_default(2)
        .await
        .expect("set non-identity t0 and tail");
    source.send(3).await.expect("send t1");
    source.send(5).await.expect("send t2");

    let integrated = integrate(&source).await.expect("integrate source");
    let mut roundtrip = differentiate(&integrated)
        .await
        .expect("differentiate integrated source");

    for t in 0..=6 {
        assert_eq!(
            roundtrip.get(t).await.expect("roundtrip value"),
            source.get(t).await.expect("source value"),
            "roundtrip mismatch at t={t}"
        );
    }
}

#[tokio::test]
async fn integrate_differentiate_roundtrip_for_zero_initial_stream() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "integrate_diff_zero_initial", group.clone())
        .await
        .expect("create stream");
    source.send(4).await.expect("send t1");
    source.send(-2).await.expect("send t2");
    source.send(6).await.expect("send t3");

    let diff = differentiate(&source).await.expect("differentiate source");
    let mut roundtrip = integrate(&diff)
        .await
        .expect("integrate differentiated source");

    for t in 0..=6 {
        assert_eq!(
            roundtrip.get(t).await.expect("roundtrip value"),
            source.get(t).await.expect("source value"),
            "I(D(x)) must equal zero-initial x at t={t}"
        );
    }
}

#[tokio::test]
async fn differentiate_is_input_minus_delay() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "differentiate_minus_delay", group.clone())
        .await
        .expect("create stream");
    source
        .set_default(4)
        .await
        .expect("set non-identity t0 and tail");
    source.send(7).await.expect("send t1");
    source.send(1).await.expect("send t2");

    let mut diff = differentiate(&source).await.expect("differentiate source");
    let mut delayed = delay(&source).await.expect("delay source");

    for t in 0..=5 {
        let current = source.get(t).await.expect("source value");
        let previous = delayed.get(t).await.expect("delayed value");
        assert_eq!(
            diff.get(t).await.expect("diff value"),
            current - previous,
            "differentiate mismatch at t={t}"
        );
    }
}

#[tokio::test]
async fn lift1_applies_function_to_stream() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "lift1_input", group.clone())
        .await
        .expect("create stream");
    source.send(3).await.expect("send t1");
    source.send(5).await.expect("send t2");

    let delayed = delay(&source).await.expect("delay source");

    let mut lifted = lift1(&delayed, group.clone(), |value: &i64| value * 2)
        .await
        .expect("apply lift1");
    assert_eq!(lifted.current_time(), 2);
    assert_eq!(lifted.semantic_horizon(), 3);
    assert_eq!(lifted.get(0).await.expect("t0"), 0);
    assert_eq!(lifted.get(1).await.expect("t1"), 0);
    assert_eq!(lifted.get(2).await.expect("t2"), 6);
    assert_eq!(lifted.get(3).await.expect("t3"), 10);
    assert_eq!(lifted.get(4).await.expect("t4"), 0);
}

#[tokio::test]
async fn lift2_combines_two_streams() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut left_source = Stream::new(db.clone(), "lift2_left", group.clone())
        .await
        .expect("create left");
    left_source.send(1).await.expect("left t1");
    left_source.send(3).await.expect("left t2");
    let left = delay(&left_source).await.expect("delay left");

    let mut right = Stream::new(db.clone(), "lift2_right", group.clone())
        .await
        .expect("create right");
    right.set_default(5).await.expect("set right default");
    right.send(5).await.expect("right t1");
    right.send(7).await.expect("right t2");

    let mut combined = lift2(&left, &right, group.clone(), |l: &i64, r: &i64| l + r)
        .await
        .expect("apply lift2");
    assert_eq!(combined.current_time(), 2);
    assert_eq!(combined.semantic_horizon(), 3);
    assert_eq!(combined.get(0).await.expect("t0"), 5);
    assert_eq!(combined.get(1).await.expect("t1"), 5);
    assert_eq!(combined.get(2).await.expect("t2"), 8);
    assert_eq!(combined.get(3).await.expect("t3"), 8);
    assert_eq!(combined.get(4).await.expect("t4"), 5);
}

#[tokio::test]
async fn runtime_incrementalize2_matches_d_lift_i_definition() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut left_delta = Stream::new(db.clone(), "incrementalize2_law_left", group.clone())
        .await
        .expect("create left delta");
    left_delta.send(1).await.expect("left t1");
    left_delta.send(2).await.expect("left t2");
    left_delta.send(-1).await.expect("left t3");

    let mut right_delta = Stream::new(db.clone(), "incrementalize2_law_right", group.clone())
        .await
        .expect("create right delta");
    right_delta.send(10).await.expect("right t1");
    right_delta.send(-5).await.expect("right t2");
    right_delta.send(20).await.expect("right t3");

    let mut optimized = incrementalize2(
        &left_delta,
        &right_delta,
        group.clone(),
        |l: &i64, r: &i64| l * r,
    )
    .await
    .expect("incrementalized product");

    let integrated_left = integrate(&left_delta).await.expect("integrate left deltas");
    let integrated_right = integrate(&right_delta)
        .await
        .expect("integrate right deltas");
    let lifted_query = lift2(
        &integrated_left,
        &integrated_right,
        group.clone(),
        |l: &i64, r: &i64| l * r,
    )
    .await
    .expect("lift product query");
    let mut reference = differentiate(&lifted_query)
        .await
        .expect("differentiate lifted query");

    for t in 0..=6 {
        assert_eq!(
            optimized.get(t).await.expect("optimized value"),
            reference.get(t).await.expect("reference value"),
            "incrementalize2 must match D(up-arrow(Q)(I(input))) at t={t}"
        );
    }
}

#[tokio::test]
async fn incrementalize2_preserves_tail_cancellation() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut left = Stream::new(db.clone(), "incrementalize2_left", group.clone())
        .await
        .expect("create left");
    left.send(1).await.expect("left t1");
    left.send(2).await.expect("left t2");

    let mut right = Stream::new(db.clone(), "incrementalize2_right", group.clone())
        .await
        .expect("create right");
    right.send(10).await.expect("right t1");
    right.send(20).await.expect("right t2");

    let mut delta_product = incrementalize2(&left, &right, group.clone(), |l: &i64, r: &i64| l * r)
        .await
        .expect("incrementalize product");

    assert_eq!(delta_product.current_time(), 2);
    assert_eq!(delta_product.semantic_horizon(), 3);
    assert_eq!(delta_product.get(0).await.expect("t0"), 0);
    assert_eq!(delta_product.get(1).await.expect("t1"), 10);
    assert_eq!(delta_product.get(2).await.expect("t2"), 80);
    assert_eq!(delta_product.get(3).await.expect("t3"), 0);
    assert_eq!(delta_product.get(4).await.expect("t4"), 0);
}

#[tokio::test]
async fn derived_stream_reopens_with_registered_evaluator() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "persist_delay_input", group.clone())
        .await
        .expect("create stream");
    source.send(5).await.expect("send t1");
    source.send(10).await.expect("send t2");
    source.send(15).await.expect("send t3");

    let mut delayed = delay(&source).await.expect("apply delay");
    let namespace = delayed.namespace().to_string();
    delayed.flush().await.expect("flush delayed stream");

    let mut reopened = Stream::new(db, namespace, group)
        .await
        .expect("reopen delayed stream");
    assert_eq!(reopened.current_time(), 3);
    assert_eq!(reopened.semantic_horizon(), 4);
    assert_eq!(reopened.get(4).await.expect("t4"), 15);
    assert_eq!(reopened.get(5).await.expect("t5"), 0);
}

#[tokio::test]
async fn builtin_time_stream_reopens_without_registered_evaluator() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "persist_builtin_time_input", group.clone())
        .await
        .expect("create stream");
    source.send(2).await.expect("send t1");
    source.send(3).await.expect("send t2");

    let mut delayed = delay(&source).await.expect("apply delay");
    let delayed_namespace = delayed.namespace().to_string();
    delayed.flush().await.expect("flush delayed stream");
    unregister_stream_evaluator_for_test(&delayed_namespace);
    let mut reopened_delay = Stream::new(db.clone(), delayed_namespace, group.clone())
        .await
        .expect("reopen delay from descriptor");
    assert_eq!(reopened_delay.get(3).await.expect("delay t3"), 3);

    let mut diff = differentiate(&source).await.expect("differentiate source");
    let diff_namespace = diff.namespace().to_string();
    diff.flush().await.expect("flush diff stream");
    unregister_stream_evaluator_for_test(&diff_namespace);
    let mut reopened_diff = Stream::new(db.clone(), diff_namespace, group.clone())
        .await
        .expect("reopen differentiate from descriptor");
    assert_eq!(reopened_diff.get(2).await.expect("diff t2"), 1);

    let mut integrated = integrate(&source).await.expect("integrate source");
    let integrate_namespace = integrated.namespace().to_string();
    integrated.flush().await.expect("flush integrate stream");
    unregister_stream_evaluator_for_test(&integrate_namespace);
    let mut reopened_integrate = Stream::new(db, integrate_namespace, group)
        .await
        .expect("reopen integrate from descriptor");
    assert_eq!(reopened_integrate.get(2).await.expect("integrate t2"), 5);
}

#[tokio::test]
async fn builtin_addition_stream_reopens_without_registered_evaluator() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut left = Stream::new(db.clone(), "persist_builtin_add_left", group.clone())
        .await
        .expect("create left stream");
    left.send(2).await.expect("send left t1");
    left.send(3).await.expect("send left t2");

    let mut right = Stream::new(db.clone(), "persist_builtin_add_right", group.clone())
        .await
        .expect("create right stream");
    right.send(5).await.expect("send right t1");
    right.send(7).await.expect("send right t2");

    let addition = StreamAddition::from_stream(&left);
    let mut sum = addition.add(&left, &right).await;
    let sum_namespace = sum.namespace().to_string();
    sum.flush().await.expect("flush sum stream");
    unregister_stream_evaluator_for_test(&sum_namespace);
    let mut reopened_sum = Stream::new(db.clone(), sum_namespace, group.clone())
        .await
        .expect("reopen sum from descriptor");
    assert_eq!(reopened_sum.get(2).await.expect("sum t2"), 10);

    let mut neg = addition.neg(&left).await;
    let neg_namespace = neg.namespace().to_string();
    neg.flush().await.expect("flush neg stream");
    unregister_stream_evaluator_for_test(&neg_namespace);
    let mut reopened_neg = Stream::new(db, neg_namespace, group)
        .await
        .expect("reopen neg from descriptor");
    assert_eq!(reopened_neg.get(2).await.expect("neg t2"), -3);
}

#[tokio::test]
async fn closure_derived_stream_rejects_reopen_without_evaluator_graph() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(
        db.clone(),
        "persist_closure_missing_evaluator_input",
        group.clone(),
    )
    .await
    .expect("create stream");
    source.send(5).await.expect("send t1");

    let mut lifted = lift1(&source, group.clone(), |value| value * 2)
        .await
        .expect("apply lift");
    let namespace = lifted.namespace().to_string();
    lifted.flush().await.expect("flush lifted stream");
    unregister_stream_evaluator_for_test(&namespace);

    let err = match Stream::new(db, namespace, group).await {
        Ok(_) => panic!("derived stream should not reopen without evaluator graph"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("without its in-memory DBSP evaluator graph"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn stream_elimination_sums_exact_eventually_identity_stream() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "stream_elimination_input", group.clone())
        .await
        .expect("create stream");
    source.send(1).await.expect("send t1");
    source.send(2).await.expect("send t2");

    let delayed = delay(&source).await.expect("delay source");
    let eliminated = stream_elimination(&delayed)
        .await
        .expect("eliminate delayed stream");
    assert_eq!(eliminated, 3);
}

#[tokio::test]
async fn stream_elimination_rejects_non_identity_tail() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(db.clone(), "stream_elimination_non_identity_tail", group)
        .await
        .expect("create stream");
    source
        .set_default(1)
        .await
        .expect("set non-identity default");

    let err = match stream_elimination(&source).await {
        Ok(_) => panic!("non-identity tail should fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("eventually-identity input stream"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn bounded_stream_elimination_sums_non_identity_tail_range() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut source = Stream::new(
        db.clone(),
        "bounded_stream_elimination_non_identity_tail",
        group,
    )
    .await
    .expect("create stream");
    source.send(2).await.expect("send t1");
    source.send(4).await.expect("send t2");
    source
        .set_default(7)
        .await
        .expect("set non-identity default");

    let prefix = stream_elimination_prefix(&source, 4)
        .await
        .expect("bounded prefix elimination");
    assert_eq!(prefix, 20);

    let range = stream_elimination_range(&source, 2, 5)
        .await
        .expect("bounded range elimination");
    assert_eq!(range, 25);
}

#[tokio::test]
async fn bounded_stream_elimination_rejects_invalid_bounds() {
    let db = build_db().await;
    let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
    let source = Stream::new(db, "bounded_stream_elimination_invalid_bounds", group)
        .await
        .expect("create stream");

    let err = stream_elimination_range(&source, -1, 2)
        .await
        .expect_err("negative start should fail");
    assert!(
        err.to_string().contains("start cannot be negative"),
        "unexpected error: {err}"
    );

    let err = stream_elimination_range(&source, 2, 1)
        .await
        .expect_err("inverted bounds should fail");
    assert!(
        err.to_string()
            .contains("end must be greater than or equal to start"),
        "unexpected error: {err}"
    );
}
