use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::operations::basic::{differentiate, integrate};
use crate::stream::operations::{
    lifted_differentiate, lifted_integrate, lifted_stream_elimination,
};
use crate::stream::tests::common::build_db;
use crate::stream::util::{collect_values, push_value_in_place, set_default_in_place};

static TEST_SUFFIX: AtomicU64 = AtomicU64::new(0);

struct IntegerGroup;

#[async_trait]
impl AbelianGroup<i64> for IntegerGroup {
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

async fn build_outer_handle_stream(
    table: Arc<dyn KeyValueTable>,
    suffix: u64,
) -> (Stream<StreamHandle>, Arc<dyn AbelianGroup<i64>>) {
    let value_group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

    let mut inner = Stream::with_table(
        table.clone(),
        format!("lifted_scalar_wrapper_inner_{suffix}"),
        value_group.clone(),
    )
    .await
    .expect("create inner stream");
    inner.send(2).await.expect("send inner t1");
    inner.send(5).await.expect("send inner t2");
    inner.flush().await.expect("flush inner t2");
    let first_handle = inner.handle();

    inner.send(9).await.expect("send inner t3");
    inner.flush().await.expect("flush inner t3");
    let second_handle = inner.handle();

    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(first_handle.clone()));
    let mut outer = Stream::with_table(
        table,
        format!("lifted_scalar_wrapper_outer_{suffix}"),
        handle_group,
    )
    .await
    .expect("create outer stream");
    set_default_in_place(&mut outer, first_handle);
    push_value_in_place(&mut outer, second_handle);
    outer.flush().await.expect("flush outer stream");

    (outer, value_group)
}

#[tokio::test]
async fn lifted_differentiate_and_integrate_wrap_resolved_inner_streams() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let suffix = TEST_SUFFIX.fetch_add(1, Ordering::Relaxed);

    let (outer, value_group) = build_outer_handle_stream(table, suffix).await;

    let mut differentiated = lifted_differentiate(&outer, value_group.clone())
        .await
        .expect("build lifted differentiate");
    differentiated
        .flush()
        .await
        .expect("flush lifted differentiate");
    let diff_handles = collect_values(&differentiated, differentiated.current_time())
        .await
        .expect("collect lifted differentiate handles");

    let input_handles = collect_values(&outer, outer.current_time())
        .await
        .expect("collect wrapper input handles");

    for (idx, out_handle) in diff_handles.iter().enumerate() {
        let expected_inner = outer
            .resolve_handle(&input_handles[idx], value_group.clone())
            .await
            .expect("resolve expected differentiate input");
        let mut expected = differentiate(&expected_inner)
            .await
            .expect("differentiate expected inner stream");
        expected
            .flush()
            .await
            .expect("flush expected differentiate");
        let expected_values = collect_values(&expected, expected.current_time())
            .await
            .expect("collect expected differentiate values");

        let observed_inner = differentiated
            .resolve_handle(out_handle, value_group.clone())
            .await
            .expect("resolve observed differentiate output");
        let observed_values = collect_values(&observed_inner, observed_inner.current_time())
            .await
            .expect("collect observed differentiate values");

        assert_eq!(
            observed_values, expected_values,
            "lifted differentiate wrapper must match direct differentiate output"
        );
    }

    let mut integrated = lifted_integrate(&outer, value_group.clone())
        .await
        .expect("build lifted integrate");
    integrated.flush().await.expect("flush lifted integrate");
    let integrated_handles = collect_values(&integrated, integrated.current_time())
        .await
        .expect("collect lifted integrate handles");

    for (idx, out_handle) in integrated_handles.iter().enumerate() {
        let expected_inner = outer
            .resolve_handle(&input_handles[idx], value_group.clone())
            .await
            .expect("resolve expected integrate input");
        let mut expected = integrate(&expected_inner)
            .await
            .expect("integrate expected inner stream");
        expected.flush().await.expect("flush expected integrate");
        let expected_values = collect_values(&expected, expected.current_time())
            .await
            .expect("collect expected integrate values");

        let observed_inner = integrated
            .resolve_handle(out_handle, value_group.clone())
            .await
            .expect("resolve observed integrate output");
        let observed_values = collect_values(&observed_inner, observed_inner.current_time())
            .await
            .expect("collect observed integrate values");

        assert_eq!(
            observed_values, expected_values,
            "lifted integrate wrapper must match direct integrate output"
        );
    }
}

#[tokio::test]
async fn lifted_stream_elimination_reads_latest_resolved_values() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let suffix = TEST_SUFFIX.fetch_add(1, Ordering::Relaxed);

    let (outer, value_group) = build_outer_handle_stream(table, suffix).await;
    let input_handles = collect_values(&outer, outer.current_time())
        .await
        .expect("collect wrapper input handles");

    let mut eliminated = lifted_stream_elimination(&outer, value_group.clone())
        .await
        .expect("build lifted stream elimination");
    eliminated
        .flush()
        .await
        .expect("flush lifted stream elimination");

    let values = collect_values(&eliminated, eliminated.current_time())
        .await
        .expect("collect lifted elimination values");

    let mut expected = Vec::with_capacity(input_handles.len());
    for handle in input_handles {
        let mut resolved = outer
            .resolve_handle(&handle, value_group.clone())
            .await
            .expect("resolve expected elimination input");
        expected.push(
            resolved
                .latest()
                .await
                .expect("load expected latest value for elimination"),
        );
    }

    assert_eq!(
        values, expected,
        "lifted elimination must emit latest value from each resolved inner stream"
    );
}
