use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::Column;
use datafusion::common::stats::Precision;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown, lit};
use datafusion::physical_plan::collect;

use crate::mv::registry::{MaterializedViewHandle, MaterializedViewRegistry};
use crate::table_provider::{DynamicStateTableProvider, MaterializedViewTableProvider};

use super::MV_VERSION_COLUMN;
use super::filters::extract_mv_version_filter;

fn id_label_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn id_schema(nullable: bool) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Int64,
        nullable,
    )]))
}

fn arrow_i64_utf8_batch(schema: Arc<Schema>, rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, label)| *label),
            )),
        ],
    )
    .expect("build Arrow id/label batch")
}

fn arrow_i64_batch(schema: Arc<Schema>, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(
            values.iter().copied(),
        ))],
    )
    .expect("build Arrow i64 batch")
}

fn publish_id_label_snapshot(
    view: &MaterializedViewHandle,
    version: i64,
    schema: Arc<Schema>,
    rows: &[(i64, &str)],
) {
    view.publish_arrow_version(
        version,
        vec![arrow_i64_utf8_batch(schema, rows)],
        Vec::new(),
    );
}

fn publish_i64_snapshot(
    view: &MaterializedViewHandle,
    version: i64,
    schema: Arc<Schema>,
    values: &[i64],
) {
    view.publish_arrow_version(version, vec![arrow_i64_batch(schema, values)], Vec::new());
}

fn encoded_i64_utf8_row(id: i64, label: &str) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + 1 + 8 + 1 + 4 + label.len());
    encoded.extend_from_slice(&(2_u32).to_le_bytes());
    encoded.push(0x01);
    encoded.extend_from_slice(&id.to_le_bytes());
    encoded.push(0x02);
    let label_bytes = label.as_bytes();
    encoded.extend_from_slice(
        &u32::try_from(label_bytes.len())
            .expect("label length")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(label_bytes);
    encoded
}

fn encoded_i64_row(value: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + 1 + 8);
    encoded.extend_from_slice(&(1_u32).to_le_bytes());
    encoded.push(0x01);
    encoded.extend_from_slice(&value.to_le_bytes());
    encoded
}

fn publish_encoded_id_label_overlay(
    view: &MaterializedViewHandle,
    version: u64,
    rows: &[(i64, &str)],
) {
    view.append_encoded_overlay_batch(
        version,
        rows.iter()
            .map(|(id, label)| (encoded_i64_utf8_row(*id, label), 1)),
    );
}

fn publish_encoded_i64_overlay(view: &MaterializedViewHandle, version: u64, values: &[i64]) {
    view.append_encoded_overlay_batch(
        version,
        values.iter().map(|value| (encoded_i64_row(*value), 1)),
    );
}

async fn count_star(ctx: &SessionContext, table_name: &str) -> i64 {
    let batches = ctx
        .sql(&format!("SELECT COUNT(*) AS row_count FROM {table_name}"))
        .await
        .expect("build count query")
        .collect()
        .await
        .expect("collect count query");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count array")
        .value(0)
}

#[tokio::test]
async fn materialized_view_provider_emits_rows() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_test");

    let schema = id_label_schema();
    publish_id_label_snapshot(view.as_ref(), 1, Arc::clone(&schema), &[(1, "one")]);
    publish_id_label_snapshot(
        view.as_ref(),
        2,
        Arc::clone(&schema),
        &[(1, "one"), (2, "two")],
    );

    let provider = MaterializedViewTableProvider::new(registry.clone(), "mv_test", schema);
    let latest = provider
        .build_batches_for_test()
        .await
        .expect("build latest");
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].num_rows(), 2);
    assert_eq!(latest[0].num_columns(), 3);

    let as_of = provider
        .build_batches_at_version(1)
        .await
        .expect("build as of version");
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].num_rows(), 1);
}

#[tokio::test]
async fn materialized_view_provider_resolves_logical_versions_to_encoded_overlay() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_logical_version_test");

    let logical_version = 42_u64;
    let schema = id_label_schema();
    publish_encoded_id_label_overlay(view.as_ref(), logical_version, &[(7, "seven")]);
    let provider = MaterializedViewTableProvider::new(registry, "mv_logical_version_test", schema);

    let as_of = provider
        .build_batches_at_version(logical_version)
        .await
        .expect("build logical as of version");
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].num_rows(), 1);
}

#[tokio::test]
async fn materialized_view_provider_hides_unpublished_dbsp_state() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.register("mv_unpublished_state");

    let schema = id_label_schema();
    let provider = MaterializedViewTableProvider::new(registry, "mv_unpublished_state", schema);

    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build unpublished snapshot");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
}

#[tokio::test]
async fn materialized_view_provider_resolves_published_empty_logical_versions() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_empty_logical_versions");

    let schema = id_label_schema();
    let rows = [(9, "nine")];
    publish_id_label_snapshot(view.as_ref(), 1, Arc::clone(&schema), &rows);
    publish_id_label_snapshot(view.as_ref(), 2, Arc::clone(&schema), &rows);
    publish_id_label_snapshot(view.as_ref(), 3, Arc::clone(&schema), &rows);
    let provider =
        MaterializedViewTableProvider::new(registry, "mv_empty_logical_versions", schema);

    let as_of_two = provider
        .build_batches_at_version(2)
        .await
        .expect("build logical version 2");
    assert_eq!(as_of_two.len(), 1);
    assert_eq!(as_of_two[0].num_rows(), 1);

    let as_of_three = provider
        .build_batches_at_version(3)
        .await
        .expect("build logical version 3");
    assert_eq!(as_of_three.len(), 1);
    assert_eq!(as_of_three[0].num_rows(), 1);
}

#[tokio::test]
async fn materialized_view_provider_applies_projection_and_limit_in_scan() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_projection_limit");

    let schema = id_label_schema();
    publish_id_label_snapshot(
        view.as_ref(),
        1,
        Arc::clone(&schema),
        &[(1, "one"), (2, "two")],
    );
    let provider = MaterializedViewTableProvider::new(registry, "mv_projection_limit", schema);
    let session = SessionContext::new();
    let state = session.state();
    let projection = vec![1usize];
    let plan = provider
        .scan(&state, Some(&projection), &[], Some(1))
        .await
        .expect("scan");
    let batches = collect(plan, session.state().task_ctx())
        .await
        .expect("collect");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 1);
}

#[tokio::test]
async fn dynamic_state_provider_applies_scan_limit() {
    let schema = id_schema(true);
    let provider = DynamicStateTableProvider::new(Arc::clone(&schema));
    provider
        .set_batches(vec![
            arrow_i64_batch(Arc::clone(&schema), &[1, 2]),
            arrow_i64_batch(Arc::clone(&schema), &[3, 4]),
        ])
        .expect("seed dynamic provider");

    let session = SessionContext::new();
    let state = session.state();
    let plan = provider
        .scan(&state, None, &[], Some(3))
        .await
        .expect("scan dynamic provider with limit");
    let batches = collect(plan, session.state().task_ctx())
        .await
        .expect("collect dynamic provider batches");

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    let values = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id array");
            (0..values.len())
                .map(|idx| values.value(idx))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 2, 3]);
}

#[tokio::test]
async fn dynamic_state_provider_statistics_respect_scan_limit() {
    let schema = id_schema(true);
    let provider = DynamicStateTableProvider::new(Arc::clone(&schema));
    provider
        .set_batches(vec![
            arrow_i64_batch(Arc::clone(&schema), &[1, 2]),
            arrow_i64_batch(Arc::clone(&schema), &[3, 4]),
        ])
        .expect("seed dynamic provider");

    let session = SessionContext::new();
    let state = session.state();
    let plan = provider
        .scan(&state, None, &[], Some(3))
        .await
        .expect("scan dynamic provider with limit");
    let stats = plan
        .partition_statistics(None)
        .expect("dynamic provider statistics");

    assert_eq!(stats.num_rows, Precision::Exact(3));
    assert!(matches!(stats.total_byte_size, Precision::Inexact(_)));
}

#[tokio::test]
async fn materialized_view_provider_empty_then_populated() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_empty");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let provider =
        MaterializedViewTableProvider::new(Arc::clone(&registry), "mv_empty", schema.clone());
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build empty batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);

    publish_i64_snapshot(view.as_ref(), 1, Arc::clone(&schema), &[5]);

    let populated = provider
        .build_batches_for_test()
        .await
        .expect("build populated batches");
    assert_eq!(populated.len(), 1);
    assert_eq!(populated[0].num_rows(), 1);
}

#[tokio::test]
async fn materialized_view_provider_recovers_authoritative_row_count_from_latest_snapshot() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_count_recovery");

    let schema = id_schema(true);
    let handle_version = 1_u64;
    publish_i64_snapshot(
        view.as_ref(),
        handle_version as i64,
        Arc::clone(&schema),
        &[1, 2, 3],
    );
    let provider =
        MaterializedViewTableProvider::new(Arc::clone(&registry), "mv_count_recovery", schema);

    assert_eq!(view.authoritative_row_count(), None);
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build recovered snapshot");
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(view.authoritative_row_count_for(handle_version), Some(3));
    assert_eq!(view.authoritative_row_count(), None);
    let second = provider
        .build_batches_for_test()
        .await
        .expect("build recovered snapshot again");
    assert_eq!(second[0].num_rows(), 3);
}

#[tokio::test]
async fn materialized_view_provider_recovers_authoritative_row_count_from_overlay_snapshot() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_overlay_count_recovery");

    let schema = id_schema(true);
    let handle_version = 1_u64;
    let latest_version = handle_version.saturating_add(1);
    publish_encoded_i64_overlay(view.as_ref(), handle_version, &[1, 2]);
    publish_encoded_i64_overlay(view.as_ref(), latest_version, &[3]);
    let provider = MaterializedViewTableProvider::new(
        Arc::clone(&registry),
        "mv_overlay_count_recovery",
        schema,
    );

    assert_eq!(view.authoritative_row_count(), None);
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build recovered overlay snapshot");
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(view.authoritative_row_count_for(latest_version), Some(3));
    assert_eq!(view.authoritative_row_count(), Some(3));
    let second = provider
        .build_batches_for_test()
        .await
        .expect("build recovered overlay snapshot again");
    assert_eq!(second[0].num_rows(), 3);
}

#[tokio::test]
async fn materialized_view_provider_invalidates_stale_authoritative_count_after_version_advance() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_stale_count_recovery");
    view.publish_logical_version(0);
    assert!(view.seed_authoritative_row_count_if_latest(0, 0));

    let schema = id_schema(true);
    publish_i64_snapshot(view.as_ref(), 1, Arc::clone(&schema), &[9]);
    let provider = MaterializedViewTableProvider::new(
        Arc::clone(&registry),
        "mv_stale_count_recovery",
        schema,
    );

    assert_eq!(view.authoritative_row_count_for(0), None);
    assert_eq!(view.authoritative_row_count_for(1), Some(1));
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build stale-count recovery snapshot");
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(view.authoritative_row_count_for(1), Some(1));
    let second = provider
        .build_batches_for_test()
        .await
        .expect("build stale-count recovery snapshot again");
    assert_eq!(second[0].num_rows(), 1);
}

#[tokio::test]
async fn materialized_view_provider_builds_mv_version_only_batches_from_authoritative_state() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_version_only");

    let schema = id_schema(true);
    publish_i64_snapshot(view.as_ref(), 7, Arc::clone(&schema), &[1, 2, 3]);
    let provider = MaterializedViewTableProvider::new(registry, "mv_version_only", schema);
    let session = SessionContext::new();
    let state = session.state();
    let projection = vec![1usize];
    let plan = provider
        .scan(&state, Some(&projection), &[], None)
        .await
        .expect("scan mv version");
    let batches = collect(plan, session.state().task_ctx())
        .await
        .expect("collect mv version batches");
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    let version_values: Vec<u64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("mv version array")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(version_values, vec![7, 7, 7]);
}

#[tokio::test]
async fn materialized_view_provider_preserves_user_mv_version_column() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_user_version_column");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(MV_VERSION_COLUMN, DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![7, 8])),
        ],
    )
    .expect("record batch");
    view.publish_arrow_version(11, vec![batch], Vec::new());

    let provider =
        MaterializedViewTableProvider::new(Arc::clone(&registry), "mv_user_version_column", schema);
    assert_eq!(provider.schema().fields().len(), 2);
    let ctx = SessionContext::new();
    ctx.register_table(
        "mv_user_version_column",
        Arc::new(provider) as Arc<dyn TableProvider>,
    )
    .expect("register mv provider");

    let batches = ctx
        .sql(
            "SELECT id, __mv_version \
             FROM mv_user_version_column \
             WHERE __mv_version = 7",
        )
        .await
        .expect("build query")
        .collect()
        .await
        .expect("collect query");
    let ids = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id array");
            (0..values.len())
                .map(|idx| values.value(idx))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn materialized_view_provider_answers_count_star_from_authoritative_state() {
    for nullable in [true, false] {
        let view_name = if nullable {
            "mv_count_fast_nullable"
        } else {
            "mv_count_fast_non_null"
        };
        let registry = Arc::new(MaterializedViewRegistry::new());
        let view = registry.register(view_name);
        let schema = id_schema(nullable);
        publish_i64_snapshot(view.as_ref(), 11, Arc::clone(&schema), &[1, 2, 3]);
        let provider = MaterializedViewTableProvider::new(registry, view_name, schema);
        let ctx = SessionContext::new();
        ctx.register_table(view_name, Arc::new(provider) as Arc<dyn TableProvider>)
            .expect("register mv provider");

        assert_eq!(count_star(&ctx, view_name).await, 3, "{view_name}");
    }
}

#[tokio::test]
async fn materialized_view_provider_hides_unpublished_authoritative_count_until_version_visible() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_count_visibility");

    let schema = id_schema(true);
    let provider = MaterializedViewTableProvider::new(registry, "mv_count_visibility", schema);
    let ctx = SessionContext::new();
    ctx.register_table(
        "mv_count_visibility",
        Arc::new(provider) as Arc<dyn TableProvider>,
    )
    .expect("register mv provider");

    assert_eq!(count_star(&ctx, "mv_count_visibility").await, 0);

    publish_i64_snapshot(view.as_ref(), 2, id_schema(true), &[7]);

    assert_eq!(count_star(&ctx, "mv_count_visibility").await, 1);
}

#[tokio::test]
async fn materialized_view_provider_keeps_latest_visible_count_while_next_version_is_staged() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_count_staged_visibility");

    let schema = id_schema(true);
    publish_i64_snapshot(view.as_ref(), 1, Arc::clone(&schema), &[1]);
    let provider =
        MaterializedViewTableProvider::new(registry, "mv_count_staged_visibility", schema);
    let ctx = SessionContext::new();
    ctx.register_table(
        "mv_count_staged_visibility",
        Arc::new(provider) as Arc<dyn TableProvider>,
    )
    .expect("register mv provider");

    assert_eq!(count_star(&ctx, "mv_count_staged_visibility").await, 1);

    publish_i64_snapshot(view.as_ref(), 2, id_schema(true), &[1, 2]);
    assert_eq!(count_star(&ctx, "mv_count_staged_visibility").await, 2);
}

#[tokio::test]
async fn materialized_view_provider_uses_cached_count_on_first_overlay_visible_version() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_overlay_first_visible_count");

    let schema = id_schema(true);
    publish_encoded_i64_overlay(view.as_ref(), 0, &[]);
    publish_encoded_i64_overlay(view.as_ref(), 1, &[7]);
    let provider =
        MaterializedViewTableProvider::new(registry, "mv_overlay_first_visible_count", schema);
    let session = SessionContext::new();
    session
        .register_table(
            "mv_overlay_first_visible_count",
            Arc::new(provider) as Arc<dyn TableProvider>,
        )
        .expect("register mv provider");

    assert_eq!(
        count_star(&session, "mv_overlay_first_visible_count").await,
        1
    );
    assert_eq!(view.authoritative_row_count_for(1), Some(1));
}

#[test]
fn mv_version_filter_is_extracted() {
    let mv_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name(MV_VERSION_COLUMN))),
        Operator::Eq,
        Box::new(lit(7_u64)),
    ));
    let other_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("auction"))),
        Operator::Eq,
        Box::new(lit(42_i64)),
    ));
    let filters = vec![mv_filter.clone(), other_filter.clone()];
    let (version, retained) = extract_mv_version_filter(&filters);
    assert_eq!(version, Some(7));
    assert_eq!(retained, vec![other_filter.clone()]);

    let (none_version, unchanged) = extract_mv_version_filter(std::slice::from_ref(&other_filter));
    assert!(none_version.is_none());
    assert_eq!(unchanged, vec![other_filter.clone()]);

    let (first_version, retained_duplicates) =
        extract_mv_version_filter(&[mv_filter.clone(), mv_filter.clone()]);
    assert_eq!(first_version, Some(7));
    assert!(retained_duplicates.is_empty());

    let conflicting_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name(MV_VERSION_COLUMN))),
        Operator::Eq,
        Box::new(lit(8_u64)),
    ));
    let (first_version, retained_conflict) =
        extract_mv_version_filter(&[mv_filter.clone(), conflicting_filter.clone()]);
    assert_eq!(first_version, Some(7));
    assert_eq!(retained_conflict, vec![conflicting_filter]);
}

#[test]
fn mv_version_pushdown_is_conflict_aware() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let provider = MaterializedViewTableProvider::new(registry, "mv_pushdown", id_schema(true));
    let version_seven = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name(MV_VERSION_COLUMN))),
        Operator::Eq,
        Box::new(lit(7_u64)),
    ));
    let version_eight = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name(MV_VERSION_COLUMN))),
        Operator::Eq,
        Box::new(lit(8_u64)),
    ));
    let filter_refs = [&version_seven, &version_seven, &version_eight];

    let pushdown = provider
        .supports_filters_pushdown(&filter_refs)
        .expect("pushdown support");
    assert_eq!(
        pushdown,
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Unsupported,
        ]
    );
}
