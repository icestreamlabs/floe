use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::common::Column;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_plan::collect;
use datafusion::scalar::ScalarValue;
use dbsp::StreamRetention;
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use object_store::{ObjectStore, memory::InMemory};
use slatedb::Db;

use crate::dbsp_bridge::DbspBridge;
use crate::encoding::encode_projected_row_key;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::namespaces;
use crate::table_provider::MaterializedViewTableProvider;

use super::filters::extract_mv_version_filter;
use super::{MV_VERSION_COLUMN, SourceTableProvider};

#[tokio::test]
async fn materialized_view_provider_emits_rows() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_test");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open("mv-provider", store).await.expect("open SlateDB"));
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view("mv_test", StreamRetention::KeepLast { keep_last: 1 })
        .await
        .expect("dbsp view");
    let row_one = vec![
        ScalarValue::Int64(Some(1)),
        ScalarValue::Utf8(Some("one".into())),
    ];
    dbsp_view.add_delta(encode_projected_row_key(&row_one).expect("encode"), 1);
    let version_one = dbsp_view
        .flush()
        .await
        .expect("flush first version")
        .version;
    let row_two = vec![
        ScalarValue::Int64(Some(2)),
        ScalarValue::Utf8(Some("two".into())),
    ];
    dbsp_view.add_delta(encode_projected_row_key(&row_two).expect("encode"), 1);
    dbsp_view.flush().await.expect("flush second version");
    let handle_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = handle_view.into_parts();
    view.set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));
    view.publish_logical_version(i64::try_from(version).expect("logical version"));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));

    let provider = MaterializedViewTableProvider::new(registry.clone(), "mv_test", schema);
    let latest = provider
        .build_batches_for_test()
        .await
        .expect("build latest");
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].num_rows(), 2);
    assert_eq!(latest[0].num_columns(), 3);

    let as_of = provider
        .build_batches_at_version(version_one)
        .await
        .expect("build as of version");
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].num_rows(), 1);
}

#[tokio::test]
async fn materialized_view_provider_resolves_logical_versions_to_dbsp_handles() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_logical_version_test");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("mv-provider-logical-version", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_logical_version_test",
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");
    let row = vec![
        ScalarValue::Int64(Some(7)),
        ScalarValue::Utf8(Some("seven".into())),
    ];
    dbsp_view.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
    let handle = dbsp_view.flush().await.expect("flush logical version test");
    let logical_version = 42_i64;
    view.publish_version(logical_version, handle.clone());
    let latest_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = latest_view.into_parts();
    view.set_dbsp_state(
        DbspPersistedState::new(dict, table, namespace, version)
            .with_logical_version(logical_version as u64),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let provider = MaterializedViewTableProvider::new(registry, "mv_logical_version_test", schema);

    let as_of = provider
        .build_batches_at_version(logical_version as u64)
        .await
        .expect("build logical as of version");
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].num_rows(), 1);
}

#[tokio::test]
async fn materialized_view_provider_hides_unpublished_dbsp_state() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_unpublished_state");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("mv-provider-unpublished-state", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_unpublished_state",
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");
    let row = vec![
        ScalarValue::Int64(Some(9)),
        ScalarValue::Utf8(Some("nine".into())),
    ];
    dbsp_view.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
    dbsp_view.flush().await.expect("flush unpublished state");
    let handle_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = handle_view.into_parts();
    view.set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
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

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("mv-provider-empty-logical-version", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_empty_logical_versions",
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");
    let row = vec![
        ScalarValue::Int64(Some(9)),
        ScalarValue::Utf8(Some("nine".into())),
    ];
    dbsp_view.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
    let handle = dbsp_view.flush().await.expect("flush base version");
    let latest_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = latest_view.into_parts();
    view.set_dbsp_state(
        DbspPersistedState::new(dict, table, namespace, version).with_logical_version(3),
    );
    view.publish_version(1, handle);
    view.publish_logical_version(2);
    view.publish_logical_version(3);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
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

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("mv-provider-limit", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_projection_limit",
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");
    dbsp_view.add_delta(
        encode_projected_row_key(&vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("one".into())),
        ])
        .expect("encode one"),
        1,
    );
    dbsp_view.add_delta(
        encode_projected_row_key(&vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Utf8(Some("two".into())),
        ])
        .expect("encode two"),
        1,
    );
    dbsp_view.flush().await.expect("flush");
    let handle_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = handle_view.into_parts();
    view.set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));
    view.publish_logical_version(i64::try_from(version).expect("logical version"));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
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
async fn materialized_view_provider_empty_then_populated() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.register("mv_empty");
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

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open("mv-empty", store).await.expect("open SlateDB"));
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view("mv_empty", StreamRetention::KeepLast { keep_last: 1 })
        .await
        .expect("view");
    let row = vec![ScalarValue::Int64(Some(5))];
    dbsp_view.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
    dbsp_view.flush().await.expect("flush view");
    let handle_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = handle_view.into_parts();
    registry
        .get("mv_empty")
        .expect("view registered")
        .set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));
    registry
        .get("mv_empty")
        .expect("view registered")
        .publish_logical_version(i64::try_from(version).expect("logical version"));

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

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("mv-provider-count-recovery", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_count_recovery",
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");
    for id in [1_i64, 2_i64, 3_i64] {
        dbsp_view.add_delta(
            encode_projected_row_key(&vec![ScalarValue::Int64(Some(id))]).expect("encode row"),
            1,
        );
    }
    dbsp_view.flush().await.expect("flush count recovery");
    let handle_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, handle_version) = handle_view.into_parts();
    view.set_dbsp_state(DbspPersistedState::new(
        dict,
        table,
        namespace,
        handle_version,
    ));
    view.publish_logical_version(i64::try_from(handle_version).expect("logical version"));

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let provider =
        MaterializedViewTableProvider::new(Arc::clone(&registry), "mv_count_recovery", schema);

    assert_eq!(view.authoritative_row_count(), None);
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build recovered snapshot");
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(view.authoritative_row_count(), Some(3));
}

#[tokio::test]
async fn materialized_view_provider_recovers_authoritative_row_count_from_overlay_snapshot() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_overlay_count_recovery");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("mv-provider-overlay-count-recovery", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(db).await.expect("bridge");
    let mut dbsp_view = bridge
        .new_view(
            "mv_overlay_count_recovery",
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("dbsp view");
    for id in [1_i64, 2_i64] {
        dbsp_view.add_delta(
            encode_projected_row_key(&vec![ScalarValue::Int64(Some(id))]).expect("encode row"),
            1,
        );
    }
    dbsp_view.flush().await.expect("flush overlay base");
    let handle_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, handle_version) = handle_view.into_parts();
    view.set_dbsp_state(DbspPersistedState::new(
        dict,
        table,
        namespace,
        handle_version,
    ));
    view.publish_logical_version(i64::try_from(handle_version).expect("logical version"));
    view.append_encoded_overlay_batch(
        handle_version.saturating_add(1),
        vec![(
            encode_projected_row_key(&vec![ScalarValue::Int64(Some(3))]).expect("encode row"),
            1,
        )],
    );

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
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
    assert_eq!(view.authoritative_row_count(), Some(3));
}

#[tokio::test]
async fn materialized_view_provider_invalidates_stale_authoritative_count_after_version_advance() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_stale_count_recovery");
    view.publish_logical_version(0);
    assert!(view.seed_authoritative_row_count_if_latest(0, 0));
    view.append_encoded_overlay_batch(
        1,
        vec![(
            encode_projected_row_key(&vec![ScalarValue::Int64(Some(9))]).expect("encode row"),
            1,
        )],
    );

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let provider = MaterializedViewTableProvider::new(
        Arc::clone(&registry),
        "mv_stale_count_recovery",
        schema,
    );

    assert_eq!(view.authoritative_row_count_for(0), Some(0));
    assert_eq!(view.authoritative_row_count_for(1), None);
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build stale-count recovery snapshot");
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(view.authoritative_row_count_for(1), Some(1));
}

#[tokio::test]
async fn materialized_view_provider_builds_mv_version_only_batches_from_authoritative_state() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_version_only");
    view.mark_state_authoritative();
    view.publish_logical_version(7);
    for id in [1_i64, 2_i64, 3_i64] {
        let row = vec![ScalarValue::Int64(Some(id))];
        view.apply_encoded_state_batch(
            7,
            &[(encode_projected_row_key(&row).expect("encode row"), 1)],
        )
        .expect("apply authoritative row");
    }

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
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
async fn materialized_view_provider_answers_count_star_from_authoritative_state() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_count_fast");
    view.mark_state_authoritative();
    view.publish_logical_version(11);
    for id in [1_i64, 2_i64, 3_i64] {
        let row = vec![ScalarValue::Int64(Some(id))];
        view.apply_encoded_state_batch(
            11,
            &[(encode_projected_row_key(&row).expect("encode row"), 1)],
        )
        .expect("apply authoritative row");
    }

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let provider = MaterializedViewTableProvider::new(registry, "mv_count_fast", schema);
    let ctx = SessionContext::new();
    ctx.register_table(
        "mv_count_fast",
        Arc::new(provider) as Arc<dyn TableProvider>,
    )
    .expect("register mv provider");

    let batches = ctx
        .sql("SELECT COUNT(*) AS row_count FROM mv_count_fast")
        .await
        .expect("build count query")
        .collect()
        .await
        .expect("collect count query");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count array")
        .value(0);
    assert_eq!(count, 3);
}

#[tokio::test]
async fn materialized_view_provider_answers_count_star_from_authoritative_non_null_state() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_count_fast_non_null");
    view.mark_state_authoritative();
    view.publish_logical_version(11);
    for id in [1_i64, 2_i64, 3_i64] {
        let row = vec![ScalarValue::Int64(Some(id))];
        view.apply_encoded_state_batch(
            11,
            &[(encode_projected_row_key(&row).expect("encode row"), 1)],
        )
        .expect("apply authoritative row");
    }

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let provider = MaterializedViewTableProvider::new(registry, "mv_count_fast_non_null", schema);
    let ctx = SessionContext::new();
    ctx.register_table(
        "mv_count_fast_non_null",
        Arc::new(provider) as Arc<dyn TableProvider>,
    )
    .expect("register mv provider");

    let batches = ctx
        .sql("SELECT COUNT(*) AS row_count FROM mv_count_fast_non_null")
        .await
        .expect("build count query")
        .collect()
        .await
        .expect("collect count query");
    assert_eq!(batches.len(), 1);
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count array")
        .value(0);
    assert_eq!(count, 3);
}

#[tokio::test]
async fn materialized_view_provider_hides_unpublished_authoritative_count_until_version_visible() {
    let registry = Arc::new(MaterializedViewRegistry::new());
    let view = registry.register("mv_count_visibility");
    view.mark_state_authoritative();

    let row = vec![ScalarValue::Int64(Some(7))];
    view.apply_encoded_state_batch(
        2,
        &[(encode_projected_row_key(&row).expect("encode row"), 1)],
    )
    .expect("apply authoritative row");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let provider = MaterializedViewTableProvider::new(registry, "mv_count_visibility", schema);
    let ctx = SessionContext::new();
    ctx.register_table(
        "mv_count_visibility",
        Arc::new(provider) as Arc<dyn TableProvider>,
    )
    .expect("register mv provider");

    let before_publish = ctx
        .sql("SELECT COUNT(*) AS row_count FROM mv_count_visibility")
        .await
        .expect("build pre-publish count query")
        .collect()
        .await
        .expect("collect pre-publish count query");
    let count_before = before_publish[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count array")
        .value(0);
    assert_eq!(count_before, 0);

    view.publish_logical_version(2);

    let after_publish = ctx
        .sql("SELECT COUNT(*) AS row_count FROM mv_count_visibility")
        .await
        .expect("build post-publish count query")
        .collect()
        .await
        .expect("collect post-publish count query");
    let count_after = after_publish[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count array")
        .value(0);
    assert_eq!(count_after, 1);
}

#[tokio::test]
async fn source_provider_emits_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("source-provider", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let namespace = namespaces::source("nexmark_bid").expect("namespace");
    let mut stream = bridge
        .new_stream(
            namespace.clone(),
            dbsp::StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("stream");
    let row = vec![
        ScalarValue::Int64(Some(42)),
        ScalarValue::Utf8(Some("chan".into())),
    ];
    stream.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
    stream.flush().await.expect("flush");

    let bridge = Arc::new(tokio::sync::Mutex::new(bridge));
    let source = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new("auction", SourceDataType::Int64),
            SourceColumn::new("channel", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let schema = source.to_arrow_schema();
    let provider =
        SourceTableProvider::new(bridge, "nexmark_bid", "nexmark_bid", schema, None).unwrap();
    let batches = provider
        .build_batches_for_test()
        .await
        .expect("build batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let auction_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("auction array");
    assert_eq!(auction_col.value(0), 42);
}

#[tokio::test]
async fn source_provider_applies_projection_and_limit_in_scan() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("source-provider-limit", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let namespace = namespaces::source("orders").expect("namespace");
    let mut stream = bridge
        .new_stream(namespace, dbsp::StreamRetention::KeepLast { keep_last: 1 })
        .await
        .expect("stream");
    let row_one = vec![
        ScalarValue::Int64(Some(1)),
        ScalarValue::Utf8(Some("a".into())),
    ];
    let row_two = vec![
        ScalarValue::Int64(Some(2)),
        ScalarValue::Utf8(Some("b".into())),
    ];
    stream.add_delta(encode_projected_row_key(&row_one).expect("encode one"), 1);
    stream.add_delta(encode_projected_row_key(&row_two).expect("encode two"), 1);
    stream.flush().await.expect("flush");

    let bridge = Arc::new(tokio::sync::Mutex::new(bridge));
    let source = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("label", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let provider =
        SourceTableProvider::new(bridge, "orders", "orders", source.to_arrow_schema(), None)
            .expect("provider");
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
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn source_provider_preserves_row_count_for_empty_projection() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("source-provider-empty-projection", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let namespace = namespaces::source("orders_count").expect("namespace");
    let mut stream = bridge
        .new_stream(namespace, dbsp::StreamRetention::KeepLast { keep_last: 1 })
        .await
        .expect("stream");
    for id in [1_i64, 2_i64, 3_i64] {
        stream.add_delta(
            encode_projected_row_key(&vec![ScalarValue::Int64(Some(id))]).expect("encode"),
            1,
        );
    }
    stream.flush().await.expect("flush");

    let bridge = Arc::new(tokio::sync::Mutex::new(bridge));
    let source = SourceDefinition::new(
        "orders_count",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source definition");
    let provider = SourceTableProvider::new(
        bridge,
        "orders_count",
        "orders_count",
        source.to_arrow_schema(),
        None,
    )
    .expect("provider");
    let session = SessionContext::new();
    let state = session.state();
    let plan = provider
        .scan(&state, Some(&vec![]), &[], None)
        .await
        .expect("scan");
    let batches = collect(plan, session.state().task_ctx())
        .await
        .expect("collect");
    assert_eq!(batches[0].num_columns(), 0);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn source_provider_pushes_down_primary_key_filters() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("source-provider-pk", store)
            .await
            .expect("open SlateDB"),
    );
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let namespace = namespaces::source("orders_pk").expect("namespace");
    let mut stream = bridge
        .new_stream(namespace, dbsp::StreamRetention::KeepLast { keep_last: 1 })
        .await
        .expect("stream");
    let rows = [
        vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("one".into())),
        ],
        vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Utf8(Some("two".into())),
        ],
        vec![
            ScalarValue::Int64(Some(3)),
            ScalarValue::Utf8(Some("three".into())),
        ],
    ];
    for row in rows {
        stream.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
    }
    stream.flush().await.expect("flush");

    let bridge = Arc::new(tokio::sync::Mutex::new(bridge));
    let source = SourceDefinition::new(
        "orders_pk",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("label", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let provider = SourceTableProvider::new(
        bridge,
        "orders_pk",
        "orders_pk",
        source.to_arrow_schema(),
        Some("id"),
    )
    .expect("provider");

    let eq_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("id"))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::Int64(Some(2)), None)),
    ));
    let statuses = provider
        .supports_filters_pushdown(&[&eq_filter])
        .expect("pushdown support");
    assert_eq!(
        statuses[0],
        datafusion::logical_expr::TableProviderFilterPushDown::Exact
    );

    let session = SessionContext::new();
    let state = session.state();
    let eq_plan = provider
        .scan(&state, None, std::slice::from_ref(&eq_filter), None)
        .await
        .expect("scan eq");
    let eq_batches = collect(eq_plan, session.state().task_ctx())
        .await
        .expect("collect eq");
    let ids = eq_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id array");
    assert_eq!(ids.len(), 1);
    assert_eq!(ids.value(0), 2);

    let in_filter = Expr::InList(datafusion::logical_expr::expr::InList {
        expr: Box::new(Expr::Column(Column::from_name("id"))),
        list: vec![
            Expr::Literal(ScalarValue::Int64(Some(1)), None),
            Expr::Literal(ScalarValue::Int64(Some(3)), None),
        ],
        negated: false,
    });
    let in_plan = provider
        .scan(
            &state,
            Some(&vec![1]),
            std::slice::from_ref(&in_filter),
            None,
        )
        .await
        .expect("scan in");
    let in_batches = collect(in_plan, session.state().task_ctx())
        .await
        .expect("collect in");
    let labels = in_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("label array");
    assert_eq!(labels.len(), 2);
}

#[test]
fn mv_version_filter_is_extracted() {
    let mv_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name(MV_VERSION_COLUMN))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::UInt64(Some(7)), None)),
    ));
    let other_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("auction"))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::Int64(Some(42)), None)),
    ));
    let filters = vec![mv_filter.clone(), other_filter.clone()];
    let (version, retained) = extract_mv_version_filter(&filters);
    assert_eq!(version, Some(7));
    assert_eq!(retained, vec![other_filter.clone()]);

    let (none_version, unchanged) = extract_mv_version_filter(std::slice::from_ref(&other_filter));
    assert!(none_version.is_none());
    assert_eq!(unchanged, vec![other_filter.clone()]);

    let (first_version, _) = extract_mv_version_filter(&[mv_filter.clone(), mv_filter.clone()]);
    assert_eq!(first_version, Some(7));
}
