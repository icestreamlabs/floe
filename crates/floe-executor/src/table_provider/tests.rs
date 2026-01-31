use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Column;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
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
    let mut dbsp_view = bridge.new_view("mv_test").await.expect("dbsp view");
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
    let mut dbsp_view = bridge.new_view("mv_empty").await.expect("view");
    let row = vec![ScalarValue::Int64(Some(5))];
    dbsp_view.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
    dbsp_view.flush().await.expect("flush view");
    let handle_view = dbsp_view.latest_handle_view();
    let (dict, table, namespace, version) = handle_view.into_parts();
    registry
        .get("mv_empty")
        .expect("view registered")
        .set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));

    let populated = provider
        .build_batches_for_test()
        .await
        .expect("build populated batches");
    assert_eq!(populated.len(), 1);
    assert_eq!(populated[0].num_rows(), 1);
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
    let provider = SourceTableProvider::new(bridge, "nexmark_bid", "nexmark_bid", schema).unwrap();
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
