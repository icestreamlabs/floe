use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::materialized_view::{MaterializedViewHandle, MaterializedViewRegistry};
use floe_executor::{FloeQueryContext, load_or_register_mv};
use floe_storage::SlateCatalog;
use object_store::{ObjectStore, memory::InMemory};
use slatedb::Db;

const VIEW_NAME: &str = "mv_q1";

struct BuiltViewFixture {
    db: Arc<Db>,
    registry: Arc<MaterializedViewRegistry>,
    schema: SchemaRef,
    versions: Vec<u64>,
}

#[tokio::test]
async fn load_or_register_mv_makes_view_queryable() {
    let fixture = build_q1_fixture("mv-loader-registers", vec![(1, 2, 100)]).await;
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let mut bridge = DbspBridge::new(Arc::clone(&fixture.db))
        .await
        .expect("bridge");

    load_or_register_mv(
        &session,
        Arc::clone(&fixture.registry),
        &mut bridge,
        VIEW_NAME,
    )
    .await
    .expect("load mv");

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let batches = df.collect().await.expect("collect");
    assert_eq!(int_rows(&batches), vec![vec![1, 2, 100]]);
}

#[tokio::test]
async fn load_or_register_mv_registers_arrow_only_view() {
    let db = test_db("mv-loader-overlay-only").await;
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let registry = Arc::new(MaterializedViewRegistry::new());
    let schema = q1_schema();
    registry.set_schema(VIEW_NAME, Arc::clone(&schema));
    let handle = registry.register(VIEW_NAME.to_string());
    publish_q1_snapshot(handle.as_ref(), 1, Arc::clone(&schema), &[(1, 2, 100)]);

    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME)
        .await
        .expect("load overlay-only mv");

    assert!(
        handle.dbsp_state().is_none(),
        "Arrow-only MV should not force SlateDB state hydration"
    );

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let batches = df.collect().await.expect("collect");
    assert_eq!(int_rows(&batches), vec![vec![1, 2, 100]]);
}

#[tokio::test]
async fn mv_loader_reads_published_arrow_versions() {
    let fixture =
        build_q1_fixture("mv-loader-arrow-versions", vec![(1, 2, 100), (2, 3, 140)]).await;
    let registry = Arc::clone(&fixture.registry);
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let mut bridge = DbspBridge::new(Arc::clone(&fixture.db))
        .await
        .expect("bridge");

    load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME)
        .await
        .expect("load hybrid overlay");

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let batches = df.collect().await.expect("collect");
    assert_eq!(int_rows(&batches), vec![vec![1, 2, 100], vec![2, 3, 140]]);
}

#[tokio::test]
async fn mv_loader_supports_as_of_filter() {
    let fixture = build_q1_fixture("mv-loader-as-of", vec![(1, 2, 100), (2, 3, 140)]).await;
    let version_one = fixture.versions[0];
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let mut bridge = DbspBridge::new(Arc::clone(&fixture.db))
        .await
        .expect("bridge");

    load_or_register_mv(
        &session,
        Arc::clone(&fixture.registry),
        &mut bridge,
        VIEW_NAME,
    )
    .await
    .expect("load mv");

    let df_latest = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let latest = df_latest.collect().await.expect("collect latest");
    assert_eq!(int_rows(&latest), vec![vec![1, 2, 100], vec![2, 3, 140]]);

    let df = session
        .sql(&format!(
            "SELECT auction, bidder, price FROM mv_q1 WHERE __mv_version = {} ORDER BY auction",
            version_one
        ))
        .await
        .expect("plan AS OF SQL");
    let query_batches = df.collect().await.expect("collect as-of query");
    assert_eq!(int_rows(&query_batches), vec![vec![1, 2, 100]]);
}

#[tokio::test]
async fn mv_loader_does_not_recover_legacy_state_after_registry_restart() {
    let fixture = build_q1_fixture("mv-loader-restart", vec![(1, 2, 100), (2, 3, 140)]).await;
    let schema = Arc::clone(&fixture.schema);
    let restarted_registry = Arc::new(MaterializedViewRegistry::new());
    restarted_registry.set_schema(VIEW_NAME, schema);

    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let mut bridge = DbspBridge::new(Arc::clone(&fixture.db))
        .await
        .expect("bridge");

    load_or_register_mv(
        &session,
        Arc::clone(&restarted_registry),
        &mut bridge,
        VIEW_NAME,
    )
    .await
    .expect("load after restart");

    let latest_version = restarted_registry
        .get(VIEW_NAME)
        .and_then(|handle| handle.latest_version())
        .and_then(|version| u64::try_from(version).ok())
        .unwrap_or(0);
    assert_eq!(latest_version, 0);

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let batches = df.collect().await.expect("collect batches");
    assert_eq!(int_rows(&batches), Vec::<Vec<i64>>::new());
}

async fn build_q1_fixture(test_name: &str, rows: Vec<(i64, i64, i64)>) -> BuiltViewFixture {
    let db = test_db(test_name).await;
    let schema = q1_schema();
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.set_schema(VIEW_NAME, Arc::clone(&schema));
    let handle = mv_registry.register(VIEW_NAME.to_string());
    let mut versions = Vec::new();
    let mut snapshot = Vec::new();
    for (idx, row) in rows.into_iter().enumerate() {
        snapshot.push(row);
        let version = u64::try_from(idx + 1).expect("version fits u64");
        publish_q1_snapshot(
            handle.as_ref(),
            version as i64,
            Arc::clone(&schema),
            &snapshot,
        );
        versions.push(version);
    }

    BuiltViewFixture {
        db,
        registry: mv_registry,
        schema,
        versions,
    }
}

fn q1_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]))
}

fn publish_q1_snapshot(
    handle: &MaterializedViewHandle,
    version: i64,
    schema: SchemaRef,
    rows: &[(i64, i64, i64)],
) {
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(auction, _, _)| *auction),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, bidder, _)| *bidder),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, _, price)| *price),
            )),
        ],
    )
    .expect("build Q1 Arrow snapshot");
    handle.publish_arrow_version(version, vec![batch], Vec::new());
}

fn int_rows(batches: &[RecordBatch]) -> Vec<Vec<i64>> {
    if batches.is_empty() {
        return Vec::new();
    }
    let columns = batches[0].num_columns();
    first_n_int_columns(batches, columns)
}

fn first_n_int_columns(batches: &[RecordBatch], column_count: usize) -> Vec<Vec<i64>> {
    let mut rows = Vec::new();
    for batch in batches {
        let arrays: Vec<&Int64Array> = (0..column_count.min(batch.num_columns()))
            .map(|idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 column")
            })
            .collect();
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(arrays.len());
            for array in &arrays {
                if array.is_null(row_idx) {
                    panic!("unexpected NULL value");
                }
                row.push(array.value(row_idx));
            }
            rows.push(row);
        }
    }
    rows
}

async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}
