use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{col, table_scan};
use datafusion::scalar::ScalarValue;
use dbsp::Stream;
use dbsp::circuit::CircuitPlan;
use dbsp::handles::{ZSetHandle, ZSetHandleView};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::dbsp_graph_builder::{BuildInputs, DbspGraphBuilder};
use floe_executor::dbsp_plan::{
    DbspPlanBuilder, ValidatedPlan, nexmark_bid_table, nexmark_config, validate_dbsp_plan,
};
use floe_executor::encoding::decode_projected_row_key;
use floe_executor::materialized_view::MaterializedViewRegistry;
use floe_executor::outer_stream::OuterStreamRegistry;
use floe_executor::{FloeQueryContext, load_or_register_mv};
use floe_storage::SlateCatalog;
use object_store::{ObjectStore, memory::InMemory};
use slatedb::Db;

const VIEW_NAME: &str = "mv_q1";
const SOURCE_NAME: &str = "nexmark_bid";

struct BuiltViewFixture {
    db: Arc<Db>,
    registry: Arc<MaterializedViewRegistry>,
    schema: SchemaRef,
    versions: Vec<u64>,
}

#[tokio::test]
async fn load_or_register_mv_makes_view_queryable() {
    let fixture = build_q1_fixture("mv-loader-registers", vec![bid_row(1, 2, 50)]).await;
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
async fn mv_loader_supports_as_of_filter() {
    let fixture = build_q1_fixture(
        "mv-loader-as-of",
        vec![bid_row(1, 2, 50), bid_row(2, 3, 70)],
    )
    .await;
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

    let as_of_rows = rows_at_version(&fixture.registry, version_one).await;
    assert_eq!(as_of_rows, vec![vec![1, 2, 100]]);
}

#[tokio::test]
async fn mv_loader_recovers_after_registry_restart() {
    let fixture = build_q1_fixture(
        "mv-loader-restart",
        vec![bid_row(1, 2, 50), bid_row(2, 3, 70)],
    )
    .await;
    let schema = Arc::clone(&fixture.schema);
    let restarted_registry = Arc::new(MaterializedViewRegistry::new());
    restarted_registry.set_schema(VIEW_NAME, schema);
    if let Some(state) = fixture
        .registry
        .get(VIEW_NAME)
        .and_then(|handle| handle.dbsp_state())
    {
        let handle = restarted_registry.register(VIEW_NAME.to_string());
        handle.set_dbsp_state(state);
    }

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
        .and_then(|handle| handle.dbsp_state())
        .expect("reconstructed state")
        .version();
    assert_eq!(latest_version, *fixture.versions.last().unwrap());

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let batches = df.collect().await.expect("collect batches");
    assert_eq!(int_rows(&batches), vec![vec![1, 2, 100], vec![2, 3, 140]]);
}

async fn build_q1_fixture(test_name: &str, bids: Vec<Vec<ScalarValue>>) -> BuiltViewFixture {
    let db = test_db(test_name).await;
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let plan = build_q1_plan();

    let available_sources = [SOURCE_NAME]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let ValidatedPlan {
        required_sources, ..
    } = validate_dbsp_plan(&plan, &available_sources, VIEW_NAME).expect("validate plan");

    let mut outer =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");
    let mut versions = Vec::new();
    {
        let writer = outer.writer_mut(SOURCE_NAME).expect("bid writer");
        for row in bids {
            writer.append(&row, 1).expect("append row");
            let handle = writer.flush().await.expect("flush row");
            versions.push(handle.version);
        }
    }

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("graph builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&outer, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: VIEW_NAME,
            view_name: VIEW_NAME,
            plan: &plan,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
        })
        .await
        .expect("build graph");

    let schema = mv_registry.schema(VIEW_NAME).expect("schema stored");

    BuiltViewFixture {
        db,
        registry: mv_registry,
        schema,
        versions,
    }
}

async fn rows_at_version(registry: &Arc<MaterializedViewRegistry>, version: u64) -> Vec<Vec<i64>> {
    let handle = registry.get(VIEW_NAME).expect("view registered");
    let state = handle.dbsp_state().expect("view state available");
    let view = ZSetHandleView::new(
        state.dictionary(),
        state.table(),
        state.namespace().to_string(),
        version,
    );
    let snapshot = view.materialize().await.expect("materialize snapshot");
    let mut rows = Vec::new();
    for (key, diff) in snapshot {
        if diff <= 0 {
            continue;
        }
        let decoded = decode_projected_row_key(&key).expect("decode mv row");
        let ints = row_values_to_ints(&decoded, 3);
        for _ in 0..diff {
            rows.push(ints.clone());
        }
    }
    rows
}

fn build_q1_plan() -> CircuitPlan {
    let schema = nexmark_bid_table().schema().to_arrow_schema();
    let doubled_price = (col("price") + col("price")).alias("price");
    let logical = table_scan(Some(SOURCE_NAME), &schema, None)
        .expect("scan")
        .project(vec![col("auction"), col("bidder"), doubled_price])
        .expect("project")
        .build()
        .expect("build logical");
    let planner = DbspPlanBuilder::new(nexmark_config());
    planner.build(&logical).expect("circuit plan")
}

fn gather_handle_streams(
    registry: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, Stream<ZSetHandle>> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.delta_handle_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn bid_row(auction: i64, bidder: i64, price: i64) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(auction)),
        ScalarValue::Int64(Some(bidder)),
        ScalarValue::Int64(Some(price)),
        ScalarValue::Utf8(Some("channel".to_string())),
        ScalarValue::Utf8(Some("url".to_string())),
        ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
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

fn row_values_to_ints(values: &[ScalarValue], count: usize) -> Vec<i64> {
    values
        .iter()
        .take(count)
        .map(|value| match value {
            ScalarValue::Int64(Some(v)) => *v,
            other => panic!("expected Int64 scalar, found {other:?}"),
        })
        .collect()
}
