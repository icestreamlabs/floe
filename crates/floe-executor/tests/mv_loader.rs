use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{col, table_scan};
use dbsp::StreamRetention;
use dbsp::circuit::CircuitPlan;
use dbsp::handles::ZSetHandle;
use floe_executor::GraphTaskError;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::dbsp_graph_builder::{BuildInputs, DbspGraphBuilder};
use floe_executor::dbsp_plan::{
    DbspPlanBuilder, ValidatedPlan, nexmark_bid_table, nexmark_config, validate_dbsp_plan,
};
use floe_executor::materialized_view::MaterializedViewRegistry;
use floe_executor::outer_stream::OuterStreamRegistry;
use floe_executor::{FloeQueryContext, load_or_register_mv};
use floe_storage::SlateCatalog;
use object_store::{ObjectStore, memory::InMemory};
use slatedb::Db;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
    let fixture = build_q1_fixture("mv-loader-registers", vec![encode_bid_row(1, 2, 50)]).await;
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
async fn load_or_register_mv_registers_overlay_only_view() {
    let db = test_db("mv-loader-overlay-only").await;
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.set_schema(
        VIEW_NAME,
        Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
        ])),
    );
    let handle = registry.register(VIEW_NAME.to_string());
    handle.append_encoded_overlay_batch(1, vec![(encode_q1_row(1, 2, 100), 1)]);

    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME)
        .await
        .expect("load overlay-only mv");

    assert!(
        handle.dbsp_state().is_none(),
        "overlay-only MV should not force SlateDB state hydration"
    );

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let batches = df.collect().await.expect("collect");
    assert_eq!(int_rows(&batches), vec![vec![1, 2, 100]]);
}

#[tokio::test]
async fn mv_loader_reads_persisted_base_plus_overlay() {
    let fixture =
        build_q1_fixture("mv-loader-hybrid-overlay", vec![encode_bid_row(1, 2, 50)]).await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.set_schema(VIEW_NAME, Arc::clone(&fixture.schema));
    let source_handle = fixture.registry.get(VIEW_NAME).expect("source handle");
    let handle = registry.register(VIEW_NAME.to_string());
    let logical_base_version = source_handle
        .latest_version()
        .and_then(|version| u64::try_from(version).ok())
        .unwrap_or_else(|| *fixture.versions.last().unwrap_or(&1));
    if let Some(state) = source_handle.dbsp_state() {
        let state_version = state.version();
        let state = state.with_logical_version(logical_base_version);
        handle.set_dbsp_state(state.clone());
        handle.publish_version(
            logical_base_version as i64,
            ZSetHandle {
                ns: state.namespace().to_string(),
                version: state_version,
            },
        );
    } else {
        let base_rows = source_handle
            .snapshot_encoded()
            .into_iter()
            .collect::<Vec<_>>();
        handle.append_encoded_overlay_batch(logical_base_version, base_rows);
    }
    handle.append_encoded_overlay_batch(
        logical_base_version + 1,
        vec![(encode_q1_row(2, 3, 140), 1)],
    );

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
    let fixture = build_q1_fixture(
        "mv-loader-as-of",
        vec![encode_bid_row(1, 2, 50), encode_bid_row(2, 3, 70)],
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
}

#[tokio::test]
async fn mv_loader_recovers_after_registry_restart() {
    let fixture = build_q1_fixture(
        "mv-loader-restart",
        vec![encode_bid_row(1, 2, 50), encode_bid_row(2, 3, 70)],
    )
    .await;
    let schema = Arc::clone(&fixture.schema);
    let restarted_registry = Arc::new(MaterializedViewRegistry::new());
    restarted_registry.set_schema(VIEW_NAME, schema);
    if let Some(source_handle) = fixture.registry.get(VIEW_NAME) {
        let handle = restarted_registry.register(VIEW_NAME.to_string());
        if let Some(state) = source_handle.dbsp_state() {
            handle.set_dbsp_state(state);
        }
        if let Some((_, target_version, overlay)) = source_handle.encoded_overlay_batches(None) {
            handle.append_encoded_overlay_batch(target_version, overlay);
        }
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
        .and_then(|handle| handle.latest_version())
        .and_then(|version| u64::try_from(version).ok())
        .unwrap_or(0);
    assert!(
        latest_version == 0 || latest_version == *fixture.versions.last().unwrap(),
        "expected latest version to be unknown (overlay-only) or match source frontier"
    );

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await
        .expect("plan SQL");
    let batches = df.collect().await.expect("collect batches");
    assert_eq!(int_rows(&batches), vec![vec![1, 2, 100], vec![2, 3, 140]]);
}

async fn build_q1_fixture(test_name: &str, bids: Vec<Vec<u8>>) -> BuiltViewFixture {
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
            writer.append_encoded(row, 1).expect("append row");
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
    let transient_streams = gather_transient_streams(&outer, &source_refs);
    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    builder
        .build(BuildInputs {
            graph_id: VIEW_NAME,
            view_name: VIEW_NAME,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
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
) -> HashMap<String, dbsp::DeltaHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.delta_handle_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn gather_transient_streams(
    registry: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, floe_executor::outer_stream::TransientSourceHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.transient_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn encode_q1_row(auction: i64, bidder: i64, price: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + (3 * 9));
    encoded.extend_from_slice(&(3_u32).to_le_bytes());
    append_i64(&mut encoded, auction);
    append_i64(&mut encoded, bidder);
    append_i64(&mut encoded, price);
    encoded
}

fn encode_bid_row(auction: i64, bidder: i64, price: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + (4 * 9) + 32);
    encoded.extend_from_slice(&(7_u32).to_le_bytes());
    append_i64(&mut encoded, auction);
    append_i64(&mut encoded, bidder);
    append_i64(&mut encoded, price);
    append_utf8(&mut encoded, "channel");
    append_utf8(&mut encoded, "url");
    append_timestamp_millis(&mut encoded, 1_700_000_000_000);
    append_utf8(&mut encoded, "extra");
    encoded
}

fn append_i64(encoded: &mut Vec<u8>, value: i64) {
    encoded.push(0x01);
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn append_timestamp_millis(encoded: &mut Vec<u8>, value: i64) {
    encoded.push(0x03);
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn append_utf8(encoded: &mut Vec<u8>, value: &str) {
    encoded.push(0x02);
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len()).expect("utf8 field length");
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(bytes);
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
