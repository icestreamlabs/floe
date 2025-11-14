use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::Column;
use datafusion::logical_expr::{JoinType, col, lit, table_scan};
use datafusion::scalar::ScalarValue;
use dbsp::Stream;
use dbsp::handles::{ZSetHandle, ZSetHandleView};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::dbsp_graph_builder::{BuildInputs, DbspGraphBuilder};
use floe_executor::dbsp_plan::{
    DbspPlanBuilder, nexmark_auction_table, nexmark_bid_table, nexmark_config,
    nexmark_person_table, validate_dbsp_plan,
};
use floe_executor::encoding::decode_projected_row_key;
use floe_executor::materialized_view::MaterializedViewRegistry;
use floe_executor::outer_stream::OuterStreamRegistry;
use object_store::memory::InMemory;
use slatedb::Db;

fn arrow_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

#[tokio::test]
async fn filter_and_projection_materializes_mv() {
    let db = test_db("filter-projection").await;
    let view_name = "mv_price";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
            .filter(col("bidder").eq(lit(42i64)))
            .expect("filter")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append(&bid_row(1, 42, 99), 1)
        .expect("append bidder 42");
    bid_writer.flush().await.expect("flush first step");
    bid_writer
        .append(&bid_row(2, 7, 50), 1)
        .expect("append bidder 7");
    bid_writer.flush().await.expect("flush second step");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let outputs = builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
        })
        .await
        .expect("build graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![vec![ScalarValue::Int64(Some(99))]]);
}

#[tokio::test]
async fn inner_join_materializes_mv() {
    let db = test_db("inner-join").await;
    let view_name = "mv_join";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let person_schema = nexmark_person_schema();
        let auction_schema = nexmark_auction_schema();
        let right = table_scan(Some("nexmark_person"), &person_schema, None)
            .expect("person scan")
            .project(vec![col("id").alias("person_id"), col("name")])
            .expect("person project")
            .build()
            .expect("person plan");
        let logical = table_scan(Some("nexmark_auction"), &auction_schema, None)
            .expect("auction scan")
            .join(
                right,
                JoinType::Inner,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("person_id")],
                ),
                None,
            )
            .expect("join")
            .project(vec![col("id"), col("name")])
            .expect("project")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_person", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let person_writer = registry
        .writer_mut("nexmark_person")
        .expect("person writer");
    person_writer
        .append(&person_row(100, "alice"), 1)
        .expect("append alice");
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append(&auction_row(10, 100), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let outputs = builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
        })
        .await
        .expect("build join graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::Int64(Some(10)),
            ScalarValue::Utf8(Some("alice".to_string())),
        ]]
    );
}

#[tokio::test]
async fn rebuild_recovers_materialized_view_without_reingest() {
    let db = test_db("rebuild").await;
    let view_name = "mv_rebuild";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .filter(col("bidder").eq(lit(42i64)))
            .expect("filter")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    writer.append(&bid_row(1, 42, 80), 1).expect("append row");
    writer.flush().await.expect("flush one");
    writer
        .append(&bid_row(2, 42, 81), 1)
        .expect("append second");
    writer.flush().await.expect("flush two");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("auction", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);

    {
        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
        let outputs = builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan: &plan,
                mv_registry: Arc::clone(&mv_registry),
                outer_handle_streams: &handle_streams,
            })
            .await
            .expect("initial build");
        assert_eq!(outputs.required_sources, required_sources.clone());
    }

    materialized_rows(&mv_registry, view_name).await;

    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let outputs = builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
        })
        .await
        .expect("rebuild");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows.len(), 2);
}

fn gather_handle_streams(
    registry: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, Stream<ZSetHandle>> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.handle_stream(source) {
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

fn person_row(id: i64, name: &str) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Utf8(Some(name.to_string())),
        ScalarValue::Utf8(Some("email".to_string())),
        ScalarValue::Utf8(Some("card".to_string())),
        ScalarValue::Utf8(Some("city".to_string())),
        ScalarValue::Utf8(Some("state".to_string())),
        ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

fn auction_row(id: i64, seller: i64) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Utf8(Some("item".to_string())),
        ScalarValue::Utf8(Some("desc".to_string())),
        ScalarValue::Int64(Some(10)),
        ScalarValue::Int64(Some(20)),
        ScalarValue::Int64(Some(seller)),
        ScalarValue::Int64(Some(5)),
        ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), None),
        ScalarValue::TimestampMillisecond(Some(1_700_000_100_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}

fn nexmark_bid_schema() -> Arc<Schema> {
    nexmark_bid_table().schema().to_arrow_schema()
}

fn nexmark_person_schema() -> Arc<Schema> {
    nexmark_person_table().schema().to_arrow_schema()
}

fn nexmark_auction_schema() -> Arc<Schema> {
    nexmark_auction_table().schema().to_arrow_schema()
}

async fn materialized_rows(
    registry: &MaterializedViewRegistry,
    view_name: &str,
) -> Vec<Vec<ScalarValue>> {
    let handle = registry.get(view_name).expect("view registered");
    let state = handle.dbsp_state().expect("mv state");
    let view = ZSetHandleView::new(
        state.dictionary(),
        state.table(),
        state.namespace().to_string(),
        state.version(),
    );
    let snapshot = view.materialize().await.expect("materialize view");
    let mut rows = Vec::new();
    for (key, diff) in snapshot {
        let decoded = decode_projected_row_key(&key).expect("decode row");
        if diff > 0 {
            for _ in 0..diff {
                rows.push(decoded.clone());
            }
        }
    }
    rows
}
