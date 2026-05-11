use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};
use datafusion::logical_expr::{col, lit, table_scan};
use dbsp::StreamRetention;
use dbsp::storage::{KeyValueTable, SlateTable};
use floe_cdc::{CdcApplyResult, CdcTableStore};
use floe_cdc_core::{
    CdcChange, CdcColumn, CdcPrimaryKey, CdcRow, CdcRowKey, CdcSourceId, CdcSourcePosition,
    CdcTableId, CdcTableSchema, CdcTransactionId, ChangeBatch, TransactionBatch, UpstreamTableRef,
};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::dbsp_graph_builder::{BuildInputs, DbspGraphBuilder};
use floe_executor::dbsp_plan::{
    DbspPlanBuilder, nexmark_bid_table, nexmark_config, validate_dbsp_plan,
};
use floe_executor::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars};
use floe_executor::materialized_view::MaterializedViewRegistry;
use floe_executor::outer_stream::OuterStreamRegistry;
use floe_executor::source_decoder::SourceRowDecoder;
use floe_executor::stream_types::EncodedDelta;
use object_store::memory::InMemory;
use slatedb::Db;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const SOURCE_NAME: &str = "nexmark_bid";
const VIEW_NAME: &str = "mv_cdc_bid_filter";

#[tokio::test]
#[serial_test::serial]
async fn cdc_apply_deltas_drive_mv_insert_update_and_delete() {
    let db = test_db("cdc-dbsp-insert-update-delete").await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let cdc_store = CdcTableStore::new(Arc::clone(&table));
    let cdc_schema = cdc_bid_schema();
    let cdc_source_id = CdcSourceId::new("pg_main").expect("source id");
    let cdc_table_id = cdc_schema.table_id().clone();
    let cdc_schemas = HashMap::from([(cdc_table_id.clone(), cdc_schema)]);
    let decoder = SourceRowDecoder::new(bid_source_definition());

    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let plan = cdc_filter_plan().await;
    let available_sources = BTreeSet::from([SOURCE_NAME.to_string()]);
    let required_sources = validate_dbsp_plan(&plan, &available_sources, VIEW_NAME)
        .expect("validate plan")
        .required_sources;
    let mut outer_streams =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
            .await
            .expect("outer streams");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(VIEW_NAME);
    mv_registry.set_schema(
        VIEW_NAME,
        Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
        ])),
    );

    let source_refs = required_sources
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let handle_streams = gather_handle_streams(&outer_streams, &source_refs);
    let transient_streams = gather_transient_streams(&outer_streams, &source_refs);
    let (task_tx, _task_rx) = mpsc::unbounded_channel();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    builder
        .build(BuildInputs {
            graph_id: VIEW_NAME,
            view_name: VIEW_NAME,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
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

    apply_cdc_transaction(
        &cdc_store,
        &cdc_schemas,
        &mut outer_streams,
        &decoder,
        tx(
            &cdc_source_id,
            "0/10",
            vec![CdcChange::Insert {
                row: bid_row(1, 7, 500),
            }],
        ),
        1,
    )
    .await;
    wait_for_logical_version(&mv_registry, VIEW_NAME, 1).await;
    assert_eq!(
        visible_rows(&mv_registry, VIEW_NAME).await,
        vec![int_row(&[1, 7, 500])]
    );

    apply_cdc_transaction(
        &cdc_store,
        &cdc_schemas,
        &mut outer_streams,
        &decoder,
        tx(
            &cdc_source_id,
            "0/20",
            vec![CdcChange::Update {
                key: Some(bid_key(1, 7)),
                before: None,
                after: bid_row(1, 7, 50),
            }],
        ),
        2,
    )
    .await;
    wait_for_logical_version(&mv_registry, VIEW_NAME, 2).await;
    assert_eq!(
        visible_rows(&mv_registry, VIEW_NAME).await,
        Vec::<TestRow>::new()
    );

    apply_cdc_transaction(
        &cdc_store,
        &cdc_schemas,
        &mut outer_streams,
        &decoder,
        tx(
            &cdc_source_id,
            "0/30",
            vec![CdcChange::Update {
                key: Some(bid_key(1, 7)),
                before: None,
                after: bid_row(1, 7, 700),
            }],
        ),
        3,
    )
    .await;
    wait_for_logical_version(&mv_registry, VIEW_NAME, 3).await;
    assert_eq!(
        visible_rows(&mv_registry, VIEW_NAME).await,
        vec![int_row(&[1, 7, 700])]
    );

    apply_cdc_transaction(
        &cdc_store,
        &cdc_schemas,
        &mut outer_streams,
        &decoder,
        tx(
            &cdc_source_id,
            "0/40",
            vec![CdcChange::Delete {
                key: Some(bid_key(1, 7)),
                before: None,
            }],
        ),
        4,
    )
    .await;
    wait_for_logical_version(&mv_registry, VIEW_NAME, 4).await;
    assert_eq!(
        visible_rows(&mv_registry, VIEW_NAME).await,
        Vec::<TestRow>::new()
    );
}

async fn apply_cdc_transaction(
    store: &CdcTableStore,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    outer_streams: &mut OuterStreamRegistry,
    decoder: &SourceRowDecoder,
    transaction: TransactionBatch,
    version: i64,
) {
    let apply_result = store
        .apply_transaction(schemas, &transaction)
        .await
        .expect("apply CDC transaction");
    let encoded = encode_apply_result(decoder, &apply_result);
    let writer = outer_streams
        .writer_mut(SOURCE_NAME)
        .expect("CDC source outer stream writer");
    writer
        .append_encoded_batch(encoded)
        .expect("append encoded CDC deltas");
    outer_streams
        .tick_all_with_version(version)
        .await
        .expect("tick CDC deltas");
}

fn encode_apply_result(
    decoder: &SourceRowDecoder,
    apply_result: &CdcApplyResult,
) -> Vec<EncodedDelta> {
    apply_result
        .table_deltas()
        .iter()
        .flat_map(|table_deltas| {
            table_deltas.deltas().iter().map(|delta| {
                let (row, _) = decoder
                    .encode_row_values(delta.row().values())
                    .expect("encode CDC row delta");
                (row, delta.diff())
            })
        })
        .collect()
}

async fn cdc_filter_plan() -> floe_executor::dbsp_plan::CircuitPlan {
    let logical = table_scan(
        Some(SOURCE_NAME),
        &nexmark_bid_table().schema().to_arrow_schema(),
        None,
    )
    .expect("scan")
    .filter(col("price").gt_eq(lit(100_i64)))
    .expect("filter")
    .project(vec![col("auction"), col("bidder"), col("price")])
    .expect("project")
    .build()
    .expect("build logical");
    DbspPlanBuilder::new(nexmark_config())
        .build(&logical)
        .expect("build DBSP plan")
}

fn bid_source_definition() -> SourceDefinition {
    SourceDefinition::new(
        SOURCE_NAME,
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, true),
        ],
    )
    .expect("source definition")
}

fn cdc_bid_schema() -> CdcTableSchema {
    CdcTableSchema::new(
        CdcTableId::new(SOURCE_NAME).expect("table id"),
        UpstreamTableRef::new("public", SOURCE_NAME).expect("upstream table"),
        vec![
            CdcColumn::new("auction", ColumnType::Int64, false).expect("auction"),
            CdcColumn::new("bidder", ColumnType::Int64, false).expect("bidder"),
            CdcColumn::new("price", ColumnType::Int64, false).expect("price"),
            CdcColumn::new("channel", ColumnType::Utf8, true).expect("channel"),
            CdcColumn::new("url", ColumnType::Utf8, true).expect("url"),
            CdcColumn::new("date_time", ColumnType::TimestampMillis, false).expect("date_time"),
            CdcColumn::new("extra", ColumnType::Utf8, true).expect("extra"),
        ],
        CdcPrimaryKey::new(["auction", "bidder"]).expect("primary key"),
    )
    .expect("CDC schema")
}

fn tx(source_id: &CdcSourceId, lsn: &str, changes: Vec<CdcChange>) -> TransactionBatch {
    TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new(format!("tx-{lsn}")).expect("txid")),
        None,
        CdcSourcePosition::postgres(lsn, None).expect("source position"),
        vec![
            ChangeBatch::new(CdcTableId::new(SOURCE_NAME).expect("table id"), changes)
                .expect("change batch"),
        ],
    )
    .expect("transaction")
}

fn bid_key(auction: i64, bidder: i64) -> CdcRowKey {
    CdcRowKey::new([RowValue::Int64(auction), RowValue::Int64(bidder)]).expect("row key")
}

fn bid_row(auction: i64, bidder: i64, price: i64) -> CdcRow {
    CdcRow::new([
        Some(RowValue::Int64(auction)),
        Some(RowValue::Int64(bidder)),
        Some(RowValue::Int64(price)),
        Some(RowValue::Utf8("web".to_string())),
        Some(RowValue::Utf8("https://example.invalid/bid".to_string())),
        Some(RowValue::TimestampMillis(1_700_000_000_000)),
        Some(RowValue::Utf8("extra".to_string())),
    ])
    .expect("CDC row")
}

fn gather_handle_streams(
    registry: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, dbsp::stream::DeltaHandleStream> {
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

async fn wait_for_logical_version(
    registry: &MaterializedViewRegistry,
    view_name: &str,
    target_version: i64,
) {
    let handle = registry.get(view_name).expect("view registered");
    if handle.latest_version().unwrap_or(-1) >= target_version {
        return;
    }
    let mut rx = handle.version_watch();
    timeout(Duration::from_secs(5), async {
        loop {
            if rx.borrow().unwrap_or(-1) >= target_version {
                break;
            }
            rx.changed().await.expect("version watch update");
        }
    })
    .await
    .expect("wait for logical version");
}

type TestRow = Vec<Option<EncodedRowScalar>>;

async fn visible_rows(registry: &MaterializedViewRegistry, view_name: &str) -> Vec<TestRow> {
    let handle = registry.get(view_name).expect("view registered");
    let mut rows = Vec::new();
    for (encoded, diff) in handle.snapshot_encoded() {
        if diff > 0 {
            let row = decode_all_encoded_row_scalars(&encoded).expect("decode row");
            for _ in 0..diff {
                rows.push(row.clone());
            }
        }
    }
    rows
}

fn int_row(values: &[i64]) -> TestRow {
    values
        .iter()
        .copied()
        .map(EncodedRowScalar::Int64)
        .map(Some)
        .collect()
}

async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}
