use super::transient_topn::{
    TransientDirectPartitionTopNConfig, TransientDirectPartitionTopNProcessor,
    TransientDirectTop1Config, TransientDirectTop1PartitionKey, TransientDirectTop1PartitionLayout,
    TransientDirectTop1Processor, TransientTop1Processor, TransientTopNKeyLayout,
    TransientTopNProcessor,
};
use super::*;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use chrono::Utc;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Column;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::logical_expr::expr_fn::create_udf;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, Signature, TypeSignature, Volatility,
};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, col, lit, table_scan};
use datafusion::prelude::SessionContext;
use dbsp::DbspJoin;
use dbsp::DbspPredicate;
use dbsp::join::TransientJoinInputBatch;
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::StreamCursor;
use dbsp::stream::util::materialize_zset_handle;
use floe_core::source::{AppendIngestEvent, SourceColumn, SourceDataType, SourceDefinition};
use object_store::memory::InMemory;
use serde_json::{Value, json};
use slatedb::Db;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::GraphTaskError;
use crate::dbsp_bridge::DbspBridge;
use crate::dbsp_plan::{
    CircuitNode, CircuitPlan, DbspNodeKind, DbspPlanBuilder, DbspProjectNode, DbspSelectNode,
    DbspSourceNode, ProjectItem, nexmark_auction_alias_table, nexmark_auction_table,
    nexmark_bid_alias_table, nexmark_bid_table, nexmark_config, validate_dbsp_plan,
};
use crate::mv::registry::MaterializedViewRegistry;
use crate::outer_stream::OuterStreamRegistry;
use crate::source_decoder::SourceRowDecoder;

mod benchmark_child_transforms;
mod persistent_topn;
mod root_shapes;
mod source_requirements;
mod transient_source_tasks;
mod transient_transforms;

fn benchmark_join_logical_plan() -> LogicalPlan {
    let bid = nexmark_bid_table();
    let auction = nexmark_auction_table();
    let bid_schema = bid.schema().to_arrow_schema();
    let auction_schema = auction.schema().to_arrow_schema();
    table_scan(Some("nexmark_bid"), &bid_schema, None)
        .expect("bid scan")
        .join(
            table_scan(Some("nexmark_auction"), &auction_schema, None)
                .expect("auction scan")
                .build()
                .expect("auction logical"),
            JoinType::Inner,
            (
                vec![Column::from_name("auction")],
                vec![Column::from_name("id")],
            ),
            None,
        )
        .expect("join")
        .filter(col("category").eq(lit(10i64)))
        .expect("filter")
        .project(vec![
            col("auction"),
            col("bidder"),
            col("price").alias("projected_price"),
            col("seller"),
        ])
        .expect("project")
        .build()
        .expect("logical plan")
}

async fn sql_plan_with_auction_and_bid(sql: &str) -> LogicalPlan {
    let ctx = SessionContext::new();
    let bid_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_bid_table().schema().to_arrow_schema(),
    ));
    let auction_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_auction_table().schema().to_arrow_schema(),
    ));
    ctx.register_table("nexmark_bid", bid_provider)
        .expect("register nexmark_bid");
    ctx.register_table("nexmark_auction", auction_provider)
        .expect("register nexmark_auction");
    register_planner_test_udfs(&ctx);
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .expect("build logical plan");
    let optimized = ctx.state().optimize(&plan).expect("optimize logical plan");
    if logical_plan_uses_only_dbsp_supported_types(&optimized) {
        optimized
    } else {
        plan
    }
}

async fn sql_plan_with_auction_and_bid_aliases(sql: &str) -> LogicalPlan {
    let ctx = SessionContext::new();
    let bid_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_bid_table().schema().to_arrow_schema(),
    ));
    let auction_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_auction_table().schema().to_arrow_schema(),
    ));
    let bid_alias_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_bid_alias_table().schema().to_arrow_schema(),
    ));
    let auction_alias_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(
        nexmark_auction_alias_table().schema().to_arrow_schema(),
    ));
    ctx.register_table("nexmark_bid", bid_provider)
        .expect("register nexmark_bid");
    ctx.register_table("nexmark_auction", auction_provider)
        .expect("register nexmark_auction");
    ctx.register_table("bid", bid_alias_provider)
        .expect("register bid alias");
    ctx.register_table("auction", auction_alias_provider)
        .expect("register auction alias");
    register_planner_test_udfs(&ctx);
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .expect("build logical plan");
    let optimized = ctx.state().optimize(&plan).expect("optimize logical plan");
    if logical_plan_uses_only_dbsp_supported_types(&optimized) {
        optimized
    } else {
        plan
    }
}

fn logical_plan_uses_only_dbsp_supported_types(plan: &LogicalPlan) -> bool {
    logical_plan_node_supported(plan)
        && plan
            .inputs()
            .into_iter()
            .all(logical_plan_uses_only_dbsp_supported_types)
}

fn logical_plan_node_supported(plan: &LogicalPlan) -> bool {
    plan.schema()
        .fields()
        .iter()
        .all(|field| dbsp_supported_arrow_type(field.data_type()))
}

fn dbsp_supported_arrow_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::Utf8
            | DataType::Boolean
            | DataType::Timestamp(TimeUnit::Millisecond, None)
    )
}

fn register_planner_test_udfs(ctx: &SessionContext) {
    let proctime: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::TimestampMillisecondArray::from(vec![None::<i64>; len]),
            )))
        },
    );
    let passthrough_ts: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args.first().cloned().unwrap_or_else(|| {
                ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::TimestampMillisecondArray::from(vec![None::<i64>; 1]),
                ))
            }))
        },
    );
    let date_format_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(
                        datafusion::arrow::array::TimestampMillisecondArray::from(vec![
                            None::<i64>;
                            len
                        ]),
                    ))
                })
                .into_array(len)?;
            let fmt = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(
                        vec![None::<&str>; len],
                    )))
                })
                .into_array(len)?;
            let (Some(ts), Some(fmt)) = (
                ts.as_any()
                    .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>(),
                fmt.as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>(),
            ) else {
                return Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(vec![None::<&str>; len]),
                )));
            };

            let values = (0..len)
                .map(|row_idx| {
                    if ts.is_null(row_idx) || fmt.is_null(row_idx) {
                        return None;
                    }
                    let dt = chrono::DateTime::<Utc>::from_timestamp_millis(ts.value(row_idx))?;
                    let pattern = fmt
                        .value(row_idx)
                        .replace("yyyy", "%Y")
                        .replace("MM", "%m")
                        .replace("dd", "%d")
                        .replace("HH", "%H")
                        .replace("mm", "%M")
                        .replace("ss", "%S");
                    Some(dt.format(&pattern).to_string())
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::StringArray::from(values),
            )))
        },
    );
    let hour_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(
                        datafusion::arrow::array::TimestampMillisecondArray::from(vec![
                            None::<i64>;
                            len
                        ]),
                    ))
                })
                .into_array(len)?;
            let Some(ts) = ts
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
            else {
                return Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::Int64Array::from(vec![None::<i64>; len]),
                )));
            };

            let values = (0..len)
                .map(|row_idx| {
                    (!ts.is_null(row_idx))
                        .then(|| ts.value(row_idx).div_euclid(3_600_000).rem_euclid(24))
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::Int64Array::from(values),
            )))
        },
    );
    let count_char_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(
                        vec![None::<&str>; len],
                    )))
                })
                .into_array(len)?;
            let needle = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(
                        vec![None::<&str>; len],
                    )))
                })
                .into_array(len)?;
            let (Some(text), Some(needle)) = (
                text.as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>(),
                needle
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>(),
            ) else {
                return Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::Int64Array::from(vec![None::<i64>; len]),
                )));
            };

            let values = (0..len)
                .map(|row_idx| {
                    if text.is_null(row_idx) || needle.is_null(row_idx) {
                        return None;
                    }
                    let haystack = text.value(row_idx);
                    let token = needle.value(row_idx);
                    Some(if token.is_empty() {
                        0
                    } else {
                        i64::try_from(haystack.matches(token).count()).unwrap_or(i64::MAX)
                    })
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::Int64Array::from(values),
            )))
        },
    );
    ctx.register_udf(create_udf(
        "proctime",
        vec![],
        DataType::Timestamp(TimeUnit::Millisecond, None),
        Volatility::Volatile,
        proctime,
    ));
    ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
        datafusion::logical_expr::expr_fn::SimpleScalarUDF::new_with_signature(
            "tumble",
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::clone(&passthrough_ts),
        ),
    ));
    ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
        datafusion::logical_expr::expr_fn::SimpleScalarUDF::new_with_signature(
            "hop",
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::clone(&passthrough_ts),
        ),
    ));
    ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
        datafusion::logical_expr::expr_fn::SimpleScalarUDF::new_with_signature(
            "session",
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::clone(&passthrough_ts),
        ),
    ));
    ctx.register_udf(create_udf(
        "date_format",
        vec![
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Utf8,
        ],
        DataType::Utf8,
        Volatility::Immutable,
        date_format_udf,
    ));
    ctx.register_udf(create_udf(
        "hour",
        vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        hour_udf,
    ));
    ctx.register_udf(create_udf(
        "count_char",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        count_char_udf,
    ));
}

async fn assert_tick_matches_transform(
    table: &Arc<dyn KeyValueTable>,
    cursor: &mut StreamCursor<dbsp::handles::ZSetHandle>,
    source_batch: Vec<(Vec<u8>, i64)>,
    transform: &Arc<DeltaTransformFn>,
    label: &str,
) {
    let (_, handle) = cursor.next().await.expect("next child handle");
    let mut cache = HashMap::new();
    let actual = materialize_zset_handle::<Vec<u8>>(Arc::clone(table), &mut cache, &handle)
        .await
        .expect("materialize child handle");
    let expected =
        consolidate_encoded_deltas(transform(Arc::new(source_batch)).await.expect("transform"));
    assert_eq!(actual, expected, "{label}");
}

fn consolidate_encoded_deltas(deltas: Vec<(Vec<u8>, i64)>) -> HashMap<Vec<u8>, i64> {
    let mut map = HashMap::new();
    for (row, diff) in deltas {
        let next = map.get(&row).copied().unwrap_or(0i64).saturating_add(diff);
        if next == 0 {
            map.remove(&row);
        } else {
            map.insert(row, next);
        }
    }
    map
}

fn required_mask(
    requirements: &[PlanSourceRequirements],
    definition: &SourceDefinition,
    source_name: &str,
) -> Arc<[bool]> {
    let requirement = requirements
        .iter()
        .find(|requirement| requirement.source_name == source_name)
        .unwrap_or_else(|| panic!("missing source requirement for {source_name}"));
    let mut mask = vec![false; definition.columns().len()];
    for column_idx in &requirement.required_columns {
        mask[*column_idx] = true;
    }
    Arc::from(mask)
}

fn encode_event(decoder: &SourceRowDecoder, payload: Value, source: &str) -> Vec<u8> {
    let event = AppendIngestEvent::new(source, payload);
    decoder
        .encode_row_key(&event)
        .expect("encode append ingest event")
        .0
}

fn test_topn_key_layout() -> TransientTopNKeyLayout {
    TransientTopNKeyLayout {
        input_schema: nexmark_bid_table().schema().clone(),
        partition_columns: Arc::new(vec![0]),
        order_columns: Arc::new(vec![2]),
        order_types: Arc::new(vec![DbspScalarType::Int64]),
        precompute_evaluator: None,
    }
}

fn test_topn_node(limit: usize, offset: usize) -> DbspTopNNode {
    let input_schema = nexmark_bid_table().schema().clone();
    let order_by = vec![
        dbsp::OrderExpr::try_new(col("price"), Arc::clone(&input_schema), true, true)
            .expect("order expr"),
    ];
    DbspTopNNode::try_new(input_schema, vec![col("auction")], order_by, limit, offset)
        .expect("topn node")
}

fn bid_event_payload(auction: i64, bidder: i64, price: i64) -> Value {
    bid_event_payload_with_channel_and_ts(auction, bidder, price, "channel", 1_700_000_000_000i64)
}

fn bid_event_payload_with_channel_and_ts(
    auction: i64,
    bidder: i64,
    price: i64,
    channel: &str,
    date_time: i64,
) -> Value {
    json!({
        "auction": auction,
        "bidder": bidder,
        "price": price,
        "channel": channel,
        "url": "https://example.invalid/bid",
        "date_time": date_time,
        "extra": "extra"
    })
}

fn auction_event_payload(id: i64, seller: i64, category: i64) -> Value {
    json!({
        "id": id,
        "item_name": "item",
        "description": "description",
        "initial_bid": 1i64,
        "reserve": 2i64,
        "seller": seller,
        "category": category,
        "expires": 1_700_000_000_000i64,
        "date_time": 1_700_000_000_000i64,
        "extra": "extra"
    })
}

fn nexmark_bid_source_definition() -> SourceDefinition {
    SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new("auction", SourceDataType::Int64),
            SourceColumn::new("bidder", SourceDataType::Int64),
            SourceColumn::new("price", SourceDataType::Int64),
            SourceColumn::new("channel", SourceDataType::Utf8),
            SourceColumn::new("url", SourceDataType::Utf8),
            SourceColumn::new("date_time", SourceDataType::TimestampMillis),
            SourceColumn::new("extra", SourceDataType::Utf8),
        ],
    )
    .expect("bid definition")
}

fn nexmark_auction_source_definition() -> SourceDefinition {
    SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("item_name", SourceDataType::Utf8),
            SourceColumn::new("description", SourceDataType::Utf8),
            SourceColumn::new("initial_bid", SourceDataType::Int64),
            SourceColumn::new("reserve", SourceDataType::Int64),
            SourceColumn::new("seller", SourceDataType::Int64),
            SourceColumn::new("category", SourceDataType::Int64),
            SourceColumn::new("expires", SourceDataType::TimestampMillis),
            SourceColumn::new("date_time", SourceDataType::TimestampMillis),
            SourceColumn::new("extra", SourceDataType::Utf8),
        ],
    )
    .expect("auction definition")
}

async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}
