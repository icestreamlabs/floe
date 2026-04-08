use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::common::Column;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::functions_aggregate::expr_fn::{avg, count, count_distinct, max, min, sum};
use datafusion::logical_expr::expr_fn::ExprFunctionExt;
use datafusion::logical_expr::expr_fn::create_udf;
use datafusion::logical_expr::{
    ColumnarValue, Expr, JoinType, ScalarFunctionImplementation, Volatility, col, lit, table_scan,
};
use datafusion::prelude::SessionContext;
use dbsp::StreamRetention;
use dbsp::handles::ZSetHandle;
use dbsp::storage::SlateTable;
use floe_executor::GraphTaskError;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::dbsp_graph_builder::{BuildInputs, DbspGraphBuilder};
use floe_executor::dbsp_plan::{
    DbspPlanBuilder, nexmark_auction_table, nexmark_bid_table, nexmark_config,
    nexmark_person_table, validate_dbsp_plan,
};
use floe_executor::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars};
use floe_executor::materialized_view::MaterializedViewRegistry;
use floe_executor::outer_stream::OuterStreamRegistry;
use floe_executor::source_journal::SourceBatchJournal;
use object_store::memory::InMemory;
use regex::Regex;
use slatedb::Db;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

fn arrow_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

fn udf_batch_len(args: &[ColumnarValue]) -> usize {
    args.iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1)
}

fn split_index_value(text: &str, delimiter: &str, index: i64) -> Option<String> {
    if index < 0 || delimiter.is_empty() {
        return None;
    }
    text.split(delimiter)
        .nth(index as usize)
        .map(str::to_string)
}

async fn sql_plan(sql: &str) -> datafusion::logical_expr::LogicalPlan {
    let ctx = SessionContext::new();
    let provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(nexmark_bid_schema()));
    ctx.register_table("nexmark_bid", provider)
        .expect("register nexmark_bid");
    register_planner_test_udfs(&ctx);
    ctx.state()
        .create_logical_plan(sql)
        .await
        .expect("build logical plan")
}

fn register_planner_test_udfs(ctx: &SessionContext) {
    let passthrough_int64: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let array: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>; len]));
            Ok(ColumnarValue::Array(array))
        },
    );
    let date_format_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(TimestampMillisecondArray::from(vec![
                        None::<i64>;
                        len
                    ])))
                })
                .into_array(len)?;
            let fmt = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let (Some(ts), Some(fmt)) = (
                ts.as_any().downcast_ref::<TimestampMillisecondArray>(),
                fmt.as_any().downcast_ref::<StringArray>(),
            ) else {
                let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
                return Ok(ColumnarValue::Array(array));
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
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        },
    );
    let regexp_extract_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let pattern = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let group = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![None::<i64>; len])))
                })
                .into_array(len)?;
            let (Some(text), Some(pattern), Some(group)) = (
                text.as_any().downcast_ref::<StringArray>(),
                pattern.as_any().downcast_ref::<StringArray>(),
                group.as_any().downcast_ref::<Int64Array>(),
            ) else {
                let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
                return Ok(ColumnarValue::Array(array));
            };

            let mut cache: HashMap<String, Option<Regex>> = HashMap::new();
            let values = (0..len)
                .map(|row_idx| {
                    if text.is_null(row_idx) || pattern.is_null(row_idx) || group.is_null(row_idx) {
                        return None;
                    }
                    let group_idx = group.value(row_idx);
                    if group_idx < 0 {
                        return None;
                    }
                    let pattern_text = pattern.value(row_idx);
                    let regex = cache
                        .entry(pattern_text.to_string())
                        .or_insert_with(|| Regex::new(pattern_text).ok());
                    let regex = regex.as_ref()?;
                    let captures = regex.captures(text.value(row_idx))?;
                    let matched = captures.get(group_idx as usize)?;
                    Some(matched.as_str().to_string())
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        },
    );
    let split_index_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let delimiter = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let index = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![None::<i64>; len])))
                })
                .into_array(len)?;
            let (Some(text), Some(delimiter), Some(index)) = (
                text.as_any().downcast_ref::<StringArray>(),
                delimiter.as_any().downcast_ref::<StringArray>(),
                index.as_any().downcast_ref::<Int64Array>(),
            ) else {
                let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
                return Ok(ColumnarValue::Array(array));
            };

            let values = (0..len)
                .map(|row_idx| {
                    if text.is_null(row_idx) || delimiter.is_null(row_idx) || index.is_null(row_idx)
                    {
                        return None;
                    }
                    split_index_value(
                        text.value(row_idx),
                        delimiter.value(row_idx),
                        index.value(row_idx),
                    )
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        },
    );
    let proctime: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![None::<i64>; len]));
            Ok(ColumnarValue::Array(array))
        },
    );
    let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
    ctx.register_udf(create_udf(
        "proctime",
        vec![],
        ts,
        Volatility::Volatile,
        proctime,
    ));
    ctx.register_udf(create_udf(
        "hour",
        vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        Arc::clone(&passthrough_int64),
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
        "regexp_extract",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        regexp_extract_udf,
    ));
    ctx.register_udf(create_udf(
        "split_index",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        split_index_udf,
    ));
    ctx.register_udf(create_udf(
        "count_char",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        passthrough_int64,
    ));
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
        .append_encoded(encoded_bid_row(1, 42, 99), 1)
        .expect("append bidder 42");
    bid_writer.flush().await.expect("flush first step");
    bid_writer
        .append_encoded(encoded_bid_row(2, 7, 50), 1)
        .expect("append bidder 7");
    bid_writer.flush().await.expect("flush second step");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    let outputs = builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[99])]);
}

#[tokio::test]
async fn source_batch_journal_replay_recovers_overlay_view() {
    let db = test_db("source-batch-journal-replay").await;
    let view_name = "mv_source_batch_journal";
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema.clone());

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let journal = SourceBatchJournal::new(Arc::new(SlateTable::new(Arc::clone(&db))));
    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        writer
            .append_encoded(encoded_bid_row(1, 42, 99), 1)
            .expect("append bidder 42");
        writer
            .append_encoded(encoded_bid_row(2, 7, 50), 1)
            .expect("append bidder 7");
        let batch = writer
            .pending_transient_batch(1)
            .expect("pending transient batch");
        journal
            .append("nexmark_bid", 1, None, &batch.deltas)
            .await
            .expect("append source journal");
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick transient source root");
    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[99])]);

    let mut restarted_bridge = DbspBridge::new(Arc::clone(&db))
        .await
        .expect("restarted bridge");
    let mut restarted_registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut restarted_bridge)
            .await
            .expect("restarted outer streams");
    restarted_registry.set_durable_enabled("nexmark_bid", false);

    let restarted_mv_registry = Arc::new(MaterializedViewRegistry::new());
    restarted_mv_registry.register(view_name);
    restarted_mv_registry.set_schema(view_name, arrow_schema);

    let restarted_handle_streams = gather_handle_streams(&restarted_registry, &source_refs);
    let restarted_transient_streams = gather_transient_streams(&restarted_registry, &source_refs);
    let mut restarted_builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("restarted builder");
    restarted_builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&restarted_mv_registry),
            outer_handle_streams: &restarted_handle_streams,
            outer_transient_streams: &restarted_transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("rebuild graph");

    journal
        .replay_committed_entries_up_to(&mut restarted_registry, 1, &required_sources)
        .await
        .expect("replay source journal");
    wait_for_logical_version(&restarted_mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&restarted_mv_registry, view_name, 1).await;

    let restarted_rows = visible_rows(&restarted_mv_registry, view_name).await;
    assert_eq!(restarted_rows, rows);
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
        .append_encoded(encoded_person_row(100, "alice"), 1)
        .expect("append alice");
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    let outputs = builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build join graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_utf8_row(10, Some("alice"))]);
}

#[tokio::test]
async fn pushed_join_filter_keeps_advancing_with_static_build_side() {
    let db = test_db("join-filter-pushdown-static-build").await;
    let view_name = "mv_join_filter_pushdown";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let bid_schema = nexmark_bid_schema();
        let auction_schema = nexmark_auction_schema();
        let logical = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .join(
                table_scan(Some("nexmark_auction"), &auction_schema, None)
                    .expect("auction scan")
                    .build()
                    .expect("auction plan"),
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
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("projected_price", DataType::Int64, true),
            Field::new("seller", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(1, 100, 10), 1)
        .expect("append matching auction");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(2, 200, 5), 1)
        .expect("append filtered auction");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction setup");

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(1, 42, 10), 1)
            .expect("append first matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(2, 7, 20), 1)
            .expect("append filtered bid");
        bid_writer
            .append_encoded(encoded_bid_row(1, 8, 30), 1)
            .expect("append second matching bid");
    }
    registry
        .tick_all_with_version(2)
        .await
        .expect("tick first bid batch");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(1, 9, 40), 1)
            .expect("append later matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(2, 10, 50), 1)
            .expect("append later filtered bid");
    }
    registry
        .tick_all_with_version(3)
        .await
        .expect("tick second bid batch");

    wait_for_logical_version(&mv_registry, view_name, 3).await;
    wait_for_visible_row_count(&mv_registry, view_name, 3).await;

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(2, 11, 60), 1)
            .expect("append no-op filtered bid");
    }
    registry
        .tick_all_with_version(4)
        .await
        .expect("tick no-op bid batch");

    wait_for_logical_version(&mv_registry, view_name, 4).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    rows.sort_by_key(|row| scalar_i64(row.get(1)));
    assert_eq!(
        rows,
        vec![
            int_row(&[1, 8, 30, 100]),
            int_row(&[1, 9, 40, 100]),
            int_row(&[1, 42, 10, 100])
        ]
    );
}

#[tokio::test]
async fn pushed_join_filter_preserves_rows_with_source_journal_fast_path() {
    let db = test_db("join-filter-transient-join-inputs").await;
    let view_name = "mv_join_filter_transient_inputs";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let bid_schema = nexmark_bid_schema();
        let auction_schema = nexmark_auction_schema();
        let logical = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .join(
                table_scan(Some("nexmark_auction"), &auction_schema, None)
                    .expect("auction scan")
                    .build()
                    .expect("auction plan"),
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
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
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
    registry.set_durable_enabled("nexmark_bid", false);
    registry.set_durable_enabled("nexmark_auction", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("projected_price", DataType::Int64, true),
            Field::new("seller", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(1, 100, 10), 1)
        .expect("append matching auction");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(2, 200, 5), 1)
        .expect("append filtered auction");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction setup");

    let expected_rows = 64usize;
    for idx in 0..expected_rows {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(1, 1_000 + idx as i64, 10 + idx as i64), 1)
            .expect("append matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(2, 2_000 + idx as i64, 20 + idx as i64), 1)
            .expect("append filtered bid");
        registry
            .tick_all_with_version(i64::try_from(idx + 2).expect("version"))
            .await
            .expect("tick bid batch");
    }

    wait_for_visible_row_count(&mv_registry, view_name, expected_rows).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by_key(|row| scalar_i64(row.get(1)));
    assert_eq!(rows.len(), expected_rows);
    for (idx, row) in rows.iter().enumerate() {
        assert_eq!(
            row,
            &int_row(&[1, 1_000 + idx as i64, 10 + idx as i64, 100])
        );
    }
}

#[tokio::test]
async fn pushed_join_filter_source_journal_replay_recovers_with_static_build_side() {
    let db = test_db("join-filter-transient-join-inputs-replay").await;
    let view_name = "mv_join_filter_transient_inputs_replay";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let bid_schema = nexmark_bid_schema();
        let auction_schema = nexmark_auction_schema();
        let logical = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .join(
                table_scan(Some("nexmark_auction"), &auction_schema, None)
                    .expect("auction scan")
                    .build()
                    .expect("auction plan"),
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
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
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
    registry.set_durable_enabled("nexmark_bid", false);
    registry.set_durable_enabled("nexmark_auction", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("projected_price", DataType::Int64, true),
        Field::new("seller", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema.clone());

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let journal = SourceBatchJournal::new(Arc::new(SlateTable::new(Arc::clone(&db))));

    {
        let auction_writer = registry
            .writer_mut("nexmark_auction")
            .expect("auction writer");
        auction_writer
            .append_encoded(encoded_auction_row_with_category(1, 100, 10), 1)
            .expect("append matching auction");
        auction_writer
            .append_encoded(encoded_auction_row_with_category(2, 200, 5), 1)
            .expect("append filtered auction");
        let batch = auction_writer
            .pending_transient_batch(1)
            .expect("pending transient auction batch");
        journal
            .append("nexmark_auction", 1, None, &batch.deltas)
            .await
            .expect("append auction source journal");
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction setup");

    let expected_rows = 8usize;
    let output_version = i64::try_from(expected_rows).expect("output version");
    let max_version = i64::try_from(expected_rows + 1).expect("max version");
    let max_version_u64 = u64::try_from(max_version).expect("max version u64");
    for idx in 0..expected_rows {
        let version = i64::try_from(idx + 2).expect("version");
        {
            let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
            bid_writer
                .append_encoded(encoded_bid_row(1, 1_000 + idx as i64, 10 + idx as i64), 1)
                .expect("append matching bid");
            bid_writer
                .append_encoded(encoded_bid_row(2, 2_000 + idx as i64, 20 + idx as i64), 1)
                .expect("append filtered bid");
            let batch = bid_writer
                .pending_transient_batch(version)
                .expect("pending transient bid batch");
            journal
                .append(
                    "nexmark_bid",
                    u64::try_from(version).expect("bid version u64"),
                    None,
                    &batch.deltas,
                )
                .await
                .expect("append bid source journal");
        }
        registry
            .tick_all_with_version(version)
            .await
            .expect("tick bid batch");
    }

    wait_for_logical_version(&mv_registry, view_name, output_version).await;
    wait_for_visible_row_count(&mv_registry, view_name, expected_rows).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by_key(|row| scalar_i64(row.get(1)));

    let mut restarted_bridge = DbspBridge::new(Arc::clone(&db))
        .await
        .expect("restarted bridge");
    let mut restarted_registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut restarted_bridge)
            .await
            .expect("restarted outer streams");
    restarted_registry.set_durable_enabled("nexmark_bid", false);
    restarted_registry.set_durable_enabled("nexmark_auction", false);

    let restarted_mv_registry = Arc::new(MaterializedViewRegistry::new());
    restarted_mv_registry.register(view_name);
    restarted_mv_registry.set_schema(view_name, arrow_schema);

    let restarted_handle_streams = gather_handle_streams(&restarted_registry, &source_refs);
    let restarted_transient_streams = gather_transient_streams(&restarted_registry, &source_refs);
    let mut restarted_builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("restarted builder");
    restarted_builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&restarted_mv_registry),
            outer_handle_streams: &restarted_handle_streams,
            outer_transient_streams: &restarted_transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("rebuild graph");

    journal
        .replay_committed_entries_up_to(&mut restarted_registry, max_version_u64, &required_sources)
        .await
        .expect("replay source journal");
    wait_for_logical_version(&restarted_mv_registry, view_name, output_version).await;
    wait_for_visible_row_count(&restarted_mv_registry, view_name, expected_rows).await;

    let mut restarted_rows = visible_rows(&restarted_mv_registry, view_name).await;
    restarted_rows.sort_by_key(|row| scalar_i64(row.get(1)));
    assert_eq!(restarted_rows, rows);
}

#[tokio::test]
async fn inner_join_materializes_mv_with_transient_join_root_fast_path() {
    let db = test_db("inner-join-transient-root").await;
    let view_name = "mv_join_transient_root";
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
        .append_encoded(encoded_person_row(100, "alice"), 1)
        .expect("append alice");
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    let outputs = builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient join graph");

    assert_eq!(outputs.required_sources, required_sources);
    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_utf8_row(10, Some("alice"))]);
}

#[tokio::test]
async fn left_outer_join_materializes_null_extended_rows() {
    let db = test_db("left-outer-join").await;
    let view_name = "mv_left_join";
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
                JoinType::Left,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("person_id")],
                ),
                None,
            )
            .expect("left join")
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
        .append_encoded(encoded_person_row(100, "alice"), 1)
        .expect("append alice");
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append matched auction");
    auction_writer
        .append_encoded(encoded_auction_row(11, 999), 1)
        .expect("append unmatched auction");
    auction_writer.flush().await.expect("flush auctions");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build left join graph");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_utf8_row(10, Some("alice")), int_utf8_row(11, None)]
    );
}

#[tokio::test]
async fn left_outer_join_live_updates_preserve_logical_versions_on_noop_ticks() {
    let db = test_db("left-outer-join-live-noop").await;
    let view_name = "mv_left_join_live_noop";
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
                JoinType::Left,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("person_id")],
                ),
                None,
            )
            .expect("left join")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build left join graph");

    {
        let auction_writer = registry
            .writer_mut("nexmark_auction")
            .expect("auction writer");
        auction_writer
            .append_encoded(encoded_auction_row(11, 999), 1)
            .expect("append unmatched auction");
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick unmatched auction");
    wait_for_logical_version(&mv_registry, view_name, 1).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![int_utf8_row(11, None)]
    );

    {
        let person_writer = registry
            .writer_mut("nexmark_person")
            .expect("person writer");
        person_writer
            .append_encoded(encoded_person_row(100, "alice"), 1)
            .expect("append unrelated person");
    }
    registry
        .tick_all_with_version(2)
        .await
        .expect("tick unrelated person");
    wait_for_logical_version(&mv_registry, view_name, 2).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![int_utf8_row(11, None)]
    );

    {
        let person_writer = registry
            .writer_mut("nexmark_person")
            .expect("person writer");
        person_writer
            .append_encoded(encoded_person_row(999, "bob"), 1)
            .expect("append matching person");
    }
    registry
        .tick_all_with_version(3)
        .await
        .expect("tick matching person");
    wait_for_logical_version(&mv_registry, view_name, 3).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![int_utf8_row(11, Some("bob"))]
    );
}

#[tokio::test]
async fn aggregate_materializes_mv() {
    let db = test_db("aggregate").await;
    let view_name = "mv_aggregate";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    sum(col("price")).alias("total"),
                    min(col("price")).alias("min_price"),
                    max(col("price")).alias("max_price"),
                    avg(col("price")).alias("avg_price"),
                ],
            )
            .expect("aggregate")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let view_handle = mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
        Field::new("min_price", DataType::Int64, true),
        Field::new("max_price", DataType::Int64, true),
        Field::new("avg_price", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append bidder 42");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), 1)
        .expect("append bidder 42");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 5), 1)
        .expect("append bidder 7");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("aggregate update timeout")
        .expect("aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let mut expected = vec![
        int_row(&[7, 1, 5, 5, 5, 5]),
        int_row(&[42, 2, 40, 10, 30, 20]),
    ];
    sort_rows_by_first_column(&mut expected);
    assert_eq!(rows, expected);

    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), -1)
        .expect("remove bidder 42");
    bid_writer.flush().await.expect("flush removal");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("aggregate update timeout")
        .expect("aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let mut expected = vec![
        int_row(&[7, 1, 5, 5, 5, 5]),
        int_row(&[42, 1, 10, 10, 10, 10]),
    ];
    sort_rows_by_first_column(&mut expected);
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn topn_materializes_mv() {
    let db = test_db("topn").await;
    let view_name = "mv_topn";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
            .sort(vec![col("price").sort(false, true)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit")
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
        .append_encoded(encoded_bid_row(1, 7, 10), 1)
        .expect("append 10");
    bid_writer
        .append_encoded(encoded_bid_row(2, 8, 30), 1)
        .expect("append 30");
    bid_writer
        .append_encoded(encoded_bid_row(3, 9, 20), 1)
        .expect("append 20");
    bid_writer
        .append_encoded(encoded_bid_row(4, 10, 30), 1)
        .expect("append 30 again");
    bid_writer.flush().await.expect("flush bids");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build topn graph");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[30]), int_row(&[30])]);
}

#[tokio::test]
async fn topn_materializes_mv_from_transient_source_journal() {
    let db = test_db("topn_transient_source").await;
    let view_name = "mv_topn_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
            .sort(vec![col("price").sort(false, true)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit")
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient topn graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 7, 10), 1)
        .expect("append 10");
    bid_writer
        .append_encoded(encoded_bid_row(2, 8, 30), 1)
        .expect("append 30");
    bid_writer
        .append_encoded(encoded_bid_row(3, 9, 20), 1)
        .expect("append 20");
    bid_writer
        .append_encoded(encoded_bid_row(4, 10, 30), 1)
        .expect("append 30 again");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[30]), int_row(&[30])]);
}

#[tokio::test]
async fn row_number_topn_with_post_projection_materializes_from_transient_source_journal() {
    let db = test_db("row-number-topn-transient-source").await;
    let view_name = "mv_row_number_topn_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
             FROM (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra, \
                   ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
                   FROM nexmark_bid) ranked \
             WHERE rank_number <= 2",
        )
        .await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("channel", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
            Field::new(
                "dateTime",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient row-number topn graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 11, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 12, 40), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 20, 5), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 21, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 22, 10), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 4).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (
            scalar_i64(left.first()),
            scalar_i64(left.get(2)),
            scalar_i64(left.get(1)),
        );
        let right_key = (
            scalar_i64(right.first()),
            scalar_i64(right.get(2)),
            scalar_i64(right.get(1)),
        );
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            bid_row(1, 12, 40),
            bid_row(1, 10, 50),
            bid_row(2, 22, 10),
            bid_row(2, 21, 15),
        ]
    );
}

#[tokio::test]
async fn row_number_top1_with_post_projection_recomputes_from_transient_source_journal() {
    let db = test_db("row-number-top1-transient-source").await;
    let view_name = "mv_row_number_top1_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
             FROM (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra, \
                   ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
                   FROM nexmark_bid) ranked \
             WHERE rank_number <= 1",
        )
        .await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("channel", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
            Field::new(
                "dateTime",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient row-number top1 graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 11, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 12, 40), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 20, 5), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 21, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 22, 10), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (scalar_i64(left.first()), scalar_i64(left.get(2)));
        let right_key = (scalar_i64(right.first()), scalar_i64(right.get(2)));
        left_key.cmp(&right_key)
    });
    assert_eq!(rows, vec![bid_row(1, 10, 50), bid_row(2, 21, 15)]);

    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), -1)
        .expect("remove top row");
    bid_writer.flush().await.expect("flush removal");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (scalar_i64(left.first()), scalar_i64(left.get(2)));
        let right_key = (scalar_i64(right.first()), scalar_i64(right.get(2)));
        left_key.cmp(&right_key)
    });
    assert_eq!(rows, vec![bid_row(1, 12, 40), bid_row(2, 21, 15)]);
}

#[tokio::test]
async fn row_number_top1_with_two_int64_partition_keys_and_timestamp_order_recomputes_from_transient_source_journal()
 {
    let db = test_db("row-number-top1-two-int64-partitions-transient-source").await;
    let view_name = "mv_row_number_top1_two_int64_partitions_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra
             FROM (SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra,
                   ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number
                   FROM nexmark_bid) ranked
             WHERE rank_number <= 1"#,
        )
        .await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("channel", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
            Field::new(
                "dateTime",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient row-number top1 graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 10, 50, 1_700_000_000_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 10, 60, 1_700_000_100_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 11, 20, 1_700_000_050_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 20, 5, 1_700_000_010_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 20, 15, 1_700_000_005_000), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 3).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (
            scalar_i64(left.get(1)),
            scalar_i64(left.first()),
            scalar_timestamp_millis(left.get(5)),
        );
        let right_key = (
            scalar_i64(right.get(1)),
            scalar_i64(right.first()),
            scalar_timestamp_millis(right.get(5)),
        );
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            bid_row_with_ts(1, 10, 60, 1_700_000_100_000),
            bid_row_with_ts(1, 11, 20, 1_700_000_050_000),
            bid_row_with_ts(2, 20, 5, 1_700_000_010_000),
        ]
    );

    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 10, 60, 1_700_000_100_000), -1)
        .expect("remove top row");
    bid_writer.flush().await.expect("flush removal");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    wait_for_visible_row_count(&mv_registry, view_name, 3).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (
            scalar_i64(left.get(1)),
            scalar_i64(left.first()),
            scalar_timestamp_millis(left.get(5)),
        );
        let right_key = (
            scalar_i64(right.get(1)),
            scalar_i64(right.first()),
            scalar_timestamp_millis(right.get(5)),
        );
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            bid_row_with_ts(1, 10, 50, 1_700_000_000_000),
            bid_row_with_ts(1, 11, 20, 1_700_000_050_000),
            bid_row_with_ts(2, 20, 5, 1_700_000_010_000),
        ]
    );
}

#[tokio::test]
async fn aggregate_with_post_projection_materializes_from_transient_source_journal() {
    let db = test_db("aggregate-transient-source").await;
    let view_name = "mv_aggregate_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(lit(1i64)).alias("bid_count"),
                    sum(col("price")).alias("total_price"),
                ],
            )
            .expect("aggregate")
            .project(vec![col("bidder"), col("bid_count"), col("total_price")])
            .expect("project")
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("bidder", DataType::Int64, true),
            Field::new("bid_count", DataType::Int64, true),
            Field::new("total_price", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient aggregate graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 10, 25), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 11, 40), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[10, 2, 75]), int_row(&[11, 1, 40])]);
}

#[tokio::test]
async fn source_projection_with_proctime_materializes_mv() {
    let db = test_db("source-projection-proctime").await;
    let view_name = "mv_source_projection_proctime";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan("SELECT bidder, PROCTIME() AS p_time FROM nexmark_bid").await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("bidder", DataType::Int64, true),
            Field::new(
                "p_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_and_null_timestamp_row(42)]);
}

#[tokio::test]
async fn source_filter_projection_with_count_char_materializes_from_transient_source_journal() {
    let db = test_db("source-filter-projection-count-char").await;
    let view_name = "mv_source_filter_projection_count_char";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price * 908 / 1000 AS price, \
             CASE \
               WHEN HOUR(date_time) >= 8 AND HOUR(date_time) <= 18 THEN 'dayTime' \
               WHEN HOUR(date_time) <= 6 OR HOUR(date_time) >= 20 THEN 'nightTime' \
               ELSE 'otherTime' \
             END AS bid_time_type, \
             date_time AS \"dateTime\", \
             extra, \
             COUNT_CHAR(extra, 'c') AS c_counts \
             FROM nexmark_bid \
             WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000",
        )
        .await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("bid_time_type", DataType::Utf8, true),
            Field::new(
                "dateTime",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
            Field::new("c_counts", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 2_000_000), 1)
        .expect("append matching bid");
    bid_writer
        .append_encoded(encoded_bid_row(2, 7, 100), 1)
        .expect("append filtered bid");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(
        rows,
        vec![count_char_projection_row(
            1,
            42,
            1_816_000,
            "nightTime",
            1_700_000_000_000,
            "extra",
            0,
        )]
    );
}

#[tokio::test]
async fn source_projection_with_regexp_extract_materializes_from_transient_source_journal() {
    let db = test_db("source-projection-regexp-extract").await;
    let view_name = "mv_source_projection_regexp_extract";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, \
             CASE \
               WHEN lower(channel) = 'apple' THEN '0' \
               WHEN lower(channel) = 'google' THEN '1' \
               WHEN lower(channel) = 'facebook' THEN '2' \
               WHEN lower(channel) = 'baidu' THEN '3' \
               ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) \
             END AS channel_id \
             FROM nexmark_bid \
             WHERE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL \
                OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')",
        )
        .await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("channel", DataType::Utf8, true),
            Field::new("channel_id", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(1, 42, 10, "APPLE", "https://example.com/no-channel"),
            1,
        )
        .expect("append apple bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(
                2,
                7,
                20,
                "web",
                "https://example.com/x/item/1?q=1&channel_id=abc123&foo=1",
            ),
            1,
        )
        .expect("append regexp bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(3, 8, 30, "web", "https://example.com/no-match"),
            1,
        )
        .expect("append filtered bid");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            channel_id_projection_row(1, 42, 10, "APPLE", "0"),
            channel_id_projection_row(2, 7, 20, "web", "abc123"),
        ]
    );
}

#[tokio::test]
async fn source_projection_with_split_index_materializes_from_transient_source_journal() {
    let db = test_db("source-projection-split-index").await;
    let view_name = "mv_source_projection_split_index";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, \
             SPLIT_INDEX(url, '/', 3) AS dir1, \
             SPLIT_INDEX(url, '/', 4) AS dir2, \
             SPLIT_INDEX(url, '/', 5) AS dir3 \
             FROM nexmark_bid",
        )
        .await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("channel", DataType::Utf8, true),
            Field::new("dir1", DataType::Utf8, true),
            Field::new("dir2", DataType::Utf8, true),
            Field::new("dir3", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(
                1,
                42,
                10,
                "web",
                "https://example.com/dirA/item/123?q=1",
            ),
            1,
        )
        .expect("append full split bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(2, 7, 20, "web", "https://example.com/only"),
            1,
        )
        .expect("append short split bid");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            split_index_projection_row(
                1,
                42,
                10,
                "web",
                Some("dirA"),
                Some("item"),
                Some("123?q=1"),
            ),
            split_index_projection_row(2, 7, 20, "web", Some("only"), None, None),
        ]
    );
}

#[tokio::test]
async fn distinct_materializes_unique_rows() {
    let db = test_db("distinct-single").await;
    let view_name = "mv_distinct_bidder";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("bidder")])
            .expect("project")
            .distinct()
            .expect("distinct")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let view_handle = mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![Field::new("bidder", DataType::Int64, true)]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build distinct graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append first bidder");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 20), 1)
        .expect("append duplicate bidder");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 30), 1)
        .expect("append second bidder");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("distinct update timeout")
        .expect("distinct update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7]), int_row(&[42])]);
}

#[tokio::test]
async fn count_distinct_aggregate_materializes_mv() {
    let db = test_db("count-distinct-aggregate").await;
    let view_name = "mv_count_distinct_aggregate";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                ],
            )
            .expect("aggregate")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let view_handle = mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("distinct_auctions", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build count-distinct aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 5), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("count-distinct aggregate update timeout")
        .expect("count-distinct aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 1, 1]), int_row(&[42, 3, 2])]);
}

#[tokio::test]
async fn count_distinct_aggregate_materializes_from_transient_source_journal() {
    let db = test_db("count-distinct-aggregate-transient").await;
    let view_name = "mv_count_distinct_aggregate_transient";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                ],
            )
            .expect("aggregate")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let _view_handle = mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("distinct_auctions", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient count-distinct aggregate graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 5), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 1, 1]), int_row(&[42, 3, 2])]);
}

#[tokio::test]
async fn q16_style_aggregate_keeps_single_group_across_transient_ticks() {
    let db = test_db("q16-transient-aggregate-date-format").await;
    let view_name = "mv_q16_transient";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let logical = sql_plan(
        "SELECT channel, DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day, \
                MAX(DATE_FORMAT(date_time, 'HH:mm')) AS minute, \
                COUNT(*) AS total_bids, \
                COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, \
                COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, \
                COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, \
                COUNT(DISTINCT bidder) AS total_bidders, \
                COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, \
                COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, \
                COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, \
                COUNT(DISTINCT auction) AS total_auctions, \
                COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, \
                COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, \
                COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions \
         FROM nexmark_bid \
         GROUP BY channel, DATE_FORMAT(date_time, 'yyyy-MM-dd')",
    )
    .await;
    let plan = DbspPlanBuilder::new(nexmark_config())
        .build(&logical)
        .expect("circuit plan");

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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let _view_handle = mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("channel", DataType::Utf8, true),
            Field::new("day", DataType::Utf8, true),
            Field::new("minute", DataType::Utf8, true),
            Field::new("total_bids", DataType::Int64, true),
            Field::new("rank1_bids", DataType::Int64, true),
            Field::new("rank2_bids", DataType::Int64, true),
            Field::new("rank3_bids", DataType::Int64, true),
            Field::new("total_bidders", DataType::Int64, true),
            Field::new("rank1_bidders", DataType::Int64, true),
            Field::new("rank2_bidders", DataType::Int64, true),
            Field::new("rank3_bidders", DataType::Int64, true),
            Field::new("total_auctions", DataType::Int64, true),
            Field::new("rank1_auctions", DataType::Int64, true),
            Field::new("rank2_auctions", DataType::Int64, true),
            Field::new("rank3_auctions", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build q16 transient aggregate graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 42, 9_999, 1_700_000_036_211), 1)
        .expect("append tick 1");
    bid_writer.flush().await.expect("flush tick 1");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![vec![
            Some(EncodedRowScalar::Utf8("channel".to_string())),
            Some(EncodedRowScalar::Utf8("2023-11-14".to_string())),
            Some(EncodedRowScalar::Utf8("22:13".to_string())),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
        ]]
    );

    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 99, 15_000, 1_700_000_096_211), 1)
        .expect("append tick 2");
    bid_writer.flush().await.expect("flush tick 2");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![vec![
            Some(EncodedRowScalar::Utf8("channel".to_string())),
            Some(EncodedRowScalar::Utf8("2023-11-14".to_string())),
            Some(EncodedRowScalar::Utf8("22:14".to_string())),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
        ]]
    );

    bid_writer
        .append_encoded(
            encoded_bid_row_with_ts(3, 7, 1_200_000, 1_700_000_156_211),
            1,
        )
        .expect("append tick 3");
    bid_writer.flush().await.expect("flush tick 3");

    wait_for_logical_version(&mv_registry, view_name, 3).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![vec![
            Some(EncodedRowScalar::Utf8("channel".to_string())),
            Some(EncodedRowScalar::Utf8("2023-11-14".to_string())),
            Some(EncodedRowScalar::Utf8("22:15".to_string())),
            Some(EncodedRowScalar::Int64(3)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(3)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(3)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
        ]]
    );
}

#[tokio::test]
async fn filtered_count_distinct_aggregate_materializes_mv() {
    let db = test_db("filtered-count-distinct-aggregate").await;
    let view_name = "mv_filtered_count_distinct_aggregate";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count(col("price"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered count")
                        .alias("lt20_cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                    count_distinct(col("auction"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered distinct count")
                        .alias("lt20_distinct_auctions"),
                ],
            )
            .expect("aggregate")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let view_handle = mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("lt20_cnt", DataType::Int64, true),
        Field::new("distinct_auctions", DataType::Int64, true),
        Field::new("lt20_distinct_auctions", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build filtered count-distinct aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 25), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("filtered count-distinct aggregate update timeout")
        .expect("filtered count-distinct aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_row(&[7, 1, 0, 1, 0]), int_row(&[42, 3, 2, 2, 2])]
    );
}

#[tokio::test]
async fn filtered_count_distinct_aggregate_materializes_with_parallel_ingest_view() {
    let db = test_db("filtered-count-distinct-parallel").await;
    let ingest_view_name = "mv_parallel_ingest_count";
    let result_view_name = "mv_parallel_filtered_count_distinct";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let ingest_plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                Vec::<Expr>::new(),
                vec![count(col("price")).alias("row_count")],
            )
            .expect("aggregate")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let result_plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count(col("price"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered count")
                        .alias("lt20_cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                    count_distinct(col("auction"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered distinct count")
                        .alias("lt20_distinct_auctions"),
                ],
            )
            .expect("aggregate")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let mut required_sources =
        validate_dbsp_plan(&ingest_plan, &available_sources, ingest_view_name)
            .expect("validate ingest plan")
            .required_sources;
    required_sources.extend(
        validate_dbsp_plan(&result_plan, &available_sources, result_view_name)
            .expect("validate result plan")
            .required_sources,
    );

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(ingest_view_name);
    mv_registry.set_schema(
        ingest_view_name,
        arrow_schema(vec![Field::new("row_count", DataType::Int64, true)]),
    );
    let result_view_handle = mv_registry.register(result_view_name);
    mv_registry.set_schema(
        result_view_name,
        arrow_schema(vec![
            Field::new("bidder", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, true),
            Field::new("lt20_cnt", DataType::Int64, true),
            Field::new("distinct_auctions", DataType::Int64, true),
            Field::new("lt20_distinct_auctions", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);

    builder
        .build(BuildInputs {
            graph_id: ingest_view_name,
            view_name: ingest_view_name,
            plan: &ingest_plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build ingest graph");

    builder
        .build(BuildInputs {
            graph_id: result_view_name,
            view_name: result_view_name,
            plan: &result_plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build result graph");

    let mut version_rx = result_view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 25), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("parallel filtered count-distinct aggregate update timeout")
        .expect("parallel filtered count-distinct aggregate update");

    let mut rows = materialized_rows(&mv_registry, result_view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_row(&[7, 1, 0, 1, 0]), int_row(&[42, 3, 2, 2, 2])]
    );
}

#[tokio::test]
async fn distinct_subquery_aggregate_counts_unique_rows() {
    let db = test_db("distinct-aggregate").await;
    let view_name = "mv_distinct_count";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let distinct = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("auction"), col("bidder")])
            .expect("project")
            .distinct()
            .expect("distinct")
            .build()
            .expect("build distinct");
        let logical = datafusion::logical_expr::LogicalPlanBuilder::from(distinct)
            .aggregate(Vec::<Expr>::new(), vec![count(col("auction"))])
            .expect("aggregate")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let view_handle = mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![Field::new("count", DataType::Int64, true)]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build distinct aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    // Unique (auction, bidder) pairs: (1,42), (1,7), (2,7) => count 3.
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 7, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 7, 40), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("distinct aggregate update timeout")
        .expect("distinct aggregate update");

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[3])]);
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
    writer
        .append_encoded(encoded_bid_row(1, 42, 80), 1)
        .expect("append row");
    writer.flush().await.expect("flush one");
    writer
        .append_encoded(encoded_bid_row(2, 42, 81), 1)
        .expect("append second");
    writer.flush().await.expect("flush two");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("auction", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    {
        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
        let outputs = builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan: &plan,
                cancel: CancellationToken::new(),
                task_events: task_tx.clone(),
                mv_registry: Arc::clone(&mv_registry),
                outer_handle_streams: &handle_streams,
                outer_transient_streams: &transient_streams,
                enable_source_batch_journal: false,
                mv_retention: StreamRetention::KeepLast { keep_last: 1 },
                watermark: Arc::new(AtomicI64::new(-1)),
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
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("rebuild");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn cancel_stops_materialized_view_updates() {
    let db = test_db("cancel-updates").await;
    let view_name = "mv_cancel_updates";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
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
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let view_handle = mv_registry.register(view_name);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let cancel = CancellationToken::new();
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: cancel.clone(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let mut version_rx = view_handle.version_watch();
    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        writer
            .append_encoded(encoded_bid_row(1, 42, 99), 1)
            .expect("append first");
        writer.flush().await.expect("flush first");
    }
    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("expected version update")
        .expect("version watch update");
    let first_version = view_handle.latest_version().expect("latest version");

    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(20)).await;

    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        writer
            .append_encoded(encoded_bid_row(2, 42, 100), 1)
            .expect("append second");
        writer.flush().await.expect("flush second");
    }

    let update = timeout(Duration::from_millis(100), version_rx.changed()).await;
    assert!(update.is_err(), "expected no update after cancel");
    assert_eq!(view_handle.latest_version(), Some(first_version));
}

#[tokio::test]
async fn graph_task_error_is_reported() {
    let db = test_db("graph-task-error").await;
    let view_name = "mv_error";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
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

    let registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let cancel = CancellationToken::new();

    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: cancel.clone(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    tokio::task::yield_now().await;

    let mut stream = handle_streams
        .get("nexmark_bid")
        .expect("bid stream")
        .clone();
    stream
        .send(ZSetHandle {
            ns: "missing_namespace".to_string(),
            version: 99,
        })
        .await
        .expect("send invalid handle");
    stream.flush().await.expect("flush invalid handle");

    let event = timeout(Duration::from_millis(200), task_rx.recv())
        .await
        .expect("graph task error timeout")
        .expect("graph task error");
    assert_eq!(event.graph_id, view_name);
    assert!(
        event.task.contains("map")
            || event.task.contains("attach-view")
            || event.task.contains("materialize-view"),
        "unexpected task label: {}",
        event.task
    );
    let message = event.error.to_string();
    assert!(!message.is_empty(), "expected error message");
    drop(cancel);
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

async fn wait_for_visible_row_count(
    registry: &MaterializedViewRegistry,
    view_name: &str,
    expected_rows: usize,
) {
    timeout(Duration::from_secs(5), async {
        loop {
            if visible_rows(registry, view_name).await.len() >= expected_rows {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("wait for visible rows");
}

type TestRow = Vec<Option<EncodedRowScalar>>;

fn sort_rows_by_first_column(rows: &mut [TestRow]) {
    rows.sort_by_key(|row| match row.first() {
        Some(Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value))) => {
            *value
        }
        _ => 0,
    });
}

fn int_row(values: &[i64]) -> TestRow {
    values
        .iter()
        .copied()
        .map(EncodedRowScalar::Int64)
        .map(Some)
        .collect()
}

fn int_utf8_row(id: i64, label: Option<&str>) -> TestRow {
    vec![
        Some(EncodedRowScalar::Int64(id)),
        match label {
            Some(label) => Some(EncodedRowScalar::Utf8(label.to_string())),
            None => None,
        },
    ]
}

fn int_and_null_timestamp_row(id: i64) -> TestRow {
    vec![Some(EncodedRowScalar::Int64(id)), None]
}

fn count_char_projection_row(
    auction: i64,
    bidder: i64,
    projected_price: i64,
    bid_time_type: &str,
    date_time_ms: i64,
    extra: &str,
    c_counts: i64,
) -> TestRow {
    vec![
        Some(EncodedRowScalar::Int64(auction)),
        Some(EncodedRowScalar::Int64(bidder)),
        Some(EncodedRowScalar::Int64(projected_price)),
        Some(EncodedRowScalar::Utf8(bid_time_type.to_string())),
        Some(EncodedRowScalar::TimestampMillis(date_time_ms)),
        Some(EncodedRowScalar::Utf8(extra.to_string())),
        Some(EncodedRowScalar::Int64(c_counts)),
    ]
}

fn channel_id_projection_row(
    auction: i64,
    bidder: i64,
    price: i64,
    channel: &str,
    channel_id: &str,
) -> TestRow {
    vec![
        Some(EncodedRowScalar::Int64(auction)),
        Some(EncodedRowScalar::Int64(bidder)),
        Some(EncodedRowScalar::Int64(price)),
        Some(EncodedRowScalar::Utf8(channel.to_string())),
        Some(EncodedRowScalar::Utf8(channel_id.to_string())),
    ]
}

fn split_index_projection_row(
    auction: i64,
    bidder: i64,
    price: i64,
    channel: &str,
    dir1: Option<&str>,
    dir2: Option<&str>,
    dir3: Option<&str>,
) -> TestRow {
    vec![
        Some(EncodedRowScalar::Int64(auction)),
        Some(EncodedRowScalar::Int64(bidder)),
        Some(EncodedRowScalar::Int64(price)),
        Some(EncodedRowScalar::Utf8(channel.to_string())),
        dir1.map(|value| EncodedRowScalar::Utf8(value.to_string())),
        dir2.map(|value| EncodedRowScalar::Utf8(value.to_string())),
        dir3.map(|value| EncodedRowScalar::Utf8(value.to_string())),
    ]
}

fn scalar_i64(value: Option<&Option<EncodedRowScalar>>) -> i64 {
    match value {
        Some(Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value))) => {
            *value
        }
        _ => 0,
    }
}

fn scalar_timestamp_millis(value: Option<&Option<EncodedRowScalar>>) -> i64 {
    match value {
        Some(Some(EncodedRowScalar::TimestampMillis(value) | EncodedRowScalar::Int64(value))) => {
            *value
        }
        _ => 0,
    }
}

enum EncodedTestField<'a> {
    Int64(i64),
    Utf8(&'a str),
    TimestampMillis(i64),
}

fn encode_test_row(columns: &[EncodedTestField<'_>]) -> Vec<u8> {
    let count = u32::try_from(columns.len()).expect("encoded test row column count");
    let mut encoded = Vec::with_capacity(4 + (columns.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for column in columns {
        match column {
            EncodedTestField::Int64(value) => {
                encoded.push(0x01);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            EncodedTestField::Utf8(value) => {
                encoded.push(0x02);
                let bytes = value.as_bytes();
                let len = u32::try_from(bytes.len()).expect("encoded utf8 length");
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
            EncodedTestField::TimestampMillis(value) => {
                encoded.push(0x03);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
    encoded
}

fn encoded_bid_row_with_ts(auction: i64, bidder: i64, price: i64, date_time_ms: i64) -> Vec<u8> {
    encode_test_row(&[
        EncodedTestField::Int64(auction),
        EncodedTestField::Int64(bidder),
        EncodedTestField::Int64(price),
        EncodedTestField::Utf8("channel"),
        EncodedTestField::Utf8("url"),
        EncodedTestField::TimestampMillis(date_time_ms),
        EncodedTestField::Utf8("extra"),
    ])
}

fn encoded_bid_row_with_channel_url(
    auction: i64,
    bidder: i64,
    price: i64,
    channel: &str,
    url: &str,
) -> Vec<u8> {
    encode_test_row(&[
        EncodedTestField::Int64(auction),
        EncodedTestField::Int64(bidder),
        EncodedTestField::Int64(price),
        EncodedTestField::Utf8(channel),
        EncodedTestField::Utf8(url),
        EncodedTestField::TimestampMillis(1_700_000_000_000),
        EncodedTestField::Utf8("extra"),
    ])
}

fn encoded_bid_row(auction: i64, bidder: i64, price: i64) -> Vec<u8> {
    encoded_bid_row_with_ts(auction, bidder, price, 1_700_000_000_000)
}

fn encoded_person_row(id: i64, name: &str) -> Vec<u8> {
    encode_test_row(&[
        EncodedTestField::Int64(id),
        EncodedTestField::Utf8(name),
        EncodedTestField::Utf8("email"),
        EncodedTestField::Utf8("card"),
        EncodedTestField::Utf8("city"),
        EncodedTestField::Utf8("state"),
        EncodedTestField::TimestampMillis(1_700_000_000_000),
        EncodedTestField::Utf8("extra"),
    ])
}

fn encoded_auction_row(id: i64, seller: i64) -> Vec<u8> {
    encoded_auction_row_with_category(id, seller, 5)
}

fn encoded_auction_row_with_category(id: i64, seller: i64, category: i64) -> Vec<u8> {
    encode_test_row(&[
        EncodedTestField::Int64(id),
        EncodedTestField::Utf8("item"),
        EncodedTestField::Utf8("desc"),
        EncodedTestField::Int64(10),
        EncodedTestField::Int64(20),
        EncodedTestField::Int64(seller),
        EncodedTestField::Int64(category),
        EncodedTestField::TimestampMillis(1_700_000_000_000),
        EncodedTestField::TimestampMillis(1_700_000_100_000),
        EncodedTestField::Utf8("extra"),
    ])
}

fn bid_row_with_ts(auction: i64, bidder: i64, price: i64, date_time_ms: i64) -> TestRow {
    decode_row_to_values(&encoded_bid_row_with_ts(
        auction,
        bidder,
        price,
        date_time_ms,
    ))
}

fn bid_row(auction: i64, bidder: i64, price: i64) -> TestRow {
    bid_row_with_ts(auction, bidder, price, 1_700_000_000_000)
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

async fn materialized_rows(registry: &MaterializedViewRegistry, view_name: &str) -> Vec<TestRow> {
    let handle = registry.get(view_name).expect("view registered");
    let snapshot = handle.snapshot_encoded();
    let mut rows = Vec::new();
    for (key, diff) in snapshot {
        if diff > 0 {
            let row = decode_row_to_values(&key);
            for _ in 0..diff {
                rows.push(row.clone());
            }
        }
    }
    rows
}

async fn visible_rows(registry: &MaterializedViewRegistry, view_name: &str) -> Vec<TestRow> {
    let handle = registry.get(view_name).expect("view registered");
    if handle.dbsp_state().is_some() {
        return materialized_rows(registry, view_name).await;
    }

    let mut rows = Vec::new();
    for (encoded, diff) in handle.snapshot_encoded() {
        if diff > 0 {
            let row = decode_row_to_values(&encoded);
            for _ in 0..diff {
                rows.push(row.clone());
            }
        }
    }
    rows
}

fn decode_row_to_values(encoded: &[u8]) -> TestRow {
    decode_all_encoded_row_scalars(encoded).expect("decode row")
}
