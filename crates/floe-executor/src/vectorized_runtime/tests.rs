use super::*;
use crate::namespaces;
use crate::source_decoder::{SourceArrowBatchBuilder, SourceArrowBatches};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::expr_fn::SimpleScalarUDF;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature, Volatility,
};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::storage::{KeyValueTable, SlateTable, keyspace};
use floe_core::source::{
    AppendIngestEvent, SourceColumn, SourceDataType, SourceDefinition, SourceRegistry,
};
use object_store::memory::InMemory;
use serde_json::json;
use slatedb::{Db, config::ScanOptions};

use crate::table_provider::MaterializedViewTableProvider;

fn int64_values(batch: &RecordBatch, column_idx: usize) -> Vec<i64> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    (0..values.len()).map(|idx| values.value(idx)).collect()
}

fn timestamp_millis_values(batch: &RecordBatch, column_idx: usize) -> Vec<i64> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("timestamp(ms) column");
    (0..values.len()).map(|idx| values.value(idx)).collect()
}

fn test_hop_udf() -> ScalarUDF {
    let passthrough_ts: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            if let Some(first) = args.first() {
                return Ok(first.clone());
            }
            Ok(ColumnarValue::Array(Arc::new(
                TimestampMillisecondArray::from(vec![None::<i64>]),
            )))
        },
    );
    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "hop",
        Signature::one_of(
            vec![TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Int64,
                DataType::Int64,
            ])],
            Volatility::Immutable,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        passthrough_ts,
    ))
}

fn date_days_values(batch: &RecordBatch, column_idx: usize) -> Vec<i32> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("Date32 column");
    (0..values.len()).map(|idx| values.value(idx)).collect()
}

fn decimal128_values(batch: &RecordBatch, column_idx: usize) -> Vec<i128> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Decimal128 column");
    (0..values.len()).map(|idx| values.value(idx)).collect()
}

fn string_values(batch: &RecordBatch, column_idx: usize) -> Vec<String> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    (0..values.len())
        .map(|idx| values.value(idx).to_string())
        .collect()
}

fn float64_values(batch: &RecordBatch, column_idx: usize) -> Vec<f64> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float64 column");
    (0..values.len()).map(|idx| values.value(idx)).collect()
}

fn id_note_rows(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let notes = string_values(batch, 1);
        rows.extend(ids.into_iter().zip(notes));
    }
    rows.sort();
    rows
}

async fn scan_materialized_view_table(
    registry: Arc<MaterializedViewRegistry>,
    view_name: &str,
    schema: SchemaRef,
    sql: &str,
) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(
        view_name,
        Arc::new(MaterializedViewTableProvider::new(
            registry,
            view_name.to_string(),
            schema,
        )),
    )
    .expect("register materialized view table provider");
    ctx.sql(sql)
        .await
        .expect("plan materialized view query")
        .collect()
        .await
        .expect("collect materialized view query")
}

async fn materialized_view_snapshot_for(
    handle: &crate::mv::registry::MaterializedViewHandle,
    schema: SchemaRef,
    version: i64,
) -> Vec<RecordBatch> {
    if let Some(snapshot) = handle.arrow_snapshot_for(version) {
        return snapshot.as_ref().clone();
    }
    <crate::mv::registry::MaterializedViewHandle as crate::mv::runtime::MaterializedView>::columnar_snapshot_for(
        handle,
        schema,
        version,
    )
    .await
    .expect("load columnar materialized view snapshot")
    .expect("columnar materialized view snapshot")
    .as_ref()
    .clone()
}

fn id_count_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let counts = int64_values(batch, 1);
        rows.extend(ids.into_iter().zip(counts));
    }
    rows.sort();
    rows
}

fn id_count_sum_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let counts = int64_values(batch, 1);
        let sums = int64_values(batch, 2);
        rows.extend(
            ids.into_iter()
                .zip(counts)
                .zip(sums)
                .map(|((id, count), sum)| (id, count, sum)),
        );
    }
    rows.sort();
    rows
}

fn bool_count_rows(batches: &[RecordBatch]) -> Vec<(bool, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let flags = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean column");
        let counts = int64_values(batch, 1);
        rows.extend((0..flags.len()).map(|idx| (flags.value(idx), counts[idx])));
    }
    rows.sort();
    rows
}

fn single_int_rows(batches: &[RecordBatch]) -> Vec<i64> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        rows.extend(int64_values(batch, 0));
    }
    rows.sort();
    rows
}

fn timestamp_pair_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let first = timestamp_millis_values(batch, 0);
        let last = timestamp_millis_values(batch, 1);
        rows.extend(first.into_iter().zip(last));
    }
    rows.sort();
    rows
}

fn date_pair_rows(batches: &[RecordBatch]) -> Vec<(i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let first = date_days_values(batch, 0);
        let last = date_days_values(batch, 1);
        rows.extend(first.into_iter().zip(last));
    }
    rows.sort();
    rows
}

fn decimal_stats_rows(batches: &[RecordBatch]) -> Vec<(i128, i128, i128, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let sums = decimal128_values(batch, 0);
        let mins = decimal128_values(batch, 1);
        let maxes = decimal128_values(batch, 2);
        let distincts = int64_values(batch, 3);
        rows.extend(
            sums.into_iter()
                .zip(mins)
                .zip(maxes)
                .zip(distincts)
                .map(|(((sum, min), max), distinct)| (sum, min, max, distinct)),
        );
    }
    rows.sort();
    rows
}

fn bid_topn_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let auctions = int64_values(batch, 0);
        let bidders = int64_values(batch, 1);
        let prices = int64_values(batch, 2);
        rows.extend(
            auctions
                .into_iter()
                .zip(bidders)
                .zip(prices)
                .map(|((auction, bidder), price)| (auction, bidder, price)),
        );
    }
    rows.sort();
    rows
}

fn bid_topn_timestamp_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let auctions = int64_values(batch, 0);
        let bidders = int64_values(batch, 1);
        let prices = int64_values(batch, 2);
        let times = timestamp_millis_values(batch, 3);
        rows.extend(
            auctions
                .into_iter()
                .zip(bidders)
                .zip(prices)
                .zip(times)
                .map(|(((auction, bidder), price), time)| (auction, bidder, price, time)),
        );
    }
    rows.sort();
    rows
}

fn join_topn_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let bidders = int64_values(batch, 11);
        let prices = int64_values(batch, 12);
        rows.extend(
            ids.into_iter()
                .zip(bidders)
                .zip(prices)
                .map(|((id, bidder), price)| (id, bidder, price)),
        );
    }
    rows.sort();
    rows
}

fn join_topn_rows_with_extra(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, String)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let bidders = int64_values(batch, 11);
        let prices = int64_values(batch, 12);
        let extras = string_values(batch, 14);
        rows.extend(
            ids.into_iter()
                .zip(bidders)
                .zip(prices)
                .zip(extras)
                .map(|(((id, bidder), price), extra)| (id, bidder, price, extra)),
        );
    }
    rows.sort();
    rows
}

fn weighted_join_topn_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let bidders = int64_values(batch, 11);
        let prices = int64_values(batch, 12);
        let weights = int64_values(batch, batch.num_columns() - 1);
        rows.extend(
            ids.into_iter()
                .zip(bidders)
                .zip(prices)
                .zip(weights)
                .map(|(((id, bidder), price), weight)| (id, bidder, price, weight)),
        );
    }
    rows.sort();
    rows
}

fn grouped_stats_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64, i64, f64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let auctions = int64_values(batch, 0);
        let totals = int64_values(batch, 1);
        let cheap = int64_values(batch, 2);
        let mins = int64_values(batch, 3);
        let maxes = int64_values(batch, 4);
        let avgs = float64_values(batch, 5);
        let sums = int64_values(batch, 6);
        rows.extend(
            auctions
                .into_iter()
                .zip(totals)
                .zip(cheap)
                .zip(mins)
                .zip(maxes)
                .zip(avgs)
                .zip(sums)
                .map(|((((((auction, total), cheap), min), max), avg), sum)| {
                    (auction, total, cheap, min, max, avg, sum)
                }),
        );
    }
    rows.sort_by_key(|row| row.0);
    rows
}

fn category_avg_rows(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let categories = int64_values(batch, 0);
        let avgs = float64_values(batch, 1);
        rows.extend(categories.into_iter().zip(avgs));
    }
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.partial_cmp(&right.1).expect("finite average"))
    });
    rows
}

type DistinctStatsRow = (String, String, String, i64, i64, i64, i64, i64);

fn distinct_stats_rows(batches: &[RecordBatch]) -> Vec<DistinctStatsRow> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let channels = string_values(batch, 0);
        let days = string_values(batch, 1);
        let minutes = string_values(batch, 2);
        let total_bids = int64_values(batch, 3);
        let cheap_bids = int64_values(batch, 4);
        let total_bidders = int64_values(batch, 5);
        let cheap_bidders = int64_values(batch, 6);
        let total_auctions = int64_values(batch, 7);
        rows.extend(
            channels
                .into_iter()
                .zip(days)
                .zip(minutes)
                .zip(total_bids)
                .zip(cheap_bids)
                .zip(total_bidders)
                .zip(cheap_bidders)
                .zip(total_auctions)
                .map(
                    |(
                        (
                            (((((channel, day), minute), total_bid), cheap_bid), total_bidder),
                            cheap_bidder,
                        ),
                        total_auction,
                    )| {
                        (
                            channel,
                            day,
                            minute,
                            total_bid,
                            cheap_bid,
                            total_bidder,
                            cheap_bidder,
                            total_auction,
                        )
                    },
                ),
        );
    }
    rows.sort();
    rows
}

fn join_rows(batches: &[RecordBatch]) -> Vec<(i64, String, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let order_ids = int64_values(batch, 0);
        let regions = string_values(batch, 1);
        let amounts = int64_values(batch, 2);
        rows.extend(
            order_ids
                .into_iter()
                .zip(regions)
                .zip(amounts)
                .map(|((order_id, region), amount)| (order_id, region, amount)),
        );
    }
    rows.sort();
    rows
}

fn weighted_id_note_rows(batches: &[RecordBatch]) -> Vec<(i64, String, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let ids = int64_values(batch, 0);
        let notes = string_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            ids.into_iter()
                .zip(notes)
                .zip(weights)
                .map(|((id, note), weight)| (id, note, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_join_rows(batches: &[RecordBatch]) -> Vec<(i64, String, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let order_ids = int64_values(batch, 0);
        let regions = string_values(batch, 1);
        let amounts = int64_values(batch, 2);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            order_ids
                .into_iter()
                .zip(regions)
                .zip(amounts)
                .zip(weights)
                .map(|(((order_id, region), amount), weight)| (order_id, region, amount, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_id_count_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let ids = int64_values(batch, 0);
        let counts = int64_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            ids.into_iter()
                .zip(counts)
                .zip(weights)
                .map(|((id, count), weight)| (id, count, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_bool_count_rows(batches: &[RecordBatch]) -> Vec<(bool, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let flags = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean column");
        let counts = int64_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend((0..flags.len()).map(|idx| (flags.value(idx), counts[idx], weights[idx])));
    }
    rows.sort();
    rows
}

fn weighted_single_int_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let values = int64_values(batch, 0);
        let weights = int64_values(batch, weight_idx);
        rows.extend(values.into_iter().zip(weights));
    }
    rows.sort();
    rows
}

fn weighted_timestamp_pair_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let first = timestamp_millis_values(batch, 0);
        let last = timestamp_millis_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            first
                .into_iter()
                .zip(last)
                .zip(weights)
                .map(|((first, last), weight)| (first, last, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_date_pair_rows(batches: &[RecordBatch]) -> Vec<(i32, i32, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let first = date_days_values(batch, 0);
        let last = date_days_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            first
                .into_iter()
                .zip(last)
                .zip(weights)
                .map(|((first, last), weight)| (first, last, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_decimal_stats_rows(batches: &[RecordBatch]) -> Vec<(i128, i128, i128, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let sums = decimal128_values(batch, 0);
        let mins = decimal128_values(batch, 1);
        let maxes = decimal128_values(batch, 2);
        let distincts = int64_values(batch, 3);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            sums.into_iter()
                .zip(mins)
                .zip(maxes)
                .zip(distincts)
                .zip(weights)
                .map(|((((sum, min), max), distinct), weight)| (sum, min, max, distinct, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_category_avg_rows(batches: &[RecordBatch]) -> Vec<(i64, f64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let categories = int64_values(batch, 0);
        let avgs = float64_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            categories
                .into_iter()
                .zip(avgs)
                .zip(weights)
                .map(|((category, avg), weight)| (category, avg, weight)),
        );
    }
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.1.partial_cmp(&right.1).expect("finite average"))
    });
    rows
}

fn weighted_bid_topn_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let auctions = int64_values(batch, 0);
        let bidders = int64_values(batch, 1);
        let prices = int64_values(batch, 2);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            auctions
                .into_iter()
                .zip(bidders)
                .zip(prices)
                .zip(weights)
                .map(|(((auction, bidder), price), weight)| (auction, bidder, price, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_bid_topn_timestamp_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let auctions = int64_values(batch, 0);
        let bidders = int64_values(batch, 1);
        let prices = int64_values(batch, 2);
        let times = timestamp_millis_values(batch, 3);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            auctions
                .into_iter()
                .zip(bidders)
                .zip(prices)
                .zip(times)
                .zip(weights)
                .map(|((((auction, bidder), price), time), weight)| {
                    (auction, bidder, price, time, weight)
                }),
        );
    }
    rows.sort();
    rows
}

async fn build_operator_state_table(name: &str) -> Arc<dyn KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
    Arc::new(SlateTable::new(db))
}

async fn assert_stateless_source_namespace_empty(table: &Arc<dyn KeyValueTable>, view_name: &str) {
    let namespace = format!(
        "{}/columnar/stateless/input",
        namespaces::materialized_view(view_name).expect("materialized view namespace")
    );
    let prefix = keyspace::namespace_prefix(keyspace::prefix::ZSET, &namespace);
    let entries = table
        .scan_prefix(&prefix, &ScanOptions::default())
        .await
        .expect("scan stateless source namespace");
    assert!(
        entries.is_empty(),
        "stateless source namespace should be ephemeral, found {} persisted keys",
        entries.len()
    );
}

fn assert_columnar_join_strategy(runtime: &VectorizedExecutionRuntime, expected: &str) {
    let MaterializedViewOperator::Join(state) = &runtime.materialized_views[0].operator else {
        panic!("materialized view is not a join operator");
    };
    assert_eq!(state.execution_strategy_name(), expected);
}

async fn assert_incremental_plan_rejected(
    sources: &SourceRegistry,
    view_name: &str,
    query: &str,
    output_schema: SchemaRef,
) {
    let table_name = format!("vectorized-unsupported-{view_name}");
    let table = build_operator_state_table(&table_name).await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let result = VectorizedExecutionRuntime::new_with_options(
        sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            view_name,
            query,
            output_schema,
        )],
        registry,
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("unsupported materialized view {view_name} planned successfully"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("requires a supported SlateDB-backed columnar DBSP operator")
            || message.contains("build vectorized ASOF DataFrame"),
        "unexpected rejection for {view_name}: {err:#}"
    );
}

#[tokio::test]
async fn fallback_only_shapes_are_rejected_without_incremental_operators() {
    let mut sources = SourceRegistry::new();
    sources.register(
        SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
            ],
        )
        .expect("orders source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "bids",
            vec![
                SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
                SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            ],
        )
        .expect("bids source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "auctions",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
                SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            ],
        )
        .expect("auctions source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "people",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("name", SourceDataType::Utf8, false),
            ],
        )
        .expect("people source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "auction",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            ],
        )
        .expect("auction source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "bid",
            vec![
                SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
                SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            ],
        )
        .expect("bid source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "auction_sellers",
            vec![SourceColumn::new_nullable(
                "seller",
                SourceDataType::Int64,
                false,
            )],
        )
        .expect("auction sellers source definition"),
    );

    let id_note_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let count_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));
    let person_price_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let asof_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Int64, true),
    ]));
    let auction_seller_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let auction_price_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let auction_schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        false,
    )]));
    let key_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let cases: Vec<(&str, &str, SchemaRef)> = vec![
        (
            "mv_values_join",
            "SELECT o.id, v.note FROM orders AS o JOIN \
             (VALUES (1, 'one'), (3, 'three')) AS v(id, note) ON o.id = v.id",
            Arc::clone(&id_note_schema),
        ),
        (
            "mv_distinct_aggregate",
            "SELECT COUNT(auction) AS c FROM (SELECT DISTINCT auction, bidder FROM bids) d",
            Arc::clone(&count_schema),
        ),
        (
            "mv_three_way_join",
            "SELECT p.id AS person_id, b.price \
             FROM people p \
             JOIN auctions a ON p.id = a.seller \
             JOIN bids b ON a.id = b.auction",
            Arc::clone(&person_price_schema),
        ),
        (
            "mv_three_way_aggregate",
            "SELECT p.id AS person_id, COUNT(b.price) AS price \
             FROM people p \
             JOIN auctions a ON p.id = a.seller \
             JOIN bids b ON a.id = b.auction \
             GROUP BY p.id",
            Arc::clone(&person_price_schema),
        ),
        (
            "mv_asof_join",
            "SELECT a.id, b.price \
             FROM auction a ASOF JOIN bid b \
             MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") \
             ON a.id = b.auction",
            Arc::clone(&asof_schema),
        ),
        (
            "mv_distinct_join",
            "SELECT d.auction, a.seller \
             FROM (SELECT DISTINCT auction FROM bids) d \
             JOIN auctions a ON d.auction = a.id",
            Arc::clone(&auction_seller_schema),
        ),
        (
            "mv_subquery",
            "SELECT auction, price FROM bids WHERE auction IN (SELECT id FROM auctions)",
            Arc::clone(&auction_price_schema),
        ),
        (
            "mv_self_join",
            "SELECT l.id AS auction, r.amount AS price \
             FROM orders l JOIN orders r ON l.id = r.id",
            Arc::clone(&auction_price_schema),
        ),
        (
            "mv_range_join",
            "SELECT b.auction, a.seller \
             FROM bids b JOIN auctions a ON b.price >= a.initial_bid",
            Arc::clone(&auction_seller_schema),
        ),
        (
            "mv_distinct_topn",
            "SELECT DISTINCT auction FROM bids ORDER BY auction DESC LIMIT 2",
            Arc::clone(&auction_schema),
        ),
        (
            "mv_global_join_topn",
            "SELECT b.auction, a.seller \
             FROM bids b JOIN auctions a ON b.auction = a.id \
             ORDER BY b.price DESC LIMIT 2",
            Arc::clone(&auction_seller_schema),
        ),
        (
            "mv_aggregate_over_join_topn",
            "SELECT auction, COUNT(*) AS price \
             FROM (SELECT b.auction, b.price \
                   FROM bids b JOIN auctions a ON b.auction = a.id \
                   ORDER BY b.price DESC LIMIT 2) t \
             GROUP BY auction",
            Arc::clone(&auction_price_schema),
        ),
        (
            "mv_union_topn",
            "SELECT key \
             FROM (SELECT auction AS key FROM bids UNION ALL SELECT id AS key FROM auctions) u \
             ORDER BY key DESC LIMIT 2",
            Arc::clone(&key_schema),
        ),
        (
            "mv_union_over_distinct",
            "SELECT key \
             FROM (SELECT DISTINCT auction AS key FROM bids \
             UNION ALL SELECT id AS key FROM auctions) u",
            Arc::clone(&key_schema),
        ),
        (
            "mv_join_over_join",
            "SELECT j.auction, a.seller \
             FROM (SELECT l.auction, r.price FROM bids l JOIN bids r ON l.auction = r.auction WHERE l.price < r.price) j \
             JOIN auctions a ON j.auction = a.id",
            Arc::clone(&auction_seller_schema),
        ),
    ];

    for (view_name, query, schema) in cases {
        assert_incremental_plan_rejected(&sources, view_name, query, schema).await;
    }
}

#[tokio::test]
async fn pruned_execution_batches_do_not_prune_query_provider() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("note", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let required_columns = Some(Arc::<[bool]>::from(vec![true, false]));
    let mut builder = SourceArrowBatchBuilder::new_with_execution_required_columns(
        definition.clone(),
        1,
        required_columns,
    );
    builder
        .append_event(&AppendIngestEvent::new(
            "orders",
            json!({"id": 1, "note": "kept"}),
        ))
        .expect("append source event");
    let batches = builder
        .finish()
        .expect("finish source batches")
        .expect("source batches");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-query-provider-pruned").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_source_query_tables()
            .with_operator_state_table(table),
    )
    .await
    .expect("runtime");

    let SourceArrowBatches::ExecutionAndQuery { execution, query } = batches else {
        panic!("expected execution and query batches");
    };
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![execution], vec![query])
        .await
        .expect("append source batches");
    runtime.run_tick(1).await.expect("run vectorized tick");

    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    let id = snapshot[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("mv id column")
        .value(0);
    assert_eq!(id, 1);

    let provider = runtime
        .table_providers()
        .into_iter()
        .find_map(|(name, provider)| (name == "orders").then_some(provider))
        .expect("orders query provider");
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider)
        .expect("register query provider");
    let batches = ctx
        .sql("SELECT note FROM orders")
        .await
        .expect("query provider sql")
        .collect()
        .await
        .expect("collect query provider rows");
    let note = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("note column")
        .value(0);
    assert_eq!(note, "kept");
}

#[tokio::test]
async fn primary_key_cdc_delta_updates_filter_project_mv_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-primary-key-cdc-stateless").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id, amount FROM orders WHERE amount >= 20",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_source_query_tables()
            .with_operator_state_table(table),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let update_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 40, 30])),
        ],
    )
    .expect("cdc source rows");
    let weighted = weighted_batch_from_diffs(&update_rows, &weighted_schema, &[-1, 1, -1])
        .expect("weighted cdc rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply cdc delta");
    runtime.run_tick(2).await.expect("cdc tick");

    let handle = registry.get("mv_orders").expect("materialized view");
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, amount FROM mv_orders",
    )
    .await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 40)]);

    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 40, 1), (2, 30, -1)]
    );

    let provider = runtime
        .table_providers()
        .into_iter()
        .find_map(|(name, provider)| (name == "orders").then_some(provider))
        .expect("orders query provider");
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider)
        .expect("register orders provider");
    let source_rows = ctx
        .sql("SELECT id, amount FROM orders ORDER BY id")
        .await
        .expect("source query")
        .collect()
        .await
        .expect("collect source rows");
    assert_eq!(source_rows.len(), 1);
    assert_eq!(int64_values(&source_rows[0], 0), vec![1]);
    assert_eq!(int64_values(&source_rows[0], 1), vec![40]);
}

#[tokio::test]
async fn filter_project_uses_slate_backed_columnar_stateless_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("note", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "d"])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-stateless").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::clone(&schema);
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id * 2 AS id, note FROM orders WHERE id * 2 >= 4",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );
    assert!(
        registry.get("mv_orders").is_some(),
        "materialized view handle must exist before the first tick"
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");
    assert_stateless_source_namespace_empty(&table, "mv_orders").await;

    let handle = registry.get("mv_orders").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, note FROM mv_orders",
    )
    .await;
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(4, "b".to_string()), (8, "d".to_string())]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, -1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");
    assert_stateless_source_namespace_empty(&table, "mv_orders").await;

    assert!(handle.arrow_snapshot_for(2).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, note FROM mv_orders",
    )
    .await;
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(6, "c".to_string()), (8, "d".to_string())]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(4, "b".to_string(), -1), (6, "c".to_string(), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id * 2 AS id, note FROM orders WHERE id * 2 >= 4",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders")
        .expect("recovered materialized view");
    assert!(recovered_handle.arrow_snapshot_for(3).is_none());
    let recovered_snapshot = scan_materialized_view_table(
        Arc::clone(&recovery_registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, note FROM mv_orders",
    )
    .await;
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![(6, "c".to_string()), (8, "d".to_string())]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn values_relation_uses_slate_backed_columnar_constant_operator() {
    let sources = SourceRegistry::new();
    let table = build_operator_state_table("vectorized-columnar-constant-values").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let query = "SELECT id, note FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, note)";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );

    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_values").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("initial snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    let delta = handle.arrow_delta_for(1).expect("initial delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(1, "a".to_string(), 1), (2, "b".to_string(), 1)]
    );

    runtime.run_tick(2).await.expect("stable tick");
    let snapshot = handle.arrow_snapshot_for(2).expect("stable snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    let delta = handle.arrow_delta_for(2).expect("stable empty delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_values")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn empty_values_relation_persists_columnar_constant_state() {
    let sources = SourceRegistry::new();
    let table = build_operator_state_table("vectorized-columnar-constant-empty-values").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let query = "SELECT id, note FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, note) WHERE id > 10";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_empty_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );

    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_empty_values").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("empty snapshot");
    assert!(id_note_rows(&snapshot).is_empty());
    let delta = handle.arrow_delta_for(1).expect("empty delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));
    assert!(
        table
            .get_bytes(b"mv/mv_empty_values/columnar/constant/state/initialized")
            .await
            .expect("read initialized marker")
            .is_some()
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_empty_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );
    recovered.run_tick(2).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_empty_values")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(2)
        .expect("recovered empty snapshot");
    assert!(id_note_rows(&recovered_snapshot).is_empty());
    let recovered_delta = recovered_handle
        .arrow_delta_for(2)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn sort_passthrough_uses_slate_backed_columnar_stateless_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-sort-stateless").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::clone(&schema);
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders ORDER BY id DESC",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_orders").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 3]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    assert!(handle.arrow_snapshot_for(2).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 3, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders ORDER BY id DESC",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders")
        .expect("recovered materialized view");
    assert!(recovered_handle.arrow_snapshot_for(3).is_none());
    let recovered_snapshot = scan_materialized_view_table(
        Arc::clone(&recovery_registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 3, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn union_all_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let shipments = SourceDefinition::new(
        "shipments",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("shipments source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("initial orders");
    let initial_shipments = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("initial shipments");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(shipments);
    let table = build_operator_state_table("vectorized-columnar-union").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION ALL SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "shipments",
            vec![initial_shipments.clone()],
            vec![initial_shipments],
        )
        .await
        .expect("append initial shipments");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_union_ids").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 2, 4]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let order_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("order retract");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted order retract");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    let shipment_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![5]))],
    )
    .expect("shipment insert");
    let weighted = weighted_batch_from_diffs(&shipment_insert, &weighted_schema, &[1])
        .expect("weighted shipment insert");
    runtime
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment insert");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4, 5]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (5, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION ALL SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_ids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4, 5]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let shipment_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("shipment retract");
    let weighted = weighted_batch_from_diffs(&shipment_retract, &weighted_schema, &[-1])
        .expect("weighted shipment retract");
    recovered
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment retract");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot = recovered_handle.arrow_snapshot_for(4).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 4, 5]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1)]);
}

#[tokio::test]
async fn source_union_values_relation_uses_slate_backed_columnar_union_operator() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("initial orders");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    let table = build_operator_state_table("vectorized-columnar-union-values").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let query = "SELECT id FROM orders UNION ALL \
                 SELECT id FROM (VALUES (2), (4)) AS v(id)";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_union_values").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("initial snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 2, 4]);
    let delta = handle.arrow_delta_for(1).expect("initial delta");
    assert_eq!(
        weighted_single_int_rows(&delta),
        vec![(1, 1), (2, 1), (2, 1), (4, 1)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 5]))],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted source delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("updated snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4, 5]);
    let delta = handle.arrow_delta_for(2).expect("updated delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (5, 1)]);

    table
        .delete(b"mv/mv_union_values/columnar/union/constant_1/state/initialized")
        .await
        .expect("delete initialized marker");

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_values")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4, 5]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn union_distinct_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let shipments = SourceDefinition::new(
        "shipments",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("shipments source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("initial orders");
    let initial_shipments = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("initial shipments");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(shipments);
    let table = build_operator_state_table("vectorized-columnar-union-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "shipments",
            vec![initial_shipments.clone()],
            vec![initial_shipments],
        )
        .await
        .expect("append initial shipments");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_union_ids").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let order_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("order retract");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted order retract");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_ids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let shipment_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("shipment retract");
    let weighted = weighted_batch_from_diffs(&shipment_retract, &weighted_schema, &[-1])
        .expect("weighted shipment retract");
    recovered
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment retract");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot = recovered_handle.arrow_snapshot_for(4).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 4]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1)]);
}

#[tokio::test]
async fn ordered_union_distinct_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let shipments = SourceDefinition::new(
        "shipments",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("shipments source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("initial orders");
    let initial_shipments = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("initial shipments");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(shipments);
    let table = build_operator_state_table("vectorized-columnar-ordered-union-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let query = "SELECT id FROM orders WHERE id <= 2 UNION SELECT id FROM shipments WHERE id >= 2 ORDER BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_ordered_union_ids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "shipments",
            vec![initial_shipments.clone()],
            vec![initial_shipments],
        )
        .await
        .expect("append initial shipments");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_ordered_union_ids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let order_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("order retract");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted order retract");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_ordered_union_ids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_ordered_union_ids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let shipment_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("shipment retract");
    let weighted = weighted_batch_from_diffs(&shipment_retract, &weighted_schema, &[-1])
        .expect("weighted shipment retract");
    recovered
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment retract");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot = recovered_handle.arrow_snapshot_for(4).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 4]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1)]);
}

#[tokio::test]
async fn count_group_by_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-count").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_counts").expect("materialized view");
    let version = handle.latest_version().expect("mv version");
    let snapshot = handle.arrow_snapshot_for(version).expect("mv snapshot");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(int64_values(&snapshot[0], 0), vec![1, 2]);
    assert_eq!(int64_values(&snapshot[0], 1), vec![2, 1]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(int64_values(&snapshot[0], 0), vec![1, 2, 3]);
    assert_eq!(int64_values(&snapshot[0], 1), vec![1, 2, 1]);

    let delta = handle.arrow_delta_for(2).expect("mv delta");
    let delta = delta
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect::<Vec<_>>();
    assert_eq!(delta.len(), 1);
    let weight_idx = delta[0]
        .schema()
        .index_of(WEIGHT_COLUMN_NAME)
        .expect("weight column");
    assert_eq!(int64_values(delta[0], 0), vec![1, 1, 2, 2, 3]);
    assert_eq!(int64_values(delta[0], 1), vec![2, 1, 1, 2, 1]);
    assert_eq!(int64_values(delta[0], weight_idx), vec![-1, 1, -1, 1, 1]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(recovered_snapshot.len(), 1);
    assert_eq!(int64_values(&recovered_snapshot[0], 0), vec![1, 2, 3]);
    assert_eq!(int64_values(&recovered_snapshot[0], 1), vec![1, 2, 1]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn distinct_uses_slate_backed_grouped_count_state_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids",
            "SELECT DISTINCT id FROM orders",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_ids").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let retract_one = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_one, &weighted_schema, &[-1])
        .expect("weighted source row");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids",
            "SELECT DISTINCT id FROM orders",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_ids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_last = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_last, &weighted_schema, &[-1])
        .expect("weighted source row");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("last retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(1, -1)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3]))],
    )
    .expect("source insert row");
    let weighted =
        weighted_batch_from_diffs(&insert, &weighted_schema, &[1]).expect("weighted source row");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted insert");
    recovered.run_tick(5).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 5)
            .await;
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);
    let delta = recovered_handle.arrow_delta_for(5).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, 1)]);
}

#[tokio::test]
async fn ordered_distinct_uses_slate_backed_grouped_count_state_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-ordered-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let query = "SELECT DISTINCT id FROM orders ORDER BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_order_ids_ordered")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let retract_one = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_one, &weighted_schema, &[-1])
        .expect("weighted source row");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_ids_ordered")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_last = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_last, &weighted_schema, &[-1])
        .expect("weighted source row");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply last duplicate retract");
    recovered
        .run_tick(4)
        .await
        .expect("last duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(1, -1)]);
}

#[tokio::test]
async fn grouped_count_with_hidden_key_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("ts", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 10])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-hidden").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_counts").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1), (1, 1), (2, 1)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1), (1, 2), (2, 1)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_counts")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 1), (1, 2), (2, 1)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let hidden_key_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("hidden key insert rows");
    recovered
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![hidden_key_insert.clone()],
            vec![hidden_key_insert],
        )
        .await
        .expect("append hidden key source rows");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (1, 2), (2, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-recovery delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);
}

#[tokio::test]
async fn append_only_hop_grouped_count_recovers_compact_state() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("source definition")
    .with_property("append_only", "true");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-hop-append").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_counts",
            r#"SELECT auction, COUNT(*) AS count FROM bids GROUP BY auction, HOP("dateTime", 1000, 3000)"#,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        vec![test_hop_udf()],
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_counts").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        id_count_rows(&snapshot),
        vec![(1, 1), (1, 1), (1, 2), (1, 2)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_counts",
            r#"SELECT auction, COUNT(*) AS count FROM bids GROUP BY auction, HOP("dateTime", 1000, 3000)"#,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        vec![test_hop_udf()],
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(2).await.expect("recovered empty tick");

    let duplicate = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![1000])),
        ],
    )
    .expect("duplicate source batch");
    recovered
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![duplicate.clone()],
            vec![duplicate],
        )
        .await
        .expect("append duplicate source row");
    recovered.run_tick(3).await.expect("post-recovery tick");

    let recovered_handle = recovery_registry
        .get("mv_bid_counts")
        .expect("recovered materialized view");
    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        id_count_rows(&snapshot),
        vec![(1, 1), (1, 2), (1, 3), (1, 3)]
    );
    let delta = recovered_handle
        .arrow_delta_for(3)
        .expect("post-recovery delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![
            (1, 1, -1),
            (1, 2, -1),
            (1, 2, -1),
            (1, 2, 1),
            (1, 3, 1),
            (1, 3, 1),
        ]
    );
}

#[tokio::test]
async fn grouped_count_supports_boolean_group_key_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("active", SourceDataType::Bool, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-bool-key").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("active", DataType::Boolean, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_active_counts",
            "SELECT active, COUNT(*) AS count FROM events GROUP BY active",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_active_counts").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(bool_count_rows(&snapshot), vec![(false, 1), (true, 2)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(BooleanArray::from(vec![false])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(bool_count_rows(&snapshot), vec![(false, 2), (true, 2)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bool_count_rows(&delta),
        vec![(false, 1, -1), (false, 2, 1)]
    );
}

#[tokio::test]
async fn grouped_max_with_hidden_key_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("ts", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 10])),
            Arc::new(Int64Array::from(vec![50, 40, 60])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-max-hidden").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "max_price",
        DataType::Int64,
        false,
    )]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_max",
            "SELECT MAX(price) AS max_price FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedMax
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_max").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 50, 60]);

    let lower_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("lower source insert rows");
    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![lower_insert.clone()],
            vec![lower_insert],
        )
        .await
        .expect("append lower source rows");
    runtime.run_tick(2).await.expect("lower insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 50, 60]);
    let delta = handle.arrow_delta_for(2).expect("unchanged max delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let higher_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![70])),
        ],
    )
    .expect("higher source insert rows");
    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![higher_insert.clone()],
            vec![higher_insert],
        )
        .await
        .expect("append higher source rows");
    runtime.run_tick(3).await.expect("higher insert tick");

    let snapshot = handle.arrow_snapshot_for(3).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 60, 70]);
    let delta = handle.arrow_delta_for(3).expect("higher max delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(50, -1), (70, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_max",
            "SELECT MAX(price) AS max_price FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedMax
    );
    recovered.run_tick(4).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_max")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![40, 60, 70]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(4)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![70])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(5).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(5)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 50, 60]);
    let delta = recovered_handle
        .arrow_delta_for(5)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(50, 1), (70, -1)]);
}

#[tokio::test]
async fn grouped_stats_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 30, 100])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("cheap_bids", DataType::Int64, false),
        Field::new("min_price", DataType::Int64, true),
        Field::new("max_price", DataType::Int64, true),
        Field::new("avg_price", DataType::Float64, true),
        Field::new("sum_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, \
        COUNT(*) AS total_bids, \
        COUNT(*) FILTER (WHERE price < 50) AS cheap_bids, \
        MIN(price) AS min_price, \
        MAX(price) AS max_price, \
        AVG(price) AS avg_price, \
        SUM(price) AS sum_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_stats").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 2, 2, 10, 30, 20.0, 40), (2, 1, 0, 100, 100, 100.0, 100),]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 3, 2, 10, 50, 30.0, 90), (2, 1, 0, 100, 100, 100.0, 100),]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_bid_stats")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        grouped_stats_rows(&recovered_snapshot),
        vec![(1, 3, 2, 10, 50, 30.0, 90), (2, 1, 0, 100, 100, 100.0, 100),]
    );

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 2, 2, 10, 30, 20.0, 40), (2, 1, 0, 100, 100, 100.0, 100),]
    );
}

#[tokio::test]
async fn grouped_stats_can_publish_columnar_versions_without_arrow_snapshots() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 30, 100])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-no-arrow").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("sum_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, SUM(price) AS sum_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(table)
            .without_grouped_stats_arrow_snapshots(),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_stats").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    assert_eq!(handle.authoritative_row_count_for(1), Some(2));
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_bid_stats",
        Arc::clone(&output_schema),
        "SELECT auction, total_bids, sum_price FROM mv_bid_stats",
    )
    .await;
    assert_eq!(id_count_sum_rows(&snapshot), vec![(1, 2, 40), (2, 1, 100)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    assert!(handle.arrow_snapshot_for(2).is_none());
    assert_eq!(handle.authoritative_row_count_for(2), Some(2));
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_bid_stats",
        output_schema,
        "SELECT auction, total_bids, sum_price FROM mv_bid_stats",
    )
    .await;
    assert_eq!(id_count_sum_rows(&snapshot), vec![(1, 3, 90), (2, 1, 100)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 2, -1), (1, 3, 1)]);
}

#[tokio::test]
async fn append_only_grouped_stats_recovers_from_dense_compact_state_snapshot() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_APPEND_ONLY_PROPERTY, "true");
    let schema = definition.to_arrow_schema();
    let group_count = 1024_i64;
    let auctions = (0..group_count).collect::<Vec<_>>();
    let prices = auctions
        .iter()
        .map(|auction| auction * 10)
        .collect::<Vec<_>>();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(auctions.clone())),
            Arc::new(Int64Array::from(prices)),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-dense-compact-snapshot")
            .await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("sum_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, SUM(price) AS sum_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(Arc::clone(&table))
            .without_grouped_stats_arrow_snapshots(),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_stats").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    assert_eq!(
        handle.authoritative_row_count_for(1),
        Some(group_count as usize)
    );

    let logged_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![7000])),
        ],
    )
    .expect("logged source insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![logged_insert.clone()],
            vec![logged_insert],
        )
        .await
        .expect("append logged source rows");
    runtime.run_tick(2).await.expect("logged tick");

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(table)
            .without_grouped_stats_arrow_snapshots(),
    )
    .await
    .expect("recovered runtime");

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![9000])),
        ],
    )
    .expect("recovered source insert batch");
    recovered
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append recovered source rows");
    recovered.run_tick(3).await.expect("recovered tick");

    let snapshot = scan_materialized_view_table(
        Arc::clone(&recovery_registry),
        "mv_bid_stats",
        output_schema,
        "SELECT auction, total_bids, sum_price FROM mv_bid_stats WHERE auction = 7",
    )
    .await;
    assert_eq!(id_count_sum_rows(&snapshot), vec![(7, 3, 16070)]);
}

#[tokio::test]
async fn append_only_grouped_stats_recovers_distinct_presence_segments() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_APPEND_ONLY_PROPERTY, "true");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-append-distinct-segments")
            .await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("distinct_bidders", DataType::Int64, false),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, \
        COUNT(DISTINCT bidder) AS distinct_bidders FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![20, 30])),
        ],
    )
    .expect("recovered source insert batch");
    recovered
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append recovered source rows");
    recovered.run_tick(2).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_bid_stats")
        .expect("recovered materialized view");
    let snapshot = recovered_handle
        .arrow_snapshot_for(2)
        .expect("recovered snapshot");
    assert_eq!(id_count_sum_rows(&snapshot), vec![(1, 4, 3)]);
}

#[tokio::test]
async fn append_only_grouped_stats_rejects_negative_source_delta() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_APPEND_ONLY_PROPERTY, "true");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-append-only").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("min_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, MIN(price) AS min_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        registry,
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    runtime
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    let err = runtime
        .run_tick(2)
        .await
        .expect_err("append-only grouped-stats should reject retractions");
    let err = format!("{err:#}");
    assert!(
        err.contains("append-only grouped-stats"),
        "unexpected error: {err:#}"
    );
}

#[tokio::test]
async fn sum_group_by_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-sum").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) AS total FROM orders GROUP BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_totals").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 35), (2, 5)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, -1), (1, 35, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 35), (2, 5)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, 1), (1, 35, -1)]
    );
}

#[tokio::test]
async fn ordered_sum_group_by_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-ordered-sum").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) AS total \
        FROM orders \
        GROUP BY id \
        ORDER BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_order_totals_ordered")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 35), (2, 5)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, -1), (1, 35, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_totals_ordered")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 35), (2, 5)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, 1), (1, 35, -1)]
    );
}

#[tokio::test]
async fn having_grouped_stats_uses_slate_backed_post_aggregate_filter_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 5, 25])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-having").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) AS total \
        FROM orders GROUP BY id HAVING SUM(amount) >= 20";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_large_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_large_order_totals")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 25)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 25), (2, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 25, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_large_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_large_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 25), (2, 25)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 25)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 25, -1)]);
}

#[tokio::test]
async fn final_aggregate_projection_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-final-projection").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("adjusted_total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) + 1 AS adjusted_total FROM orders GROUP BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_adjusted_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_adjusted_order_totals")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 31), (2, 6)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 36), (2, 6)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 31, -1), (1, 36, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_adjusted_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_adjusted_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 36), (2, 6)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 31), (2, 6)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 31, 1), (1, 36, -1)]
    );
}

#[tokio::test]
async fn aggregate_subquery_having_projection_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, true),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(20), None])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-subquery-having").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, total FROM (\
        SELECT id, SUM(amount) AS total, COUNT(amount) AS amount_count, AVG(amount) AS avg_amount \
        FROM orders GROUP BY id\
    ) a WHERE amount_count > 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_totals").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 2])),
            Arc::new(Int64Array::from(vec![Some(5), Some(15)])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 20)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(2, 20, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 30), (2, 20)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![Some(20)])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 30, -1)]);
}

#[tokio::test]
async fn global_count_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "amount",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![10, 20, 5]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-global-count").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(*) AS total FROM orders";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_count",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_count").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_count",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_count")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, 1), (4, -1)]);
}

#[tokio::test]
async fn global_sum_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "amount",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![10, 20, 5]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-global-sum").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]));
    let query = "SELECT SUM(amount) AS total FROM orders";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_sum",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_sum").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![35]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![42]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(35, -1), (42, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_sum",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_sum")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![42]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![35]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(35, 1), (42, -1)]);
}

#[tokio::test]
async fn grouped_stats_supports_distinct_counts_and_string_max_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("day", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("minute", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["apple", "apple", "apple"])),
            Arc::new(StringArray::from(vec![
                "2026-06-08",
                "2026-06-08",
                "2026-06-08",
            ])),
            Arc::new(StringArray::from(vec!["10:00", "10:05", "09:55"])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 101, 100])),
            Arc::new(Int64Array::from(vec![50, 150, 75])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("minute", DataType::Utf8, true),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("cheap_bids", DataType::Int64, false),
        Field::new("total_bidders", DataType::Int64, false),
        Field::new("cheap_bidders", DataType::Int64, false),
        Field::new("total_auctions", DataType::Int64, false),
    ]));
    let query = "SELECT channel, day, \
        MAX(minute) AS minute, \
        COUNT(*) AS total_bids, \
        COUNT(*) FILTER (WHERE price < 100) AS cheap_bids, \
        COUNT(DISTINCT bidder) AS total_bidders, \
        COUNT(DISTINCT bidder) FILTER (WHERE price < 100) AS cheap_bidders, \
        COUNT(DISTINCT auction) AS total_auctions \
        FROM events GROUP BY channel, day";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_event_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_event_stats").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        distinct_stats_rows(&snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:05".to_string(),
            3,
            2,
            2,
            2,
            2,
        )]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["apple"])),
            Arc::new(StringArray::from(vec!["2026-06-08"])),
            Arc::new(StringArray::from(vec!["10:10"])),
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![102])),
            Arc::new(Int64Array::from(vec![80])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        distinct_stats_rows(&snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:10".to_string(),
            4,
            3,
            3,
            3,
            3,
        )]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_event_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_event_stats")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        distinct_stats_rows(&recovered_snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:10".to_string(),
            4,
            3,
            3,
            3,
            3,
        )]
    );

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["apple"])),
            Arc::new(StringArray::from(vec!["2026-06-08"])),
            Arc::new(StringArray::from(vec!["10:00"])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(
        distinct_stats_rows(&snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:10".to_string(),
            3,
            2,
            3,
            2,
            3,
        )]
    );
}

#[tokio::test]
async fn grouped_stats_supports_string_distinct_count_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "channel",
            SourceDataType::Utf8,
            true,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some("web"),
            Some("web"),
            Some("mobile"),
            None,
        ]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-string-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "distinct_channels",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(DISTINCT channel) AS distinct_channels FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_distinct_channels",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_channels")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some("email"),
            Some("web"),
        ]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (3, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_distinct_channels",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_distinct_channels")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![3]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![Some("mobile")]))],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, 1), (3, -1)]);
}

#[tokio::test]
async fn grouped_stats_supports_timestamp_min_max_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "event_time",
            SourceDataType::TimestampMillis,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1000, 500, 750,
        ]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-timestamp-minmax").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let ts_type = DataType::Timestamp(TimeUnit::Millisecond, None);
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("first_ts", ts_type.clone(), false),
        Field::new("last_ts", ts_type, false),
    ]));
    let query = "SELECT MIN(event_time) AS first_ts, MAX(event_time) AS last_ts FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_event_bounds",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_event_bounds").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(timestamp_pair_rows(&snapshot), vec![(500, 1000)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![400, 1200]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(timestamp_pair_rows(&snapshot), vec![(400, 1200)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_timestamp_pair_rows(&delta),
        vec![(400, 1200, 1), (500, 1000, -1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_event_bounds",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_event_bounds")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(timestamp_pair_rows(&recovered_snapshot), vec![(400, 1200)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![400]))],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(timestamp_pair_rows(&snapshot), vec![(500, 1200)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_timestamp_pair_rows(&delta),
        vec![(400, 1200, -1), (500, 1200, 1)]
    );
}

#[tokio::test]
async fn grouped_stats_supports_date_min_max_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "event_day",
            SourceDataType::DateDays,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Date32Array::from(vec![10, 5, 7]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-date-minmax").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("first_day", DataType::Date32, false),
        Field::new("last_day", DataType::Date32, false),
    ]));
    let query = "SELECT MIN(event_day) AS first_day, MAX(event_day) AS last_day FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_event_days",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_event_days").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(date_pair_rows(&snapshot), vec![(5, 10)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Date32Array::from(vec![4, 12]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(date_pair_rows(&snapshot), vec![(4, 12)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_date_pair_rows(&delta),
        vec![(4, 12, 1), (5, 10, -1)]
    );
}

#[tokio::test]
async fn grouped_stats_supports_timestamp_distinct_count_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "event_time",
            SourceDataType::TimestampMillis,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1000, 1000, 500,
        ]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-timestamp-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "distinct_times",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(DISTINCT event_time) AS distinct_times FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_distinct_times",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_times")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![750, 1000]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (3, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_distinct_times",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_distinct_times")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![3]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![500]))],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, 1), (3, -1)]);
}

#[tokio::test]
async fn grouped_stats_supports_boolean_distinct_count_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "active",
            SourceDataType::Bool,
            true,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BooleanArray::from(vec![Some(true), None]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-boolean-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "distinct_flags",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(DISTINCT active) AS distinct_flags FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_distinct_flags",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_flags")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BooleanArray::from(vec![Some(false)]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(1, -1), (2, 1)]);
}

#[tokio::test]
async fn grouped_stats_supports_decimal_stats_incrementally() {
    let definition = SourceDefinition::new(
        "payments",
        vec![SourceColumn::new_nullable(
            "amount",
            SourceDataType::Decimal128 {
                precision: 10,
                scale: 2,
            },
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let amount_type = DataType::Decimal128(10, 2);
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![1000_i128, 2000, 1000]).with_data_type(amount_type.clone()),
        )],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-decimal").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("total_amount", DataType::Decimal128(20, 2), false),
        Field::new("min_amount", amount_type.clone(), false),
        Field::new("max_amount", amount_type.clone(), false),
        Field::new("distinct_amounts", DataType::Int64, false),
    ]));
    let query = "SELECT SUM(amount) AS total_amount, MIN(amount) AS min_amount, MAX(amount) AS max_amount, COUNT(DISTINCT amount) AS distinct_amounts FROM payments";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_payment_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "payments",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_payment_stats").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(decimal_stats_rows(&snapshot), vec![(4000, 1000, 2000, 2)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![500_i128, 3000]).with_data_type(amount_type.clone()),
        )],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query(
            "payments",
            vec![insert.clone()],
            vec![insert],
        )
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(decimal_stats_rows(&snapshot), vec![(7500, 500, 3000, 4)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_decimal_stats_rows(&delta),
        vec![(4000, 1000, 2000, 2, -1), (7500, 500, 3000, 4, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_payment_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_payment_stats")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        decimal_stats_rows(&recovered_snapshot),
        vec![(7500, 500, 3000, 4)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![500_i128]).with_data_type(amount_type),
        )],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("payments", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(decimal_stats_rows(&snapshot), vec![(7000, 1000, 3000, 3)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_decimal_stats_rows(&delta),
        vec![(7000, 1000, 3000, 3, 1), (7500, 500, 3000, 4, -1)]
    );
}

#[tokio::test]
async fn join_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("customer_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let customers = SourceDefinition::new(
        "customers",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Utf8, false),
        ],
    )
    .expect("customers source definition");
    let orders_schema = orders.to_arrow_schema();
    let customers_schema = customers.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(Int64Array::from(vec![50, 60, 70])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["west", "east"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o JOIN customers c ON o.customer_id = c.id \
        WHERE c.region = 'west'";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_west_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "incremental_inner");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![initial_customers.clone()],
            vec![initial_customers],
        )
        .await
        .expect("append initial customers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_west_orders").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_rows(&snapshot), vec![(1, "west".to_string(), 50)]);

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(StringArray::from(vec!["west"])),
        ],
    )
    .expect("customer insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![customer_insert.clone()],
            vec![customer_insert],
        )
        .await
        .expect("append customer insert");
    runtime.run_tick(2).await.expect("right delta tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(3, "west".to_string(), 70, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_west_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_west_orders")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_rows(&recovered_snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let order_insert = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(Int64Array::from(vec![80])),
        ],
    )
    .expect("order insert batch");
    recovered
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![order_insert.clone()],
            vec![order_insert],
        )
        .await
        .expect("append order insert");
    recovered.run_tick(4).await.expect("left delta tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(
        join_rows(&snapshot),
        vec![
            (1, "west".to_string(), 50),
            (3, "west".to_string(), 70),
            (4, "west".to_string(), 80),
        ]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-insert delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(4, "west".to_string(), 80, 1)]
    );

    let customer_retract = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["west"])),
        ],
    )
    .expect("customer retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&customers_schema)
        .expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&customer_retract, &weighted_schema, &[-1])
        .expect("weighted customer retract");
    recovered
        .apply_weighted_source_delta("customers", weighted)
        .await
        .expect("apply customer retract");
    recovered.run_tick(5).await.expect("right retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 5)
            .await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(3, "west".to_string(), 70), (4, "west".to_string(), 80)]
    );
    let delta = recovered_handle
        .arrow_delta_for(5)
        .expect("post-retract delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(1, "west".to_string(), 50, -1)]
    );
}

#[tokio::test]
async fn ordered_join_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("customer_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let customers = SourceDefinition::new(
        "customers",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Utf8, false),
        ],
    )
    .expect("customers source definition");
    let orders_schema = orders.to_arrow_schema();
    let customers_schema = customers.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(Int64Array::from(vec![50, 60, 70])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["west", "east"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-ordered-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o JOIN customers c ON o.customer_id = c.id \
        WHERE c.region = 'west' \
        ORDER BY order_id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_ordered_west_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "incremental_inner");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![initial_customers.clone()],
            vec![initial_customers],
        )
        .await
        .expect("append initial customers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_ordered_west_orders")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_rows(&snapshot), vec![(1, "west".to_string(), 50)]);

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(StringArray::from(vec!["west"])),
        ],
    )
    .expect("customer insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![customer_insert.clone()],
            vec![customer_insert],
        )
        .await
        .expect("append customer insert");
    runtime.run_tick(2).await.expect("right delta tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(3, "west".to_string(), 70, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_ordered_west_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "incremental_inner");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_ordered_west_orders")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_rows(&recovered_snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let order_insert = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(Int64Array::from(vec![80])),
        ],
    )
    .expect("order insert batch");
    recovered
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![order_insert.clone()],
            vec![order_insert],
        )
        .await
        .expect("append order insert");
    recovered.run_tick(4).await.expect("left delta tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(
        join_rows(&snapshot),
        vec![
            (1, "west".to_string(), 50),
            (3, "west".to_string(), 70),
            (4, "west".to_string(), 80),
        ]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-insert delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(4, "west".to_string(), 80, 1)]
    );
}

#[tokio::test]
async fn multi_column_join_uses_slate_backed_columnar_operator_semantics() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("customer_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let customers = SourceDefinition::new(
        "customers",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Utf8, false),
        ],
    )
    .expect("customers source definition");
    let orders_schema = orders.to_arrow_schema();
    let customers_schema = customers.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 10, 11])),
            Arc::new(Int64Array::from(vec![50, 60, 70])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 10, 11])),
            Arc::new(Int64Array::from(vec![50, 60, 80])),
            Arc::new(StringArray::from(vec!["west", "east", "north"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-multi-column-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o JOIN customers c \
        ON o.customer_id = c.id AND o.amount = c.amount";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_customer_amount_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "incremental_inner");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![initial_customers.clone()],
            vec![initial_customers],
        )
        .await
        .expect("append initial customers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_customer_amount_orders")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(1, "west".to_string(), 50), (2, "east".to_string(), 60)]
    );

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int64Array::from(vec![70])),
            Arc::new(StringArray::from(vec!["south"])),
        ],
    )
    .expect("customer insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![customer_insert.clone()],
            vec![customer_insert],
        )
        .await
        .expect("append customer insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![
            (1, "west".to_string(), 50),
            (2, "east".to_string(), 60),
            (3, "south".to_string(), 70),
        ]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(3, "south".to_string(), 70, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_customer_amount_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_customer_amount_orders")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_rows(&recovered_snapshot),
        vec![
            (1, "west".to_string(), 50),
            (2, "east".to_string(), 60),
            (3, "south".to_string(), 70),
        ]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn join_topn_uses_slate_backed_columnar_operator_incrementally() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("itemName", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initialBid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["item-1", "item-2"])),
            Arc::new(StringArray::from(vec!["description-1", "description-2"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from(vec![10, 10])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 100])),
            Arc::new(Int64Array::from(vec![101, 102])),
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 11, 9, 12])),
            Arc::new(Int64Array::from(vec![100, 200, 200, 50])),
            Arc::new(TimestampMillisecondArray::from(vec![20, 15, 15, 25])),
            Arc::new(StringArray::from(vec![
                "bid-extra-10",
                "bid-extra-11",
                "bid-extra-09",
                "bid-extra-12",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-join-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("itemName", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("initialBid", DataType::Int64, false),
        Field::new("reserve", DataType::Int64, false),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "expires",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("seller", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new(
            "bidTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("bidExtra", DataType::Utf8, false),
    ]));
    let query = "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", \
        expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" \
        FROM (SELECT a.id, a.\"itemName\", a.description, a.\"initialBid\", a.reserve, \
        a.\"dateTime\", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, \
        b.price, b.\"dateTime\" AS \"bidTime\", b.extra AS \"bidExtra\", \
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.\"dateTime\" ASC, \
        b.bidder ASC, b.extra ASC) AS rownum \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
        WHERE rownum <= 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bid",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bid").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    assert_eq!(
        join_topn_rows_with_extra(&snapshot),
        vec![
            (1, 9, 200, "bid-extra-09".to_string()),
            (2, 12, 50, "bid-extra-12".to_string())
        ]
    );

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(TimestampMillisecondArray::from(vec![30])),
            Arc::new(StringArray::from(vec!["bid-extra-13"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 13, 300), (2, 12, 50)]);
    let delta = handle.arrow_delta_for(2).expect("better bid delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, -1), (1, 13, 300, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bid",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bid")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_topn_rows(&recovered_snapshot),
        vec![(1, 13, 300), (2, 12, 50)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(TimestampMillisecondArray::from(vec![30])),
            Arc::new(StringArray::from(vec!["bid-extra-13"])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, 1), (1, 13, 300, -1)]
    );
    assert_eq!(
        join_topn_rows_with_extra(&snapshot),
        vec![
            (1, 9, 200, "bid-extra-09".to_string()),
            (2, 12, 50, "bid-extra-12".to_string())
        ]
    );
}

#[tokio::test]
async fn q6_shape_uses_grouped_stats_over_grouped_max_join_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("itemName", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initialBid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["item-1", "item-2", "item-3"])),
            Arc::new(StringArray::from(vec![
                "description-1",
                "description-2",
                "description-3",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(TimestampMillisecondArray::from(vec![10, 10, 10])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 100, 100])),
            Arc::new(Int64Array::from(vec![10, 10, 20])),
            Arc::new(Int64Array::from(vec![7, 7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
                "auction-extra-3",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![101, 102, 201, 301])),
            Arc::new(Int64Array::from(vec![100, 120, 110, 300])),
            Arc::new(TimestampMillisecondArray::from(vec![20, 25, 30, 40])),
            Arc::new(StringArray::from(vec![
                "bid-extra-101",
                "bid-extra-102",
                "bid-extra-201",
                "bid-extra-301",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q6-grouped-max-rewrite").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("moving_avg_price", DataType::Float64, true),
    ]));
    let query = "SELECT seller, AVG(price) AS moving_avg_price FROM (\
        SELECT a.seller, b.price, b.\"dateTime\", \
        ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, \
        b.\"dateTime\" ASC, b.bidder ASC, b.extra ASC) AS rownum \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
        WHERE rownum <= 1 GROUP BY seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q6",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_q6").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 115.0), (20, 300.0)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![200])),
            Arc::new(TimestampMillisecondArray::from(vec![35])),
            Arc::new(StringArray::from(vec!["bid-extra-202"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 160.0), (20, 300.0)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 115.0, -1), (10, 160.0, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q6",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_q6")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        category_avg_rows(&recovered_snapshot),
        vec![(10, 160.0), (20, 300.0)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![200])),
            Arc::new(TimestampMillisecondArray::from(vec![35])),
            Arc::new(StringArray::from(vec!["bid-extra-202"])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 115.0), (20, 300.0)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 160.0, -1), (10, 115.0, 1)]
    );
}

#[tokio::test]
async fn cdc_q9_shape_uses_incremental_join_topn_semantics() {
    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["item-1", "item-2"])),
            Arc::new(StringArray::from(vec!["description-1", "description-2"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![10, 10])),
            Arc::new(Int64Array::from(vec![100, 100])),
            Arc::new(Int64Array::from(vec![101, 102])),
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 11, 9, 12])),
            Arc::new(Int64Array::from(vec![100, 200, 200, 50])),
            Arc::new(StringArray::from(vec!["web", "web", "web", "web"])),
            Arc::new(StringArray::from(vec!["/10", "/11", "/09", "/12"])),
            Arc::new(Int64Array::from(vec![20, 15, 15, 25])),
            Arc::new(StringArray::from(vec![
                "bid-extra-10",
                "bid-extra-11",
                "bid-extra-09",
                "bid-extra-12",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-cdc-q9-shape").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("itemName", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("initialBid", DataType::Int64, false),
        Field::new("reserve", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
        Field::new("expires", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("bidTime", DataType::Int64, false),
        Field::new("bidExtra", DataType::Utf8, false),
    ]));
    let query = "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", \
        expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" \
        FROM (SELECT a.id, a.item_name AS \"itemName\", a.description, \
        a.initial_bid AS \"initialBid\", a.reserve, a.auction_time AS \"dateTime\", \
        a.expires, a.seller, a.category, a.auction_extra AS extra, b.auction, b.bidder, \
        b.price, b.bid_time AS \"bidTime\", b.bid_extra AS \"bidExtra\", \
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.bid_time ASC, \
        b.bidder ASC, b.bid_extra ASC) AS rownum \
        FROM (SELECT id, item_name, description, initial_bid, reserve, \
        date_time AS auction_time, expires, seller, category, extra AS auction_extra \
        FROM nexmark_auction) a JOIN (SELECT auction, bidder, price, date_time AS bid_time, \
        extra AS bid_extra FROM nexmark_bid) b ON a.id = b.auction \
        WHERE b.bid_time BETWEEN a.auction_time AND a.expires) ranked \
        WHERE rownum <= 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_cdc_q9",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_cdc_q9").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    assert_eq!(
        join_topn_rows_with_extra(&snapshot),
        vec![
            (1, 9, 200, "bid-extra-09".to_string()),
            (2, 12, 50, "bid-extra-12".to_string())
        ]
    );

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![104])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(StringArray::from(vec!["web"])),
            Arc::new(StringArray::from(vec!["/13"])),
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(StringArray::from(vec!["bid-extra-13"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![better_bid.clone()],
            vec![better_bid.clone()],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let delta = handle.arrow_delta_for(2).expect("better bid delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, -1), (1, 13, 300, 1)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&better_bid, &weighted_schema, &[-1])
        .expect("weighted retract better bid");
    runtime
        .apply_weighted_source_delta("nexmark_bid", weighted)
        .await
        .expect("apply weighted bid retract");
    runtime.run_tick(3).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 3).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    let delta = handle.arrow_delta_for(3).expect("post-retract delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, 1), (1, 13, 300, -1)]
    );
}

#[tokio::test]
async fn cdc_q6_shape_uses_incremental_top_bid_grouped_avg_semantics() {
    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["item-1", "item-2", "item-3"])),
            Arc::new(StringArray::from(vec![
                "description-1",
                "description-2",
                "description-3",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(Int64Array::from(vec![10, 10, 10])),
            Arc::new(Int64Array::from(vec![100, 100, 100])),
            Arc::new(Int64Array::from(vec![10, 10, 20])),
            Arc::new(Int64Array::from(vec![7, 7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
                "auction-extra-3",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 201, 301])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![101, 102, 201, 301])),
            Arc::new(Int64Array::from(vec![100, 120, 110, 300])),
            Arc::new(StringArray::from(vec!["web", "web", "web", "web"])),
            Arc::new(StringArray::from(vec!["/101", "/102", "/201", "/301"])),
            Arc::new(Int64Array::from(vec![20, 25, 30, 40])),
            Arc::new(StringArray::from(vec![
                "bid-extra-101",
                "bid-extra-102",
                "bid-extra-201",
                "bid-extra-301",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-cdc-q6-shape").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("moving_avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price \
        FROM (SELECT a.seller, b.price, b.date_time, \
        ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, \
        b.date_time ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked \
        WHERE rownum <= 1 GROUP BY seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_cdc_q6",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_cdc_q6").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 115), (20, 300)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![200])),
            Arc::new(StringArray::from(vec!["web"])),
            Arc::new(StringArray::from(vec!["/202"])),
            Arc::new(Int64Array::from(vec![35])),
            Arc::new(StringArray::from(vec!["bid-extra-202"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![better_bid.clone()],
            vec![better_bid.clone()],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 160), (20, 300)]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&better_bid, &weighted_schema, &[-1])
        .expect("weighted retract better bid");
    runtime
        .apply_weighted_source_delta("nexmark_bid", weighted)
        .await
        .expect("apply weighted bid retract");
    runtime.run_tick(3).await.expect("retract tick");

    let snapshot = handle.arrow_snapshot_for(3).expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 115), (20, 300)]);

    let deleted_auction = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["item-3"])),
            Arc::new(StringArray::from(vec!["description-3"])),
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![20])),
            Arc::new(Int64Array::from(vec![8])),
            Arc::new(StringArray::from(vec!["auction-extra-3"])),
        ],
    )
    .expect("deleted auction batch");
    let weighted_auction_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&auction_schema)
            .expect("weighted auction schema");
    let weighted_auction =
        weighted_batch_from_diffs(&deleted_auction, &weighted_auction_schema, &[-1])
            .expect("weighted auction delete");
    runtime
        .apply_weighted_source_delta("nexmark_auction", weighted_auction)
        .await
        .expect("apply weighted auction delete");
    runtime.run_tick(4).await.expect("auction delete tick");

    let snapshot = handle
        .arrow_snapshot_for(4)
        .expect("post-auction-delete snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 115)]);
}

#[tokio::test]
async fn cdc_q6_generated_mutations_match_query_provider_semantics() {
    const BASE_TS: i64 = 1_700_000_000_000;
    const BID_INITIAL_ROWS: i64 = 10_000;
    const AUCTION_INITIAL_ROWS: i64 = 1_112;
    const PERSON_KEYSPACE: i64 = 1_112;
    const BID_UPDATES: i64 = 4_444;
    const BID_DELETES: i64 = 2_222;
    const BID_INSERTS: i64 = 2_222;
    const AUCTION_UPDATES: i64 = 556;
    const AUCTION_DELETES: i64 = 278;
    const AUCTION_INSERTS: i64 = 278;
    const LIVE_AUCTION_KEYSPACE: i64 = AUCTION_INITIAL_ROWS + AUCTION_INSERTS;

    fn auction_batch(
        schema: &SchemaRef,
        ids: impl IntoIterator<Item = (i64, bool)>,
    ) -> RecordBatch {
        let mut id_values = Vec::new();
        let mut item_names = Vec::new();
        let mut descriptions = Vec::new();
        let mut initial_bids = Vec::new();
        let mut reserves = Vec::new();
        let mut date_times = Vec::new();
        let mut expires = Vec::new();
        let mut sellers = Vec::new();
        let mut categories = Vec::new();
        let mut extras = Vec::new();
        for (id, updated) in ids {
            id_values.push(id);
            item_names.push(format!("item_{id}"));
            descriptions.push(format!("auction description {id}"));
            initial_bids.push(100 + (id % 10_000));
            reserves.push(1000 + (id % 100_000) + if updated { 31 } else { 0 });
            date_times.push(BASE_TS + id);
            expires.push(BASE_TS + id + 86_400_000 + if updated { 1000 } else { 0 });
            sellers.push(((id - 1) % PERSON_KEYSPACE) + 1);
            let category = ((id - 1) % 20) + 1;
            categories.push(if updated {
                if category == 20 { 1 } else { category + 1 }
            } else {
                category
            });
            extras.push(if updated {
                format!("auction_extra_{id}_updated")
            } else {
                format!("auction_extra_{id}")
            });
        }
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(id_values)),
                Arc::new(StringArray::from(item_names)),
                Arc::new(StringArray::from(descriptions)),
                Arc::new(Int64Array::from(initial_bids)),
                Arc::new(Int64Array::from(reserves)),
                Arc::new(Int64Array::from(date_times)),
                Arc::new(Int64Array::from(expires)),
                Arc::new(Int64Array::from(sellers)),
                Arc::new(Int64Array::from(categories)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("auction batch")
    }

    fn bid_batch(
        schema: &SchemaRef,
        ids: impl IntoIterator<Item = (i64, bool)>,
        auction_keyspace: i64,
    ) -> RecordBatch {
        let mut id_values = Vec::new();
        let mut auctions = Vec::new();
        let mut bidders = Vec::new();
        let mut prices = Vec::new();
        let mut channels = Vec::new();
        let mut urls = Vec::new();
        let mut date_times = Vec::new();
        let mut extras = Vec::new();
        for (id, updated) in ids {
            id_values.push(id);
            let auction = ((id - 1) % auction_keyspace) + 1;
            auctions.push(auction);
            bidders.push(((id - 1) % PERSON_KEYSPACE) + 1);
            prices.push(1000 + ((id * 17) % 2_000_000) + if updated { 17 } else { 0 });
            let channel = if updated {
                match id % 4 {
                    0 => "apple",
                    1 => "google",
                    2 => "facebook",
                    _ => "baidu",
                }
            } else {
                match id % 5 {
                    0 => "apple",
                    1 => "google",
                    2 => "facebook",
                    3 => "baidu",
                    _ => "web",
                }
            };
            channels.push(channel.to_string());
            urls.push(if updated {
                format!(
                    "https://cdc.example.com/watch/channel_id={}/u/{id}",
                    (id + 7) % 100
                )
            } else {
                format!(
                    "https://nexmark.example.com/auction/{auction}/bid/{id}?channel_id={}",
                    id % 100
                )
            });
            date_times.push(BASE_TS + id + if updated { 1000 } else { 0 });
            extras.push(if updated {
                format!("bid_extra_ccc_{id}_updated")
            } else {
                format!("bid_extra_ccc_{id}")
            });
        }
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(id_values)),
                Arc::new(Int64Array::from(auctions)),
                Arc::new(Int64Array::from(bidders)),
                Arc::new(Int64Array::from(prices)),
                Arc::new(StringArray::from(channels)),
                Arc::new(StringArray::from(urls)),
                Arc::new(Int64Array::from(date_times)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("bid batch")
    }

    async fn apply_weighted(
        runtime: &mut VectorizedExecutionRuntime,
        source_name: &str,
        schema: &SchemaRef,
        batch: RecordBatch,
        diffs: &[i64],
    ) {
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(schema).expect("weighted schema");
        let weighted =
            weighted_batch_from_diffs(&batch, &weighted_schema, diffs).expect("weighted delta");
        runtime
            .apply_weighted_source_delta(source_name, weighted)
            .await
            .expect("apply weighted delta");
    }

    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);

    let table = build_operator_state_table("vectorized-columnar-cdc-q6-generated-mutations").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("moving_avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price \
        FROM (SELECT a.seller, b.price, b.date_time, \
        ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, \
        b.date_time ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked \
        WHERE rownum <= 1 GROUP BY seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_cdc_q6_generated",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(Arc::clone(&table))
            .with_source_query_tables(),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    let initial_auctions = auction_batch(
        &auction_schema,
        (1..=AUCTION_INITIAL_ROWS).map(|id| (id, false)),
    );
    let initial_bids = bid_batch(
        &bid_schema,
        (1..=BID_INITIAL_ROWS).map(|id| (id, false)),
        AUCTION_INITIAL_ROWS,
    );
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let bid_update_batch = bid_batch(
        &bid_schema,
        (1..=BID_UPDATES)
            .map(|id| (id, false))
            .chain((1..=BID_UPDATES).map(|id| (id, true))),
        AUCTION_INITIAL_ROWS,
    );
    let bid_update_diffs = std::iter::repeat_n(-1, BID_UPDATES as usize)
        .chain(std::iter::repeat_n(1, BID_UPDATES as usize))
        .collect::<Vec<_>>();
    apply_weighted(
        &mut runtime,
        "nexmark_bid",
        &bid_schema,
        bid_update_batch,
        &bid_update_diffs,
    )
    .await;
    runtime.run_tick(2).await.expect("bid update tick");

    let bid_delete_start = BID_UPDATES + 1;
    let bid_delete_batch = bid_batch(
        &bid_schema,
        (bid_delete_start..bid_delete_start + BID_DELETES).map(|id| (id, false)),
        AUCTION_INITIAL_ROWS,
    );
    let bid_delete_diffs = vec![-1; BID_DELETES as usize];
    apply_weighted(
        &mut runtime,
        "nexmark_bid",
        &bid_schema,
        bid_delete_batch,
        &bid_delete_diffs,
    )
    .await;
    runtime.run_tick(3).await.expect("bid delete tick");

    let bid_insert_batch = bid_batch(
        &bid_schema,
        (BID_INITIAL_ROWS + 1..=BID_INITIAL_ROWS + BID_INSERTS).map(|id| (id, false)),
        LIVE_AUCTION_KEYSPACE,
    );
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![bid_insert_batch.clone()],
            vec![bid_insert_batch],
        )
        .await
        .expect("append bid inserts");
    runtime.run_tick(4).await.expect("bid insert tick");

    let auction_update_batch = auction_batch(
        &auction_schema,
        (1..=AUCTION_UPDATES)
            .map(|id| (id, false))
            .chain((1..=AUCTION_UPDATES).map(|id| (id, true))),
    );
    let auction_update_diffs = std::iter::repeat_n(-1, AUCTION_UPDATES as usize)
        .chain(std::iter::repeat_n(1, AUCTION_UPDATES as usize))
        .collect::<Vec<_>>();
    apply_weighted(
        &mut runtime,
        "nexmark_auction",
        &auction_schema,
        auction_update_batch,
        &auction_update_diffs,
    )
    .await;
    runtime.run_tick(5).await.expect("auction update tick");

    let auction_delete_start = AUCTION_UPDATES + 1;
    let auction_delete_batch = auction_batch(
        &auction_schema,
        (auction_delete_start..auction_delete_start + AUCTION_DELETES).map(|id| (id, false)),
    );
    let auction_delete_diffs = vec![-1; AUCTION_DELETES as usize];
    apply_weighted(
        &mut runtime,
        "nexmark_auction",
        &auction_schema,
        auction_delete_batch,
        &auction_delete_diffs,
    )
    .await;
    runtime.run_tick(6).await.expect("auction delete tick");

    let auction_insert_batch = auction_batch(
        &auction_schema,
        (AUCTION_INITIAL_ROWS + 1..=AUCTION_INITIAL_ROWS + AUCTION_INSERTS).map(|id| (id, false)),
    );
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![auction_insert_batch.clone()],
            vec![auction_insert_batch],
        )
        .await
        .expect("append auction inserts");
    runtime.run_tick(7).await.expect("auction insert tick");

    let handle = registry
        .get("mv_cdc_q6_generated")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(7).expect("mv snapshot");
    let actual = id_count_rows(&snapshot);

    let ctx = SessionContext::new();
    for (name, provider) in runtime.table_providers() {
        ctx.register_table(&name, provider)
            .expect("register source table");
    }
    let expected = ctx
        .sql(query)
        .await
        .expect("plan expected q6")
        .collect()
        .await
        .expect("collect expected q6");
    assert_eq!(actual, id_count_rows(&expected));
}

#[tokio::test]
async fn topn_over_join_avg_uses_grouped_stats_input_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-topn-over-join-avg").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let query = "SELECT key, value FROM (\
        SELECT auction AS key, CAST(avg_price AS BIGINT) AS value \
        FROM (SELECT b.auction, AVG(b.price) AS avg_price \
            FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j \
        ORDER BY avg_price DESC LIMIT 2) s";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_avg_bid",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_avg_bid").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 150), (2, 50)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 150), (2, 300)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(2, 50, -1), (2, 300, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_avg_bid",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_avg_bid")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 150), (2, 300)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 150), (2, 50)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(2, 50, 1), (2, 300, -1)]
    );
}

#[tokio::test]
async fn global_aggregate_over_join_avg_topn_uses_grouped_stats_topn_input_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table =
        build_operator_state_table("vectorized-columnar-aggregate-over-join-avg-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]));
    let query = "SELECT SUM(value) AS total FROM (\
        SELECT auction AS key, CAST(avg_price AS BIGINT) AS value \
        FROM (SELECT b.auction, AVG(b.price) AS avg_price \
            FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j \
        ORDER BY avg_price DESC LIMIT 2) s";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_avg_total",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_avg_total").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![200]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![450]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(200, -1), (450, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_avg_total",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_avg_total")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![450]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![200]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(200, 1), (450, -1)]);
}

#[tokio::test]
async fn global_topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-global-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price FROM bids ORDER BY price DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bids").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![25, 7])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (4, 50, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, -1), (4, 50, 25, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 20, 20), (4, 50, 25)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, 1), (4, 50, 25, -1)]
    );
}

#[tokio::test]
async fn hidden_sort_key_topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-hidden-sort-key-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT auction FROM bids ORDER BY price DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_hidden_sort_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_hidden_sort_top_bids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5, 6])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![25, 7])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 5]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, -1), (5, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_hidden_sort_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_hidden_sort_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![2, 5]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, 1), (5, -1)]);
}

#[tokio::test]
async fn filtered_topn_wrappers_use_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-filtered-topn-wrappers").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let global_query = "SELECT auction, price \
        FROM (SELECT auction, price FROM bids ORDER BY price DESC LIMIT 3) t \
        WHERE price > 18";
    let partitioned_query = "SELECT auction, price \
        FROM (SELECT auction, price, \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn \
            FROM bids) ranked \
        WHERE rn <= 2 AND price > 18";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_partitioned_topn",
                partitioned_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        runtime.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let global_handle = registry
        .get("mv_filtered_global_topn")
        .expect("global materialized view");
    let partitioned_handle = registry
        .get("mv_filtered_partitioned_topn")
        .expect("partitioned materialized view");
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(1).expect("snapshot")),
        vec![(1, 20), (1, 30)]
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                partitioned_handle.as_ref(),
                Arc::clone(&output_schema),
                1,
            )
            .await
        ),
        vec![(1, 20), (1, 30)]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![25, 40])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let expected_after_insert = vec![(1, 25), (1, 30), (2, 40)];
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(2).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                partitioned_handle.as_ref(),
                Arc::clone(&output_schema),
                2,
            )
            .await
        ),
        expected_after_insert
    );
    let expected_insert_delta = vec![(1, 20, -1), (1, 25, 1), (2, 40, 1)];
    assert_eq!(
        weighted_id_count_rows(&global_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );
    assert_eq!(
        weighted_id_count_rows(&partitioned_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_partitioned_topn",
                partitioned_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        recovered.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_global = recovery_registry
        .get("mv_filtered_global_topn")
        .expect("recovered global materialized view");
    let recovered_partitioned = recovery_registry
        .get("mv_filtered_partitioned_topn")
        .expect("recovered partitioned materialized view");
    assert_eq!(
        id_count_rows(&recovered_global.arrow_snapshot_for(3).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                recovered_partitioned.as_ref(),
                Arc::clone(&output_schema),
                3,
            )
            .await
        ),
        expected_after_insert
    );
    assert!(
        recovered_global
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
    assert!(
        recovered_partitioned
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let expected_after_retract = vec![(1, 20), (1, 30), (2, 40)];
    assert_eq!(
        id_count_rows(&recovered_global.arrow_snapshot_for(4).expect("snapshot")),
        expected_after_retract
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                recovered_partitioned.as_ref(),
                Arc::clone(&output_schema),
                4,
            )
            .await
        ),
        expected_after_retract
    );
    let expected_retract_delta = vec![(1, 20, 1), (1, 25, -1)];
    assert_eq!(
        weighted_id_count_rows(&recovered_global.arrow_delta_for(4).expect("delta")),
        expected_retract_delta
    );
    assert_eq!(
        weighted_id_count_rows(&recovered_partitioned.arrow_delta_for(4).expect("delta")),
        expected_retract_delta
    );
}

#[tokio::test]
async fn ordered_topn_wrappers_use_slate_backed_columnar_operator_semantics() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10, 30, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-ordered-topn-wrappers").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let global_query = "SELECT auction, price \
        FROM (SELECT auction, price FROM bids ORDER BY price DESC LIMIT 3) t \
        ORDER BY auction";
    let row_number_query = "SELECT auction, price \
        FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bids) ranked \
        WHERE rn <= 3 \
        ORDER BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_row_number_topn",
                row_number_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        runtime.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let global_handle = registry
        .get("mv_ordered_global_topn")
        .expect("global materialized view");
    let row_number_handle = registry
        .get("mv_ordered_row_number_topn")
        .expect("row-number materialized view");
    let expected_initial = vec![(1, 10), (2, 30), (3, 20)];
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(1).expect("snapshot")),
        expected_initial
    );
    assert_eq!(
        id_count_rows(&row_number_handle.arrow_snapshot_for(1).expect("snapshot")),
        expected_initial
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![40, 25])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let expected_after_insert = vec![(2, 30), (4, 40), (5, 25)];
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(2).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(&row_number_handle.arrow_snapshot_for(2).expect("snapshot")),
        expected_after_insert
    );
    let expected_insert_delta = vec![(1, 10, -1), (3, 20, -1), (4, 40, 1), (5, 25, 1)];
    assert_eq!(
        weighted_id_count_rows(&global_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );
    assert_eq!(
        weighted_id_count_rows(&row_number_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_row_number_topn",
                row_number_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        recovered.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_global = recovery_registry
        .get("mv_ordered_global_topn")
        .expect("recovered global materialized view");
    let recovered_row_number = recovery_registry
        .get("mv_ordered_row_number_topn")
        .expect("recovered row-number materialized view");
    assert_eq!(
        id_count_rows(&recovered_global.arrow_snapshot_for(3).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(
            &recovered_row_number
                .arrow_snapshot_for(3)
                .expect("snapshot")
        ),
        expected_after_insert
    );
    assert!(
        recovered_global
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
    assert!(
        recovered_row_number
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
}

#[tokio::test]
async fn global_row_number_topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-global-row-number-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price \
        FROM (SELECT auction, bidder, price, \
            ROW_NUMBER() OVER (ORDER BY price DESC) AS rank_number \
            FROM bids) ranked \
        WHERE rank_number <= 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_global_ranked_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_global_ranked_bids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![25, 7])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (4, 50, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, -1), (4, 50, 25, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_global_ranked_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_global_ranked_bids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 20, 20), (4, 50, 25)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, 1), (4, 50, 25, -1)]
    );
}

#[tokio::test]
async fn row_number_predicate_variants_use_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-row-number-predicate-variants").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let reversed_query = "SELECT auction, bidder, price \
        FROM (SELECT auction, bidder, price, \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
            FROM bids) ranked \
        WHERE 2 >= rank_number";
    let equality_query = "SELECT auction, bidder, price \
        FROM (SELECT auction, bidder, price, \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
            FROM bids) ranked \
        WHERE rank_number = 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_reversed_ranked_bids",
                reversed_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_second_ranked_bids",
                equality_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert!(
        runtime
            .materialized_views
            .iter()
            .all(|mv| mv.operator.mode() == MaterializedViewExecutionMode::ColumnarTopN)
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let reversed = registry
        .get("mv_reversed_ranked_bids")
        .expect("reversed materialized view");
    let equality = registry
        .get("mv_second_ranked_bids")
        .expect("equality materialized view");
    let reversed_snapshot =
        materialized_view_snapshot_for(reversed.as_ref(), Arc::clone(&output_schema), 1).await;
    let equality_snapshot =
        materialized_view_snapshot_for(equality.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        bid_topn_rows(&reversed_snapshot),
        vec![(1, 20, 20), (1, 30, 30), (2, 40, 15), (2, 50, 5)]
    );
    assert_eq!(
        bid_topn_rows(&equality_snapshot),
        vec![(1, 20, 20), (2, 50, 5)]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![99])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let expected_snapshot = vec![(1, 30, 30), (1, 99, 25), (2, 40, 15), (2, 50, 5)];
    let reversed_snapshot =
        materialized_view_snapshot_for(reversed.as_ref(), Arc::clone(&output_schema), 2).await;
    let equality_snapshot =
        materialized_view_snapshot_for(equality.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(bid_topn_rows(&reversed_snapshot), expected_snapshot);
    assert_eq!(
        bid_topn_rows(&equality_snapshot),
        vec![(1, 99, 25), (2, 50, 5)]
    );
    let expected_delta = vec![(1, 20, 20, -1), (1, 99, 25, 1)];
    assert_eq!(
        weighted_bid_topn_rows(&reversed.arrow_delta_for(2).expect("reversed delta")),
        expected_delta
    );
    assert_eq!(
        weighted_bid_topn_rows(&equality.arrow_delta_for(2).expect("equality delta")),
        expected_delta
    );
}

#[tokio::test]
async fn global_topn_offset_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![30, 25, 20, 15])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-global-topn-offset").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price FROM bids ORDER BY price DESC LIMIT 2 OFFSET 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_offset_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_offset_top_bids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(2, 20, 25), (3, 30, 20)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5, 6])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![40, 18])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 10, 30), (2, 20, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 10, 30, 1), (3, 30, 20, -1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_offset_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_offset_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 10, 30), (2, 20, 25)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![40])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(2, 20, 25), (3, 30, 20)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 10, 30, -1), (3, 30, 20, 1)]
    );
}

#[tokio::test]
async fn topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price FROM (\
        SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
        FROM bids) ranked WHERE rank_number <= 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bids").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        bid_topn_rows(&snapshot),
        vec![(1, 20, 20), (1, 30, 30), (2, 40, 5)]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![25, 50])),
            Arc::new(Int64Array::from(vec![25, 7])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        bid_topn_rows(&snapshot),
        vec![(1, 25, 25), (1, 30, 30), (2, 40, 5), (3, 50, 7)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 20, 20, -1), (1, 25, 25, 1), (3, 50, 7, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 25, 25), (1, 30, 30), (2, 40, 5), (3, 50, 7)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(
        bid_topn_rows(&snapshot),
        vec![(1, 20, 20), (1, 25, 25), (2, 40, 5), (3, 50, 7)]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 20, 20, 1), (1, 30, 30, -1)]
    );
}

#[tokio::test]
async fn under_limit_topn_projection_uses_weighted_source_delta_semantics() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 1100, 1200])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-under-limit-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let query = "SELECT auction, bidder, price, \"dateTime\" FROM (\
        SELECT auction, bidder, price, date_time AS \"dateTime\", \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, date_time ASC) \
                AS rank_number \
        FROM bids) ranked WHERE rank_number <= 10";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_under_limit_top_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_under_limit_top_bids")
        .expect("materialized view");
    assert_eq!(
        bid_topn_timestamp_rows(
            &materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await
        ),
        vec![(1, 10, 10, 1000), (1, 20, 20, 1100), (2, 30, 30, 1200)]
    );

    let changes = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![102, 104])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![20, 40])),
            Arc::new(Int64Array::from(vec![20, 40])),
            Arc::new(TimestampMillisecondArray::from(vec![1100, 900])),
        ],
    )
    .expect("source changes");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&changes, &weighted_schema, &[-1, 1]).expect("weighted changes");
    runtime
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted source delta");
    runtime.run_tick(2).await.expect("weighted delta tick");

    assert_eq!(
        bid_topn_timestamp_rows(
            &materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await
        ),
        vec![(1, 10, 10, 1000), (1, 40, 40, 900), (2, 30, 30, 1200)]
    );
    assert_eq!(
        weighted_bid_topn_timestamp_rows(&handle.arrow_delta_for(2).expect("mv delta")),
        vec![(1, 20, 20, 1100, -1), (1, 40, 40, 900, 1)]
    );
}

#[tokio::test]
async fn count_group_by_requires_slate_backed_operator_state_table() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));

    let result = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("count MV should require SlateDB-backed operator state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("requires SlateDB-backed operator state"),
        "{err:#}"
    );
}

#[tokio::test]
async fn filter_project_requires_slate_backed_operator_state_table() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let result = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders WHERE amount > 10",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("filter/project MV should require SlateDB-backed operator state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("requires SlateDB-backed operator state"),
        "{err:#}"
    );
}

#[tokio::test]
async fn source_query_tables_are_not_maintained_by_default() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-source-query-default").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders",
            Arc::clone(&output_schema),
        )],
        registry,
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");

    assert!(runtime.table_providers().is_empty());
}

#[tokio::test]
async fn source_query_tables_can_be_limited_by_name() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("orders source definition");
    let raw_events = SourceDefinition::new(
        "raw_events",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("raw_events source definition");
    let nexmark_bid = SourceDefinition::new(
        "nexmark_bid",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("nexmark_bid source definition");
    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(raw_events);
    sources.register(nexmark_bid);

    let runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        Vec::new(),
        Arc::new(MaterializedViewRegistry::new()),
        VectorizedExecutionRuntimeOptions::default()
            .with_source_query_tables_for(["orders", "nexmark_bid"]),
    )
    .await
    .expect("runtime");

    let mut names = runtime
        .table_providers()
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    names.sort();

    assert_eq!(names, vec!["nexmark_bid".to_string(), "orders".to_string()]);
}

#[tokio::test]
async fn source_query_tables_include_explicit_aliases_when_unrestricted() {
    let nexmark_bid = SourceDefinition::new(
        "nexmark_bid",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("nexmark_bid source definition")
    .with_property(SOURCE_QUERY_ALIAS_PROPERTY, "bid");
    let mut sources = SourceRegistry::new();
    sources.register(nexmark_bid);

    let runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        Vec::new(),
        Arc::new(MaterializedViewRegistry::new()),
        VectorizedExecutionRuntimeOptions::default().with_source_query_tables(),
    )
    .await
    .expect("runtime");

    let mut names = runtime
        .table_providers()
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    names.sort();

    assert_eq!(names, vec!["bid".to_string(), "nexmark_bid".to_string()]);
}

#[test]
fn weighted_batch_from_diffs_rejects_non_unit_weights() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source batch");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");

    let err = weighted_batch_from_diffs(&batch, &weighted_schema, &[2])
        .expect_err("non-unit diffs should be rejected");

    assert!(
        err.to_string().contains("diff must be -1, 0, or 1"),
        "{err:#}"
    );
}

#[tokio::test]
async fn sum_group_by_requires_slate_backed_operator_state_table() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));

    let result = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals",
            "SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("sum group-by MV should require SlateDB-backed operator state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("requires SlateDB-backed operator state"),
        "{err:#}"
    );
}

#[tokio::test]
async fn aggregate_topn_uses_slate_backed_columnar_topn_operator_semantics() {
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let bids_schema = bids.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 5, 20, 7])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-aggregate-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT auction, SUM(price) AS total \
        FROM bids \
        GROUP BY auction \
        ORDER BY total DESC \
        LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_auction_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_top_auction_totals")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 15), (2, 20)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![40])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![bid_insert.clone()],
            vec![bid_insert],
        )
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20), (3, 47)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 15, -1), (3, 47, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_auction_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_auction_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(2, 20), (3, 47)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&bid_retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 15), (3, 47)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 15, 1), (2, 20, -1)]
    );
}

#[tokio::test]
async fn join_aggregate_uses_slate_backed_columnar_operator_semantics() {
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let auctions_schema = auctions.to_arrow_schema();
    let bids_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 10])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 110, 120])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-join-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("bid_count", DataType::Int64, false),
    ]));
    let query = "SELECT a.category, COUNT(*) AS bid_count \
        FROM auctions a JOIN bids b ON a.id = b.auction \
        GROUP BY a.category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_category_bid_counts",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auctions",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_category_bid_counts")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 3)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![130])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![bid_insert.clone()],
            vec![bid_insert],
        )
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 4)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(10, 3, -1), (10, 4, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_category_bid_counts",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_category_bid_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(10, 4)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&bid_retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 3)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(10, 3, 1), (10, 4, -1)]
    );
}

#[tokio::test]
async fn q4_uses_incremental_grouped_stats_composition_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, true),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, true),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, true),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let auction_batch = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(10),
                Some(10),
                Some(10),
                None,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(100),
                Some(100),
                Some(100),
                Some(100),
            ])),
            Arc::new(Int64Array::from(vec![10, 10, 20, 10])),
        ],
    )
    .expect("auction batch");
    let bid_batch = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3, 3, 4])),
            Arc::new(Int64Array::from(vec![100, 200, 50, 500, 300, 400, 1000])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(20),
                Some(15),
                Some(25),
                None,
                Some(30),
                Some(200),
                Some(40),
            ])),
        ],
    )
    .expect("bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q4-generic-join-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("avg_max", DataType::Float64, true),
    ]));
    let query = "SELECT category, AVG(max) AS avg_max \
        FROM (SELECT MAX(b.price) AS max, a.category \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires \
        GROUP BY a.id, a.category) per_auction GROUP BY category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_avg_price",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![auction_batch.clone()],
            vec![auction_batch],
        )
        .await
        .expect("append auctions");
    runtime.run_tick(1).await.expect("auction-only tick");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![bid_batch.clone()],
            vec![bid_batch],
        )
        .await
        .expect("append bids");
    runtime.run_tick(2).await.expect("bid tick");

    let handle = registry.get("mv_q4_avg_price").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 125.0), (20, 300.0)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![150])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(40)])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid.clone()],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(3).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(3).expect("updated snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 175.0), (20, 300.0)]);
    let delta = handle.arrow_delta_for(3).expect("updated delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 125.0, -1), (10, 175.0, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_avg_price",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(4).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_q4_avg_price")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("recovered snapshot");
    assert_eq!(
        category_avg_rows(&recovered_snapshot),
        vec![(10, 175.0), (20, 300.0)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(4)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&better_bid, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply better bid retract");
    recovered.run_tick(5).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(5)
        .expect("post-retract snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 125.0), (20, 300.0)]);
    let delta = recovered_handle
        .arrow_delta_for(5)
        .expect("post-retract delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 175.0, -1), (10, 125.0, 1)]
    );
}

#[tokio::test]
async fn q4_nexmark_shape_uses_incremental_grouped_stats_composition_semantics() {
    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let auction_batch = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["item_1", "item_2"])),
            Arc::new(StringArray::from(vec!["desc_1", "desc_2"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![1000, 1000])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_700_086_400_001_i64,
                1_700_086_400_002,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_700_000_000_001_i64,
                1_700_000_000_002,
            ])),
            Arc::new(StringArray::from(vec![
                "auction_extra_1",
                "auction_extra_2",
            ])),
        ],
    )
    .expect("auction batch");
    let bid_batch = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![11, 12, 13])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["web", "web", "web"])),
            Arc::new(StringArray::from(vec!["/a", "/b", "/c"])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_700_000_000_001_i64,
                1_700_000_000_002,
                1_700_000_000_003,
            ])),
            Arc::new(StringArray::from(vec![
                "bid_extra_1",
                "bid_extra_2",
                "bid_extra_3",
            ])),
        ],
    )
    .expect("bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q4-nexmark-shape").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price \
        FROM (SELECT MAX(b.price) AS max, a.category \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires \
        GROUP BY a.id, a.category) per_auction GROUP BY category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_nexmark",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![bid_batch.clone()],
            vec![bid_batch],
        )
        .await
        .expect("append bids");
    runtime.run_tick(1).await.expect("bid-only q4 tick");
    let handle = registry.get("mv_q4_nexmark").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("bid-only snapshot");
    assert!(id_count_rows(&snapshot).is_empty());

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![auction_batch.clone()],
            vec![auction_batch],
        )
        .await
        .expect("append auctions");
    runtime.run_tick(2).await.expect("auction q4 tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 200), (20, 300)]);
}

#[tokio::test]
async fn q4_nexmark_generated_batches_use_incremental_grouped_stats_semantics() {
    const BASE_TS_MS: i64 = 1_700_000_000_000;

    fn nexmark_auction_definition() -> SourceDefinition {
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
        .expect("auction source definition")
    }

    fn nexmark_bid_definition() -> SourceDefinition {
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
        .expect("bid source definition")
    }

    fn generated_auction_batch(schema: SchemaRef, start: usize, rows: usize) -> RecordBatch {
        let mut ids = Vec::with_capacity(rows);
        let mut item_names = Vec::with_capacity(rows);
        let mut descriptions = Vec::with_capacity(rows);
        let mut initial_bids = Vec::with_capacity(rows);
        let mut reserves = Vec::with_capacity(rows);
        let mut sellers = Vec::with_capacity(rows);
        let mut categories = Vec::with_capacity(rows);
        let mut expires = Vec::with_capacity(rows);
        let mut date_times = Vec::with_capacity(rows);
        let mut extras = Vec::with_capacity(rows);
        for auction_idx in start..(start + rows) {
            let idx = i64::try_from(auction_idx).expect("auction idx");
            let initial_bid = 5_000 + (idx % 25_000);
            let date_time = BASE_TS_MS + idx;
            ids.push(idx);
            item_names.push(format!("item_{auction_idx}"));
            descriptions.push(format!("auction_description_{auction_idx}"));
            initial_bids.push(initial_bid);
            reserves.push(initial_bid + 500);
            sellers.push(50_000 + idx);
            categories.push(((idx - 1).rem_euclid(10)) + 1);
            expires.push(date_time + 86_400_000);
            date_times.push(date_time);
            extras.push(format!("auction_extra_{auction_idx}"));
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(item_names)),
                Arc::new(StringArray::from(descriptions)),
                Arc::new(Int64Array::from(initial_bids)),
                Arc::new(Int64Array::from(reserves)),
                Arc::new(Int64Array::from(sellers)),
                Arc::new(Int64Array::from(categories)),
                Arc::new(TimestampMillisecondArray::from(expires)),
                Arc::new(TimestampMillisecondArray::from(date_times)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("generated auction batch")
    }

    fn generated_bid_batch(schema: SchemaRef, start: usize, rows: usize) -> RecordBatch {
        let mut auctions = Vec::with_capacity(rows);
        let mut bidders = Vec::with_capacity(rows);
        let mut prices = Vec::with_capacity(rows);
        let mut channels = Vec::with_capacity(rows);
        let mut urls = Vec::with_capacity(rows);
        let mut date_times = Vec::with_capacity(rows);
        let mut extras = Vec::with_capacity(rows);
        for bid_idx in start..(start + rows) {
            let idx = i64::try_from(bid_idx).expect("bid idx");
            let auction = i64::try_from((bid_idx - 1) % 10_000 + 1).expect("auction id");
            let channel = match bid_idx % 5 {
                0 => "web",
                1 => "apple",
                2 => "google",
                3 => "facebook",
                _ => "baidu",
            };
            auctions.push(auction);
            bidders.push(10_000 + idx);
            prices.push(1_000 + (idx % 50_000));
            channels.push(channel.to_string());
            urls.push(format!("https://example.com/item/{bid_idx}"));
            date_times.push(BASE_TS_MS + idx);
            extras.push(format!("bid_extra_{bid_idx}"));
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(auctions)),
                Arc::new(Int64Array::from(bidders)),
                Arc::new(Int64Array::from(prices)),
                Arc::new(StringArray::from(channels)),
                Arc::new(StringArray::from(urls)),
                Arc::new(TimestampMillisecondArray::from(date_times)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("generated bid batch")
    }

    let auctions = nexmark_auction_definition();
    let bids = nexmark_bid_definition();
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q4-generated-batches").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, true),
        Field::new("avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price \
        FROM (SELECT MAX(b.price) AS max, a.category \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires \
        GROUP BY a.id, a.category) per_auction GROUP BY category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_generated",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    for (version, (start, rows)) in [(1, 8192), (8193, 1808)].into_iter().enumerate() {
        let bid_batch = generated_bid_batch(Arc::clone(&bid_schema), start, rows);
        runtime
            .append_source_batches_for_execution_and_query(
                "nexmark_bid",
                vec![bid_batch.clone()],
                vec![bid_batch],
            )
            .await
            .expect("append generated bids");
        let auction_batch = generated_auction_batch(Arc::clone(&auction_schema), start, rows);
        runtime
            .append_source_batches_for_execution_and_query(
                "nexmark_auction",
                vec![auction_batch.clone()],
                vec![auction_batch],
            )
            .await
            .expect("append generated auctions");
        runtime
            .run_tick((version + 1) as i64)
            .await
            .expect("generated q4 tick");
    }

    let handle = registry.get("mv_q4_generated").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        id_count_rows(&snapshot),
        vec![
            (1, 5996),
            (2, 5997),
            (3, 5998),
            (4, 5999),
            (5, 6000),
            (6, 6001),
            (7, 6002),
            (8, 6003),
            (9, 6004),
            (10, 6005),
        ]
    );
}

#[tokio::test]
async fn union_aggregate_uses_slate_backed_columnar_operator_semantics() {
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 110, 120])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-union-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("row_count", DataType::Int64, false),
    ]));
    let query = "SELECT key, COUNT(*) AS row_count \
        FROM (SELECT auction AS key FROM bids UNION ALL SELECT id AS key FROM auctions) u \
        GROUP BY key";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_key_counts",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnionGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime
        .append_source_batches_for_execution_and_query(
            "auctions",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_union_key_counts")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 3), (2, 1), (3, 1)]);

    let auction_insert = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("auction insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "auctions",
            vec![auction_insert.clone()],
            vec![auction_insert],
        )
        .await
        .expect("append auction insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 3), (2, 2), (3, 1)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(2, 1, -1), (2, 2, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_key_counts",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnionGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_key_counts")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 3), (2, 2), (3, 1)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&bid_retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (2, 2), (3, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 2, 1), (1, 3, -1)]);
}

#[tokio::test]
async fn unsupported_incremental_plan_without_state_table_is_rejected() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));

    let result = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals",
            "SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("unsupported aggregate MV planned without operator state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("requires SlateDB-backed operator state"),
        "{err:#}"
    );
}
