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

fn test_tumble_udf() -> ScalarUDF {
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
        "tumble",
        Signature::one_of(
            vec![TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
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

include!("tests/plan_and_counts.rs");
include!("tests/grouped_stats.rs");
include!("tests/joins.rs");
include!("tests/topn.rs");
include!("tests/compositions.rs");
