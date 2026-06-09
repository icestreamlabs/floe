use super::*;
use crate::source_decoder::{SourceArrowBatchBuilder, SourceArrowBatches};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::storage::{KeyValueTable, SlateTable};
use floe_core::source::{
    AppendIngestEvent, SourceColumn, SourceDataType, SourceDefinition, SourceRegistry,
};
use object_store::memory::InMemory;
use serde_json::json;
use slatedb::Db;

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

fn nullable_string_values(batch: &RecordBatch, column_idx: usize) -> Vec<Option<String>> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    (0..values.len())
        .map(|idx| {
            if values.is_null(idx) {
                None
            } else {
                Some(values.value(idx).to_string())
            }
        })
        .collect()
}

fn nullable_int64_values(batch: &RecordBatch, column_idx: usize) -> Vec<Option<i64>> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    (0..values.len())
        .map(|idx| {
            if values.is_null(idx) {
                None
            } else {
                Some(values.value(idx))
            }
        })
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

fn top_avg_rows(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let groups = int64_values(batch, 0);
        let avgs = float64_values(batch, 1);
        rows.extend(groups.into_iter().zip(avgs));
    }
    rows.sort_by_key(|row| row.0);
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

fn distinct_stats_rows(
    batches: &[RecordBatch],
) -> Vec<(String, String, String, i64, i64, i64, i64, i64)> {
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

fn outer_join_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<String>, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let order_ids = int64_values(batch, 0);
        let regions = nullable_string_values(batch, 1);
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

fn full_outer_join_rows(
    batches: &[RecordBatch],
) -> Vec<(Option<i64>, Option<String>, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let order_ids = nullable_int64_values(batch, 0);
        let regions = nullable_string_values(batch, 1);
        let amounts = nullable_int64_values(batch, 2);
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

fn asof_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let prices = nullable_int64_values(batch, 1);
        rows.extend(ids.into_iter().zip(prices));
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

fn weighted_outer_join_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<String>, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let order_ids = int64_values(batch, 0);
        let regions = nullable_string_values(batch, 1);
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

fn weighted_full_outer_join_rows(
    batches: &[RecordBatch],
) -> Vec<(Option<i64>, Option<String>, Option<i64>, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let order_ids = nullable_int64_values(batch, 0);
        let regions = nullable_string_values(batch, 1);
        let amounts = nullable_int64_values(batch, 2);
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

fn weighted_asof_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i64>, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let ids = int64_values(batch, 0);
        let prices = nullable_int64_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            ids.into_iter()
                .zip(prices)
                .zip(weights)
                .map(|((id, price), weight)| (id, price, weight)),
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

fn self_join_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let left_amounts = int64_values(batch, 1);
        let right_amounts = int64_values(batch, 2);
        rows.extend(
            ids.into_iter()
                .zip(left_amounts)
                .zip(right_amounts)
                .map(|((id, left_amount), right_amount)| (id, left_amount, right_amount)),
        );
    }
    rows.sort();
    rows
}

fn weighted_self_join_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let ids = int64_values(batch, 0);
        let left_amounts = int64_values(batch, 1);
        let right_amounts = int64_values(batch, 2);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            ids.into_iter()
                .zip(left_amounts)
                .zip(right_amounts)
                .zip(weights)
                .map(|(((id, left_amount), right_amount), weight)| {
                    (id, left_amount, right_amount, weight)
                }),
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

fn weighted_top_avg_rows(batches: &[RecordBatch]) -> Vec<(i64, f64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let groups = int64_values(batch, 0);
        let avgs = float64_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            groups
                .into_iter()
                .zip(avgs)
                .zip(weights)
                .map(|((group, avg), weight)| (group, avg, weight)),
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

async fn build_operator_state_table(name: &str) -> Arc<dyn KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
    Arc::new(SlateTable::new(db))
}

fn assert_columnar_join_strategy(runtime: &VectorizedExecutionRuntime, expected: &str) {
    let actual = runtime.materialized_views[0]
        .columnar_join
        .as_ref()
        .map(|state| state.execution_strategy_name());
    assert_eq!(actual, Some(expected));
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
        vec![VectorizedMaterializedViewPlan::new(
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

    let handle = registry.get("mv_orders").expect("materialized view");
    let version = handle.latest_version().expect("mv version");
    let snapshot = handle.arrow_snapshot_for(version).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
    let version = handle.latest_version().expect("mv version");
    let snapshot = handle.arrow_snapshot_for(version).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders",
            "SELECT id, note FROM orders WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(2, "b".to_string()), (4, "d".to_string())]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["b", "c"])),
        ],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(3, "c".to_string()), (4, "d".to_string())]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(2, "b".to_string(), -1), (3, "c".to_string(), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders",
            "SELECT id, note FROM orders WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarStateless
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![(3, "c".to_string()), (4, "d".to_string())]
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
async fn source_join_values_relation_uses_slate_backed_columnar_join_operator() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-composed-values-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let query = "SELECT o.id, v.note FROM orders AS o JOIN \
                 (VALUES (1, 'one'), (3, 'three')) AS v(id, note) ON o.id = v.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_orders_values").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("initial snapshot");
    assert_eq!(id_note_rows(&snapshot), vec![(1, "one".to_string())]);
    let delta = handle.arrow_delta_for(1).expect("initial delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(1, "one".to_string(), 1)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("updated snapshot");
    assert_eq!(id_note_rows(&snapshot), vec![(3, "three".to_string())]);
    let delta = handle.arrow_delta_for(2).expect("updated delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(1, "one".to_string(), -1), (3, "three".to_string(), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "snapshot_diff");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders_values")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![(3, "three".to_string())]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 3, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarStateless
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4, 5]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (5, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_ids")
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        vec![VectorizedMaterializedViewPlan::new(
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_ids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle.arrow_snapshot_for(4).expect("mv snapshot");
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

    let snapshot = recovered_handle.arrow_snapshot_for(5).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_ids_ordered")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1), (1, 2), (2, 1)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-recovery snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (1, 2), (2, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-recovery delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 3, 2, 10, 50, 30.0, 90), (2, 1, 0, 100, 100, 100.0, 100),]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 2, 2, 10, 30, 20.0, 40), (2, 1, 0, 100, 100, 100.0, 100),]
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
async fn aggregate_over_distinct_subquery_uses_slate_backed_columnar_operator_semantics() {
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let schema = bids.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 10, 20, 10])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-distinct-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));
    let query = "SELECT COUNT(auction) AS c \
        FROM (SELECT DISTINCT auction, bidder FROM bids) d";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_distinct_bid_count",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctAggregate
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_bid_count")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_distinct_bid_count",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctAggregate
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_distinct_bid_count")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_west_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-insert snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(5)
        .expect("post-retract snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "incremental_inner");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_ordered_west_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-insert snapshot");
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
async fn left_outer_join_uses_slate_backed_columnar_operator_semantics() {
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
    let table = build_operator_state_table("vectorized-columnar-left-outer-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o LEFT JOIN customers c ON o.customer_id = c.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_customer_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
        .get("mv_customer_orders")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        outer_join_rows(&snapshot),
        vec![
            (1, Some("west".to_string()), 50),
            (2, Some("east".to_string()), 60),
            (3, None, 70),
        ]
    );

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(StringArray::from(vec!["north"])),
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        outer_join_rows(&snapshot),
        vec![
            (1, Some("west".to_string()), 50),
            (2, Some("east".to_string()), 60),
            (3, Some("north".to_string()), 70),
        ]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_outer_join_rows(&delta),
        vec![(3, None, 70, -1), (3, Some("north".to_string()), 70, 1),]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_customer_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_customer_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        outer_join_rows(&recovered_snapshot),
        vec![
            (1, Some("west".to_string()), 50),
            (2, Some("east".to_string()), 60),
            (3, Some("north".to_string()), 70),
        ]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

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
    recovered.run_tick(4).await.expect("right retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(
        outer_join_rows(&snapshot),
        vec![
            (1, None, 50),
            (2, Some("east".to_string()), 60),
            (3, Some("north".to_string()), 70),
        ]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_outer_join_rows(&delta),
        vec![(1, None, 50, 1), (1, Some("west".to_string()), 50, -1),]
    );
}

#[tokio::test]
async fn left_anti_join_uses_slate_backed_columnar_operator_semantics() {
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
    let table = build_operator_state_table("vectorized-columnar-left-anti-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, o.amount \
        FROM orders o LEFT ANTI JOIN customers c ON o.customer_id = c.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_unmatched_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
        .get("mv_unmatched_orders")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(3, 70)]);

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(StringArray::from(vec!["north"])),
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert!(snapshot.iter().all(|batch| batch.num_rows() == 0));
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(3, 70, -1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_unmatched_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_unmatched_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert!(recovered_snapshot.iter().all(|batch| batch.num_rows() == 0));
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

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
    recovered.run_tick(4).await.expect("right retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 50)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 50, 1)]);
}

#[tokio::test]
async fn right_semi_join_uses_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(Int64Array::from(vec![50, 60])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(StringArray::from(vec!["west", "east", "north"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-right-semi-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let query = "SELECT c.id, c.region \
        FROM orders o RIGHT SEMI JOIN customers c ON o.customer_id = c.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_matched_customers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
        .get("mv_matched_customers")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(10, "west".to_string()), (11, "east".to_string())]
    );

    let order_insert = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(Int64Array::from(vec![70])),
        ],
    )
    .expect("order insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![order_insert.clone()],
            vec![order_insert],
        )
        .await
        .expect("append order insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![
            (10, "west".to_string()),
            (11, "east".to_string()),
            (12, "north".to_string()),
        ]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(12, "north".to_string(), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_matched_customers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_matched_customers")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![
            (10, "west".to_string()),
            (11, "east".to_string()),
            (12, "north".to_string()),
        ]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let order_retract = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("order retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&orders_schema)
        .expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted retract");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(11, "east".to_string()), (12, "north".to_string())]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(10, "west".to_string(), -1)]
    );
}

#[tokio::test]
async fn right_anti_join_uses_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(Int64Array::from(vec![50, 60])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(StringArray::from(vec!["west", "east", "north"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-right-anti-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let query = "SELECT c.id, c.region \
        FROM orders o RIGHT ANTI JOIN customers c ON o.customer_id = c.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_unmatched_customers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
        .get("mv_unmatched_customers")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_note_rows(&snapshot), vec![(12, "north".to_string())]);

    let order_insert = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(Int64Array::from(vec![70])),
        ],
    )
    .expect("order insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![order_insert.clone()],
            vec![order_insert],
        )
        .await
        .expect("append order insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert!(snapshot.iter().all(|batch| batch.num_rows() == 0));
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(12, "north".to_string(), -1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_unmatched_customers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_unmatched_customers")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert!(recovered_snapshot.iter().all(|batch| batch.num_rows() == 0));
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let order_retract = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("order retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&orders_schema)
        .expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted retract");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_note_rows(&snapshot), vec![(10, "west".to_string())]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(10, "west".to_string(), 1)]
    );
}

#[tokio::test]
async fn full_outer_join_uses_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![10, 11, 99])),
            Arc::new(Int64Array::from(vec![50, 60, 70])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 12])),
            Arc::new(StringArray::from(vec!["west", "north"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-full-outer-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o FULL OUTER JOIN customers c ON o.customer_id = c.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_all_customer_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
        .get("mv_all_customer_orders")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        full_outer_join_rows(&snapshot),
        vec![
            (None, Some("north".to_string()), None),
            (Some(1), Some("west".to_string()), Some(50)),
            (Some(2), None, Some(60)),
            (Some(3), None, Some(70)),
        ]
    );

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(StringArray::from(vec!["east"])),
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        full_outer_join_rows(&snapshot),
        vec![
            (None, Some("north".to_string()), None),
            (Some(1), Some("west".to_string()), Some(50)),
            (Some(2), Some("east".to_string()), Some(60)),
            (Some(3), None, Some(70)),
        ]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_full_outer_join_rows(&delta),
        vec![
            (Some(2), None, Some(60), -1),
            (Some(2), Some("east".to_string()), Some(60), 1),
        ]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_all_customer_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_all_customer_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        full_outer_join_rows(&recovered_snapshot),
        vec![
            (None, Some("north".to_string()), None),
            (Some(1), Some("west".to_string()), Some(50)),
            (Some(2), Some("east".to_string()), Some(60)),
            (Some(3), None, Some(70)),
        ]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

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
        .expect("weighted retract");
    recovered
        .apply_weighted_source_delta("customers", weighted)
        .await
        .expect("apply customer retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(
        full_outer_join_rows(&snapshot),
        vec![
            (None, Some("north".to_string()), None),
            (Some(1), None, Some(50)),
            (Some(2), Some("east".to_string()), Some(60)),
            (Some(3), None, Some(70)),
        ]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_full_outer_join_rows(&delta),
        vec![
            (Some(1), None, Some(50), 1),
            (Some(1), Some("west".to_string()), Some(50), -1),
        ]
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_customer_amount_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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
async fn three_way_join_uses_slate_backed_columnar_multijoin_operator_semantics() {
    let people = SourceDefinition::new(
        "people",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("name", SourceDataType::Utf8, false),
        ],
    )
    .expect("people source definition");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
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
    let people_schema = people.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let bids_schema = bids.to_arrow_schema();
    let initial_people = RecordBatch::try_new(
        Arc::clone(&people_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alice", "bob"])),
        ],
    )
    .expect("initial people batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 102])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(people);
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-three-way-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT p.id AS person_id, b.price \
        FROM people p \
        JOIN auctions a ON p.id = a.seller \
        JOIN bids b ON a.id = b.auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_person_bid_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarMultiJoin
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "people",
            vec![initial_people.clone()],
            vec![initial_people],
        )
        .await
        .expect("append initial people");
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
        .get("mv_person_bid_prices")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![101])),
            Arc::new(Int64Array::from(vec![30])),
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
    runtime.run_tick(2).await.expect("bid insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 30)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(2, 30, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_person_bid_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarMultiJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_person_bid_prices")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 10), (2, 30)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let auction_retract = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .expect("auction retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&auctions_schema)
        .expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&auction_retract, &weighted_schema, &[-1])
        .expect("weighted auction retract");
    recovered
        .apply_weighted_source_delta("auctions", weighted)
        .await
        .expect("apply auction retract");
    recovered.run_tick(4).await.expect("auction retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 30)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 10, -1)]);
}

#[tokio::test]
async fn join_over_three_way_join_uses_slate_backed_columnar_join_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let people = SourceDefinition::new(
        "people",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Int64, false),
        ],
    )
    .expect("people source definition");
    let regions = SourceDefinition::new(
        "regions",
        vec![
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("tier", SourceDataType::Int64, false),
        ],
    )
    .expect("regions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let people_schema = people.to_arrow_schema();
    let regions_schema = regions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .expect("initial auctions batch");
    let initial_people = RecordBatch::try_new(
        Arc::clone(&people_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1000, 2000])),
        ],
    )
    .expect("initial people batch");
    let initial_regions = RecordBatch::try_new(
        Arc::clone(&regions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial regions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    sources.register(people);
    sources.register(regions);
    let table = build_operator_state_table("vectorized-columnar-join-over-three-way-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("tier", DataType::Int64, false),
    ]));
    let query = "SELECT j.auction, r.tier \
        FROM (\
            SELECT b.auction, a.seller, p.region \
            FROM bids b \
            JOIN auctions a ON b.auction = a.id \
            JOIN people p ON a.seller = p.id\
        ) j \
        JOIN regions r ON j.seller = r.seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_over_three_way_join",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
    runtime
        .append_source_batches_for_execution_and_query(
            "people",
            vec![initial_people.clone()],
            vec![initial_people],
        )
        .await
        .expect("append initial people");
    runtime
        .append_source_batches_for_execution_and_query(
            "regions",
            vec![initial_regions.clone()],
            vec![initial_regions],
        )
        .await
        .expect("append initial regions");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_join_over_three_way_join")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(100, 10)]);

    let auction_insert = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![101])),
            Arc::new(Int64Array::from(vec![2])),
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(100, 10), (101, 20)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(101, 20, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_over_three_way_join",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "snapshot_diff");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_join_over_three_way_join")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(100, 10), (101, 20)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn ordered_three_way_join_uses_slate_backed_columnar_multijoin_operator_semantics() {
    let people = SourceDefinition::new(
        "people",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("name", SourceDataType::Utf8, false),
        ],
    )
    .expect("people source definition");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
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
    let people_schema = people.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let bids_schema = bids.to_arrow_schema();
    let initial_people = RecordBatch::try_new(
        Arc::clone(&people_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alice", "bob"])),
        ],
    )
    .expect("initial people batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 102])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(people);
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-ordered-three-way-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT p.id AS person_id, b.price \
        FROM people p \
        JOIN auctions a ON p.id = a.seller \
        JOIN bids b ON a.id = b.auction \
        ORDER BY person_id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_ordered_person_bid_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarMultiJoin
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "people",
            vec![initial_people.clone()],
            vec![initial_people],
        )
        .await
        .expect("append initial people");
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
        .get("mv_ordered_person_bid_prices")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![101])),
            Arc::new(Int64Array::from(vec![30])),
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
    runtime.run_tick(2).await.expect("bid insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 30)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(2, 30, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_ordered_person_bid_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarMultiJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_ordered_person_bid_prices")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 10), (2, 30)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn topn_over_three_way_join_uses_slate_backed_columnar_multijoin_operator_semantics() {
    let people = SourceDefinition::new(
        "people",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("name", SourceDataType::Utf8, false),
        ],
    )
    .expect("people source definition");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
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
    let people_schema = people.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let bids_schema = bids.to_arrow_schema();
    let initial_people = RecordBatch::try_new(
        Arc::clone(&people_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .expect("initial people batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102])),
            Arc::new(Int64Array::from(vec![10, 30, 20])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(people);
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-topn-three-way-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT p.id AS person_id, b.price \
        FROM people p \
        JOIN auctions a ON p.id = a.seller \
        JOIN bids b ON a.id = b.auction \
        ORDER BY b.price DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_top_person_bid_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarMultiJoin
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "people",
            vec![initial_people.clone()],
            vec![initial_people],
        )
        .await
        .expect("append initial people");
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
        .get("mv_top_person_bid_prices")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 30), (3, 20)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![100])),
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
    runtime.run_tick(2).await.expect("bid insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 40), (2, 30)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 40, 1), (3, 20, -1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_top_person_bid_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarMultiJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_person_bid_prices")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 40), (2, 30)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn self_join_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let schema = orders.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial orders batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    let table = build_operator_state_table("vectorized-columnar-self-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("left_amount", DataType::Int64, false),
        Field::new("right_amount", DataType::Int64, false),
    ]));
    let query = "SELECT l.id, l.amount AS left_amount, r.amount AS right_amount \
        FROM orders l JOIN orders r ON l.id = r.id \
        WHERE l.amount < r.amount";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_pairs",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial orders");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_pairs").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(self_join_rows(&snapshot), vec![(1, 10, 20)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("order insert batch");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append order insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        self_join_rows(&snapshot),
        vec![(1, 10, 20), (1, 10, 30), (1, 20, 30)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_self_join_rows(&delta),
        vec![(1, 10, 30, 1), (1, 20, 30, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_pairs",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_pairs")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        self_join_rows(&recovered_snapshot),
        vec![(1, 10, 20), (1, 10, 30), (1, 20, 30)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("order retract batch");
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
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(self_join_rows(&snapshot), vec![(1, 10, 30)]);
    assert_eq!(
        weighted_self_join_rows(&delta),
        vec![(1, 10, 20, -1), (1, 20, 30, -1)]
    );
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
    .expect("auction source definition");
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
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
            Arc::new(TimestampMillisecondArray::from(vec![20, 15, 25])),
            Arc::new(StringArray::from(vec![
                "bid-extra-10",
                "bid-extra-11",
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
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.\"dateTime\" ASC) AS rownum \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
        WHERE rownum <= 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 11, 200), (2, 12, 50)]);

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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 13, 300), (2, 12, 50)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bid")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 11, 200), (2, 12, 50)]);
}

#[tokio::test]
async fn join_top_avg_uses_slate_backed_columnar_operator_incrementally() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![10, 10])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 100])),
            Arc::new(Int64Array::from(vec![101, 101])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
            Arc::new(TimestampMillisecondArray::from(vec![20, 15, 25])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-join-top-avg").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("moving_avg_price", DataType::Float64, true),
    ]));
    let query = "SELECT seller, AVG(price) AS moving_avg_price \
        FROM (SELECT a.seller, b.price, b.\"dateTime\", \
        ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
        WHERE rownum <= 1 GROUP BY seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_seller_avg",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopAvg
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

    let handle = registry.get("mv_seller_avg").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(top_avg_rows(&snapshot), vec![(101, 125.0)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(TimestampMillisecondArray::from(vec![30])),
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
    assert_eq!(top_avg_rows(&snapshot), vec![(101, 175.0)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_top_avg_rows(&delta),
        vec![(101, 125.0, -1), (101, 175.0, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_seller_avg",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopAvg
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_seller_avg")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(top_avg_rows(&recovered_snapshot), vec![(101, 175.0)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(TimestampMillisecondArray::from(vec![30])),
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
    assert_eq!(top_avg_rows(&snapshot), vec![(101, 125.0)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_top_avg_rows(&delta),
        vec![(101, 175.0, -1), (101, 125.0, 1)]
    );
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bids").expect("materialized view");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bids")
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
            VectorizedMaterializedViewPlan::new(
                "mv_filtered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        runtime.materialized_views[1].execution_mode,
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
        id_count_rows(&partitioned_handle.arrow_snapshot_for(1).expect("snapshot")),
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
        id_count_rows(&partitioned_handle.arrow_snapshot_for(2).expect("snapshot")),
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
            VectorizedMaterializedViewPlan::new(
                "mv_filtered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        recovered.materialized_views[1].execution_mode,
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
            &recovered_partitioned
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
            &recovered_partitioned
                .arrow_snapshot_for(4)
                .expect("snapshot")
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
            VectorizedMaterializedViewPlan::new(
                "mv_ordered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        runtime.materialized_views[1].execution_mode,
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
            VectorizedMaterializedViewPlan::new(
                "mv_ordered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        recovered.materialized_views[1].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
            VectorizedMaterializedViewPlan::new(
                "mv_reversed_ranked_bids",
                reversed_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
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
            .all(|mv| mv.execution_mode == MaterializedViewExecutionMode::ColumnarTopN)
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
    assert_eq!(
        bid_topn_rows(&reversed.arrow_snapshot_for(1).expect("reversed snapshot")),
        vec![(1, 20, 20), (1, 30, 30), (2, 40, 15), (2, 50, 5)]
    );
    assert_eq!(
        bid_topn_rows(&equality.arrow_snapshot_for(1).expect("equality snapshot")),
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
    assert_eq!(
        bid_topn_rows(&reversed.arrow_snapshot_for(2).expect("reversed snapshot")),
        expected_snapshot
    );
    assert_eq!(
        bid_topn_rows(&equality.arrow_snapshot_for(2).expect("equality snapshot")),
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bids").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
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
        vec![VectorizedMaterializedViewPlan::new(
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
        vec![VectorizedMaterializedViewPlan::new(
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
        vec![VectorizedMaterializedViewPlan::new(
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
        vec![VectorizedMaterializedViewPlan::new(
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
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
async fn asof_join_uses_slate_backed_columnar_operator_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 500])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(TimestampMillisecondArray::from(vec![800, 950, 700])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-asof-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Int64, true),
    ]));
    let query = "SELECT a.id, b.price \
        FROM auction a ASOF JOIN bid b \
        MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") \
        ON a.id = b.auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_asof_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarAsofJoin
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

    let handle = registry.get("mv_asof_prices").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(asof_rows(&snapshot), vec![(1, Some(20)), (2, None)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![40])),
            Arc::new(TimestampMillisecondArray::from(vec![400])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![bid_insert.clone()],
            vec![bid_insert],
        )
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(asof_rows(&snapshot), vec![(1, Some(20)), (2, Some(40))]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_asof_rows(&delta),
        vec![(2, None, -1), (2, Some(40), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_asof_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarAsofJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_asof_prices")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        asof_rows(&recovered_snapshot),
        vec![(1, Some(20)), (2, Some(40))]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn asof_join_without_equi_keys_uses_slate_backed_columnar_operator_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 500])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(TimestampMillisecondArray::from(vec![400, 800])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-asof-join-no-keys").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Int64, true),
    ]));
    let query = "SELECT a.id, b.price \
        FROM auction a ASOF JOIN bid b \
        MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\")";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_asof_global_prices",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarAsofJoin
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

    let handle = registry
        .get("mv_asof_global_prices")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(asof_rows(&snapshot), vec![(1, Some(20)), (2, Some(10))]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(TimestampMillisecondArray::from(vec![900])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![bid_insert.clone()],
            vec![bid_insert],
        )
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(asof_rows(&snapshot), vec![(1, Some(30)), (2, Some(10))]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_asof_rows(&delta),
        vec![(1, Some(20), -1), (1, Some(30), 1)]
    );
}

#[tokio::test]
async fn range_join_uses_slate_backed_columnar_operator_semantics() {
    let windows = SourceDefinition::new(
        "windows",
        vec![
            SourceColumn::new_nullable("window_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("start_ts", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("end_ts", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("windows source definition");
    let events = SourceDefinition::new(
        "events",
        vec![
            SourceColumn::new_nullable("event_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("event_ts", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("events source definition");
    let windows_schema = windows.to_arrow_schema();
    let events_schema = events.to_arrow_schema();
    let initial_windows = RecordBatch::try_new(
        Arc::clone(&windows_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from(vec![200, 300])),
        ],
    )
    .expect("initial windows batch");
    let initial_events = RecordBatch::try_new(
        Arc::clone(&events_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(TimestampMillisecondArray::from(vec![150, 250, 300])),
        ],
    )
    .expect("initial events batch");

    let mut sources = SourceRegistry::new();
    sources.register(windows);
    sources.register(events);
    let table = build_operator_state_table("vectorized-columnar-range-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("window_id", DataType::Int64, false),
        Field::new("event_id", DataType::Int64, false),
    ]));
    let query = "SELECT w.window_id, e.event_id \
        FROM windows w JOIN events e \
        ON e.event_ts >= w.start_ts AND e.event_ts < w.end_ts";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_window_events",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "windows",
            vec![initial_windows.clone()],
            vec![initial_windows],
        )
        .await
        .expect("append initial windows");
    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial_events.clone()],
            vec![initial_events],
        )
        .await
        .expect("append initial events");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_window_events").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 11)]);

    let event_insert = RecordBatch::try_new(
        Arc::clone(&events_schema),
        vec![
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(TimestampMillisecondArray::from(vec![199])),
        ],
    )
    .expect("event insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![event_insert.clone()],
            vec![event_insert],
        )
        .await
        .expect("append event insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (1, 13), (2, 11)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 13, 1)]);
}

#[tokio::test]
async fn aggregate_over_self_join_uses_slate_backed_columnar_operator_semantics() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let schema = orders.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial orders batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    let table = build_operator_state_table("vectorized-columnar-self-join-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("pair_count", DataType::Int64, false),
    ]));
    let query = "SELECT l.id, COUNT(*) AS pair_count \
        FROM orders l JOIN orders r ON l.id = r.id \
        WHERE l.amount < r.amount \
        GROUP BY l.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_pair_counts",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarSelfJoinAggregate
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial orders");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_order_pair_counts")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("order insert batch");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append order insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 3)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 3, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_pair_counts",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarSelfJoinAggregate
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_pair_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 3)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("order retract batch");
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
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, 1), (1, 3, -1)]);
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinAggregate
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
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinAggregate
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
        vec![VectorizedMaterializedViewPlan::new(
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
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarUnionAggregate
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
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 3), (2, 2), (3, 1)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(2, 1, -1), (2, 2, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
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
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarUnionAggregate
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_key_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (2, 2), (3, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 2, 1), (1, 3, -1)]);
}

#[tokio::test]
async fn union_join_uses_slate_backed_columnar_operator_semantics() {
    let persons = SourceDefinition::new(
        "persons",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("name", SourceDataType::Utf8, false),
        ],
    )
    .expect("persons source definition");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let persons_schema = persons.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let bids_schema = bids.to_arrow_schema();
    let initial_persons = RecordBatch::try_new(
        Arc::clone(&persons_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "cara"])),
        ],
    )
    .expect("initial persons batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![100, 120])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(persons);
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-union-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let query = "SELECT u.key, p.name \
        FROM (SELECT seller AS key FROM auctions UNION ALL SELECT bidder AS key FROM bids) u \
        JOIN persons p ON u.key = p.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_union_person_names",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

    runtime
        .append_source_batches_for_execution_and_query(
            "persons",
            vec![initial_persons.clone()],
            vec![initial_persons],
        )
        .await
        .expect("append initial persons");
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
        .get("mv_union_person_names")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![
            (1, "alice".to_string()),
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "cara".to_string()),
        ]
    );

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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![
            (1, "alice".to_string()),
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (2, "bob".to_string()),
            (3, "cara".to_string()),
        ]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(2, "bob".to_string(), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_union_person_names",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "snapshot_diff");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_person_names")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![
            (1, "alice".to_string()),
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (2, "bob".to_string()),
            (3, "cara".to_string()),
        ]
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

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (2, "bob".to_string()),
            (3, "cara".to_string()),
        ]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(1, "alice".to_string(), -1)]
    );
}

#[tokio::test]
async fn aggregate_join_uses_slate_backed_columnar_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 150, 80])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-aggregate-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("max_price", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let query = "SELECT m.auction, m.max_price, a.seller \
        FROM (SELECT auction, MAX(price) AS max_price FROM bids GROUP BY auction) m \
        JOIN auctions a ON m.auction = a.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_auction_max_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarAggregateJoin
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
        .get("mv_auction_max_sellers")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(self_join_rows(&snapshot), vec![(1, 150, 10), (2, 80, 20)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![200])),
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
    assert_eq!(self_join_rows(&snapshot), vec![(1, 150, 10), (2, 200, 20)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_self_join_rows(&delta),
        vec![(2, 80, 20, -1), (2, 200, 20, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_auction_max_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarAggregateJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_auction_max_sellers")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        self_join_rows(&recovered_snapshot),
        vec![(1, 150, 10), (2, 200, 20)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![150])),
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
    assert_eq!(self_join_rows(&snapshot), vec![(1, 100, 10), (2, 200, 20)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_self_join_rows(&delta),
        vec![(1, 100, 10, 1), (1, 150, 10, -1)]
    );
}

#[tokio::test]
async fn distinct_join_uses_slate_backed_columnar_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
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
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-distinct-join-composed").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let query = "SELECT d.auction, a.seller \
        FROM (SELECT DISTINCT auction FROM bids) d \
        JOIN auctions a ON d.auction = a.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_distinct_auction_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctJoin
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
        .get("mv_distinct_auction_sellers")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 20)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
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
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 20), (3, 30)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(3, 30, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_distinct_auction_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_distinct_auction_sellers")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 10), (2, 20), (3, 30)]
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
    recovered.run_tick(4).await.expect("first retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-first-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 20), (3, 30)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-first-retract delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![110])),
        ],
    )
    .expect("second bid retract batch");
    let weighted =
        weighted_batch_from_diffs(&bid_retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply second weighted retract");
    recovered.run_tick(5).await.expect("second retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(5)
        .expect("post-second-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20), (3, 30)]);
    let delta = recovered_handle
        .arrow_delta_for(5)
        .expect("post-second-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 10, -1)]);
}

#[tokio::test]
async fn intersect_except_use_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![1, 1, 2, 3, 2])),
            Arc::new(Int64Array::from(vec![90, 100, 110, 120, 130])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-intersect-except").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let intersect_query = "SELECT auction, price FROM bids WHERE price >= 100 \
        INTERSECT SELECT auction, price FROM bids WHERE auction <= 2";
    let except_query = "SELECT auction, price FROM bids WHERE price >= 100 \
        EXCEPT SELECT auction, price FROM bids WHERE auction = 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new(
                "mv_intersect_bids",
                intersect_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_except_bids",
                except_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctJoin
    );
    assert_eq!(
        runtime.materialized_views[1].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctJoin
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

    let intersect_handle = registry
        .get("mv_intersect_bids")
        .expect("intersect materialized view");
    let intersect_snapshot = intersect_handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        id_count_rows(&intersect_snapshot),
        vec![(1, 100), (2, 110), (2, 130)]
    );

    let except_handle = registry
        .get("mv_except_bids")
        .expect("except materialized view");
    let except_snapshot = except_handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&except_snapshot), vec![(1, 100), (3, 120)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new(
                "mv_intersect_bids",
                intersect_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_except_bids",
                except_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctJoin
    );
    assert_eq!(
        recovered.materialized_views[1].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctJoin
    );
    recovered.run_tick(2).await.expect("recovered tick");

    let recovered_intersect = recovery_registry
        .get("mv_intersect_bids")
        .expect("recovered intersect materialized view");
    let recovered_snapshot = recovered_intersect
        .arrow_snapshot_for(2)
        .expect("recovered intersect snapshot");
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 100), (2, 110), (2, 130)]
    );
    let recovered_delta = recovered_intersect
        .arrow_delta_for(2)
        .expect("recovered empty intersect delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let recovered_except = recovery_registry
        .get("mv_except_bids")
        .expect("recovered except materialized view");
    let recovered_snapshot = recovered_except
        .arrow_snapshot_for(2)
        .expect("recovered except snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 100), (3, 120)]);
    let recovered_delta = recovered_except
        .arrow_delta_for(2)
        .expect("recovered empty except delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(Int64Array::from(vec![110, 120])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1, -1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(3).await.expect("retract tick");

    let intersect_snapshot = recovered_intersect
        .arrow_snapshot_for(3)
        .expect("intersect snapshot after retract");
    assert_eq!(id_count_rows(&intersect_snapshot), vec![(1, 100), (2, 130)]);
    let intersect_delta = recovered_intersect
        .arrow_delta_for(3)
        .expect("intersect delta after retract");
    assert_eq!(weighted_id_count_rows(&intersect_delta), vec![(2, 110, -1)]);

    let except_snapshot = recovered_except
        .arrow_snapshot_for(3)
        .expect("except snapshot after retract");
    assert_eq!(id_count_rows(&except_snapshot), vec![(1, 100)]);
    let except_delta = recovered_except
        .arrow_delta_for(3)
        .expect("except delta after retract");
    assert_eq!(weighted_id_count_rows(&except_delta), vec![(3, 120, -1)]);
}

#[tokio::test]
async fn subquery_predicates_use_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )
    .expect("initial bids batch");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("auctions source definition");
    let auctions_schema = auctions.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![Arc::new(Int64Array::from(vec![1, 3]))],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-subquery").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let in_query = "SELECT auction, price FROM bids WHERE auction IN (SELECT id FROM auctions)";
    let not_exists_query = "SELECT b.auction, b.price FROM bids b \
        WHERE NOT EXISTS (SELECT 1 FROM auctions a WHERE a.id = b.auction)";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new("mv_in_bids", in_query, Arc::clone(&output_schema)),
            VectorizedMaterializedViewPlan::new(
                "mv_not_exists_bids",
                not_exists_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
    );
    assert_eq!(
        runtime.materialized_views[1].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
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

    let in_handle = registry.get("mv_in_bids").expect("IN materialized view");
    assert_eq!(
        id_count_rows(&in_handle.arrow_snapshot_for(1).expect("IN snapshot")),
        vec![(1, 100), (3, 300)]
    );
    let not_exists_handle = registry
        .get("mv_not_exists_bids")
        .expect("NOT EXISTS materialized view");
    assert_eq!(
        id_count_rows(
            &not_exists_handle
                .arrow_snapshot_for(1)
                .expect("NOT EXISTS snapshot")
        ),
        vec![(2, 200)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new("mv_in_bids", in_query, Arc::clone(&output_schema)),
            VectorizedMaterializedViewPlan::new(
                "mv_not_exists_bids",
                not_exists_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
    );
    assert_eq!(
        recovered.materialized_views[1].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
    );
    recovered.run_tick(2).await.expect("recovered tick");

    let recovered_in = recovery_registry
        .get("mv_in_bids")
        .expect("recovered IN materialized view");
    assert_eq!(
        id_count_rows(&recovered_in.arrow_snapshot_for(2).expect("IN snapshot")),
        vec![(1, 100), (3, 300)]
    );
    let recovered_not_exists = recovery_registry
        .get("mv_not_exists_bids")
        .expect("recovered NOT EXISTS materialized view");
    assert_eq!(
        id_count_rows(
            &recovered_not_exists
                .arrow_snapshot_for(2)
                .expect("NOT EXISTS snapshot")
        ),
        vec![(2, 200)]
    );
    assert!(
        recovered_in
            .arrow_delta_for(2)
            .expect("recovered empty IN delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
    assert!(
        recovered_not_exists
            .arrow_delta_for(2)
            .expect("recovered empty NOT EXISTS delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );

    let auction_retract = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![Arc::new(Int64Array::from(vec![3]))],
    )
    .expect("auction retract batch");
    let auctions_weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&auctions_schema)
            .expect("weighted auctions schema");
    let weighted = weighted_batch_from_diffs(&auction_retract, &auctions_weighted_schema, &[-1])
        .expect("weighted auction retract");
    recovered
        .apply_weighted_source_delta("auctions", weighted)
        .await
        .expect("apply weighted auction retract");
    recovered.run_tick(3).await.expect("auction retract tick");

    assert_eq!(
        id_count_rows(&recovered_in.arrow_snapshot_for(3).expect("IN snapshot")),
        vec![(1, 100)]
    );
    assert_eq!(
        weighted_id_count_rows(&recovered_in.arrow_delta_for(3).expect("IN delta")),
        vec![(3, 300, -1)]
    );
    assert_eq!(
        id_count_rows(
            &recovered_not_exists
                .arrow_snapshot_for(3)
                .expect("NOT EXISTS snapshot")
        ),
        vec![(2, 200), (3, 300)]
    );
    assert_eq!(
        weighted_id_count_rows(
            &recovered_not_exists
                .arrow_delta_for(3)
                .expect("NOT EXISTS delta")
        ),
        vec![(3, 300, 1)]
    );

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .expect("bid retract batch");
    let bids_weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted bids schema");
    let weighted = weighted_batch_from_diffs(&bid_retract, &bids_weighted_schema, &[-1])
        .expect("weighted bid retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("bid retract tick");

    assert!(id_count_rows(&recovered_in.arrow_snapshot_for(4).expect("IN snapshot")).is_empty());
    assert_eq!(
        weighted_id_count_rows(&recovered_in.arrow_delta_for(4).expect("IN delta")),
        vec![(1, 100, -1)]
    );
    assert_eq!(
        id_count_rows(
            &recovered_not_exists
                .arrow_snapshot_for(4)
                .expect("NOT EXISTS snapshot")
        ),
        vec![(2, 200), (3, 300)]
    );
    assert!(
        recovered_not_exists
            .arrow_delta_for(4)
            .expect("NOT EXISTS empty delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
}

#[tokio::test]
async fn scalar_subquery_predicate_uses_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )
    .expect("initial bids batch");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![SourceColumn::new_nullable(
            "initial_bid",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("auctions source definition");
    let auctions_schema = auctions.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![Arc::new(Int64Array::from(vec![150, 250]))],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-scalar-subquery").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, price FROM bids \
        WHERE price > (SELECT MIN(initial_bid) FROM auctions)";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_scalar_subquery_bids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
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
        .get("mv_scalar_subquery_bids")
        .expect("scalar subquery materialized view");
    assert_eq!(
        id_count_rows(&handle.arrow_snapshot_for(1).expect("initial snapshot")),
        vec![(2, 200), (3, 300)]
    );

    let lower_floor = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![Arc::new(Int64Array::from(vec![50]))],
    )
    .expect("lower scalar floor batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "auctions",
            vec![lower_floor.clone()],
            vec![lower_floor],
        )
        .await
        .expect("append auction row");
    runtime.run_tick(2).await.expect("scalar update tick");

    assert_eq!(
        id_count_rows(&handle.arrow_snapshot_for(2).expect("updated snapshot")),
        vec![(1, 100), (2, 200), (3, 300)]
    );
    assert_eq!(
        weighted_id_count_rows(&handle.arrow_delta_for(2).expect("updated delta")),
        vec![(1, 100, 1)]
    );
}

#[tokio::test]
async fn distinct_and_aggregate_in_subqueries_use_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )
    .expect("initial bids batch");
    let sellers = SourceDefinition::new(
        "auction_sellers",
        vec![SourceColumn::new_nullable(
            "seller",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("auction sellers source definition");
    let sellers_schema = sellers.to_arrow_schema();
    let initial_sellers = RecordBatch::try_new(
        Arc::clone(&sellers_schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 3]))],
    )
    .expect("initial sellers batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(sellers);
    let table = build_operator_state_table("vectorized-columnar-subquery-distinct-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let distinct_query = "SELECT auction, price FROM bids \
        WHERE auction IN (SELECT DISTINCT seller FROM auction_sellers)";
    let aggregate_query = "SELECT auction, price FROM bids \
        WHERE auction IN (SELECT seller FROM auction_sellers GROUP BY seller)";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new(
                "mv_distinct_in_bids",
                distinct_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_aggregate_in_bids",
                aggregate_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
    );
    assert_eq!(
        runtime.materialized_views[1].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
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
            "auction_sellers",
            vec![initial_sellers.clone()],
            vec![initial_sellers],
        )
        .await
        .expect("append initial sellers");
    runtime.run_tick(1).await.expect("initial tick");

    let distinct_handle = registry
        .get("mv_distinct_in_bids")
        .expect("distinct IN materialized view");
    assert_eq!(
        id_count_rows(
            &distinct_handle
                .arrow_snapshot_for(1)
                .expect("distinct IN snapshot")
        ),
        vec![(1, 100), (3, 300)]
    );
    let aggregate_handle = registry
        .get("mv_aggregate_in_bids")
        .expect("aggregate IN materialized view");
    assert_eq!(
        id_count_rows(
            &aggregate_handle
                .arrow_snapshot_for(1)
                .expect("aggregate IN snapshot")
        ),
        vec![(1, 100), (3, 300)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new(
                "mv_distinct_in_bids",
                distinct_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_aggregate_in_bids",
                aggregate_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
    );
    assert_eq!(
        recovered.materialized_views[1].execution_mode,
        MaterializedViewExecutionMode::ColumnarSubquery
    );
    recovered.run_tick(2).await.expect("recovered tick");

    let recovered_distinct = recovery_registry
        .get("mv_distinct_in_bids")
        .expect("recovered distinct IN materialized view");
    let recovered_aggregate = recovery_registry
        .get("mv_aggregate_in_bids")
        .expect("recovered aggregate IN materialized view");
    assert_eq!(
        id_count_rows(
            &recovered_distinct
                .arrow_snapshot_for(2)
                .expect("distinct IN recovered snapshot")
        ),
        vec![(1, 100), (3, 300)]
    );
    assert_eq!(
        id_count_rows(
            &recovered_aggregate
                .arrow_snapshot_for(2)
                .expect("aggregate IN recovered snapshot")
        ),
        vec![(1, 100), (3, 300)]
    );

    let seller_retract = RecordBatch::try_new(
        Arc::clone(&sellers_schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("duplicate seller retract");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&sellers_schema)
        .expect("weighted sellers schema");
    let weighted = weighted_batch_from_diffs(&seller_retract, &weighted_schema, &[-1])
        .expect("weighted duplicate seller retract");
    recovered
        .apply_weighted_source_delta("auction_sellers", weighted)
        .await
        .expect("apply duplicate seller retract");
    recovered.run_tick(3).await.expect("duplicate retract tick");

    assert_eq!(
        id_count_rows(
            &recovered_distinct
                .arrow_snapshot_for(3)
                .expect("distinct IN duplicate-retract snapshot")
        ),
        vec![(1, 100), (3, 300)]
    );
    assert!(
        recovered_distinct
            .arrow_delta_for(3)
            .expect("distinct IN empty duplicate-retract delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
    assert_eq!(
        id_count_rows(
            &recovered_aggregate
                .arrow_snapshot_for(3)
                .expect("aggregate IN duplicate-retract snapshot")
        ),
        vec![(1, 100), (3, 300)]
    );
    assert!(
        recovered_aggregate
            .arrow_delta_for(3)
            .expect("aggregate IN empty duplicate-retract delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );

    let seller_retract = RecordBatch::try_new(
        Arc::clone(&sellers_schema),
        vec![Arc::new(Int64Array::from(vec![3]))],
    )
    .expect("seller retract");
    let weighted = weighted_batch_from_diffs(&seller_retract, &weighted_schema, &[-1])
        .expect("weighted seller retract");
    recovered
        .apply_weighted_source_delta("auction_sellers", weighted)
        .await
        .expect("apply seller retract");
    recovered.run_tick(4).await.expect("seller retract tick");

    assert_eq!(
        id_count_rows(
            &recovered_distinct
                .arrow_snapshot_for(4)
                .expect("distinct IN seller-retract snapshot")
        ),
        vec![(1, 100)]
    );
    assert_eq!(
        weighted_id_count_rows(
            &recovered_distinct
                .arrow_delta_for(4)
                .expect("distinct IN seller-retract delta")
        ),
        vec![(3, 300, -1)]
    );
    assert_eq!(
        id_count_rows(
            &recovered_aggregate
                .arrow_snapshot_for(4)
                .expect("aggregate IN seller-retract snapshot")
        ),
        vec![(1, 100)]
    );
    assert_eq!(
        weighted_id_count_rows(
            &recovered_aggregate
                .arrow_delta_for(4)
                .expect("aggregate IN seller-retract delta")
        ),
        vec![(3, 300, -1)]
    );
}

#[tokio::test]
async fn distinct_topn_uses_slate_backed_columnar_operator_semantics() {
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let schema = bids.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 3])),
            Arc::new(Int64Array::from(vec![100, 110, 120, 130])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-distinct-topn-composed").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT DISTINCT auction FROM bids ORDER BY auction DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_distinct_top_auctions",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_top_auctions")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![140])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_distinct_top_auctions",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_distinct_top_auctions")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![3, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![140])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
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
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, 1), (4, -1)]);
}

#[tokio::test]
async fn global_join_topn_uses_slate_backed_columnar_join_topn_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-composed-join-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let query = "SELECT b.auction, a.seller \
        FROM bids b JOIN auctions a ON b.auction = a.id \
        ORDER BY b.price DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_top_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
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
        .get("mv_join_top_sellers")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 20)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![300])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20), (3, 30)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 10, -1), (3, 30, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_top_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_join_top_sellers")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(2, 20), (3, 30)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![300])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 20)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 10, 1), (3, 30, -1)]
    );
}

#[tokio::test]
async fn filtered_ordered_global_join_topn_uses_slate_backed_columnar_join_topn_operator_semantics()
{
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![100, 200, 50, 300])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-filtered-join-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let query = "SELECT auction, seller FROM (\
            SELECT b.auction, a.seller, b.price \
            FROM bids b JOIN auctions a ON b.auction = a.id \
            ORDER BY b.price DESC LIMIT 2\
        ) top_bids \
        WHERE seller >= 20 \
        ORDER BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_filtered_join_top_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
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
        .get("mv_filtered_join_top_sellers")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20), (4, 40)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![500])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(3, 30), (4, 40)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(2, 20, -1), (3, 30, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_filtered_join_top_sellers",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_filtered_join_top_sellers")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(3, 30), (4, 40)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn join_over_global_join_topn_uses_slate_backed_columnar_join_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let sellers = SourceDefinition::new(
        "sellers",
        vec![
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Int64, false),
        ],
    )
    .expect("sellers source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let sellers_schema = sellers.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("initial auctions batch");
    let initial_sellers = RecordBatch::try_new(
        Arc::clone(&sellers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![1000, 2000, 3000])),
        ],
    )
    .expect("initial sellers batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    sources.register(sellers);
    let table = build_operator_state_table("vectorized-columnar-join-over-join-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
    ]));
    let query = "SELECT top_bids.auction, s.region \
        FROM (\
            SELECT b.auction, a.seller, b.price \
            FROM bids b JOIN auctions a ON b.auction = a.id \
            ORDER BY b.price DESC LIMIT 2\
        ) top_bids \
        JOIN sellers s ON top_bids.seller = s.seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_over_join_topn",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
    runtime
        .append_source_batches_for_execution_and_query(
            "sellers",
            vec![initial_sellers.clone()],
            vec![initial_sellers],
        )
        .await
        .expect("append initial sellers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_join_over_join_topn")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1000), (2, 2000)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![300])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 2000), (3, 3000)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 1000, -1), (3, 3000, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_over_join_topn",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "snapshot_diff");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_join_over_join_topn")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(2, 2000), (3, 3000)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn join_over_join_uses_slate_backed_columnar_join_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let sellers = SourceDefinition::new(
        "sellers",
        vec![
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Int64, false),
        ],
    )
    .expect("sellers source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let sellers_schema = sellers.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial auctions batch");
    let initial_sellers = RecordBatch::try_new(
        Arc::clone(&sellers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![1000, 2000, 3000])),
        ],
    )
    .expect("initial sellers batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    sources.register(sellers);
    let table = build_operator_state_table("vectorized-columnar-join-over-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
    ]));
    let query = "SELECT j.auction, s.region \
        FROM (\
            SELECT b.auction, a.seller \
            FROM bids b JOIN auctions a ON b.auction = a.id\
        ) j \
        JOIN sellers s ON j.seller = s.seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_over_join",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "snapshot_diff");

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
    runtime
        .append_source_batches_for_execution_and_query(
            "sellers",
            vec![initial_sellers.clone()],
            vec![initial_sellers],
        )
        .await
        .expect("append initial sellers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_join_over_join")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1000), (2, 2000)]);

    let auction_insert = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![30])),
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

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        id_count_rows(&snapshot),
        vec![(1, 1000), (2, 2000), (3, 3000)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(3, 3000, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_over_join",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "snapshot_diff");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_join_over_join")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 1000), (2, 2000), (3, 3000)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn global_outer_join_topn_uses_slate_backed_columnar_join_topn_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![10, 11, 99])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
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
    let table = build_operator_state_table("vectorized-columnar-outer-join-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o LEFT JOIN customers c ON o.customer_id = c.id \
        ORDER BY o.amount DESC LIMIT 3";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_outer_join_top_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
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
            "customers",
            vec![initial_customers.clone()],
            vec![initial_customers],
        )
        .await
        .expect("append initial customers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_outer_join_top_orders")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        outer_join_rows(&snapshot),
        vec![
            (1, Some("west".to_string()), 100),
            (2, Some("east".to_string()), 200),
            (3, None, 300),
        ]
    );

    let new_customer = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![99])),
            Arc::new(StringArray::from(vec!["north"])),
        ],
    )
    .expect("new customer batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![new_customer.clone()],
            vec![new_customer],
        )
        .await
        .expect("append customer insert");
    runtime.run_tick(2).await.expect("customer insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        outer_join_rows(&snapshot),
        vec![
            (1, Some("west".to_string()), 100),
            (2, Some("east".to_string()), 200),
            (3, Some("north".to_string()), 300),
        ]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_outer_join_rows(&delta),
        vec![(3, None, 300, -1), (3, Some("north".to_string()), 300, 1),]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_outer_join_top_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_outer_join_top_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        outer_join_rows(&recovered_snapshot),
        vec![
            (1, Some("west".to_string()), 100),
            (2, Some("east".to_string()), 200),
            (3, Some("north".to_string()), 300),
        ]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn union_topn_uses_slate_backed_columnar_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![100, 110])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-union-topn-composed").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let query = "SELECT key \
        FROM (SELECT auction AS key FROM bids UNION ALL SELECT id AS key FROM auctions) u \
        ORDER BY key DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_union_top_keys",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarUnionTopN
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
        .get("mv_union_top_keys")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);

    let insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![140])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_union_top_keys",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarUnionTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_top_keys")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![3, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![140])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
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
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, 1), (4, -1)]);
}

#[tokio::test]
async fn reversed_composed_shapes_use_slate_backed_columnar_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 90, 200, 50])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-reversed-composed").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let join_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let key_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let bid_pair_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let sum_schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]));
    let grand_total_schema = Arc::new(Schema::new(vec![Field::new(
        "grand_total",
        DataType::Int64,
        true,
    )]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new(
                "mv_distinct_over_join",
                "SELECT DISTINCT b.auction, a.seller \
                    FROM bids b JOIN auctions a ON b.auction = a.id",
                Arc::clone(&join_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_join_over_topn",
                "SELECT t.auction, a.seller \
                    FROM (SELECT auction, price FROM bids ORDER BY price DESC LIMIT 2) t \
                    JOIN auctions a ON t.auction = a.id",
                Arc::clone(&join_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_aggregate_over_topn",
                "SELECT SUM(price) AS total \
                    FROM (SELECT auction, price FROM bids ORDER BY price DESC LIMIT 2) t",
                Arc::clone(&sum_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_union_over_topn",
                "SELECT key \
                    FROM (SELECT key FROM (SELECT auction AS key, price FROM bids ORDER BY price DESC LIMIT 2) t \
                    UNION ALL SELECT id AS key FROM auctions) u",
                Arc::clone(&key_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_union_over_distinct",
                "SELECT key \
                    FROM (SELECT DISTINCT auction AS key FROM bids \
                    UNION ALL SELECT id AS key FROM auctions) u",
                Arc::clone(&key_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_union_over_aggregate",
                "SELECT key \
                    FROM (SELECT auction AS key FROM (SELECT auction, COUNT(*) AS c FROM bids GROUP BY auction) a \
                    UNION ALL SELECT id AS key FROM auctions) u",
                Arc::clone(&key_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_aggregate_over_aggregate",
                "SELECT SUM(total) AS grand_total \
                    FROM (SELECT auction, SUM(price) AS total FROM bids GROUP BY auction) a",
                Arc::clone(&grand_total_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_distinct_over_distinct",
                "SELECT DISTINCT key \
                    FROM (SELECT DISTINCT auction AS key, price AS value FROM bids) d",
                Arc::clone(&key_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_filter_over_topn",
                "SELECT auction, price \
                    FROM (SELECT auction, price FROM bids ORDER BY price DESC LIMIT 3) t \
                    WHERE price > 100",
                Arc::clone(&bid_pair_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_join_over_self_join",
                "SELECT j.auction, a.seller \
                    FROM (SELECT l.auction, r.price FROM bids l JOIN bids r ON l.auction = r.auction WHERE l.price < r.price) j \
                    JOIN auctions a ON j.auction = a.id",
                Arc::clone(&join_schema),
            ),
            VectorizedMaterializedViewPlan::new(
                "mv_topn_over_self_join",
                "SELECT auction, price \
                    FROM (SELECT l.auction, r.price FROM bids l JOIN bids r ON l.auction = r.auction WHERE l.price < r.price) j \
                    ORDER BY price DESC LIMIT 2",
                Arc::clone(&bid_pair_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctJoin
    );
    assert_eq!(
        runtime.materialized_views[1].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_eq!(
        runtime.materialized_views[2].execution_mode,
        MaterializedViewExecutionMode::ColumnarAggregateTopN
    );
    assert_eq!(
        runtime.materialized_views[3].execution_mode,
        MaterializedViewExecutionMode::ColumnarUnionTopN
    );
    assert_eq!(
        runtime.materialized_views[4].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinctUnion
    );
    assert_eq!(
        runtime.materialized_views[5].execution_mode,
        MaterializedViewExecutionMode::ColumnarUnionAggregate
    );
    assert_eq!(
        runtime.materialized_views[6].execution_mode,
        MaterializedViewExecutionMode::ColumnarAggregateAggregate
    );
    assert_eq!(
        runtime.materialized_views[7].execution_mode,
        MaterializedViewExecutionMode::ColumnarDistinct
    );
    assert_eq!(
        runtime.materialized_views[8].execution_mode,
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        runtime.materialized_views[9].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinJoin
    );
    assert_eq!(
        runtime.materialized_views[10].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoinTopN
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

    let distinct_over_join = registry
        .get("mv_distinct_over_join")
        .expect("distinct-over-join materialized view");
    assert_eq!(
        id_count_rows(&distinct_over_join.arrow_snapshot_for(1).expect("snapshot")),
        vec![(1, 10), (2, 20), (3, 30)]
    );

    let join_over_topn = registry
        .get("mv_join_over_topn")
        .expect("join-over-topn materialized view");
    assert_eq!(
        id_count_rows(&join_over_topn.arrow_snapshot_for(1).expect("snapshot")),
        vec![(1, 10), (2, 20)]
    );

    let aggregate_over_topn = registry
        .get("mv_aggregate_over_topn")
        .expect("aggregate-over-topn materialized view");
    assert_eq!(
        single_int_rows(&aggregate_over_topn.arrow_snapshot_for(1).expect("snapshot")),
        vec![300]
    );

    let union_over_topn = registry
        .get("mv_union_over_topn")
        .expect("union-over-topn materialized view");
    assert_eq!(
        single_int_rows(&union_over_topn.arrow_snapshot_for(1).expect("snapshot")),
        vec![1, 1, 2, 2, 3]
    );

    let union_over_aggregate = registry
        .get("mv_union_over_aggregate")
        .expect("union-over-aggregate materialized view");
    assert_eq!(
        single_int_rows(
            &union_over_aggregate
                .arrow_snapshot_for(1)
                .expect("snapshot")
        ),
        vec![1, 1, 2, 2, 3, 3]
    );

    let union_over_distinct = registry
        .get("mv_union_over_distinct")
        .expect("union-over-distinct materialized view");
    assert_eq!(
        single_int_rows(&union_over_distinct.arrow_snapshot_for(1).expect("snapshot")),
        vec![1, 1, 2, 2, 3, 3]
    );

    let aggregate_over_aggregate = registry
        .get("mv_aggregate_over_aggregate")
        .expect("aggregate-over-aggregate materialized view");
    assert_eq!(
        single_int_rows(
            &aggregate_over_aggregate
                .arrow_snapshot_for(1)
                .expect("snapshot")
        ),
        vec![440]
    );

    let distinct_over_distinct = registry
        .get("mv_distinct_over_distinct")
        .expect("distinct-over-distinct materialized view");
    assert_eq!(
        single_int_rows(
            &distinct_over_distinct
                .arrow_snapshot_for(1)
                .expect("snapshot")
        ),
        vec![1, 2, 3]
    );

    let filter_over_topn = registry
        .get("mv_filter_over_topn")
        .expect("filter-over-topn materialized view");
    assert_eq!(
        id_count_rows(&filter_over_topn.arrow_snapshot_for(1).expect("snapshot")),
        vec![(2, 200)]
    );

    let join_over_self_join = registry
        .get("mv_join_over_self_join")
        .expect("join-over-self-join materialized view");
    assert_eq!(
        id_count_rows(&join_over_self_join.arrow_snapshot_for(1).expect("snapshot")),
        vec![(1, 10)]
    );

    let topn_over_self_join = registry
        .get("mv_topn_over_self_join")
        .expect("topn-over-self-join materialized view");
    assert_eq!(
        id_count_rows(&topn_over_self_join.arrow_snapshot_for(1).expect("snapshot")),
        vec![(1, 100)]
    );
}

#[tokio::test]
async fn join_over_row_number_topn_uses_slate_backed_columnar_join_operator_semantics() {
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
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-join-row-number-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let query = "SELECT t.auction, a.seller \
        FROM (\
            SELECT auction, price \
            FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bids) ranked \
            WHERE rn <= 2\
        ) t \
        JOIN auctions a ON t.auction = a.id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_row_number_topn",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
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
        .get("mv_join_row_number_topn")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 10), (2, 20)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![300])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20), (3, 30)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 10, -1), (3, 30, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_join_row_number_topn",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_join_row_number_topn")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(2, 20), (3, 30)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn explicit_full_refresh_policy_allows_non_incremental_mv() {
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

    VectorizedExecutionRuntime::new(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new(
                "mv_order_totals",
                "SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
                Arc::clone(&output_schema),
            )
            .allow_full_refresh(),
        ],
        Arc::clone(&registry),
    )
    .await
    .expect("explicit full-refresh policy should allow aggregate MV planning");
}
