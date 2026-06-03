use super::*;

pub(super) fn root_arrow_schema(plan: &dbsp::CircuitPlan) -> Arc<Schema> {
    plan.node(plan.root)
        .map(|node| node.output_schema.to_arrow_schema())
        .expect("root node")
}

pub(super) fn row_contains_i64(row: &TestRow, expected: i64) -> bool {
    row.iter().any(|value| {
        matches!(
            value,
            Some(EncodedRowScalar::Int64(actual) | EncodedRowScalar::TimestampMillis(actual))
                if *actual == expected
        )
    })
}

pub(super) fn gather_handle_streams(
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

pub(super) fn gather_transient_streams(
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

pub(super) async fn wait_for_logical_version(
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

pub(super) async fn wait_for_logical_version_or_task_error(
    registry: &MaterializedViewRegistry,
    view_name: &str,
    target_version: i64,
    task_rx: &mut mpsc::Receiver<GraphTaskError>,
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
            tokio::select! {
                changed = rx.changed() => {
                    changed.expect("version watch update");
                }
                maybe_event = task_rx.recv() => {
                    let event = maybe_event.expect("graph task error");
                    panic!(
                        "graph task error in {} [{}]: {}",
                        event.graph_id, event.task, event.error
                    );
                }
            }
        }
    })
    .await
    .expect("wait for logical version or task error");
}

pub(super) async fn wait_for_visible_row_count(
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

pub(super) async fn wait_for_exact_visible_row_count(
    registry: &MaterializedViewRegistry,
    view_name: &str,
    expected_rows: usize,
) {
    timeout(Duration::from_secs(5), async {
        loop {
            if visible_rows(registry, view_name).await.len() == expected_rows {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("wait for exact visible rows");
}

pub(super) type TestRow = Vec<Option<EncodedRowScalar>>;

pub(super) fn sort_rows_by_first_column(rows: &mut [TestRow]) {
    rows.sort_by_key(|row| match row.first() {
        Some(Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value))) => {
            *value
        }
        _ => 0,
    });
}

pub(super) fn int_row(values: &[i64]) -> TestRow {
    values
        .iter()
        .copied()
        .map(EncodedRowScalar::Int64)
        .map(Some)
        .collect()
}

pub(super) fn int_nullable_row(first: i64, second: Option<i64>) -> TestRow {
    vec![
        Some(EncodedRowScalar::Int64(first)),
        second.map(EncodedRowScalar::Int64),
    ]
}

pub(super) fn timestamp_int_row(start: i64, end: i64, values: &[i64]) -> TestRow {
    let mut row = vec![
        Some(EncodedRowScalar::TimestampMillis(start)),
        Some(EncodedRowScalar::TimestampMillis(end)),
    ];
    row.extend(
        values
            .iter()
            .copied()
            .map(EncodedRowScalar::Int64)
            .map(Some),
    );
    row
}

pub(super) fn int_utf8_row(id: i64, label: Option<&str>) -> TestRow {
    vec![
        Some(EncodedRowScalar::Int64(id)),
        label.map(|label| EncodedRowScalar::Utf8(label.to_string())),
    ]
}

pub(super) fn row_key(row: &TestRow) -> String {
    format!("{row:?}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ZSet<T> {
    weights: std::collections::BTreeMap<T, i64>,
}

impl<T: Ord> ZSet<T> {
    pub(super) fn from_weights(weights: impl IntoIterator<Item = (T, i64)>) -> Self {
        let mut merged = std::collections::BTreeMap::new();
        for (row, weight) in weights {
            if weight == 0 {
                continue;
            }
            let next = merged
                .get(&row)
                .copied()
                .unwrap_or(0_i64)
                .saturating_add(weight);
            if next == 0 {
                merged.remove(&row);
            } else {
                merged.insert(row, next);
            }
        }
        Self { weights: merged }
    }
}

pub(super) fn zset_from_rows(rows: &[TestRow]) -> ZSet<String> {
    ZSet::from_weights(rows.iter().map(|row| (row_key(row), 1)))
}

pub(super) fn int_and_null_timestamp_row(id: i64) -> TestRow {
    vec![Some(EncodedRowScalar::Int64(id)), None]
}

pub(super) fn count_char_projection_row(
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

pub(super) fn channel_id_projection_row(
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

pub(super) fn split_index_projection_row(
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

pub(super) fn scalar_i64(value: Option<&Option<EncodedRowScalar>>) -> i64 {
    match value {
        Some(Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value))) => {
            *value
        }
        _ => 0,
    }
}

pub(super) fn scalar_timestamp_millis(value: Option<&Option<EncodedRowScalar>>) -> i64 {
    match value {
        Some(Some(EncodedRowScalar::TimestampMillis(value) | EncodedRowScalar::Int64(value))) => {
            *value
        }
        _ => 0,
    }
}

pub(super) enum EncodedTestField<'a> {
    Int64(i64),
    Utf8(&'a str),
    TimestampMillis(i64),
}

pub(super) fn encode_test_row(columns: &[EncodedTestField<'_>]) -> Vec<u8> {
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

pub(super) fn encoded_bid_row_with_ts(
    auction: i64,
    bidder: i64,
    price: i64,
    date_time_ms: i64,
) -> Vec<u8> {
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

pub(super) fn encoded_bid_row_with_ts_and_extra(
    auction: i64,
    bidder: i64,
    price: i64,
    date_time_ms: i64,
    extra: &str,
) -> Vec<u8> {
    encode_test_row(&[
        EncodedTestField::Int64(auction),
        EncodedTestField::Int64(bidder),
        EncodedTestField::Int64(price),
        EncodedTestField::Utf8("channel"),
        EncodedTestField::Utf8("url"),
        EncodedTestField::TimestampMillis(date_time_ms),
        EncodedTestField::Utf8(extra),
    ])
}

pub(super) fn encoded_bid_row_with_channel_url(
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

pub(super) fn encoded_bid_row(auction: i64, bidder: i64, price: i64) -> Vec<u8> {
    encoded_bid_row_with_ts(auction, bidder, price, 1_700_000_000_000)
}

pub(super) fn encoded_person_row(id: i64, name: &str) -> Vec<u8> {
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

pub(super) fn encoded_auction_row(id: i64, seller: i64) -> Vec<u8> {
    encoded_auction_row_with_category(id, seller, 5)
}

pub(super) fn encoded_auction_row_with_category(id: i64, seller: i64, category: i64) -> Vec<u8> {
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

pub(super) fn encoded_auction_row_with_ts_and_extra(
    id: i64,
    seller: i64,
    category: i64,
    expires_ms: i64,
    date_time_ms: i64,
    extra: &str,
) -> Vec<u8> {
    encode_test_row(&[
        EncodedTestField::Int64(id),
        EncodedTestField::Utf8("item"),
        EncodedTestField::Utf8("desc"),
        EncodedTestField::Int64(10),
        EncodedTestField::Int64(20),
        EncodedTestField::Int64(seller),
        EncodedTestField::Int64(category),
        EncodedTestField::TimestampMillis(expires_ms),
        EncodedTestField::TimestampMillis(date_time_ms),
        EncodedTestField::Utf8(extra),
    ])
}

pub(super) fn bid_row_with_ts(auction: i64, bidder: i64, price: i64, date_time_ms: i64) -> TestRow {
    decode_row_to_values(&encoded_bid_row_with_ts(
        auction,
        bidder,
        price,
        date_time_ms,
    ))
}

pub(super) fn bid_row(auction: i64, bidder: i64, price: i64) -> TestRow {
    bid_row_with_ts(auction, bidder, price, 1_700_000_000_000)
}

pub(super) async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}

pub(super) fn nexmark_bid_schema() -> Arc<Schema> {
    nexmark_bid_table().schema().to_arrow_schema()
}

pub(super) fn nexmark_person_schema() -> Arc<Schema> {
    nexmark_person_table().schema().to_arrow_schema()
}

pub(super) fn nexmark_auction_schema() -> Arc<Schema> {
    nexmark_auction_table().schema().to_arrow_schema()
}

pub(super) async fn materialized_rows(
    registry: &MaterializedViewRegistry,
    view_name: &str,
) -> Vec<TestRow> {
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

pub(super) async fn visible_rows(
    registry: &MaterializedViewRegistry,
    view_name: &str,
) -> Vec<TestRow> {
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

pub(super) fn decode_row_to_values(encoded: &[u8]) -> TestRow {
    decode_all_encoded_row_scalars(encoded).expect("decode row")
}
