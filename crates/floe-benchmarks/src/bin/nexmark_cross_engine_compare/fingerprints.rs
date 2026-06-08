use super::*;

pub(super) fn parse_row_count_value(value: &serde_json::Value) -> Option<u64> {
    if let Some(rows) = value.as_array() {
        return parse_row_count_row(rows.first()?);
    }
    parse_row_count_row(value)
}

pub(super) fn parse_row_count_row(row: &serde_json::Value) -> Option<u64> {
    row.get("ROW_COUNT")
        .or_else(|| row.get("row_count"))
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

pub(super) fn verify_result_content_hash(
    engine: Engine,
    query_id: &str,
    observed: &ContentFingerprint,
    expected: &ContentFingerprint,
    artifact_dir: &Path,
) -> Result<()> {
    let report = format!(
        "engine={}\nquery_id={}\nobserved_result_rows={}\nobserved_content_sha256={}\nexpected_result_rows={}\nexpected_content_sha256={}\n",
        engine.as_str(),
        query_id,
        observed.row_count,
        observed.hash,
        expected.row_count,
        expected.hash
    );
    fs::write(artifact_dir.join("content_hash.txt"), report)?;
    if observed.row_count != expected.row_count || observed.hash != expected.hash {
        fs::write(
            artifact_dir.join("correctness.error"),
            format!(
                "expected_result_rows={}\nexpected_content_sha256={}\nobserved_result_rows={}\nobserved_content_sha256={}\nquery_id={}\nengine={}\n",
                expected.row_count,
                expected.hash,
                observed.row_count,
                observed.hash,
                query_id,
                engine.as_str()
            ),
        )?;
        bail!("content hash mismatch for {} {query_id}", engine.as_str());
    }
    Ok(())
}

pub(super) fn fingerprint_file_lines(path: &Path) -> Result<ContentFingerprint> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let lines = content.lines().map(ToOwned::to_owned).collect::<Vec<_>>();
    Ok(fingerprint_lines(lines))
}

pub(super) fn fingerprint_lines(mut lines: Vec<String>) -> ContentFingerprint {
    lines.sort();
    let mut hasher = Sha256::new();
    for line in &lines {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    ContentFingerprint {
        row_count: lines.len() as u64,
        hash: hex::encode(hasher.finalize()),
    }
}

pub(super) fn deterministic_nexmark_q5_fingerprint(bid_rows: u64) -> ContentFingerprint {
    let full_cycles = bid_rows / NEXMARK_BID_AUCTION_CARDINALITY;
    let remainder = bid_rows % NEXMARK_BID_AUCTION_CARDINALITY;
    let mut lines = (1..=NEXMARK_BID_AUCTION_CARDINALITY)
        .map(|auction| {
            let bids_for_auction = full_cycles + u64::from(auction <= remainder);
            (format!("{auction}\t1"), bids_for_auction * 5)
        })
        .filter(|(_, repetitions)| *repetitions > 0)
        .collect::<Vec<_>>();
    lines.sort_by(|left, right| left.0.cmp(&right.0));

    let mut hasher = Sha256::new();
    let mut row_count = 0_u64;
    for (line, repetitions) in lines {
        for _ in 0..repetitions {
            hasher.update(line.as_bytes());
            hasher.update(b"\n");
        }
        row_count += repetitions;
    }

    ContentFingerprint {
        row_count,
        hash: hex::encode(hasher.finalize()),
    }
}

pub(super) fn deterministic_nexmark_q15_fingerprint(bid_rows: u64) -> ContentFingerprint {
    #[derive(Default)]
    struct Stats {
        total_bids: u64,
        rank1_bids: u64,
        rank2_bids: u64,
        rank3_bids: u64,
        total_auctions: BTreeSet<i64>,
        rank1_auctions: BTreeSet<i64>,
        rank2_auctions: BTreeSet<i64>,
        rank3_auctions: BTreeSet<i64>,
    }

    let mut stats_by_day = BTreeMap::<String, Stats>::new();
    for bid_idx in 1..=bid_rows {
        let row = deterministic_bid_row(bid_idx);
        let stats = stats_by_day.entry(row.day).or_default();
        stats.total_bids += 1;
        stats.total_auctions.insert(row.auction);
        match price_rank(row.price) {
            PriceRank::Rank1 => {
                stats.rank1_bids += 1;
                stats.rank1_auctions.insert(row.auction);
            }
            PriceRank::Rank2 => {
                stats.rank2_bids += 1;
                stats.rank2_auctions.insert(row.auction);
            }
            PriceRank::Rank3 => {
                stats.rank3_bids += 1;
                stats.rank3_auctions.insert(row.auction);
            }
        }
    }

    fingerprint_lines(
        stats_by_day
            .into_iter()
            .map(|(day, stats)| {
                format!(
                    "{day}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_auctions.len(),
                    stats.rank1_auctions.len(),
                    stats.rank2_auctions.len(),
                    stats.rank3_auctions.len()
                )
            })
            .collect(),
    )
}

pub(super) fn deterministic_nexmark_q16_fingerprint(bid_rows: u64) -> ContentFingerprint {
    #[derive(Default)]
    struct Stats {
        max_minute: Option<String>,
        total_bids: u64,
        rank1_bids: u64,
        rank2_bids: u64,
        rank3_bids: u64,
        total_auctions: BTreeSet<i64>,
        rank1_auctions: BTreeSet<i64>,
        rank2_auctions: BTreeSet<i64>,
        rank3_auctions: BTreeSet<i64>,
    }

    let mut stats_by_group = BTreeMap::<(String, String), Stats>::new();
    for bid_idx in 1..=bid_rows {
        let row = deterministic_bid_row(bid_idx);
        let stats = stats_by_group
            .entry((row.channel.to_string(), row.day))
            .or_default();
        stats.max_minute = Some(match stats.max_minute.take() {
            Some(existing) => existing.max(row.minute),
            None => row.minute,
        });
        stats.total_bids += 1;
        stats.total_auctions.insert(row.auction);
        match price_rank(row.price) {
            PriceRank::Rank1 => {
                stats.rank1_bids += 1;
                stats.rank1_auctions.insert(row.auction);
            }
            PriceRank::Rank2 => {
                stats.rank2_bids += 1;
                stats.rank2_auctions.insert(row.auction);
            }
            PriceRank::Rank3 => {
                stats.rank3_bids += 1;
                stats.rank3_auctions.insert(row.auction);
            }
        }
    }

    fingerprint_lines(
        stats_by_group
            .into_iter()
            .map(|((channel, day), stats)| {
                let minute = stats.max_minute.unwrap_or_default();
                format!(
                    "{channel}\t{day}\t{minute}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_auctions.len(),
                    stats.rank1_auctions.len(),
                    stats.rank2_auctions.len(),
                    stats.rank3_auctions.len()
                )
            })
            .collect(),
    )
}

pub(super) fn deterministic_nexmark_q17_fingerprint(bid_rows: u64) -> ContentFingerprint {
    #[derive(Default)]
    struct Stats {
        total_bids: u64,
        rank1_bids: u64,
        rank2_bids: u64,
        rank3_bids: u64,
        min_price: Option<i64>,
        max_price: Option<i64>,
        sum_price: i64,
    }

    let mut stats_by_group = BTreeMap::<(i64, String), Stats>::new();
    for bid_idx in 1..=bid_rows {
        let row = deterministic_bid_row(bid_idx);
        let stats = stats_by_group.entry((row.auction, row.day)).or_default();
        stats.total_bids += 1;
        match price_rank(row.price) {
            PriceRank::Rank1 => stats.rank1_bids += 1,
            PriceRank::Rank2 => stats.rank2_bids += 1,
            PriceRank::Rank3 => stats.rank3_bids += 1,
        }
        stats.min_price = Some(
            stats
                .min_price
                .map_or(row.price, |value| value.min(row.price)),
        );
        stats.max_price = Some(
            stats
                .max_price
                .map_or(row.price, |value| value.max(row.price)),
        );
        stats.sum_price += row.price;
    }

    fingerprint_lines(
        stats_by_group
            .into_iter()
            .map(|((auction, day), stats)| {
                let avg_price = if stats.total_bids == 0 {
                    0
                } else {
                    stats.sum_price / i64::try_from(stats.total_bids).unwrap_or(1)
                };
                format!(
                    "{auction}\t{day}\t{}\t{}\t{}\t{}\t{}\t{}\t{avg_price}\t{}",
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.min_price.unwrap_or_default(),
                    stats.max_price.unwrap_or_default(),
                    stats.sum_price
                )
            })
            .collect(),
    )
}

struct DeterministicBidRow {
    auction: i64,
    price: i64,
    channel: &'static str,
    day: String,
    minute: String,
}

fn deterministic_bid_row(bid_idx: u64) -> DeterministicBidRow {
    let bid_idx_i64 = i64::try_from(bid_idx).unwrap_or(i64::MAX);
    let auction =
        i64::try_from((bid_idx - 1) % NEXMARK_BID_AUCTION_CARDINALITY + 1).unwrap_or(i64::MAX);
    let price = 1_000 + (bid_idx_i64 % 50_000);
    let channel = match bid_idx % 5 {
        0 => "web",
        1 => "apple",
        2 => "google",
        3 => "facebook",
        _ => "baidu",
    };
    let timestamp = DateTime::<Utc>::from_timestamp_millis(NEXMARK_BASE_TS_MS + bid_idx_i64)
        .unwrap_or(DateTime::UNIX_EPOCH);
    DeterministicBidRow {
        auction,
        price,
        channel,
        day: timestamp.format("%Y-%m-%d").to_string(),
        minute: timestamp.format("%H:%M").to_string(),
    }
}

enum PriceRank {
    Rank1,
    Rank2,
    Rank3,
}

fn price_rank(price: i64) -> PriceRank {
    if price < 10_000 {
        PriceRank::Rank1
    } else if price < 1_000_000 {
        PriceRank::Rank2
    } else {
        PriceRank::Rank3
    }
}

pub(super) fn canonical_json_line(value: &serde_json::Value) -> Result<String> {
    let canonical = canonical_json_value(value);
    serde_json::to_string(&canonical).context("serialize canonical JSON row")
}

pub(super) fn canonical_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json_value(&map[key]));
            }
            serde_json::Value::Object(sorted)
        }
        other => other.clone(),
    }
}

pub(super) fn parse_feldera_json_stream(bytes: &[u8]) -> Result<serde_json::Value> {
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        return Ok(value);
    }

    let text = String::from_utf8_lossy(bytes);
    let mut rows = Vec::new();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        rows.push(
            serde_json::from_str::<serde_json::Value>(line)
                .with_context(|| format!("parse Feldera JSON line: {line}"))?,
        );
    }
    Ok(serde_json::Value::Array(rows))
}
