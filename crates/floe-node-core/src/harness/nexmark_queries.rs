#[derive(Debug, Clone, Copy)]
pub struct NexmarkQuerySpec {
    pub id: &'static str,
    pub sql: &'static str,
}

pub const CANONICAL_NEXMARK_QUERY_IDS: [&str; 21] = [
    "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q12", "q13", "q14", "q15", "q16",
    "q17", "q18", "q19", "q20", "q21", "q22",
];

pub const CANONICAL_NEXMARK_QUERIES: &[NexmarkQuerySpec] = &[
    NexmarkQuerySpec {
        id: "q0",
        sql: "SELECT auction, bidder, price, channel, url, \"dateTime\", extra FROM bid",
    },
    NexmarkQuerySpec {
        id: "q1",
        sql: "SELECT auction, bidder, price * 89 / 100 AS converted_price, \"dateTime\", extra FROM bid",
    },
    NexmarkQuerySpec {
        id: "q2",
        sql: "SELECT auction, price FROM bid WHERE auction % 123 = 0",
    },
    NexmarkQuerySpec {
        id: "q3",
        sql: "SELECT p.name, p.city, p.state, a.id FROM auction AS a JOIN person AS p ON a.seller = p.id WHERE a.category = 10 AND p.state IN ('or', 'id', 'ca')",
    },
    NexmarkQuerySpec {
        id: "q4",
        sql: "SELECT category, AVG(max) FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category",
    },
    NexmarkQuerySpec {
        id: "q5",
        sql: "SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)",
    },
    NexmarkQuerySpec {
        id: "q6",
        sql: "SELECT seller, AVG(price) AS moving_avg_price FROM (SELECT a.seller, b.price, b.\"dateTime\", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller",
    },
    NexmarkQuerySpec {
        id: "q7",
        sql: "SELECT MAX(price) AS maxprice FROM bid GROUP BY TUMBLE(\"dateTime\", 10000)",
    },
    NexmarkQuerySpec {
        id: "q8",
        sql: "SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, TUMBLE(\"dateTime\", 10000)",
    },
    NexmarkQuerySpec {
        id: "q9",
        sql: "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" FROM (SELECT a.id, a.\"itemName\", a.description, a.\"initialBid\", a.reserve, a.\"dateTime\", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b.\"dateTime\" AS \"bidTime\", b.extra AS \"bidExtra\", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.\"dateTime\" ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked WHERE rownum <= 1",
    },
    NexmarkQuerySpec {
        id: "q12",
        sql: "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE(\"dateTime\", 10000)",
    },
    NexmarkQuerySpec {
        id: "q13",
        sql: "SELECT b.auction, b.bidder, b.price, b.\"dateTime\", a.seller AS value FROM (SELECT *, PROCTIME() AS p_time FROM bid) b JOIN auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000",
    },
    NexmarkQuerySpec {
        id: "q14",
        sql: "SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN HOUR(\"dateTime\") >= 8 AND HOUR(\"dateTime\") <= 18 THEN 'dayTime' WHEN HOUR(\"dateTime\") <= 6 OR HOUR(\"dateTime\") >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, \"dateTime\", extra, COUNT_CHAR(extra, 'c') AS c_counts FROM bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000",
    },
    NexmarkQuerySpec {
        id: "q15",
        sql: "SELECT DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd')",
    },
    NexmarkQuerySpec {
        id: "q16",
        sql: "SELECT channel, DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd') AS day, MAX(DATE_FORMAT(\"dateTime\", 'HH:mm')) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd')",
    },
    NexmarkQuerySpec {
        id: "q17",
        sql: "SELECT auction, DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd')",
    },
    NexmarkQuerySpec {
        id: "q18",
        sql: "SELECT auction, bidder, price, channel, url, \"dateTime\", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY \"dateTime\" DESC) AS rank_number FROM bid) dedup WHERE rank_number <= 1",
    },
    NexmarkQuerySpec {
        id: "q19",
        sql: "SELECT auction, bidder, price, channel, url, \"dateTime\", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 10",
    },
    NexmarkQuerySpec {
        id: "q20",
        sql: "SELECT b.auction, b.bidder, b.price, b.channel, b.url, b.\"dateTime\", b.extra, a.\"itemName\", a.description, a.\"initialBid\", a.reserve, a.\"dateTime\" AS auction_time, a.expires, a.seller, a.category, a.extra AS auction_extra FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE a.category = 10",
    },
    NexmarkQuerySpec {
        id: "q21",
        sql: "SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url, 'channel_id=([^&]*)', 1) END AS channel_id FROM bid WHERE REGEXP_EXTRACT(url, 'channel_id=([^&]*)', 1) IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')",
    },
    NexmarkQuerySpec {
        id: "q22",
        sql: "SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1, SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM bid",
    },
];

pub fn canonical_nexmark_queries() -> &'static [NexmarkQuerySpec] {
    CANONICAL_NEXMARK_QUERIES
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NexmarkSqlDialect {
    Canonical,
    Portable,
    FloeKafka,
    FloeCdc,
    PostgresExpected,
}

pub fn nexmark_query_sql(query_id: &str, dialect: NexmarkSqlDialect) -> Option<String> {
    match dialect {
        NexmarkSqlDialect::Canonical => canonical_query_sql(query_id).map(str::to_string),
        NexmarkSqlDialect::Portable => portable_query_sql(query_id).map(str::to_string),
        NexmarkSqlDialect::FloeKafka => floe_kafka_query_sql(query_id),
        NexmarkSqlDialect::FloeCdc => floe_cdc_query_sql(query_id),
        NexmarkSqlDialect::PostgresExpected => {
            postgres_expected_query_sql(query_id).map(str::to_string)
        }
    }
}

fn canonical_query_sql(query_id: &str) -> Option<&'static str> {
    CANONICAL_NEXMARK_QUERIES
        .iter()
        .find(|query| query.id == query_id)
        .map(|query| query.sql)
}

fn portable_query_sql(query_id: &str) -> Option<&'static str> {
    Some(match query_id {
        "q4" => {
            r#"SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#
        }
        "q5" => {
            r#"SELECT auction, COUNT(*) AS num
FROM (
  SELECT auction, (("dateTime" / 2000) * 2000 - 0) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 2000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 4000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 6000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 8000) AS hop_start FROM bid
) expanded
GROUP BY auction, hop_start"#
        }
        "q6" => {
            r#"SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price FROM (SELECT a.seller, b.price, b."dateTime", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q7" => r#"SELECT MAX(price) AS maxprice FROM bid GROUP BY ("dateTime" / 10000)"#,
        "q8" => {
            r#"SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, ("dateTime" / 10000)"#
        }
        "q9" => {
            r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b."dateTime" AS "bidTime", b.extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1"#
        }
        "q12" => {
            r#"SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, ("dateTime" / 10000)"#
        }
        "q13" => {
            r#"SELECT b.auction, b.bidder, b.price, b."dateTime", a.seller AS value FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000"#
        }
        "q14" => {
            r#"SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN (("dateTime" / 3600000) % 24) >= 8 AND (("dateTime" / 3600000) % 24) <= 18 THEN 'dayTime' WHEN (("dateTime" / 3600000) % 24) <= 6 OR (("dateTime" / 3600000) % 24) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, "dateTime", extra, LENGTH(extra) - LENGTH(REPLACE(extra, 'c', '')) AS c_counts FROM bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000"#
        }
        "q15" => {
            r#"SELECT ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY ("dateTime" / 86400000)"#
        }
        "q16" => {
            r#"SELECT channel, ("dateTime" / 86400000) AS day, MAX((("dateTime" / 60000) % 1440)) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, ("dateTime" / 86400000)"#
        }
        "q17" => {
            r#"SELECT auction, ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, CAST(AVG(price) AS BIGINT) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, ("dateTime" / 86400000)"#
        }
        "q18" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY "dateTime" DESC, price DESC, channel ASC, url ASC, extra ASC) AS rank_number FROM bid) dedup WHERE rank_number <= 1"#
        }
        "q19" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, "dateTime" ASC, bidder ASC, channel ASC, url ASC, extra ASC) AS rank_number FROM bid) ranked WHERE rank_number <= 10"#
        }
        "q21" => {
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') END AS channel_id FROM bid WHERE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        "q22" => {
            r#"SELECT auction, bidder, price, channel, SPLIT_PART(url, '/', 4) AS dir1, SPLIT_PART(url, '/', 5) AS dir2, SPLIT_PART(url, '/', 6) AS dir3 FROM bid"#
        }
        _ => canonical_query_sql(query_id)?,
    })
}

fn floe_kafka_query_sql(query_id: &str) -> Option<String> {
    if query_id == "q17" {
        return Some(r#"SELECT auction, DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, CAST(AVG(price) AS BIGINT) AS avg_price, SUM(price) AS sum_price FROM nexmark_bid GROUP BY auction, DATE_FORMAT(date_time, 'yyyy-MM-dd')"#.to_string());
    }
    if let Some(sql) = floe_shape_sensitive_query_sql(query_id) {
        return Some(sql.to_string());
    }
    let mut sql = canonical_query_sql(query_id)?.to_string();
    replace_floe_identifiers(&mut sql);
    Some(sql)
}

fn floe_cdc_query_sql(query_id: &str) -> Option<String> {
    if query_id == "q14" {
        return Some(r#"SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN ((date_time / 3600000) % 24) >= 8 AND ((date_time / 3600000) % 24) <= 18 THEN 'dayTime' WHEN ((date_time / 3600000) % 24) <= 6 OR ((date_time / 3600000) % 24) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, date_time AS "dateTime", extra, COUNT_CHAR(extra, 'c') AS c_counts FROM nexmark_bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000"#.to_string());
    }
    if let Some(sql) = floe_shape_sensitive_query_sql(query_id) {
        return Some(sql.to_string());
    }
    if query_id == "q21" {
        return Some(r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url, 'channel_id=([^&]*)', 1) END AS channel_id FROM nexmark_bid WHERE REGEXP_EXTRACT(url, 'channel_id=([^&]*)', 1) IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#.to_string());
    }
    if query_id == "q22" {
        return Some(r#"SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1, SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM nexmark_bid"#.to_string());
    }
    let mut sql = portable_query_sql(query_id)?.to_string();
    replace_floe_identifiers(&mut sql);
    Some(sql)
}

fn replace_floe_identifiers(sql: &mut String) {
    for (from, to) in [
        (r#""dateTime""#, "date_time"),
        (r#""itemName""#, "item_name"),
        (r#""initialBid""#, "initial_bid"),
        (r#""bidTime""#, "bid_time"),
        (r#""bidExtra""#, "bid_extra"),
    ] {
        *sql = sql.replace(from, to);
    }
    for (from, to) in [
        ("FROM person", "FROM nexmark_person"),
        ("JOIN person", "JOIN nexmark_person"),
        ("FROM auction", "FROM nexmark_auction"),
        ("JOIN auction", "JOIN nexmark_auction"),
        ("FROM bid", "FROM nexmark_bid"),
        ("JOIN bid", "JOIN nexmark_bid"),
    ] {
        *sql = sql.replace(from, to);
    }
}

fn floe_shape_sensitive_query_sql(query_id: &str) -> Option<&'static str> {
    Some(match query_id {
        "q4" => {
            r#"SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price FROM (SELECT MAX(b.price) AS max, a.category FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction WHERE b.date_time BETWEEN a.date_time AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#
        }
        "q6" => {
            r#"SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price FROM (SELECT a.seller, b.price, b.date_time, ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b.date_time ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q9" => {
            r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a.item_name AS "itemName", a.description, a.initial_bid AS "initialBid", a.reserve, a.auction_time AS "dateTime", a.expires, a.seller, a.category, a.auction_extra AS extra, b.auction, b.bidder, b.price, b.bid_time AS "bidTime", b.bid_extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.bid_time ASC, b.bidder ASC, b.bid_extra ASC) AS rownum FROM (SELECT id, item_name, description, initial_bid, reserve, date_time AS auction_time, expires, seller, category, extra AS auction_extra FROM nexmark_auction) a JOIN (SELECT auction, bidder, price, date_time AS bid_time, extra AS bid_extra FROM nexmark_bid) b ON a.id = b.auction WHERE b.bid_time BETWEEN a.auction_time AND a.expires) ranked WHERE rownum <= 1"#
        }
        "q18" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC, price DESC, channel ASC, url ASC, extra ASC) AS rank_number FROM nexmark_bid) dedup WHERE rank_number <= 1"#
        }
        "q19" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, date_time ASC, bidder ASC, channel ASC, url ASC, extra ASC) AS rank_number FROM nexmark_bid) ranked WHERE rank_number <= 10"#
        }
        "q20" => {
            r#"SELECT b.auction, b.bidder, b.price, b.channel, b.url, b.date_time AS "dateTime", b.extra, a.item_name AS "itemName", a.description, a.initial_bid AS "initialBid", a.reserve, a.date_time AS auction_time, a.expires, a.seller, a.category, a.extra AS auction_extra FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id WHERE a.category = 10"#
        }
        _ => return None,
    })
}

fn postgres_expected_query_sql(query_id: &str) -> Option<&'static str> {
    Some(match query_id {
        "q4" => {
            r#"SELECT category, CAST(FLOOR(AVG(max)) AS BIGINT) AS avg_price FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#
        }
        "q6" => {
            r#"SELECT seller, CAST(FLOOR(AVG(price)) AS BIGINT) AS moving_avg_price FROM (SELECT a.seller, b.price, b."dateTime", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q17" => {
            r#"SELECT auction, ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, CAST(FLOOR(AVG(price)) AS BIGINT) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, ("dateTime" / 86400000)"#
        }
        "q21" => {
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE substring(url from 'channel_id=([^&]*)') END AS channel_id FROM bid WHERE substring(url from 'channel_id=([^&]*)') IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        _ => portable_query_sql(query_id)?,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CANONICAL_NEXMARK_QUERY_IDS, NexmarkSqlDialect, canonical_nexmark_queries,
        nexmark_query_sql,
    };
    use std::collections::BTreeSet;

    #[test]
    fn canonical_query_set_has_expected_ids() {
        let actual = canonical_nexmark_queries()
            .iter()
            .map(|query| query.id)
            .collect::<BTreeSet<_>>();
        let expected = CANONICAL_NEXMARK_QUERY_IDS
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 21);
    }

    #[test]
    fn every_dialect_covers_the_canonical_query_set() {
        for query_id in CANONICAL_NEXMARK_QUERY_IDS {
            for dialect in [
                NexmarkSqlDialect::Canonical,
                NexmarkSqlDialect::Portable,
                NexmarkSqlDialect::FloeKafka,
                NexmarkSqlDialect::FloeCdc,
                NexmarkSqlDialect::PostgresExpected,
            ] {
                let sql = nexmark_query_sql(query_id, dialect)
                    .unwrap_or_else(|| panic!("missing {dialect:?} SQL for {query_id}"));
                assert!(
                    !sql.trim().is_empty(),
                    "empty {dialect:?} SQL for {query_id}"
                );
            }

            for dialect in [NexmarkSqlDialect::FloeKafka, NexmarkSqlDialect::FloeCdc] {
                let floe_sql = nexmark_query_sql(query_id, dialect).expect("Floe SQL must exist");
                for bare_source in [
                    "FROM bid",
                    "JOIN bid",
                    "FROM auction",
                    "JOIN auction",
                    "FROM person",
                    "JOIN person",
                ] {
                    assert!(
                        !floe_sql.contains(bare_source),
                        "{dialect:?} SQL for {query_id} retained bare source reference {bare_source}: {floe_sql}"
                    );
                }
            }
        }

        assert!(nexmark_query_sql("q11", NexmarkSqlDialect::FloeKafka).is_none());
        assert!(nexmark_query_sql("q11", NexmarkSqlDialect::FloeCdc).is_none());
    }

    #[test]
    fn floe_dialects_preserve_source_time_types_and_output_aliases() {
        let kafka_q5 = nexmark_query_sql("q5", NexmarkSqlDialect::FloeKafka).unwrap();
        let cdc_q5 = nexmark_query_sql("q5", NexmarkSqlDialect::FloeCdc).unwrap();
        assert!(kafka_q5.contains("HOP(date_time, 2000, 10000)"));
        assert!(!kafka_q5.contains("date_time / 2000"));
        assert!(cdc_q5.contains("date_time / 2000"));
        assert!(!cdc_q5.contains("HOP("));

        let cdc_q14 = nexmark_query_sql("q14", NexmarkSqlDialect::FloeCdc).unwrap();
        assert!(cdc_q14.contains("COUNT_CHAR(extra, 'c') AS c_counts"));
        assert!(cdc_q14.contains(r#"date_time AS "dateTime""#));

        for dialect in [NexmarkSqlDialect::FloeKafka, NexmarkSqlDialect::FloeCdc] {
            let q9 = nexmark_query_sql("q9", dialect).unwrap();
            assert!(q9.contains(r#"a.item_name AS "itemName""#));
            assert!(q9.contains(r#"b.bid_time AS "bidTime""#));
        }
    }
}
