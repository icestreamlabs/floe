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

#[cfg(test)]
mod tests {
    use super::{CANONICAL_NEXMARK_QUERY_IDS, canonical_nexmark_queries};
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
}
