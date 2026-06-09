use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use dbsp::storage::{KeyValueTable, SlateTable};
use floe_executor::{
    MaterializedViewRegistry, VectorizedExecutionRuntime, VectorizedExecutionRuntimeOptions,
    VectorizedMaterializedViewPlan,
};
use object_store::memory::InMemory;
use serde::Serialize;
use slatedb::Db;

use floe_executor::dbsp_plan::{DbspPlanBuilder, nexmark_config, validate_dbsp_plan};
use floe_node_core::generator;
use floe_node_core::nexmark_queries::{CANONICAL_NEXMARK_QUERY_IDS, canonical_nexmark_queries};
use floe_node_core::planner::{plan_materialized_views, planner_udfs};
use floe_node_core::source::SourceRegistry;
use floe_sql_parser::parse_materialized_view;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct QueryCoverage {
    logical_planner: bool,
    circuit_planner: bool,
    runtime_validation: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct CoverageSnapshot {
    queries: BTreeMap<String, QueryCoverage>,
}

#[derive(Debug, Clone)]
struct QueryCoverageResult {
    coverage: QueryCoverage,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ActiveRuntimeCoverage {
    vectorized_runtime: bool,
    execution_mode: Option<String>,
}

#[derive(Debug, Clone)]
struct ActiveRuntimeCoverageResult {
    coverage: ActiveRuntimeCoverage,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct ValidPlanRuntimeCase {
    id: &'static str,
    sql: &'static str,
}

#[derive(Debug, Clone)]
struct GeneratedPlanRuntimeCase {
    id: String,
    sql: String,
}

#[derive(Debug, Clone)]
enum GeneratedRuntimeCaseResult {
    RuntimeMode(String),
    DbspUnsupported(String),
}

const VALID_DBSP_RUNTIME_PLAN_CASES: &[ValidPlanRuntimeCase] = &[
    ValidPlanRuntimeCase {
        id: "projection_over_scan",
        sql: "SELECT id, name FROM person",
    },
    ValidPlanRuntimeCase {
        id: "scan_pushdown_filter_projection",
        sql: "SELECT id, name FROM person WHERE id > 5",
    },
    ValidPlanRuntimeCase {
        id: "filter_through_subquery_projection_alias",
        sql: "SELECT p FROM (SELECT price AS p, auction FROM bid) q WHERE p > 10",
    },
    ValidPlanRuntimeCase {
        id: "merged_projection_alias",
        sql: "SELECT p AS price_alias FROM (SELECT price AS p, auction AS a FROM bid) q",
    },
    ValidPlanRuntimeCase {
        id: "scalar_subquery_filter",
        sql: "SELECT auction, price FROM bid WHERE price > (SELECT MIN(\"initialBid\") FROM auction)",
    },
    ValidPlanRuntimeCase {
        id: "plain_distinct",
        sql: "SELECT DISTINCT auction FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "multi_column_distinct",
        sql: "SELECT DISTINCT auction, bidder FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "ordered_distinct",
        sql: "SELECT DISTINCT auction FROM bid ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "distinct_topn",
        sql: "SELECT DISTINCT auction FROM bid ORDER BY auction LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "left_outer_join",
        sql: "SELECT b.auction, a.seller FROM bid b LEFT JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "right_outer_join",
        sql: "SELECT b.auction, a.seller FROM bid b RIGHT JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "full_outer_join",
        sql: "SELECT b.auction, a.seller FROM bid b FULL OUTER JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "left_semi_join",
        sql: "SELECT b.auction, b.bidder FROM bid b LEFT SEMI JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "left_anti_join",
        sql: "SELECT b.auction, b.bidder FROM bid b LEFT ANTI JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "right_semi_join",
        sql: "SELECT a.id, a.seller FROM bid b RIGHT SEMI JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "right_anti_join",
        sql: "SELECT a.id, a.seller FROM bid b RIGHT ANTI JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "range_join",
        sql: "SELECT a.id, b.price FROM auction a JOIN bid b ON b.price >= a.\"initialBid\" AND b.price < a.reserve",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_range_join",
        sql: "SELECT COUNT(price) AS c FROM (SELECT a.id, b.price FROM auction a JOIN bid b ON b.price >= a.\"initialBid\" AND b.price < a.reserve) r",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_range_join",
        sql: "SELECT DISTINCT id FROM (SELECT a.id, b.price FROM auction a JOIN bid b ON b.price >= a.\"initialBid\" AND b.price < a.reserve) r",
    },
    ValidPlanRuntimeCase {
        id: "union_over_range_join",
        sql: "SELECT key FROM (SELECT id AS key FROM (SELECT a.id, b.price FROM auction a JOIN bid b ON b.price >= a.\"initialBid\" AND b.price < a.reserve) r UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "topn_over_range_join",
        sql: "SELECT id, price FROM (SELECT a.id, b.price FROM auction a JOIN bid b ON b.price >= a.\"initialBid\" AND b.price < a.reserve) r ORDER BY price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "multi_column_join",
        sql: "SELECT p.id, a.id AS auction_id FROM person p JOIN auction a ON p.id = a.seller AND p.\"dateTime\" = a.expires",
    },
    ValidPlanRuntimeCase {
        id: "join_key_filter_inference",
        sql: "SELECT p.name, a.\"itemName\" FROM person p JOIN auction a ON p.id = a.seller WHERE p.id > 10",
    },
    ValidPlanRuntimeCase {
        id: "ordered_inner_join",
        sql: "SELECT b.auction, a.seller FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.auction",
    },
    ValidPlanRuntimeCase {
        id: "join_expression_key_pruning",
        sql: "SELECT b.auction, a.seller FROM bid b JOIN auction a ON b.auction = a.id AND b.auction % 10000 = a.id % 10000",
    },
    ValidPlanRuntimeCase {
        id: "three_way_join",
        sql: "SELECT p.name, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction",
    },
    ValidPlanRuntimeCase {
        id: "ordered_three_way_join",
        sql: "SELECT p.name, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction ORDER BY p.name",
    },
    ValidPlanRuntimeCase {
        id: "topn_over_three_way_join",
        sql: "SELECT p.name, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction ORDER BY b.price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "join_over_three_way_join",
        sql: "SELECT j.seller, p2.name FROM (SELECT a.seller, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction) j JOIN person p2 ON j.seller = p2.id",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_three_way_join",
        sql: "SELECT p_name, COUNT(price) AS bid_count FROM (SELECT p.name AS p_name, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction) j GROUP BY p_name",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_three_way_join",
        sql: "SELECT DISTINCT p_name FROM (SELECT p.name AS p_name, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction) j",
    },
    ValidPlanRuntimeCase {
        id: "union_over_three_way_join",
        sql: "SELECT key FROM (SELECT seller AS key FROM (SELECT a.seller, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction) j UNION ALL SELECT bidder AS key FROM bid) u",
    },
    ValidPlanRuntimeCase {
        id: "self_join_aggregate",
        sql: "SELECT l.auction, COUNT(*) AS pair_count FROM bid l JOIN bid r ON l.auction = r.auction WHERE l.price < r.price GROUP BY l.auction",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_self_join",
        sql: "SELECT DISTINCT auction FROM (SELECT l.auction, r.price FROM bid l JOIN bid r ON l.auction = r.auction WHERE l.price < r.price) j",
    },
    ValidPlanRuntimeCase {
        id: "topn_over_self_join",
        sql: "SELECT auction, price FROM (SELECT l.auction, r.price FROM bid l JOIN bid r ON l.auction = r.auction WHERE l.price < r.price) j ORDER BY price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "union_over_self_join",
        sql: "SELECT key FROM (SELECT auction AS key FROM (SELECT l.auction, r.price FROM bid l JOIN bid r ON l.auction = r.auction WHERE l.price < r.price) j UNION ALL SELECT auction AS key FROM bid) u",
    },
    ValidPlanRuntimeCase {
        id: "join_aggregate",
        sql: "SELECT a.category, COUNT(*) AS bid_count FROM auction a JOIN bid b ON a.id = b.auction GROUP BY a.category",
    },
    ValidPlanRuntimeCase {
        id: "join_over_join_aggregate",
        sql: "SELECT j.auction, j.total_price, p.name FROM (SELECT b.auction, SUM(b.price) AS total_price FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j JOIN person p ON j.auction = p.id",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_join",
        sql: "SELECT a.auction, a.max_price, au.seller FROM (SELECT auction, MAX(price) AS max_price FROM bid GROUP BY auction) a JOIN auction au ON a.auction = au.id",
    },
    ValidPlanRuntimeCase {
        id: "distinct_join",
        sql: "SELECT d.auction, a.seller FROM (SELECT DISTINCT auction FROM bid) d JOIN auction a ON d.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_join",
        sql: "SELECT DISTINCT b.auction, a.seller FROM bid b JOIN auction a ON b.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "join_topn",
        sql: "SELECT b.auction, a.seller FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "filter_over_join_topn",
        sql: "SELECT auction, seller FROM (SELECT b.auction, a.seller, b.price FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 5) t WHERE seller > 100",
    },
    ValidPlanRuntimeCase {
        id: "ordered_over_join_topn",
        sql: "SELECT auction, seller FROM (SELECT b.auction, a.seller, b.price FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 5) t ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "join_over_join_topn",
        sql: "SELECT t.auction, p.name FROM (SELECT b.auction, a.seller, b.price FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 5) t JOIN person p ON t.seller = p.id",
    },
    ValidPlanRuntimeCase {
        id: "join_over_join",
        sql: "SELECT j.auction, p.name FROM (SELECT b.auction, a.seller FROM bid b JOIN auction a ON b.auction = a.id) j JOIN person p ON j.seller = p.id",
    },
    ValidPlanRuntimeCase {
        id: "topn_over_left_join",
        sql: "SELECT b.auction, a.seller FROM bid b LEFT JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "join_over_topn",
        sql: "SELECT t.auction, a.seller FROM (SELECT auction, price FROM bid ORDER BY price DESC LIMIT 5) t JOIN auction a ON t.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "join_over_row_number_topn",
        sql: "SELECT t.auction, a.seller FROM (SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bid) r WHERE rn <= 5) t JOIN auction a ON t.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "join_over_partitioned_row_number_topn",
        sql: "SELECT t.auction, a.seller FROM (SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) r WHERE rn <= 2) t JOIN auction a ON t.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_topn",
        sql: "SELECT auction, SUM(price) AS total FROM bid GROUP BY auction ORDER BY total DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_topn",
        sql: "SELECT SUM(price) AS total FROM (SELECT auction, price FROM bid ORDER BY price DESC LIMIT 5) t",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_row_number_topn",
        sql: "SELECT SUM(price) AS total FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bid) t WHERE rn <= 5",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_stats_topn",
        sql: "SELECT bidder, SUM(price) AS total_price, COUNT(price) AS bid_count, AVG(price) AS avg_price FROM bid GROUP BY bidder ORDER BY total_price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "global_sort_limit_topn",
        sql: "SELECT auction, price FROM bid ORDER BY price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "topn_hidden_sort_key",
        sql: "SELECT auction FROM bid ORDER BY price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "topn_expression_sort_key",
        sql: "SELECT auction, price FROM bid ORDER BY price * 2 DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "topn_hidden_expression_sort_key",
        sql: "SELECT auction FROM bid ORDER BY price * 2 DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "filter_over_topn",
        sql: "SELECT auction, price FROM (SELECT auction, price FROM bid ORDER BY price DESC LIMIT 5) t WHERE price > 100",
    },
    ValidPlanRuntimeCase {
        id: "filter_over_row_number_topn",
        sql: "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) ranked WHERE rn <= 2 AND price > 100",
    },
    ValidPlanRuntimeCase {
        id: "ordered_over_topn",
        sql: "SELECT auction, price FROM (SELECT auction, price FROM bid ORDER BY price DESC LIMIT 5) t ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "ordered_over_row_number_topn",
        sql: "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bid) ranked WHERE rn <= 5 ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_topn_hidden_sort_key",
        sql: "SELECT auction FROM bid GROUP BY auction ORDER BY SUM(price) DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "partitioned_row_number_topn",
        sql: "SELECT auction, bidder, price, channel, url, \"dateTime\", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY \"dateTime\" DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 1",
    },
    ValidPlanRuntimeCase {
        id: "global_row_number_topn",
        sql: "SELECT auction, bidder, price FROM (SELECT auction, bidder, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 5",
    },
    ValidPlanRuntimeCase {
        id: "reversed_row_number_topn",
        sql: "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) ranked WHERE 2 >= rn",
    },
    ValidPlanRuntimeCase {
        id: "row_number_equal_top1",
        sql: "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) ranked WHERE rn = 1",
    },
    ValidPlanRuntimeCase {
        id: "row_number_equal_second",
        sql: "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) ranked WHERE rn = 2",
    },
    ValidPlanRuntimeCase {
        id: "row_number_alias_projection",
        sql: "SELECT auction, bidder, price, \"bidTime\" FROM (SELECT b.auction, b.bidder, b.price, b.\"dateTime\" AS \"bidTime\", ROW_NUMBER() OVER (PARTITION BY b.auction ORDER BY b.price DESC, b.\"dateTime\" ASC) AS rownum FROM bid b) ranked WHERE rownum <= 1",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_distinct_subquery",
        sql: "SELECT COUNT(auction) AS c FROM (SELECT DISTINCT auction, bidder FROM bid) d",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_aggregate",
        sql: "SELECT SUM(total) AS grand_total FROM (SELECT auction, SUM(price) AS total FROM bid GROUP BY auction) a",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_global_aggregate",
        sql: "SELECT SUM(\"count(*)\") AS grand_total FROM (SELECT COUNT(*) FROM bid) a",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_aggregate",
        sql: "SELECT DISTINCT auction, total FROM (SELECT auction, SUM(price) AS total FROM bid GROUP BY auction) a",
    },
    ValidPlanRuntimeCase {
        id: "join_over_global_aggregate",
        sql: "SELECT a.\"count(*)\", p.name FROM (SELECT COUNT(*) FROM bid) a JOIN person p ON a.\"count(*)\" = p.id",
    },
    ValidPlanRuntimeCase {
        id: "union_over_global_aggregate",
        sql: "SELECT key FROM (SELECT \"count(*)\" AS key FROM (SELECT COUNT(*) FROM bid) a UNION ALL SELECT \"count(*)\" AS key FROM (SELECT COUNT(*) FROM auction) b) u",
    },
    ValidPlanRuntimeCase {
        id: "topn_over_global_aggregate",
        sql: "SELECT \"count(*)\" FROM (SELECT COUNT(*) FROM bid) a ORDER BY \"count(*)\" DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_topn",
        sql: "SELECT DISTINCT auction FROM (SELECT auction, price FROM bid ORDER BY price DESC LIMIT 5) t",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_row_number_topn",
        sql: "SELECT DISTINCT auction FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bid) t WHERE rn <= 5",
    },
    ValidPlanRuntimeCase {
        id: "subquery_alias_projection",
        sql: "SELECT auction FROM (SELECT auction, price FROM bid WHERE price > 100) q WHERE auction > 0",
    },
    ValidPlanRuntimeCase {
        id: "union_duplicate_source",
        sql: "SELECT auction FROM (SELECT auction, price FROM bid WHERE price > 100 UNION ALL SELECT auction, price FROM bid WHERE price <= 100) u",
    },
    ValidPlanRuntimeCase {
        id: "union_filter_projection_pushdown",
        sql: "SELECT auction FROM (SELECT auction, price FROM bid UNION ALL SELECT auction, price FROM bid) u WHERE price > 100",
    },
    ValidPlanRuntimeCase {
        id: "three_input_union",
        sql: "SELECT auction FROM bid UNION ALL SELECT auction FROM bid UNION ALL SELECT auction FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "union_distinct",
        sql: "SELECT auction FROM bid UNION SELECT auction FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "ordered_union_all",
        sql: "SELECT auction FROM bid UNION ALL SELECT auction FROM bid ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "ordered_union_distinct",
        sql: "SELECT auction FROM bid UNION SELECT auction FROM bid ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_union",
        sql: "SELECT DISTINCT auction FROM (SELECT auction FROM bid WHERE price > 100 UNION ALL SELECT auction FROM bid WHERE price <= 100) u",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_distinct_union",
        sql: "SELECT COUNT(key) AS c FROM (SELECT DISTINCT key FROM (SELECT auction AS key FROM bid UNION ALL SELECT id AS key FROM auction) u) d",
    },
    ValidPlanRuntimeCase {
        id: "union_over_distinct",
        sql: "SELECT key FROM (SELECT DISTINCT auction AS key FROM bid UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "union_aggregate",
        sql: "SELECT key, COUNT(*) AS row_count FROM (SELECT auction AS key FROM bid UNION ALL SELECT id AS key FROM auction) u GROUP BY key",
    },
    ValidPlanRuntimeCase {
        id: "union_join",
        sql: "SELECT u.key, p.name FROM (SELECT seller AS key FROM auction UNION ALL SELECT bidder AS key FROM bid) u JOIN person p ON u.key = p.id",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_union_join",
        sql: "SELECT COUNT(key) AS c FROM (SELECT u.key, p.name FROM (SELECT seller AS key FROM auction UNION ALL SELECT bidder AS key FROM bid) u JOIN person p ON u.key = p.id) j",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_union_join",
        sql: "SELECT DISTINCT key FROM (SELECT u.key, p.name FROM (SELECT seller AS key FROM auction UNION ALL SELECT bidder AS key FROM bid) u JOIN person p ON u.key = p.id) j",
    },
    ValidPlanRuntimeCase {
        id: "union_over_join",
        sql: "SELECT key FROM (SELECT b.auction AS key FROM bid b JOIN auction a ON b.auction = a.id UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "join_over_union_aggregate",
        sql: "SELECT u.key, p.name FROM (SELECT key, COUNT(*) AS c FROM (SELECT auction AS key FROM bid UNION ALL SELECT id AS key FROM auction) x GROUP BY key) u JOIN person p ON u.key = p.id",
    },
    ValidPlanRuntimeCase {
        id: "union_topn",
        sql: "SELECT key FROM (SELECT auction AS key FROM bid UNION ALL SELECT id AS key FROM auction) u ORDER BY key DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "topn_over_union_join",
        sql: "SELECT key, name FROM (SELECT u.key, p.name FROM (SELECT seller AS key FROM auction UNION ALL SELECT bidder AS key FROM bid) u JOIN person p ON u.key = p.id) j ORDER BY key DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "union_over_topn",
        sql: "SELECT key FROM (SELECT key FROM (SELECT auction AS key, price FROM bid ORDER BY price DESC LIMIT 5) t UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "union_over_row_number_topn",
        sql: "SELECT key FROM (SELECT auction AS key FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bid) t WHERE rn <= 5 UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "union_over_aggregate",
        sql: "SELECT key FROM (SELECT auction AS key FROM (SELECT auction, COUNT(*) AS c FROM bid GROUP BY auction) a UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "having_aggregate",
        sql: "SELECT auction, SUM(price) AS total FROM bid GROUP BY auction HAVING SUM(price) > 1000",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_projection_prune",
        sql: "SELECT auction, total_price FROM (SELECT auction, SUM(price) AS total_price, COUNT(price) AS bid_count, AVG(price) AS avg_price FROM bid GROUP BY auction) a",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_having_projection_prune",
        sql: "SELECT auction, total_price FROM (SELECT auction, SUM(price) AS total_price, COUNT(price) AS bid_count, AVG(price) AS avg_price FROM bid GROUP BY auction) a WHERE bid_count > 1",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_group_key_and_value_filter",
        sql: "SELECT auction, COUNT(price) AS bid_count FROM bid GROUP BY auction HAVING auction > 10 AND COUNT(price) > 1",
    },
    ValidPlanRuntimeCase {
        id: "global_count",
        sql: "SELECT COUNT(*) AS c FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "global_stats_aggregate",
        sql: "SELECT SUM(price) AS total_price, AVG(price) AS avg_price, MIN(price) AS min_price, MAX(price) AS max_price FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "ordered_grouped_aggregate",
        sql: "SELECT auction, SUM(price) AS total FROM bid GROUP BY auction ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "ordered_count_aggregate",
        sql: "SELECT auction, COUNT(*) AS bid_count FROM bid GROUP BY auction ORDER BY auction",
    },
    ValidPlanRuntimeCase {
        id: "global_sort_limit_offset_topn",
        sql: "SELECT auction, price FROM bid ORDER BY price DESC LIMIT 5 OFFSET 2",
    },
    ValidPlanRuntimeCase {
        id: "union_different_sources",
        sql: "SELECT id AS key FROM auction UNION ALL SELECT auction AS key FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "filtered_distinct_aggregate",
        sql: "SELECT COUNT(*) FILTER (WHERE price > 100) AS filtered_rows, COUNT(DISTINCT bidder) FILTER (WHERE price > 100) AS filtered_distinct_bidders FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "string_distinct_count_aggregate",
        sql: "SELECT COUNT(DISTINCT channel) AS distinct_channels FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "timestamp_min_max_aggregate",
        sql: "SELECT MIN(\"dateTime\") AS first_bid_time, MAX(\"dateTime\") AS last_bid_time FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "timestamp_distinct_count_aggregate",
        sql: "SELECT COUNT(DISTINCT \"dateTime\") AS distinct_bid_times FROM bid",
    },
    ValidPlanRuntimeCase {
        id: "hop_window_aggregate",
        sql: "SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)",
    },
    ValidPlanRuntimeCase {
        id: "window_aggregate_topn",
        sql: "SELECT auction, COUNT(*) FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000) ORDER BY \"count(*)\" DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_window_aggregate",
        sql: "SELECT SUM(\"count(*)\") AS total_num FROM (SELECT auction, COUNT(*) FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)) w",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_window_aggregate",
        sql: "SELECT DISTINCT auction FROM (SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)) w",
    },
    ValidPlanRuntimeCase {
        id: "join_over_window_aggregate",
        sql: "SELECT w.auction, a.seller FROM (SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)) w JOIN auction a ON w.auction = a.id",
    },
    ValidPlanRuntimeCase {
        id: "union_over_window_aggregate",
        sql: "SELECT key FROM (SELECT auction AS key FROM (SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)) w UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "hop_allowed_lateness_window",
        sql: "SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000, 1500)",
    },
    ValidPlanRuntimeCase {
        id: "tumble_window_aggregate",
        sql: "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE(\"dateTime\", 10000)",
    },
    ValidPlanRuntimeCase {
        id: "tumble_allowed_lateness_window",
        sql: "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE(\"dateTime\", 10000, 750)",
    },
    ValidPlanRuntimeCase {
        id: "session_window_aggregate",
        sql: "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, SESSION(\"dateTime\", 5000)",
    },
    ValidPlanRuntimeCase {
        id: "session_allowed_lateness_window",
        sql: "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, SESSION(\"dateTime\", 5000, 1200)",
    },
    ValidPlanRuntimeCase {
        id: "asof_join",
        sql: "SELECT a.id, b.price FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") ON a.id = b.auction",
    },
    ValidPlanRuntimeCase {
        id: "asof_topn",
        sql: "SELECT a.id, b.price FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") ON a.id = b.auction ORDER BY b.price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_over_asof",
        sql: "SELECT COUNT(price) AS c FROM (SELECT a.id, b.price FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") ON a.id = b.auction) q",
    },
    ValidPlanRuntimeCase {
        id: "distinct_over_asof",
        sql: "SELECT DISTINCT id FROM (SELECT a.id, b.price FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") ON a.id = b.auction) q",
    },
    ValidPlanRuntimeCase {
        id: "union_over_asof",
        sql: "SELECT key FROM (SELECT a.id AS key FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") ON a.id = b.auction UNION ALL SELECT id AS key FROM auction) u",
    },
    ValidPlanRuntimeCase {
        id: "asof_join_without_equi_keys",
        sql: "SELECT a.id, b.price FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\")",
    },
];

const GENERATED_PLAN_INPUTS: &[(&str, &str)] = &[
    (
        "scan_filter",
        "SELECT auction AS key, price AS value FROM bid WHERE price > 100",
    ),
    (
        "expression_projection",
        "SELECT auction + 1 AS key, price * 2 AS value FROM bid WHERE price * 2 > 100",
    ),
    (
        "case_projection",
        "SELECT auction AS key, CASE WHEN price > 100 THEN price ELSE 0 END AS value FROM bid",
    ),
    (
        "udf_projection",
        "SELECT auction AS key, COUNT_CHAR(extra, 'c') AS value FROM bid",
    ),
    (
        "cte_filter_projection",
        "WITH high_bids AS (SELECT auction AS key, price AS value FROM bid WHERE price > 100) SELECT key, value FROM high_bids",
    ),
    (
        "cte_aggregate",
        "WITH totals AS (SELECT auction AS key, SUM(price) AS value FROM bid GROUP BY auction) SELECT key, value FROM totals",
    ),
    (
        "values",
        "SELECT key, value FROM (VALUES (1, 100), (2, 200), (3, 300)) AS t(key, value)",
    ),
    (
        "in_subquery",
        "SELECT auction AS key, price AS value FROM bid WHERE auction IN (SELECT id FROM auction)",
    ),
    (
        "not_in_subquery",
        "SELECT auction AS key, price AS value FROM bid WHERE auction NOT IN (SELECT id FROM auction)",
    ),
    (
        "exists_subquery",
        "SELECT b.auction AS key, b.price AS value FROM bid b WHERE EXISTS (SELECT 1 FROM auction a WHERE a.id = b.auction)",
    ),
    (
        "not_exists_subquery",
        "SELECT b.auction AS key, b.price AS value FROM bid b WHERE NOT EXISTS (SELECT 1 FROM auction a WHERE a.id = b.auction)",
    ),
    (
        "scalar_subquery_filter",
        "SELECT auction AS key, price AS value FROM bid WHERE price > (SELECT MIN(\"initialBid\") FROM auction)",
    ),
    (
        "distinct_in_subquery",
        "SELECT auction AS key, price AS value FROM bid WHERE auction IN (SELECT DISTINCT id FROM auction)",
    ),
    (
        "aggregate_in_subquery",
        "SELECT auction AS key, price AS value FROM bid WHERE auction IN (SELECT seller FROM auction GROUP BY seller)",
    ),
    (
        "intersect_sources",
        "SELECT auction AS key, price AS value FROM bid INTERSECT SELECT id AS key, \"initialBid\" AS value FROM auction",
    ),
    (
        "except_sources",
        "SELECT auction AS key, price AS value FROM bid EXCEPT SELECT id AS key, \"initialBid\" AS value FROM auction",
    ),
    (
        "distinct",
        "SELECT DISTINCT auction AS key, bidder AS value FROM bid",
    ),
    (
        "aggregate",
        "SELECT auction AS key, SUM(price) AS value FROM bid GROUP BY auction",
    ),
    (
        "expression_group_aggregate",
        "SELECT auction % 10 AS key, SUM(price) AS value FROM bid GROUP BY auction % 10",
    ),
    (
        "distinct_count_aggregate",
        "SELECT auction AS key, COUNT(DISTINCT bidder) AS value FROM bid GROUP BY auction",
    ),
    (
        "having_aggregate",
        "SELECT auction AS key, SUM(price) AS value FROM bid GROUP BY auction HAVING SUM(price) > 1000",
    ),
    (
        "window_aggregate",
        "SELECT auction AS key, SUM(price) AS value FROM bid GROUP BY auction, HOP(\"dateTime\", 2000, 10000)",
    ),
    (
        "global_aggregate",
        "SELECT 0 AS key, SUM(price) AS value FROM bid",
    ),
    (
        "filtered_distinct_aggregate",
        "SELECT 0 AS key, COUNT(DISTINCT bidder) FILTER (WHERE price > 100) AS value FROM bid",
    ),
    (
        "aggregate_topn",
        "SELECT auction AS key, SUM(price) AS value FROM bid GROUP BY auction ORDER BY value DESC LIMIT 5",
    ),
    (
        "topn",
        "SELECT auction AS key, price AS value FROM bid ORDER BY price DESC LIMIT 5",
    ),
    (
        "offset_topn",
        "SELECT auction AS key, price AS value FROM bid ORDER BY price DESC LIMIT 5 OFFSET 2",
    ),
    (
        "row_number_topn",
        "SELECT auction AS key, price AS value FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bid) t WHERE rn <= 5",
    ),
    (
        "partitioned_row_number_topn",
        "SELECT auction AS key, price AS value FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) t WHERE rn <= 2",
    ),
    (
        "join",
        "SELECT b.auction AS key, b.price AS value FROM bid b JOIN auction a ON b.auction = a.id",
    ),
    (
        "residual_join",
        "SELECT b.auction AS key, b.price AS value FROM bid b JOIN auction a ON b.auction = a.id AND b.price > a.\"initialBid\"",
    ),
    (
        "expression_join",
        "SELECT b.auction AS key, b.price AS value FROM bid b JOIN auction a ON b.auction % 10000 = a.id % 10000",
    ),
    (
        "left_join",
        "SELECT b.auction AS key, a.seller AS value FROM bid b LEFT JOIN auction a ON b.auction = a.id",
    ),
    (
        "right_join",
        "SELECT b.auction AS key, a.seller AS value FROM bid b RIGHT JOIN auction a ON b.auction = a.id",
    ),
    (
        "full_join",
        "SELECT b.auction AS key, a.seller AS value FROM bid b FULL OUTER JOIN auction a ON b.auction = a.id",
    ),
    (
        "semi_join",
        "SELECT b.auction AS key, b.price AS value FROM bid b LEFT SEMI JOIN auction a ON b.auction = a.id",
    ),
    (
        "anti_join",
        "SELECT b.auction AS key, b.price AS value FROM bid b LEFT ANTI JOIN auction a ON b.auction = a.id",
    ),
    (
        "right_semi_join",
        "SELECT a.id AS key, a.seller AS value FROM bid b RIGHT SEMI JOIN auction a ON b.auction = a.id",
    ),
    (
        "right_anti_join",
        "SELECT a.id AS key, a.seller AS value FROM bid b RIGHT ANTI JOIN auction a ON b.auction = a.id",
    ),
    (
        "range_join",
        "SELECT a.id AS key, b.price AS value FROM auction a JOIN bid b ON b.price >= a.\"initialBid\" AND b.price < a.reserve",
    ),
    (
        "self_join",
        "SELECT l.auction AS key, r.price AS value FROM bid l JOIN bid r ON l.auction = r.auction WHERE l.price < r.price",
    ),
    (
        "three_way_join",
        "SELECT a.seller AS key, b.price AS value FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction",
    ),
    (
        "join_topn",
        "SELECT b.auction AS key, b.price AS value FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 5",
    ),
    (
        "join_top_avg",
        "SELECT auction AS key, CAST(avg_price AS BIGINT) AS value FROM (SELECT b.auction, AVG(b.price) AS avg_price FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j ORDER BY avg_price DESC LIMIT 5",
    ),
    (
        "union",
        "SELECT auction AS key, price AS value FROM bid UNION ALL SELECT id AS key, \"initialBid\" AS value FROM auction",
    ),
    (
        "three_input_union",
        "SELECT auction AS key, price AS value FROM bid UNION ALL SELECT id AS key, \"initialBid\" AS value FROM auction UNION ALL SELECT bidder AS key, price AS value FROM bid",
    ),
    (
        "union_distinct",
        "SELECT auction AS key, price AS value FROM bid UNION SELECT id AS key, \"initialBid\" AS value FROM auction",
    ),
    (
        "intersect",
        "SELECT auction AS key, price AS value FROM bid INTERSECT SELECT auction AS key, price AS value FROM bid WHERE price > 100",
    ),
    (
        "except",
        "SELECT auction AS key, price AS value FROM bid EXCEPT SELECT auction AS key, price AS value FROM bid WHERE price <= 100",
    ),
    (
        "distinct_union",
        "SELECT DISTINCT key, value FROM (SELECT auction AS key, price AS value FROM bid UNION ALL SELECT id AS key, \"initialBid\" AS value FROM auction) u",
    ),
    (
        "aggregate_join",
        "SELECT a.auction AS key, au.seller AS value FROM (SELECT auction, MAX(price) AS max_price FROM bid GROUP BY auction) a JOIN auction au ON a.auction = au.id",
    ),
    (
        "asof_join",
        "SELECT a.id AS key, b.price AS value FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") ON a.id = b.auction",
    ),
    (
        "asof_join_without_equi_keys",
        "SELECT a.id AS key, b.price AS value FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\")",
    ),
];

fn generated_dbsp_runtime_plan_cases() -> Vec<GeneratedPlanRuntimeCase> {
    let mut cases = Vec::new();
    for (input_id, input_sql) in GENERATED_PLAN_INPUTS {
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_identity_over_{input_id}"),
            sql: format!("SELECT key, value FROM ({input_sql}) s"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_filter_over_{input_id}"),
            sql: format!("SELECT key, value FROM ({input_sql}) s WHERE value > 100"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_ordered_over_{input_id}"),
            sql: format!("SELECT key, value FROM ({input_sql}) s ORDER BY key"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_distinct_over_{input_id}"),
            sql: format!("SELECT DISTINCT key FROM ({input_sql}) s"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_grouped_aggregate_over_{input_id}"),
            sql: format!("SELECT key, SUM(value) AS total FROM ({input_sql}) s GROUP BY key"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_global_aggregate_over_{input_id}"),
            sql: format!("SELECT SUM(value) AS total FROM ({input_sql}) s"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_topn_over_{input_id}"),
            sql: format!("SELECT key, value FROM ({input_sql}) s ORDER BY value DESC LIMIT 3"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_join_over_{input_id}"),
            sql: format!("SELECT s.key, p.name FROM ({input_sql}) s JOIN person p ON s.key = p.id"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_join_as_right_over_{input_id}"),
            sql: format!("SELECT s.key, p.name FROM person p JOIN ({input_sql}) s ON p.id = s.key"),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_left_join_as_right_over_{input_id}"),
            sql: format!(
                "SELECT p.id AS key, s.value AS value FROM person p LEFT JOIN ({input_sql}) s ON p.id = s.key"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_right_join_as_right_over_{input_id}"),
            sql: format!(
                "SELECT p.id AS key, s.value AS value FROM person p RIGHT JOIN ({input_sql}) s ON p.id = s.key"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_full_join_as_right_over_{input_id}"),
            sql: format!(
                "SELECT p.id AS key, s.value AS value FROM person p FULL OUTER JOIN ({input_sql}) s ON p.id = s.key"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_left_semi_join_as_right_over_{input_id}"),
            sql: format!(
                "SELECT p.id AS key FROM person p LEFT SEMI JOIN ({input_sql}) s ON p.id = s.key"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_left_anti_join_as_right_over_{input_id}"),
            sql: format!(
                "SELECT p.id AS key FROM person p LEFT ANTI JOIN ({input_sql}) s ON p.id = s.key"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_right_semi_join_as_right_over_{input_id}"),
            sql: format!(
                "SELECT s.key FROM person p RIGHT SEMI JOIN ({input_sql}) s ON p.id = s.key"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_right_anti_join_as_right_over_{input_id}"),
            sql: format!(
                "SELECT s.key FROM person p RIGHT ANTI JOIN ({input_sql}) s ON p.id = s.key"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_left_join_over_{input_id}"),
            sql: format!(
                "SELECT s.key, a.seller FROM ({input_sql}) s LEFT JOIN auction a ON s.key = a.id"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_right_join_over_{input_id}"),
            sql: format!(
                "SELECT s.key, a.seller FROM ({input_sql}) s RIGHT JOIN auction a ON s.key = a.id"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_full_join_over_{input_id}"),
            sql: format!(
                "SELECT s.key, a.seller FROM ({input_sql}) s FULL OUTER JOIN auction a ON s.key = a.id"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_left_semi_join_over_{input_id}"),
            sql: format!(
                "SELECT s.key FROM ({input_sql}) s LEFT SEMI JOIN auction a ON s.key = a.id"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_left_anti_join_over_{input_id}"),
            sql: format!(
                "SELECT s.key FROM ({input_sql}) s LEFT ANTI JOIN auction a ON s.key = a.id"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_right_semi_join_over_{input_id}"),
            sql: format!(
                "SELECT a.id FROM ({input_sql}) s RIGHT SEMI JOIN auction a ON s.key = a.id"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_right_anti_join_over_{input_id}"),
            sql: format!(
                "SELECT a.id FROM ({input_sql}) s RIGHT ANTI JOIN auction a ON s.key = a.id"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_union_over_{input_id}"),
            sql: format!(
                "SELECT key FROM (SELECT key FROM ({input_sql}) s UNION ALL SELECT id AS key FROM auction) u"
            ),
        });
        cases.push(GeneratedPlanRuntimeCase {
            id: format!("generated_union_distinct_over_{input_id}"),
            sql: format!(
                "SELECT key FROM (SELECT key FROM ({input_sql}) s UNION SELECT id AS key FROM auction) u"
            ),
        });
    }
    cases
}

#[tokio::test]
async fn guards_nexmark_query_coverage_regressions() {
    let actual = collect_coverage().await.expect("collect coverage");
    eprintln!("current nexmark coverage:\n{}", render_status_json(&actual));
    for (query, result) in &actual {
        if let Some(error) = &result.error {
            eprintln!("{query} diagnostic: {error}");
        }
    }

    let expected_ids = CANONICAL_NEXMARK_QUERY_IDS
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let actual_ids = actual.keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        actual_ids, expected_ids,
        "coverage harness did not evaluate exactly the canonical query ids",
    );

    let mut failures = Vec::new();
    for query in &expected_ids {
        let result = actual
            .get(query)
            .expect("actual query set already checked against canonical ids");
        let current = &result.coverage;
        if !current.logical_planner {
            failures.push(format!(
                "{query}: logical planner failed ({})",
                result.error.as_deref().unwrap_or("no diagnostic provided")
            ));
        }
        if !current.circuit_planner {
            failures.push(format!(
                "{query}: circuit planner failed ({})",
                result.error.as_deref().unwrap_or("no diagnostic provided")
            ));
        }
        if !current.runtime_validation {
            failures.push(format!(
                "{query}: runtime validation failed ({})",
                result.error.as_deref().unwrap_or("no diagnostic provided")
            ));
        }
    }

    if !failures.is_empty() {
        let summary = render_status_json(&actual);
        panic!(
            "Nexmark query coverage failure(s):\n{}\n\nCurrent status snapshot:\n{}",
            failures.join("\n"),
            summary
        );
    }
}

#[tokio::test]
async fn guards_active_vectorized_runtime_nexmark_columnar_subset() {
    let actual = collect_active_runtime_coverage()
        .await
        .expect("collect active runtime coverage");
    eprintln!(
        "current active vectorized runtime nexmark coverage:\n{}",
        render_active_runtime_status_json(&actual)
    );
    for (query, result) in &actual {
        if let Some(error) = &result.error {
            eprintln!("{query} diagnostic: {error}");
        }
    }

    let expected_supported = [
        "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q12", "q13", "q14", "q15",
        "q16", "q17", "q18", "q19", "q20", "q21", "q22",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    for query in expected_supported {
        let result = actual
            .get(query)
            .unwrap_or_else(|| panic!("coverage did not evaluate {query}"));
        assert!(
            result.coverage.vectorized_runtime,
            "{query}: active vectorized runtime rejected supported columnar stateless plan ({})",
            result.error.as_deref().unwrap_or("no diagnostic provided")
        );
        assert_ne!(
            result.coverage.execution_mode.as_deref(),
            Some("columnar_composed"),
            "{query}: active vectorized runtime used generic columnar composed fallback"
        );
        assert_ne!(
            result.coverage.execution_mode.as_deref(),
            Some("full_refresh"),
            "{query}: active vectorized runtime used full-refresh fallback"
        );
    }
}

#[tokio::test]
async fn guards_active_vectorized_runtime_valid_dbsp_plan_shapes() {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions().expect("load nexmark source definitions"));
    let planner = DbspPlanBuilder::new(nexmark_config().expect("load nexmark planner config"));
    let available_sources = available_nexmark_sources();

    let mut failures = Vec::new();
    let mut execution_modes = BTreeMap::new();
    for case in VALID_DBSP_RUNTIME_PLAN_CASES {
        match validate_active_vectorized_runtime_case(
            &registry,
            &planner,
            &available_sources,
            *case,
        )
        .await
        {
            Ok(execution_mode) => {
                execution_modes.insert(case.id, execution_mode);
            }
            Err(err) => {
                failures.push(format!("{}: {err:#}", case.id));
            }
        }
    }
    eprintln!(
        "active vectorized runtime DBSP-valid shape modes:\n{}",
        serde_json::to_string_pretty(&execution_modes).expect("serialize execution modes")
    );
    for (case_id, execution_mode) in &execution_modes {
        if execution_mode == "columnar_composed" {
            failures.push(format!(
                "{case_id}: active vectorized runtime used generic columnar composed fallback"
            ));
        }
        if execution_mode == "full_refresh" {
            failures.push(format!(
                "{case_id}: active vectorized runtime used full-refresh fallback"
            ));
        }
    }
    let expected_modes = [
        ("join_topn", "columnar_join_topn"),
        ("filter_over_join_topn", "columnar_join_topn"),
        ("ordered_over_join_topn", "columnar_join_topn"),
        ("join_over_join_topn", "columnar_join"),
        ("join_over_join", "columnar_join"),
        ("topn_over_left_join", "columnar_join_topn"),
        ("join_over_topn", "columnar_join"),
        ("join_over_row_number_topn", "columnar_join"),
        ("join_over_partitioned_row_number_topn", "columnar_join"),
        ("ordered_over_topn", "columnar_topn"),
        ("ordered_over_row_number_topn", "columnar_topn"),
        ("topn_over_self_join", "columnar_join_topn"),
        ("topn_over_three_way_join", "columnar_multijoin"),
        ("join_over_three_way_join", "columnar_join"),
        ("union_join", "columnar_join"),
        ("aggregate_join", "columnar_join"),
        ("join_over_join_aggregate", "columnar_join"),
        ("join_over_global_aggregate", "columnar_join"),
        ("join_over_window_aggregate", "columnar_join"),
    ];
    for (case_id, expected_mode) in expected_modes {
        if execution_modes.get(case_id).map(String::as_str) != Some(expected_mode) {
            failures.push(format!(
                "{case_id}: expected active vectorized runtime mode {expected_mode}, got {}",
                execution_modes
                    .get(case_id)
                    .map(String::as_str)
                    .unwrap_or("<missing>")
            ));
        }
    }

    if !failures.is_empty() {
        panic!(
            "active vectorized runtime rejected DBSP-valid plan shape(s):\n{}",
            failures.join("\n")
        );
    }
}

#[tokio::test]
async fn guards_generated_active_vectorized_runtime_dbsp_valid_compositions() {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions().expect("load nexmark source definitions"));
    let planner = DbspPlanBuilder::new(nexmark_config().expect("load nexmark planner config"));
    let available_sources = available_nexmark_sources();

    let mut failures = Vec::new();
    let mut skipped = BTreeMap::new();
    let mut execution_modes = BTreeMap::new();
    for case in generated_dbsp_runtime_plan_cases() {
        match validate_generated_active_vectorized_runtime_case(
            &registry,
            &planner,
            &available_sources,
            &case,
        )
        .await
        {
            Ok(GeneratedRuntimeCaseResult::RuntimeMode(execution_mode)) => {
                execution_modes.insert(case.id, execution_mode);
            }
            Ok(GeneratedRuntimeCaseResult::DbspUnsupported(reason)) => {
                skipped.insert(case.id, reason);
            }
            Err(err) => {
                failures.push(format!("{}: {err:#}", case.id));
            }
        }
    }

    eprintln!(
        "generated active vectorized runtime DBSP-valid shape modes:\n{}",
        serde_json::to_string_pretty(&execution_modes).expect("serialize execution modes")
    );
    eprintln!(
        "generated active vectorized runtime DBSP-valid shape count: {}",
        execution_modes.len()
    );
    eprintln!(
        "generated active vectorized runtime DBSP-unsupported shape count: {}",
        skipped.len()
    );
    eprintln!(
        "generated active vectorized runtime DBSP-unsupported shapes:\n{}",
        serde_json::to_string_pretty(&skipped).expect("serialize skipped generated cases")
    );
    assert!(
        execution_modes.len() >= 1100,
        "generated coverage unexpectedly shrank: {} DBSP-valid cases, {} DBSP-unsupported cases",
        execution_modes.len(),
        skipped.len()
    );

    for (case_id, execution_mode) in &execution_modes {
        if execution_mode == "columnar_composed" {
            failures.push(format!(
                "{case_id}: active vectorized runtime used generic columnar composed fallback"
            ));
        }
        if execution_mode == "full_refresh" {
            failures.push(format!(
                "{case_id}: active vectorized runtime used full-refresh fallback"
            ));
        }
    }

    if !failures.is_empty() {
        panic!(
            "active vectorized runtime rejected generated DBSP-valid plan shape(s):\n{}",
            failures.join("\n")
        );
    }
}

async fn collect_coverage() -> Result<BTreeMap<String, QueryCoverageResult>> {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions()?);

    let available_sources = available_nexmark_sources();

    let planner = DbspPlanBuilder::new(nexmark_config()?);

    let mut out = BTreeMap::new();
    for query in canonical_nexmark_queries() {
        let definition = match parse_materialized_view(&format!(
            "CREATE MATERIALIZED VIEW {} AS {}",
            query.id, query.sql
        )) {
            Ok(definition) => definition,
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    QueryCoverageResult {
                        coverage: QueryCoverage {
                            logical_planner: false,
                            circuit_planner: false,
                            runtime_validation: false,
                        },
                        error: Some(format!("SQL parse failed: {err}")),
                    },
                );
                continue;
            }
        };

        let logical = match plan_materialized_views(&registry, &[definition]).await {
            Ok(plans) => plans,
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    QueryCoverageResult {
                        coverage: QueryCoverage {
                            logical_planner: false,
                            circuit_planner: false,
                            runtime_validation: false,
                        },
                        error: Some(format!("logical planning failed: {err}")),
                    },
                );
                continue;
            }
        };

        let logical_plan = logical[0].logical_plan();
        let circuit = match planner.build(logical_plan) {
            Ok(plan) => plan,
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    QueryCoverageResult {
                        coverage: QueryCoverage {
                            logical_planner: true,
                            circuit_planner: false,
                            runtime_validation: false,
                        },
                        error: Some(format!("circuit planning failed: {err}")),
                    },
                );
                continue;
            }
        };

        match validate_dbsp_plan(&circuit, &available_sources, query.id) {
            Ok(_) => {
                out.insert(
                    query.id.to_string(),
                    QueryCoverageResult {
                        coverage: QueryCoverage {
                            logical_planner: true,
                            circuit_planner: true,
                            runtime_validation: true,
                        },
                        error: None,
                    },
                );
            }
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    QueryCoverageResult {
                        coverage: QueryCoverage {
                            logical_planner: true,
                            circuit_planner: true,
                            runtime_validation: false,
                        },
                        error: Some(format!("runtime validation failed: {err}")),
                    },
                );
            }
        }
    }

    Ok(out)
}

async fn validate_generated_active_vectorized_runtime_case(
    registry: &SourceRegistry,
    planner: &DbspPlanBuilder,
    available_sources: &BTreeSet<String>,
    case: &GeneratedPlanRuntimeCase,
) -> Result<GeneratedRuntimeCaseResult> {
    let definition = parse_materialized_view(&format!(
        "CREATE MATERIALIZED VIEW {} AS {}",
        case.id, case.sql
    ))
    .with_context(|| format!("SQL parse failed for {}", case.id))?;
    let logical = plan_materialized_views(registry, &[definition])
        .await
        .with_context(|| format!("logical planning failed for {}", case.id))?;
    let planned = logical
        .first()
        .with_context(|| format!("logical planner produced no MV plan for {}", case.id))?;
    let circuit = match planner.build(planned.logical_plan()) {
        Ok(plan) => plan,
        Err(err) => {
            return Ok(GeneratedRuntimeCaseResult::DbspUnsupported(format!(
                "DBSP circuit planning failed: {err:#}"
            )));
        }
    };
    if let Err(err) = validate_dbsp_plan(&circuit, available_sources, &case.id) {
        return Ok(GeneratedRuntimeCaseResult::DbspUnsupported(format!(
            "DBSP circuit validation failed: {err:#}"
        )));
    }

    let output_schema = df_schema_to_arrow(planned.logical_plan().schema())
        .with_context(|| format!("Arrow schema conversion failed for {}", case.id))?;
    let state_table = build_operator_state_table(&case.id).await?;
    let mv_plan = VectorizedMaterializedViewPlan::new(
        planned.definition().name().to_string(),
        planned.definition().query().to_string(),
        output_schema,
    );
    let runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
        registry,
        vec![mv_plan],
        Arc::new(MaterializedViewRegistry::new()),
        planner_udfs(),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(state_table),
    )
    .await
    .with_context(|| format!("active vectorized runtime rejected {}", case.id))?;

    runtime
        .materialized_view_execution_modes()
        .into_iter()
        .find(|(view_name, _)| *view_name == case.id)
        .map(|(_, mode)| GeneratedRuntimeCaseResult::RuntimeMode(mode.to_string()))
        .with_context(|| format!("runtime did not expose execution mode for {}", case.id))
}

async fn validate_active_vectorized_runtime_case(
    registry: &SourceRegistry,
    planner: &DbspPlanBuilder,
    available_sources: &BTreeSet<String>,
    case: ValidPlanRuntimeCase,
) -> Result<String> {
    let definition = parse_materialized_view(&format!(
        "CREATE MATERIALIZED VIEW {} AS {}",
        case.id, case.sql
    ))
    .with_context(|| format!("SQL parse failed for {}", case.id))?;
    let logical = plan_materialized_views(registry, &[definition])
        .await
        .with_context(|| format!("logical planning failed for {}", case.id))?;
    let planned = logical
        .first()
        .with_context(|| format!("logical planner produced no MV plan for {}", case.id))?;
    let circuit = planner
        .build(planned.logical_plan())
        .with_context(|| format!("DBSP circuit planning failed for {}", case.id))?;
    validate_dbsp_plan(&circuit, available_sources, case.id)
        .with_context(|| format!("DBSP circuit validation failed for {}", case.id))?;

    let output_schema = df_schema_to_arrow(planned.logical_plan().schema())
        .with_context(|| format!("Arrow schema conversion failed for {}", case.id))?;
    let state_table = build_operator_state_table(case.id).await?;
    let mv_plan = VectorizedMaterializedViewPlan::new(
        planned.definition().name().to_string(),
        planned.definition().query().to_string(),
        output_schema,
    );
    let runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
        registry,
        vec![mv_plan],
        Arc::new(MaterializedViewRegistry::new()),
        planner_udfs(),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(state_table),
    )
    .await
    .with_context(|| format!("active vectorized runtime rejected {}", case.id))?;

    runtime
        .materialized_view_execution_modes()
        .into_iter()
        .find(|(view_name, _)| *view_name == case.id)
        .map(|(_, mode)| mode.to_string())
        .with_context(|| format!("runtime did not expose execution mode for {}", case.id))
}

async fn collect_active_runtime_coverage() -> Result<BTreeMap<String, ActiveRuntimeCoverageResult>>
{
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions()?);

    let mut out = BTreeMap::new();
    for query in canonical_nexmark_queries() {
        let definition = match parse_materialized_view(&format!(
            "CREATE MATERIALIZED VIEW {} AS {}",
            query.id, query.sql
        )) {
            Ok(definition) => definition,
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    ActiveRuntimeCoverageResult {
                        coverage: ActiveRuntimeCoverage {
                            vectorized_runtime: false,
                            execution_mode: None,
                        },
                        error: Some(format!("SQL parse failed: {err}")),
                    },
                );
                continue;
            }
        };

        let logical = match plan_materialized_views(&registry, &[definition]).await {
            Ok(plans) => plans,
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    ActiveRuntimeCoverageResult {
                        coverage: ActiveRuntimeCoverage {
                            vectorized_runtime: false,
                            execution_mode: None,
                        },
                        error: Some(format!("logical planning failed: {err}")),
                    },
                );
                continue;
            }
        };
        let planned = &logical[0];
        let output_schema = match df_schema_to_arrow(planned.logical_plan().schema()) {
            Ok(schema) => schema,
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    ActiveRuntimeCoverageResult {
                        coverage: ActiveRuntimeCoverage {
                            vectorized_runtime: false,
                            execution_mode: None,
                        },
                        error: Some(format!("Arrow schema conversion failed: {err}")),
                    },
                );
                continue;
            }
        };
        let state_table = build_operator_state_table(query.id).await?;
        let mv_plan = VectorizedMaterializedViewPlan::new(
            planned.definition().name().to_string(),
            planned.definition().query().to_string(),
            output_schema,
        );
        let runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
            &registry,
            vec![mv_plan],
            Arc::new(MaterializedViewRegistry::new()),
            planner_udfs(),
            VectorizedExecutionRuntimeOptions::default().with_operator_state_table(state_table),
        )
        .await;

        match runtime {
            Ok(runtime) => {
                let execution_mode = runtime
                    .materialized_view_execution_modes()
                    .into_iter()
                    .find(|(view_name, _)| *view_name == query.id)
                    .map(|(_, mode)| mode.to_string());
                out.insert(
                    query.id.to_string(),
                    ActiveRuntimeCoverageResult {
                        coverage: ActiveRuntimeCoverage {
                            vectorized_runtime: true,
                            execution_mode,
                        },
                        error: None,
                    },
                );
            }
            Err(err) => {
                out.insert(
                    query.id.to_string(),
                    ActiveRuntimeCoverageResult {
                        coverage: ActiveRuntimeCoverage {
                            vectorized_runtime: false,
                            execution_mode: None,
                        },
                        error: Some(format!("active vectorized runtime rejected plan: {err}")),
                    },
                );
            }
        }
    }

    Ok(out)
}

fn available_nexmark_sources() -> BTreeSet<String> {
    [
        "nexmark_person",
        "person",
        "nexmark_auction",
        "auction",
        "nexmark_bid",
        "bid",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

async fn build_operator_state_table(name: &str) -> Result<Arc<dyn KeyValueTable>> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(format!("nexmark-active-runtime-{name}"), store).await?);
    Ok(Arc::new(SlateTable::new(db)))
}

fn df_schema_to_arrow(schema: &DFSchemaRef) -> Result<SchemaRef> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

fn render_status_json(results: &BTreeMap<String, QueryCoverageResult>) -> String {
    let snapshot = CoverageSnapshot {
        queries: results
            .iter()
            .map(|(query, status)| (query.clone(), status.coverage.clone()))
            .collect(),
    };
    serde_json::to_string_pretty(&snapshot).expect("serialize coverage")
}

fn render_active_runtime_status_json(
    results: &BTreeMap<String, ActiveRuntimeCoverageResult>,
) -> String {
    let queries = results
        .iter()
        .map(|(query, status)| (query.clone(), status.coverage.clone()))
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string_pretty(&serde_json::json!({ "queries": queries }))
        .expect("serialize active runtime coverage")
}
