use std::ops::Range;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::storage::{KeyValueTable, SlateTable};
use floe_executor::{
    MaterializedViewRegistry, VectorizedExecutionRuntime, VectorizedExecutionRuntimeOptions,
    VectorizedMaterializedViewPlan,
};
use floe_node_core::generator;
use floe_node_core::planner::planner_udfs;
use floe_node_core::source::SourceRegistry;
use object_store::memory::InMemory;
use slatedb::Db;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;
use tokio::runtime::Runtime;

const ROWS_PER_TICK: usize = 8_192;
const TICKS: usize = 8;
const Q4_REPEATED_AUCTION_ROWS: usize = 10_000;
const Q4_REPEATED_BID_ROWS: usize = 1_000_000;
const Q4_REPEATED_BID_ROWS_PER_TICK: usize = 8_192;
const Q4_REPEATED_TICKS: usize =
    (Q4_REPEATED_BID_ROWS + Q4_REPEATED_BID_ROWS_PER_TICK - 1) / Q4_REPEATED_BID_ROWS_PER_TICK;
const Q4_QUERY: &str = r#"SELECT category, AVG(max) FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#;

#[derive(Clone, Copy)]
struct NexmarkRuntimeSource {
    source_name: &'static str,
    batch: fn(SchemaRef, usize, usize) -> Result<RecordBatch>,
}

#[derive(Clone, Copy)]
struct NexmarkRuntimeCase {
    id: &'static str,
    sources: &'static [NexmarkRuntimeSource],
    view_name: &'static str,
    query: &'static str,
    output_schema: fn() -> SchemaRef,
}

struct NexmarkRuntimeSourceBatches {
    source_name: &'static str,
    batches: Vec<RecordBatch>,
}

#[derive(Default)]
struct Q4RuntimeTiming {
    auction_append: Duration,
    auction_tick: Duration,
    bid_append: Duration,
    bid_tick: Duration,
    final_snapshot: Duration,
    bid_tick_samples: Vec<Duration>,
}

impl Q4RuntimeTiming {
    fn record_bid_tick(&mut self, elapsed: Duration) {
        self.bid_tick += elapsed;
        self.bid_tick_samples.push(elapsed);
    }

    fn print(&mut self, total: Duration) {
        self.bid_tick_samples.sort_unstable();
        let bid_tick_count = self.bid_tick_samples.len();
        let bid_tick_p50 = percentile_duration(&self.bid_tick_samples, 50);
        let bid_tick_p95 = percentile_duration(&self.bid_tick_samples, 95);
        let bid_tick_max = self.bid_tick_samples.last().copied().unwrap_or_default();
        eprintln!(
            "[q4-runtime-timing] total_ms={:.3} auction_append_ms={:.3} auction_tick_ms={:.3} bid_append_ms={:.3} bid_tick_ms={:.3} final_snapshot_ms={:.3} bid_tick_count={} bid_tick_mean_ms={:.3} bid_tick_p50_ms={:.3} bid_tick_p95_ms={:.3} bid_tick_max_ms={:.3}",
            duration_ms(total),
            duration_ms(self.auction_append),
            duration_ms(self.auction_tick),
            duration_ms(self.bid_append),
            duration_ms(self.bid_tick),
            duration_ms(self.final_snapshot),
            bid_tick_count,
            duration_ms_mean(self.bid_tick, bid_tick_count),
            duration_ms(bid_tick_p50),
            duration_ms(bid_tick_p95),
            duration_ms(bid_tick_max),
        );
    }
}

fn q4_timing_enabled() -> bool {
    std::env::var_os("FLOE_PROFILE_Q4_TIMING").is_some()
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn duration_ms_mean(total: Duration, count: usize) -> f64 {
    if count == 0 {
        0.0
    } else {
        duration_ms(total) / count as f64
    }
}

fn percentile_duration(sorted: &[Duration], percentile: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() - 1) * percentile) / 100;
    sorted[idx]
}

const PROFILE_CATEGORY_COUNT: usize = 9;
const PROFILE_CATEGORY_NAMES: [&str; PROFILE_CATEGORY_COUNT] = [
    "join",
    "grouped_max_state",
    "grouped_max_output",
    "grouped_stats_state",
    "grouped_stats_output",
    "zset_segment",
    "zset_manifest",
    "zset_state",
    "other",
];

#[derive(Default)]
struct KeyValueTableProfileMetrics {
    get_calls: AtomicU64,
    get_nanos: AtomicU64,
    get_calls_by_category: [AtomicU64; PROFILE_CATEGORY_COUNT],
    get_nanos_by_category: [AtomicU64; PROFILE_CATEGORY_COUNT],
    write_calls: AtomicU64,
    write_nanos: AtomicU64,
    scan_calls: AtomicU64,
    scan_nanos: AtomicU64,
    scan_calls_by_category: [AtomicU64; PROFILE_CATEGORY_COUNT],
    scan_nanos_by_category: [AtomicU64; PROFILE_CATEGORY_COUNT],
}

struct ProfilingKeyValueTable {
    name: String,
    inner: Arc<dyn KeyValueTable>,
    metrics: Arc<KeyValueTableProfileMetrics>,
}

impl ProfilingKeyValueTable {
    fn new(name: String, inner: Arc<dyn KeyValueTable>) -> Self {
        Self {
            name,
            inner,
            metrics: Arc::new(KeyValueTableProfileMetrics::default()),
        }
    }

    fn record_elapsed(calls: &AtomicU64, nanos: &AtomicU64, start: Instant) -> u64 {
        calls.fetch_add(1, Ordering::Relaxed);
        let elapsed = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        nanos.fetch_add(elapsed, Ordering::Relaxed);
        elapsed
    }

    fn record_get_elapsed(&self, key: &[u8], start: Instant) {
        let elapsed = Self::record_elapsed(&self.metrics.get_calls, &self.metrics.get_nanos, start);
        let category = profile_category_for_key(key);
        self.metrics.get_calls_by_category[category].fetch_add(1, Ordering::Relaxed);
        self.metrics.get_nanos_by_category[category].fetch_add(elapsed, Ordering::Relaxed);
    }

    fn record_scan_elapsed(&self, range: &Range<Vec<u8>>, start: Instant) {
        let elapsed =
            Self::record_elapsed(&self.metrics.scan_calls, &self.metrics.scan_nanos, start);
        let category = profile_category_for_key(&range.start);
        self.metrics.scan_calls_by_category[category].fetch_add(1, Ordering::Relaxed);
        self.metrics.scan_nanos_by_category[category].fetch_add(elapsed, Ordering::Relaxed);
    }
}

impl Drop for ProfilingKeyValueTable {
    fn drop(&mut self) {
        let get_calls = self.metrics.get_calls.load(Ordering::Relaxed);
        let get_nanos = self.metrics.get_nanos.load(Ordering::Relaxed);
        let write_calls = self.metrics.write_calls.load(Ordering::Relaxed);
        let write_nanos = self.metrics.write_nanos.load(Ordering::Relaxed);
        let scan_calls = self.metrics.scan_calls.load(Ordering::Relaxed);
        let scan_nanos = self.metrics.scan_nanos.load(Ordering::Relaxed);
        eprintln!(
            "[key-value-table-profile] name={} get_calls={} get_ms={:.3} write_calls={} write_ms={:.3} scan_calls={} scan_ms={:.3}",
            self.name,
            get_calls,
            get_nanos as f64 / 1_000_000.0,
            write_calls,
            write_nanos as f64 / 1_000_000.0,
            scan_calls,
            scan_nanos as f64 / 1_000_000.0,
        );
        for idx in 0..PROFILE_CATEGORY_COUNT {
            let get_calls = self.metrics.get_calls_by_category[idx].load(Ordering::Relaxed);
            let get_nanos = self.metrics.get_nanos_by_category[idx].load(Ordering::Relaxed);
            let scan_calls = self.metrics.scan_calls_by_category[idx].load(Ordering::Relaxed);
            let scan_nanos = self.metrics.scan_nanos_by_category[idx].load(Ordering::Relaxed);
            if get_calls == 0 && scan_calls == 0 {
                continue;
            }
            eprintln!(
                "[key-value-table-profile-category] name={} category={} get_calls={} get_ms={:.3} scan_calls={} scan_ms={:.3}",
                self.name,
                PROFILE_CATEGORY_NAMES[idx],
                get_calls,
                get_nanos as f64 / 1_000_000.0,
                scan_calls,
                scan_nanos as f64 / 1_000_000.0,
            );
        }
    }
}

fn profile_category_for_key(key: &[u8]) -> usize {
    if contains_bytes(key, b"columnar/grouped_max/state") {
        1
    } else if contains_bytes(key, b"columnar/grouped_max/output") {
        2
    } else if contains_bytes(key, b"columnar/grouped_stats/state") {
        3
    } else if contains_bytes(key, b"columnar/grouped_stats/output") {
        4
    } else if contains_bytes(key, b"columnar/join") {
        0
    } else if contains_bytes(key, b"segment/data") {
        5
    } else if contains_bytes(key, b"manifest/columnar") {
        6
    } else if contains_bytes(key, b"version_state/current") {
        7
    } else {
        8
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[async_trait]
impl KeyValueTable for ProfilingKeyValueTable {
    async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let start = Instant::now();
        let result = self.inner.get_bytes(key).await;
        self.record_get_elapsed(key, start);
        result
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let start = Instant::now();
        let result = self.inner.write_batch(batch).await;
        let _ = Self::record_elapsed(&self.metrics.write_calls, &self.metrics.write_nanos, start);
        result
    }

    async fn scan_range_bytes(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let category_range = range.clone();
        let start = Instant::now();
        let result = self.inner.scan_range_bytes(range, options).await;
        self.record_scan_elapsed(&category_range, start);
        result
    }

    async fn scan_range_bytes_until(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
        should_continue_after_entry: &mut (
                 dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<bool> + Send
             ),
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let category_range = range.clone();
        let start = Instant::now();
        let result = self
            .inner
            .scan_range_bytes_until(range, options, should_continue_after_entry)
            .await;
        self.record_scan_elapsed(&category_range, start);
        result
    }

    async fn scan_range_bytes_for_each(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
        visit_entry: &mut (dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<()> + Send),
    ) -> Result<()> {
        let category_range = range.clone();
        let start = Instant::now();
        let result = self
            .inner
            .scan_range_bytes_for_each(range, options, visit_entry)
            .await;
        self.record_scan_elapsed(&category_range, start);
        result
    }
}

fn bench_nexmark_vectorized_runtime(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let cases = [
        NexmarkRuntimeCase {
            id: "q1",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q1",
            query: r#"SELECT auction, bidder, price * 89 / 100 AS converted_price, date_time AS "dateTime", extra FROM nexmark_bid"#,
            output_schema: q1_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q2",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q2",
            query: "SELECT auction, price FROM nexmark_bid WHERE auction % 123 = 0",
            output_schema: q2_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q3",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
            ],
            view_name: "mv_nexmark_q3",
            query: "SELECT p.name, p.city, p.state, a.id FROM auction AS a JOIN person AS p ON a.seller = p.id WHERE a.category = 10 AND p.state IN ('or', 'id', 'ca')",
            output_schema: q3_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q4",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
            ],
            view_name: "mv_nexmark_q4",
            query: Q4_QUERY,
            output_schema: q4_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q5",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q5",
            query: r#"SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP("dateTime", 2000, 10000)"#,
            output_schema: q5_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q6",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
            ],
            view_name: "mv_nexmark_q6",
            query: r#"SELECT seller, AVG(price) AS moving_avg_price FROM (SELECT a.seller, b.price, b."dateTime", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#,
            output_schema: q6_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q7",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q7",
            query: r#"SELECT MAX(price) AS maxprice FROM bid GROUP BY TUMBLE("dateTime", 10000)"#,
            output_schema: q7_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q8",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_person",
                batch: person_batch,
            }],
            view_name: "mv_nexmark_q8",
            query: r#"SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, TUMBLE("dateTime", 10000)"#,
            output_schema: q8_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q9",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
            ],
            view_name: "mv_nexmark_q9",
            query: r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b."dateTime" AS "bidTime", b.extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b."dateTime" ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1"#,
            output_schema: q9_output_schema,
        },
        NexmarkRuntimeCase {
            id: "ordered_topn",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_ordered_topn",
            query: r#"SELECT auction, price FROM (SELECT auction, price FROM bid ORDER BY price DESC LIMIT 256) t ORDER BY auction"#,
            output_schema: q2_output_schema,
        },
        NexmarkRuntimeCase {
            id: "three_way_topn",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
            ],
            view_name: "mv_nexmark_three_way_topn",
            query: r#"SELECT p.id AS person_id, b.price FROM person p JOIN auction a ON p.id = a.seller JOIN bid b ON a.id = b.auction ORDER BY b.price DESC LIMIT 256"#,
            output_schema: three_way_topn_output_schema,
        },
        NexmarkRuntimeCase {
            id: "join_over_three_way_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
            ],
            view_name: "mv_nexmark_join_over_three_way_join",
            query: r#"SELECT joined.seller, p2.id AS person_id FROM (SELECT a.seller, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction) joined JOIN person p2 ON joined.seller = p2.id"#,
            output_schema: join_over_three_way_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "aggregate_over_three_way_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
            ],
            view_name: "mv_nexmark_aggregate_over_three_way_join",
            query: r#"SELECT p.id AS person_id, COUNT(b.price) AS bid_count FROM person p JOIN auction a ON p.id = a.seller JOIN bid b ON a.id = b.auction GROUP BY p.id"#,
            output_schema: aggregate_over_three_way_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "union_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
            ],
            view_name: "mv_nexmark_union_join",
            query: r#"SELECT u.key, p.id AS person_id FROM (SELECT seller AS key FROM auction UNION ALL SELECT bidder AS key FROM bid) u JOIN person p ON u.key = p.id"#,
            output_schema: union_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "aggregate_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_aggregate_join",
            query: r#"SELECT a.auction, a.max_price, au.seller FROM (SELECT auction, MAX(price) AS max_price FROM bid GROUP BY auction) a JOIN auction au ON a.auction = au.id"#,
            output_schema: aggregate_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "grouped_count_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_grouped_count_join",
            query: r#"SELECT c.auction, c.bid_count, a.seller FROM (SELECT auction, COUNT(*) AS bid_count FROM bid GROUP BY auction) c JOIN auction a ON c.auction = a.id"#,
            output_schema: grouped_count_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "grouped_sum_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_grouped_sum_join",
            query: r#"SELECT s.auction, s.total_price, a.seller FROM (SELECT auction, SUM(price) AS total_price FROM bid GROUP BY auction) s JOIN auction a ON s.auction = a.id"#,
            output_schema: grouped_sum_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "join_aggregate_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
            ],
            view_name: "mv_nexmark_join_aggregate_join",
            query: r#"SELECT j.auction, j.total_price, p.id AS person_id FROM (SELECT b.auction, SUM(b.price) AS total_price FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j JOIN person p ON j.auction = p.id"#,
            output_schema: join_aggregate_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "join_aggregate",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_join_aggregate",
            query: r#"SELECT a.category, COUNT(*) AS bid_count FROM auction a JOIN bid b ON a.id = b.auction GROUP BY a.category"#,
            output_schema: grouped_stats_over_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "grouped_stats_over_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_grouped_stats_over_join",
            query: r#"SELECT category, COUNT(*) AS bid_count FROM (SELECT a.category AS category, b.price AS price FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) j GROUP BY category"#,
            output_schema: grouped_stats_over_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "topn_over_aggregate_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_topn_over_aggregate_join",
            query: r#"SELECT j.auction, j.max_price FROM (SELECT m.auction, m.max_price, a.seller FROM (SELECT auction, MAX(price) AS max_price FROM bid GROUP BY auction) m JOIN auction a ON m.auction = a.id) j ORDER BY j.max_price DESC LIMIT 256"#,
            output_schema: topn_over_aggregate_join_output_schema,
        },
        NexmarkRuntimeCase {
            id: "topn_over_join_avg",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_topn_over_join_avg",
            query: r#"SELECT key, value FROM (SELECT auction AS key, CAST(avg_price AS BIGINT) AS value FROM (SELECT b.auction, AVG(b.price) AS avg_price FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j ORDER BY avg_price DESC LIMIT 256) s"#,
            output_schema: topn_over_join_avg_output_schema,
        },
        NexmarkRuntimeCase {
            id: "aggregate_over_join_topn",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_aggregate_over_join_topn",
            query: r#"SELECT key, SUM(value) AS total FROM (SELECT b.auction AS key, b.price AS value FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 256) top_bids GROUP BY key"#,
            output_schema: aggregate_over_join_topn_output_schema,
        },
        NexmarkRuntimeCase {
            id: "aggregate_over_join_avg_topn",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_aggregate_over_join_avg_topn",
            query: r#"SELECT SUM(value) AS total FROM (SELECT auction AS key, CAST(avg_price AS BIGINT) AS value FROM (SELECT b.auction, AVG(b.price) AS avg_price FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j ORDER BY avg_price DESC LIMIT 256) s"#,
            output_schema: aggregate_over_join_avg_topn_output_schema,
        },
        NexmarkRuntimeCase {
            id: "aggregate_over_except_sources",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_aggregate_over_except_sources",
            query: r#"SELECT key, SUM(value) AS total FROM (SELECT auction AS key, price AS value FROM bid EXCEPT SELECT id AS key, "initialBid" AS value FROM auction) s GROUP BY key"#,
            output_schema: aggregate_over_except_sources_output_schema,
        },
        NexmarkRuntimeCase {
            id: "aggregate_over_intersect_sources",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_aggregate_over_intersect_sources",
            query: r#"SELECT SUM(value) AS total FROM (SELECT auction AS key, price AS value FROM bid INTERSECT SELECT id AS key, "initialBid" AS value FROM auction) s"#,
            output_schema: aggregate_over_intersect_sources_output_schema,
        },
        NexmarkRuntimeCase {
            id: "filtered_join_topn",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
            ],
            view_name: "mv_nexmark_filtered_join_topn",
            query: r#"SELECT auction, seller FROM (SELECT b.auction, a.seller, b.price FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 256) top_bids WHERE seller >= 0 ORDER BY auction"#,
            output_schema: filtered_join_topn_output_schema,
        },
        NexmarkRuntimeCase {
            id: "join_over_join_topn",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
            ],
            view_name: "mv_nexmark_join_over_join_topn",
            query: r#"SELECT top_bids.auction, p.id AS person_id FROM (SELECT b.auction, a.seller, b.price FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 256) top_bids JOIN person p ON top_bids.seller = p.id"#,
            output_schema: join_over_join_topn_output_schema,
        },
        NexmarkRuntimeCase {
            id: "join_over_join",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_person",
                    batch: person_join_batch,
                },
            ],
            view_name: "mv_nexmark_join_over_join",
            query: r#"SELECT joined.auction, p.id AS person_id FROM (SELECT b.auction, a.seller FROM bid b JOIN auction a ON b.auction = a.id) joined JOIN person p ON joined.seller = p.id"#,
            output_schema: join_over_join_topn_output_schema,
        },
        NexmarkRuntimeCase {
            id: "join_row_number_topn",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_join_row_number_topn",
            query: r#"SELECT t.auction, a.seller FROM (SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bid) ranked WHERE rn <= 256) t JOIN auction a ON t.auction = a.id"#,
            output_schema: filtered_join_topn_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q12",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q12",
            query: r#"SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE("dateTime", 10000)"#,
            output_schema: q12_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q13",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_q13",
            query: r#"SELECT b.auction, b.bidder, b.price, b."dateTime", a.seller AS value FROM (SELECT *, PROCTIME() AS p_time FROM bid) b JOIN auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000"#,
            output_schema: q13_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q15",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q15",
            query: r#"SELECT DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY DATE_FORMAT("dateTime", 'yyyy-MM-dd')"#,
            output_schema: q15_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q16",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q16",
            query: r#"SELECT channel, DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, MAX(DATE_FORMAT("dateTime", 'HH:mm')) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, DATE_FORMAT("dateTime", 'yyyy-MM-dd')"#,
            output_schema: q16_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q17",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q17",
            query: r#"SELECT auction, DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, DATE_FORMAT("dateTime", 'yyyy-MM-dd')"#,
            output_schema: q17_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q18",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q18",
            query: r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY "dateTime" DESC) AS rank_number FROM bid) dedup WHERE rank_number <= 1"#,
            output_schema: q18_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q19",
            sources: &[NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            }],
            view_name: "mv_nexmark_q19",
            query: r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 10"#,
            output_schema: q19_output_schema,
        },
        NexmarkRuntimeCase {
            id: "q20",
            sources: &[
                NexmarkRuntimeSource {
                    source_name: "nexmark_bid",
                    batch: bid_join_batch,
                },
                NexmarkRuntimeSource {
                    source_name: "nexmark_auction",
                    batch: auction_batch,
                },
            ],
            view_name: "mv_nexmark_q20",
            query: r#"SELECT b.auction, b.bidder, b.price, b.channel, b.url, b."dateTime", b.extra, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime" AS auction_time, a.expires, a.seller, a.category, a.extra AS auction_extra FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE a.category = 10"#,
            output_schema: q20_output_schema,
        },
    ];

    let mut group = c.benchmark_group("nexmark_vectorized_runtime_columnar");
    for case in cases {
        group.throughput(Throughput::Elements(
            (ROWS_PER_TICK * TICKS * case.sources.len()) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::new(case.id, format!("{ROWS_PER_TICK}x{TICKS}")),
            &case,
            |b, case| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(build_runtime_case(case))
                            .expect("build runtime benchmark case")
                    },
                    |(mut execution, registry, batches)| {
                        runtime
                            .block_on(run_runtime_case(case, &mut execution, registry, batches))
                            .expect("run runtime benchmark case")
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_nexmark_q4_repeated_state_runtime(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let case = NexmarkRuntimeCase {
        id: "q4_repeated_state",
        sources: &[
            NexmarkRuntimeSource {
                source_name: "nexmark_auction",
                batch: auction_batch,
            },
            NexmarkRuntimeSource {
                source_name: "nexmark_bid",
                batch: bid_batch,
            },
        ],
        view_name: "mv_nexmark_q4_repeated_state",
        query: Q4_QUERY,
        output_schema: q4_output_schema,
    };

    let mut group = c.benchmark_group("nexmark_vectorized_runtime_q4_repeated_state");
    group.throughput(Throughput::Elements(
        (Q4_REPEATED_AUCTION_ROWS + Q4_REPEATED_BID_ROWS) as u64,
    ));
    group.bench_with_input(
        BenchmarkId::new(
            case.id,
            format!(
                "{} auctions + {}x{} bids",
                Q4_REPEATED_AUCTION_ROWS, Q4_REPEATED_BID_ROWS_PER_TICK, Q4_REPEATED_TICKS
            ),
        ),
        &case,
        |b, case| {
            b.iter_batched(
                || {
                    runtime
                        .block_on(build_q4_repeated_state_runtime_case(case))
                        .expect("build q4 repeated-state runtime benchmark case")
                },
                |(mut execution, registry, auction_batch, bid_batches)| {
                    runtime
                        .block_on(run_q4_repeated_state_runtime_case(
                            case,
                            &mut execution,
                            registry,
                            auction_batch,
                            bid_batches,
                        ))
                        .expect("run q4 repeated-state runtime benchmark case")
                },
                BatchSize::SmallInput,
            );
        },
    );
    group.finish();
}

async fn build_runtime_case(
    case: &NexmarkRuntimeCase,
) -> Result<(
    VectorizedExecutionRuntime,
    Arc<MaterializedViewRegistry>,
    Vec<NexmarkRuntimeSourceBatches>,
)> {
    let mut sources = SourceRegistry::new();
    let definitions = generator::definitions()?;
    sources.extend(definitions.clone());
    let output_schema = (case.output_schema)();
    let table = build_operator_state_table(case.id).await?;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let execution = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            case.view_name,
            case.query,
            output_schema,
        )],
        Arc::clone(&registry),
        planner_udfs(),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .with_context(|| format!("build vectorized runtime for {}", case.id))?;

    let mut source_batches = Vec::with_capacity(case.sources.len());
    for source in case.sources {
        let source_schema = definitions
            .iter()
            .find(|definition| definition.name() == source.source_name)
            .ok_or_else(|| anyhow::anyhow!("missing {} definition", source.source_name))?
            .to_arrow_schema();
        let batches = (0..TICKS)
            .map(|tick| {
                (source.batch)(
                    Arc::clone(&source_schema),
                    tick * ROWS_PER_TICK,
                    ROWS_PER_TICK,
                )
            })
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("build input batches for {}", source.source_name))?;
        source_batches.push(NexmarkRuntimeSourceBatches {
            source_name: source.source_name,
            batches,
        });
    }
    Ok((execution, registry, source_batches))
}

async fn build_q4_repeated_state_runtime_case(
    case: &NexmarkRuntimeCase,
) -> Result<(
    VectorizedExecutionRuntime,
    Arc<MaterializedViewRegistry>,
    RecordBatch,
    Vec<RecordBatch>,
)> {
    let profile_timing = q4_timing_enabled();
    let total_start = Instant::now();
    let mut sources = SourceRegistry::new();
    let definitions_start = Instant::now();
    let definitions = generator::definitions()?;
    sources.extend(definitions.clone());
    let output_schema = (case.output_schema)();
    let definitions_elapsed = definitions_start.elapsed();
    let table_start = Instant::now();
    let table = build_operator_state_table(case.id).await?;
    let table_elapsed = table_start.elapsed();
    let registry = Arc::new(MaterializedViewRegistry::new());
    let runtime_start = Instant::now();
    let execution = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            case.view_name,
            case.query,
            output_schema,
        )],
        Arc::clone(&registry),
        planner_udfs(),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .with_context(|| format!("build vectorized runtime for {}", case.id))?;
    let runtime_elapsed = runtime_start.elapsed();

    let input_start = Instant::now();
    let auction_schema = definitions
        .iter()
        .find(|definition| definition.name() == "nexmark_auction")
        .ok_or_else(|| anyhow::anyhow!("missing nexmark_auction definition"))?
        .to_arrow_schema();
    let bid_schema = definitions
        .iter()
        .find(|definition| definition.name() == "nexmark_bid")
        .ok_or_else(|| anyhow::anyhow!("missing nexmark_bid definition"))?
        .to_arrow_schema();
    let auction_batch = auction_batch(auction_schema, 0, Q4_REPEATED_AUCTION_ROWS)
        .context("build q4 repeated-state auction batch")?;
    let bid_batches = (0..Q4_REPEATED_TICKS)
        .map(|tick| {
            let start = tick * Q4_REPEATED_BID_ROWS_PER_TICK;
            let remaining = Q4_REPEATED_BID_ROWS.saturating_sub(start);
            bid_batch(
                Arc::clone(&bid_schema),
                start,
                remaining.min(Q4_REPEATED_BID_ROWS_PER_TICK),
            )
        })
        .collect::<Result<Vec<_>>>()
        .context("build q4 repeated-state bid batches")?;
    let input_elapsed = input_start.elapsed();

    if profile_timing {
        eprintln!(
            "[q4-build-timing] total_ms={:.3} definitions_ms={:.3} table_ms={:.3} runtime_ms={:.3} input_batches_ms={:.3}",
            duration_ms(total_start.elapsed()),
            duration_ms(definitions_elapsed),
            duration_ms(table_elapsed),
            duration_ms(runtime_elapsed),
            duration_ms(input_elapsed),
        );
    }

    Ok((execution, registry, auction_batch, bid_batches))
}

async fn run_runtime_case(
    case: &NexmarkRuntimeCase,
    execution: &mut VectorizedExecutionRuntime,
    registry: Arc<MaterializedViewRegistry>,
    source_batches: Vec<NexmarkRuntimeSourceBatches>,
) -> Result<()> {
    for tick in 0..TICKS {
        for source in &source_batches {
            execution
                .append_source_batches_for_execution_and_query(
                    source.source_name,
                    vec![source.batches[tick].clone()],
                    Vec::new(),
                )
                .await?;
        }
        execution.run_tick((tick + 1) as i64).await?;
    }
    let handle = registry
        .get(case.view_name)
        .ok_or_else(|| anyhow::anyhow!("missing materialized view {}", case.view_name))?;
    let snapshot = handle
        .arrow_snapshot_for(TICKS as i64)
        .ok_or_else(|| anyhow::anyhow!("missing final snapshot for {}", case.view_name))?;
    let rows = snapshot.iter().map(RecordBatch::num_rows).sum::<usize>();
    black_box(rows);
    Ok(())
}

async fn run_q4_repeated_state_runtime_case(
    case: &NexmarkRuntimeCase,
    execution: &mut VectorizedExecutionRuntime,
    registry: Arc<MaterializedViewRegistry>,
    auction_batch: RecordBatch,
    bid_batches: Vec<RecordBatch>,
) -> Result<()> {
    let profile_timing = q4_timing_enabled();
    let total_start = Instant::now();
    let mut timing = Q4RuntimeTiming::default();

    let start = Instant::now();
    execution
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![auction_batch],
            Vec::new(),
        )
        .await?;
    timing.auction_append += start.elapsed();

    let start = Instant::now();
    execution.run_tick(1).await?;
    timing.auction_tick += start.elapsed();

    for (tick, bid_batch) in bid_batches.into_iter().enumerate() {
        let start = Instant::now();
        execution
            .append_source_batches_for_execution_and_query(
                "nexmark_bid",
                vec![bid_batch],
                Vec::new(),
            )
            .await?;
        timing.bid_append += start.elapsed();

        let start = Instant::now();
        execution.run_tick((tick + 2) as i64).await?;
        timing.record_bid_tick(start.elapsed());
    }

    let start = Instant::now();
    let final_version = (Q4_REPEATED_TICKS + 1) as i64;
    let handle = registry
        .get(case.view_name)
        .ok_or_else(|| anyhow::anyhow!("missing materialized view {}", case.view_name))?;
    let snapshot = handle
        .arrow_snapshot_for(final_version)
        .ok_or_else(|| anyhow::anyhow!("missing final snapshot for {}", case.view_name))?;
    let rows = snapshot.iter().map(RecordBatch::num_rows).sum::<usize>();
    black_box(rows);
    timing.final_snapshot += start.elapsed();
    if profile_timing {
        timing.print(total_start.elapsed());
    }
    Ok(())
}

async fn build_operator_state_table(name: &str) -> Result<Arc<dyn KeyValueTable>> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(format!("nexmark-vectorized-runtime-{name}"), store).await?);
    let table = Arc::new(SlateTable::new(db)) as Arc<dyn KeyValueTable>;
    if std::env::var_os("FLOE_PROFILE_KEY_VALUE_TABLE").is_some() {
        return Ok(Arc::new(ProfilingKeyValueTable::new(
            name.to_string(),
            table,
        )));
    }
    Ok(table)
}

fn bid_batch(schema: SchemaRef, start: usize, rows: usize) -> Result<RecordBatch> {
    let mut auctions = Vec::with_capacity(rows);
    let mut bidders = Vec::with_capacity(rows);
    let mut prices = Vec::with_capacity(rows);
    let mut channels = Vec::with_capacity(rows);
    let mut urls = Vec::with_capacity(rows);
    let mut date_times = Vec::with_capacity(rows);
    let mut extras = Vec::with_capacity(rows);

    for offset in 0..rows {
        let seq = start + offset;
        auctions.push((seq % 10_000) as i64);
        bidders.push((seq % 50_000) as i64);
        prices.push(1_000 + (seq % 1_000_000) as i64);
        channels.push(match seq % 4 {
            0 => "apple".to_string(),
            1 => "google".to_string(),
            2 => "facebook".to_string(),
            _ => "baidu".to_string(),
        });
        urls.push(format!(
            "https://example.test/path/{seq}?channel_id={}",
            seq % 16
        ));
        date_times.push(1_700_000_000_000_i64 + seq as i64);
        extras.push(format!("extra-{seq}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auctions)) as ArrayRef,
            Arc::new(Int64Array::from(bidders)) as ArrayRef,
            Arc::new(Int64Array::from(prices)) as ArrayRef,
            Arc::new(StringArray::from(channels)) as ArrayRef,
            Arc::new(StringArray::from(urls)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(date_times)) as ArrayRef,
            Arc::new(StringArray::from(extras)) as ArrayRef,
        ],
    )
    .context("build nexmark bid batch")
}

fn bid_join_batch(schema: SchemaRef, start: usize, rows: usize) -> Result<RecordBatch> {
    let mut auctions = Vec::with_capacity(rows);
    let mut bidders = Vec::with_capacity(rows);
    let mut prices = Vec::with_capacity(rows);
    let mut channels = Vec::with_capacity(rows);
    let mut urls = Vec::with_capacity(rows);
    let mut date_times = Vec::with_capacity(rows);
    let mut extras = Vec::with_capacity(rows);

    for offset in 0..rows {
        let seq = start + offset;
        auctions.push(seq as i64);
        bidders.push(seq as i64);
        prices.push(1_000 + (seq % 1_000_000) as i64);
        channels.push(match seq % 4 {
            0 => "apple".to_string(),
            1 => "google".to_string(),
            2 => "facebook".to_string(),
            _ => "baidu".to_string(),
        });
        urls.push(format!(
            "https://example.test/path/{seq}?channel_id={}",
            seq % 16
        ));
        date_times.push(1_700_000_000_000_i64 + seq as i64);
        extras.push(format!("extra-{seq}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auctions)) as ArrayRef,
            Arc::new(Int64Array::from(bidders)) as ArrayRef,
            Arc::new(Int64Array::from(prices)) as ArrayRef,
            Arc::new(StringArray::from(channels)) as ArrayRef,
            Arc::new(StringArray::from(urls)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(date_times)) as ArrayRef,
            Arc::new(StringArray::from(extras)) as ArrayRef,
        ],
    )
    .context("build nexmark bid join batch")
}

fn person_batch(schema: SchemaRef, start: usize, rows: usize) -> Result<RecordBatch> {
    let mut ids = Vec::with_capacity(rows);
    let mut names = Vec::with_capacity(rows);
    let mut emails = Vec::with_capacity(rows);
    let mut credit_cards = Vec::with_capacity(rows);
    let mut cities = Vec::with_capacity(rows);
    let mut states = Vec::with_capacity(rows);
    let mut date_times = Vec::with_capacity(rows);
    let mut extras = Vec::with_capacity(rows);

    for offset in 0..rows {
        let seq = start + offset;
        ids.push((seq % 50_000) as i64);
        names.push(format!("person-{seq}"));
        emails.push(format!("person-{seq}@example.test"));
        credit_cards.push(format!("411111111111{:04}", seq % 10_000));
        cities.push(match seq % 4 {
            0 => "portland".to_string(),
            1 => "boise".to_string(),
            2 => "san francisco".to_string(),
            _ => "seattle".to_string(),
        });
        states.push(match seq % 4 {
            0 => "or".to_string(),
            1 => "id".to_string(),
            2 => "ca".to_string(),
            _ => "wa".to_string(),
        });
        date_times.push(1_700_000_000_000_i64 + seq as i64);
        extras.push(format!("extra-{seq}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(emails)) as ArrayRef,
            Arc::new(StringArray::from(credit_cards)) as ArrayRef,
            Arc::new(StringArray::from(cities)) as ArrayRef,
            Arc::new(StringArray::from(states)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(date_times)) as ArrayRef,
            Arc::new(StringArray::from(extras)) as ArrayRef,
        ],
    )
    .context("build nexmark person batch")
}

fn person_join_batch(schema: SchemaRef, start: usize, rows: usize) -> Result<RecordBatch> {
    let mut ids = Vec::with_capacity(rows);
    let mut names = Vec::with_capacity(rows);
    let mut emails = Vec::with_capacity(rows);
    let mut credit_cards = Vec::with_capacity(rows);
    let mut cities = Vec::with_capacity(rows);
    let mut states = Vec::with_capacity(rows);
    let mut date_times = Vec::with_capacity(rows);
    let mut extras = Vec::with_capacity(rows);

    for offset in 0..rows {
        let seq = start + offset;
        ids.push(seq as i64);
        names.push(format!("person-{seq}"));
        emails.push(format!("person-{seq}@example.test"));
        credit_cards.push(format!("411111111111{:04}", seq % 10_000));
        cities.push(match seq % 4 {
            0 => "portland".to_string(),
            1 => "boise".to_string(),
            2 => "san francisco".to_string(),
            _ => "seattle".to_string(),
        });
        states.push(match seq % 4 {
            0 => "or".to_string(),
            1 => "id".to_string(),
            2 => "ca".to_string(),
            _ => "wa".to_string(),
        });
        date_times.push(1_700_000_000_000_i64 + seq as i64);
        extras.push(format!("extra-{seq}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(emails)) as ArrayRef,
            Arc::new(StringArray::from(credit_cards)) as ArrayRef,
            Arc::new(StringArray::from(cities)) as ArrayRef,
            Arc::new(StringArray::from(states)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(date_times)) as ArrayRef,
            Arc::new(StringArray::from(extras)) as ArrayRef,
        ],
    )
    .context("build nexmark person join batch")
}

fn auction_batch(schema: SchemaRef, start: usize, rows: usize) -> Result<RecordBatch> {
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

    for offset in 0..rows {
        let seq = start + offset;
        ids.push(seq as i64);
        item_names.push(format!("item-{seq}"));
        descriptions.push(format!("description-{seq}"));
        initial_bids.push(100 + (seq % 10_000) as i64);
        reserves.push(1_000 + (seq % 100_000) as i64);
        sellers.push(seq as i64);
        categories.push(10_i64);
        let date_time = 1_700_000_000_000_i64 + seq as i64;
        expires.push(date_time + 600_000);
        date_times.push(date_time);
        extras.push(format!("auction-extra-{seq}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(item_names)) as ArrayRef,
            Arc::new(StringArray::from(descriptions)) as ArrayRef,
            Arc::new(Int64Array::from(initial_bids)) as ArrayRef,
            Arc::new(Int64Array::from(reserves)) as ArrayRef,
            Arc::new(Int64Array::from(sellers)) as ArrayRef,
            Arc::new(Int64Array::from(categories)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(expires)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(date_times)) as ArrayRef,
            Arc::new(StringArray::from(extras)) as ArrayRef,
        ],
    )
    .context("build nexmark auction batch")
}

fn q1_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("converted_price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("extra", DataType::Utf8, true),
    ]))
}

fn q2_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]))
}

fn q3_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("id", DataType::Int64, true),
    ]))
}

fn q4_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, true),
        Field::new("avg_max", DataType::Float64, true),
    ]))
}

fn q5_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("num", DataType::Int64, false),
    ]))
}

fn q6_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, true),
        Field::new("moving_avg_price", DataType::Float64, true),
    ]))
}

fn q7_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "maxprice",
        DataType::Int64,
        true,
    )]))
}

fn q8_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("person_count", DataType::Int64, false),
    ]))
}

fn q9_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("itemName", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("initialBid", DataType::Int64, true),
        Field::new("reserve", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "expires",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("seller", DataType::Int64, true),
        Field::new("category", DataType::Int64, true),
        Field::new("extra", DataType::Utf8, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "bidTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("bidExtra", DataType::Utf8, true),
    ]))
}

fn three_way_topn_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]))
}

fn join_over_three_way_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, true),
        Field::new("person_id", DataType::Int64, true),
    ]))
}

fn aggregate_over_three_way_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, true),
        Field::new("bid_count", DataType::Int64, false),
    ]))
}

fn union_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("person_id", DataType::Int64, true),
    ]))
}

fn aggregate_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("max_price", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]))
}

fn grouped_count_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bid_count", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]))
}

fn grouped_sum_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_price", DataType::Int64, true),
        Field::new("seller", DataType::Int64, false),
    ]))
}

fn join_aggregate_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_price", DataType::Int64, true),
        Field::new("person_id", DataType::Int64, false),
    ]))
}

fn grouped_stats_over_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, true),
        Field::new("bid_count", DataType::Int64, false),
    ]))
}

fn topn_over_aggregate_join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("max_price", DataType::Int64, false),
    ]))
}

fn topn_over_join_avg_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn aggregate_over_join_topn_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]))
}

fn aggregate_over_join_avg_topn_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]))
}

fn aggregate_over_except_sources_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]))
}

fn aggregate_over_intersect_sources_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]))
}

fn filtered_join_topn_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("seller", DataType::Int64, true),
    ]))
}

fn join_over_join_topn_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("person_id", DataType::Int64, true),
    ]))
}

fn q12_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("bid_count", DataType::Int64, false),
    ]))
}

fn q13_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn q15_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("day", DataType::Utf8, true),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("rank1_bids", DataType::Int64, false),
        Field::new("rank2_bids", DataType::Int64, false),
        Field::new("rank3_bids", DataType::Int64, false),
        Field::new("total_bidders", DataType::Int64, false),
        Field::new("rank1_bidders", DataType::Int64, false),
        Field::new("rank2_bidders", DataType::Int64, false),
        Field::new("rank3_bidders", DataType::Int64, false),
        Field::new("total_auctions", DataType::Int64, false),
        Field::new("rank1_auctions", DataType::Int64, false),
        Field::new("rank2_auctions", DataType::Int64, false),
        Field::new("rank3_auctions", DataType::Int64, false),
    ]))
}

fn q16_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("channel", DataType::Utf8, true),
        Field::new("day", DataType::Utf8, true),
        Field::new("minute", DataType::Utf8, true),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("rank1_bids", DataType::Int64, false),
        Field::new("rank2_bids", DataType::Int64, false),
        Field::new("rank3_bids", DataType::Int64, false),
        Field::new("total_bidders", DataType::Int64, false),
        Field::new("rank1_bidders", DataType::Int64, false),
        Field::new("rank2_bidders", DataType::Int64, false),
        Field::new("rank3_bidders", DataType::Int64, false),
        Field::new("total_auctions", DataType::Int64, false),
        Field::new("rank1_auctions", DataType::Int64, false),
        Field::new("rank2_auctions", DataType::Int64, false),
        Field::new("rank3_auctions", DataType::Int64, false),
    ]))
}

fn q17_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("day", DataType::Utf8, true),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("rank1_bids", DataType::Int64, false),
        Field::new("rank2_bids", DataType::Int64, false),
        Field::new("rank3_bids", DataType::Int64, false),
        Field::new("min_price", DataType::Int64, true),
        Field::new("max_price", DataType::Int64, true),
        Field::new("avg_price", DataType::Float64, true),
        Field::new("sum_price", DataType::Int64, true),
    ]))
}

fn q18_output_schema() -> SchemaRef {
    bid_full_output_schema()
}

fn q19_output_schema() -> SchemaRef {
    bid_full_output_schema()
}

fn bid_full_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("extra", DataType::Utf8, true),
    ]))
}

fn q20_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("extra", DataType::Utf8, true),
        Field::new("itemName", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("initialBid", DataType::Int64, true),
        Field::new("reserve", DataType::Int64, true),
        Field::new(
            "auction_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "expires",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("seller", DataType::Int64, true),
        Field::new("category", DataType::Int64, true),
        Field::new("auction_extra", DataType::Utf8, true),
    ]))
}

criterion_group!(
    benches,
    bench_nexmark_vectorized_runtime,
    bench_nexmark_q4_repeated_state_runtime
);
criterion_main!(benches);
