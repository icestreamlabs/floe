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
        id: "self_join_aggregate",
        sql: "SELECT l.auction, COUNT(*) AS pair_count FROM bid l JOIN bid r ON l.auction = r.auction WHERE l.price < r.price GROUP BY l.auction",
    },
    ValidPlanRuntimeCase {
        id: "join_aggregate",
        sql: "SELECT a.category, COUNT(*) AS bid_count FROM auction a JOIN bid b ON a.id = b.auction GROUP BY a.category",
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
        id: "join_topn",
        sql: "SELECT b.auction, a.seller FROM bid b JOIN auction a ON b.auction = a.id ORDER BY b.price DESC LIMIT 5",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_topn",
        sql: "SELECT auction, SUM(price) AS total FROM bid GROUP BY auction ORDER BY total DESC LIMIT 5",
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
        id: "partitioned_row_number_topn",
        sql: "SELECT auction, bidder, price, channel, url, \"dateTime\", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY \"dateTime\" DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 1",
    },
    ValidPlanRuntimeCase {
        id: "global_row_number_topn",
        sql: "SELECT auction, bidder, price FROM (SELECT auction, bidder, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 5",
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
        id: "union_aggregate",
        sql: "SELECT key, COUNT(*) AS row_count FROM (SELECT auction AS key FROM bid UNION ALL SELECT id AS key FROM auction) u GROUP BY key",
    },
    ValidPlanRuntimeCase {
        id: "union_join",
        sql: "SELECT u.key, p.name FROM (SELECT seller AS key FROM auction UNION ALL SELECT bidder AS key FROM bid) u JOIN person p ON u.key = p.id",
    },
    ValidPlanRuntimeCase {
        id: "union_topn",
        sql: "SELECT key FROM (SELECT auction AS key FROM bid UNION ALL SELECT id AS key FROM auction) u ORDER BY key DESC LIMIT 5",
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
        id: "asof_join_without_equi_keys",
        sql: "SELECT a.id, b.price FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\")",
    },
];

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

    if !failures.is_empty() {
        panic!(
            "active vectorized runtime rejected DBSP-valid plan shape(s):\n{}",
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
