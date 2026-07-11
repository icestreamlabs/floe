use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
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

#[derive(Debug, Clone)]
struct GeneratedPlanRuntimeCase {
    id: String,
    sql: String,
}

#[derive(Debug, Clone)]
enum GeneratedRuntimeCaseResult {
    RuntimeMode(String),
    PlanningUnsupported,
    RuntimeUnsupported,
}

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
    }
}

#[test]
fn guards_generated_active_vectorized_runtime_dbsp_valid_compositions() {
    run_current_thread_coverage_test_on_explicit_stack(
        "generated-vectorized-runtime-coverage",
        guards_generated_active_vectorized_runtime_dbsp_valid_compositions_inner(),
    );
}

fn run_current_thread_coverage_test_on_explicit_stack<F>(name: &str, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build coverage runtime");
            runtime.block_on(future);
        })
        .expect("spawn coverage thread")
        .join()
        .expect("coverage thread panicked");
}

async fn guards_generated_active_vectorized_runtime_dbsp_valid_compositions_inner() {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions().expect("load nexmark source definitions"));
    let planner = DbspPlanBuilder::new(nexmark_config().expect("load nexmark planner config"));
    let available_sources = available_nexmark_sources();

    let cases = generated_dbsp_runtime_plan_cases();
    let case_count = cases.len();
    let unique_case_ids = cases
        .iter()
        .map(|case| case.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        unique_case_ids.len(),
        case_count,
        "generated coverage case ids must be unique"
    );

    let mut failures = Vec::new();
    let mut planning_unsupported_count = 0usize;
    let mut runtime_unsupported_count = 0usize;
    let mut execution_modes = BTreeMap::new();
    for case in cases {
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
            Ok(GeneratedRuntimeCaseResult::PlanningUnsupported) => {
                planning_unsupported_count = planning_unsupported_count.saturating_add(1);
            }
            Ok(GeneratedRuntimeCaseResult::RuntimeUnsupported) => {
                runtime_unsupported_count = runtime_unsupported_count.saturating_add(1);
            }
            Err(err) => {
                failures.push(format!("{}: {err:#}", case.id));
            }
        }
    }

    eprintln!(
        "generated runtime coverage: {} supported, {} planning-unsupported, {} runtime-unsupported",
        execution_modes.len(),
        planning_unsupported_count,
        runtime_unsupported_count,
    );
    assert_eq!(
        execution_modes.len()
            + planning_unsupported_count
            + runtime_unsupported_count
            + failures.len(),
        case_count,
        "generated coverage must classify every case"
    );
    assert!(
        execution_modes.len() >= 150,
        "generated coverage unexpectedly shrank: {} DBSP-valid cases, {} DBSP-unsupported cases",
        execution_modes.len(),
        planning_unsupported_count
    );
    let covered_modes = execution_modes
        .values()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected_modes = [
        "columnar_constant",
        "columnar_grouped_count",
        "columnar_grouped_stats",
        "columnar_join",
        "columnar_stateless",
        "columnar_topn",
        "columnar_union",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert!(
        expected_modes.is_subset(&covered_modes),
        "generated coverage no longer exercises all expected runtime modes: {covered_modes:?}"
    );

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
            tracing::debug!(case = %case.id, error = %err, "generated case is unsupported by DBSP planning");
            return Ok(GeneratedRuntimeCaseResult::PlanningUnsupported);
        }
    };
    if let Err(err) = validate_dbsp_plan(&circuit, available_sources, &case.id) {
        tracing::debug!(case = %case.id, error = %err, "generated case is unsupported by DBSP validation");
        return Ok(GeneratedRuntimeCaseResult::PlanningUnsupported);
    }

    let output_schema = df_schema_to_arrow(planned.logical_plan().schema())
        .with_context(|| format!("Arrow schema conversion failed for {}", case.id))?;
    let state_table = build_operator_state_table(&case.id).await?;
    let mv_plan = VectorizedMaterializedViewPlan::new(
        planned.definition().name().to_string(),
        planned.logical_plan().clone(),
        output_schema,
    );
    let runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
        registry,
        vec![mv_plan],
        Arc::new(MaterializedViewRegistry::new()),
        planner_udfs(),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(state_table),
    )
    .await;
    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(err) => {
            tracing::debug!(case = %case.id, error = %err, "generated case is unsupported by the active runtime");
            return Ok(GeneratedRuntimeCaseResult::RuntimeUnsupported);
        }
    };

    runtime
        .materialized_view_execution_modes()
        .into_iter()
        .find(|(view_name, _)| *view_name == case.id)
        .map(|(_, mode)| GeneratedRuntimeCaseResult::RuntimeMode(mode.to_string()))
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
            planned.logical_plan().clone(),
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
