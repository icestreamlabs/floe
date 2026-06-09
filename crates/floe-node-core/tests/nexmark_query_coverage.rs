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
        id: "three_way_join",
        sql: "SELECT p.name, b.price FROM auction a JOIN person p ON a.seller = p.id JOIN bid b ON a.id = b.auction",
    },
    ValidPlanRuntimeCase {
        id: "self_join_aggregate",
        sql: "SELECT l.auction, COUNT(*) AS pair_count FROM bid l JOIN bid r ON l.auction = r.auction WHERE l.price < r.price GROUP BY l.auction",
    },
    ValidPlanRuntimeCase {
        id: "aggregate_topn",
        sql: "SELECT auction, SUM(price) AS total FROM bid GROUP BY auction ORDER BY total DESC LIMIT 5",
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
        id: "having_aggregate",
        sql: "SELECT auction, SUM(price) AS total FROM bid GROUP BY auction HAVING SUM(price) > 1000",
    },
    ValidPlanRuntimeCase {
        id: "session_window_aggregate",
        sql: "SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, SESSION(\"dateTime\", 5000)",
    },
    ValidPlanRuntimeCase {
        id: "asof_join",
        sql: "SELECT a.id, b.price FROM auction a ASOF JOIN bid b MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") ON a.id = b.auction",
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
    }
}

#[tokio::test]
async fn guards_active_vectorized_runtime_valid_dbsp_plan_shapes() {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions().expect("load nexmark source definitions"));
    let planner = DbspPlanBuilder::new(nexmark_config().expect("load nexmark planner config"));
    let available_sources = available_nexmark_sources();

    let mut failures = Vec::new();
    for case in VALID_DBSP_RUNTIME_PLAN_CASES {
        if let Err(err) =
            validate_active_vectorized_runtime_case(&registry, &planner, &available_sources, *case)
                .await
        {
            failures.push(format!("{}: {err:#}", case.id));
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
) -> Result<()> {
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
    VectorizedExecutionRuntime::new_with_udfs_and_options(
        registry,
        vec![mv_plan],
        Arc::new(MaterializedViewRegistry::new()),
        planner_udfs(),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(state_table),
    )
    .await
    .with_context(|| format!("active vectorized runtime rejected {}", case.id))?;

    Ok(())
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
            Ok(_) => {
                out.insert(
                    query.id.to_string(),
                    ActiveRuntimeCoverageResult {
                        coverage: ActiveRuntimeCoverage {
                            vectorized_runtime: true,
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
