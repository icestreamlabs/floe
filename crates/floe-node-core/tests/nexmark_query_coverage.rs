use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::Result;
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

    let expected_supported = ["q0", "q1", "q2", "q5", "q8", "q12", "q14", "q21", "q22"]
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

async fn collect_coverage() -> Result<BTreeMap<String, QueryCoverageResult>> {
    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions()?);

    let available_sources = [
        "nexmark_person",
        "person",
        "nexmark_auction",
        "auction",
        "nexmark_bid",
        "bid",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();

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
