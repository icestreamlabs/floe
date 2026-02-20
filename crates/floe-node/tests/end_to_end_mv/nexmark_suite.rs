use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use floe_node_core::nexmark_queries::{CANONICAL_NEXMARK_QUERY_IDS, canonical_nexmark_queries};
use serde::Deserialize;

use super::harness::MvTestHarness;

#[derive(Debug, Deserialize)]
struct ExpectedCountsFile {
    queries: BTreeMap<String, ExpectedQueryCount>,
}

#[derive(Debug, Deserialize)]
struct ExpectedQueryCount {
    initial: i64,
    incremental: i64,
}

#[test]
fn fixture_files_cover_all_canonical_queries() -> Result<()> {
    let fixture_path = fixtures_dir().join("nexmark_suite_inputs.json");
    let fixture = std::fs::read_to_string(&fixture_path)
        .with_context(|| format!("read fixture file {}", fixture_path.display()))?;
    let fixture_value: serde_json::Value = serde_json::from_str(&fixture)
        .with_context(|| format!("parse fixture file {}", fixture_path.display()))?;
    assert!(fixture_value.get("initial").is_some());
    assert!(fixture_value.get("incremental").is_some());

    let expected_path = fixtures_dir().join("nexmark_suite_expected_counts.json");
    let expected_raw = std::fs::read_to_string(&expected_path)
        .with_context(|| format!("read expected file {}", expected_path.display()))?;
    let expected: ExpectedCountsFile = serde_json::from_str(&expected_raw)
        .with_context(|| format!("parse expected file {}", expected_path.display()))?;

    let expected_ids = expected.queries.keys().cloned().collect::<BTreeSet<_>>();
    let canonical_ids = CANONICAL_NEXMARK_QUERY_IDS
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    assert_eq!(expected_ids, canonical_ids);

    for (query_id, counts) in expected.queries {
        assert!(
            counts.initial >= 0,
            "{query_id} initial count must be non-negative"
        );
        assert!(
            counts.incremental >= 0,
            "{query_id} incremental count must be non-negative"
        );
    }

    Ok(())
}

#[tokio::test]
async fn builds_and_queries_all_canonical_nexmark_views() -> Result<()> {
    for query in canonical_nexmark_queries() {
        let view_name = format!("mv_{}", query.id);
        let ddl = format!("CREATE MATERIALIZED VIEW {view_name} AS {}", query.sql);
        let harness = MvTestHarness::new(&view_name, &ddl)
            .await
            .with_context(|| format!("build harness for {}", query.id))?;

        let (session, _bridge) = harness
            .session_with_view()
            .await
            .with_context(|| format!("open session for {}", query.id))?;
        let statement = format!("SELECT * FROM {view_name} LIMIT 0");
        let dataframe = session
            .sql(&statement)
            .await
            .with_context(|| format!("build query dataframe for {}", query.id))?;
        let schema = dataframe.schema().clone();
        let _ = dataframe
            .collect()
            .await
            .with_context(|| format!("execute dataframe for {}", query.id))?;
        assert!(
            !schema.fields().is_empty(),
            "{} should expose a non-empty schema",
            query.id
        );
    }

    Ok(())
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}
