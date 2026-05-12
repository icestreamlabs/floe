use std::process::Command;

use anyhow::{Context, Result, bail};
use tempfile::TempDir;

#[test]
fn dry_run_accepts_full_config_first_startup() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let config_path = temp_dir.path().join("floe.toml");
    let sink_path = temp_dir.path().join("sink.jsonl");
    let config = format!(
        r#"
[[connectors]]
type = "generator"
name = "gen"
events_per_second = 5.0
max_events = 10

[[materialized_views]]
name = "mv_cfg"
query = "SELECT auction, bidder, price FROM nexmark_bid"
if_not_exists = true

[[sinks]]
type = "file"
name = "sink_file"
path = "{}"
mv = "mv_cfg"
with_snapshot = true

[runtime]
ingest_batch_size = 128
mv_retain_last = 2
output_consolidation_mode = "key"

[storage]
await_durable = true
zset_compaction_max_chain_len = 64

[maintenance]
paused = true
inspect_namespace = ["mv::mv_cfg"]
"#,
        sink_path.to_string_lossy()
    );
    std::fs::write(&config_path, config).context("write config file")?;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--config")
        .arg(config_path.as_os_str())
        .arg("--dry-run")
        .output()
        .context("run floe-node dry-run")?;
    if !output.status.success() {
        bail!(
            "expected dry-run to succeed, stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn dry_run_reports_invalid_config_with_field_path() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let config_path = temp_dir.path().join("invalid.toml");
    let config = r#"
[[connectors]]
type = "kafka"
brokers = "localhost:9092"
topics = []
"#;
    std::fs::write(&config_path, config).context("write invalid config file")?;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--config")
        .arg(config_path.as_os_str())
        .arg("--dry-run")
        .output()
        .context("run floe-node dry-run with invalid config")?;
    assert!(
        !output.status.success(),
        "expected invalid config to fail dry-run"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("connectors[0].topics must not be empty"),
        "expected connector field-path error in stderr, got:\n{stderr}"
    );
    Ok(())
}

#[test]
fn dry_run_accepts_sql_postgres_cdc_source_table_and_mv() -> Result<()> {
    let sql = r#"
        CREATE SOURCE pg_main WITH (
            connector = 'postgres-cdc',
            connection = 'postgres://postgres:postgres@localhost/postgres',
            slot.name = 'floe_slot',
            publication.name = 'floe_pub'
        );
        CREATE TABLE orders (
            id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL,
            amount BIGINT NOT NULL,
            status TEXT
        ) FROM pg_main TABLE 'public.orders';
        CREATE MATERIALIZED VIEW mv_orders AS
        SELECT id, amount FROM orders WHERE amount > 100;
    "#;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--dry-run")
        .arg("--mv-query")
        .arg(sql)
        .output()
        .context("run floe-node SQL CDC dry-run")?;
    if !output.status.success() {
        bail!(
            "expected SQL CDC dry-run to succeed, stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn dry_run_rejects_sql_cdc_table_without_primary_key() -> Result<()> {
    let sql = r#"
        CREATE SOURCE pg_main WITH (
            connector = 'postgres-cdc',
            connection = 'postgres://postgres:postgres@localhost/postgres',
            slot.name = 'floe_slot'
        );
        CREATE TABLE orders (id BIGINT, amount BIGINT)
        FROM pg_main TABLE 'public.orders';
    "#;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--dry-run")
        .arg("--mv-query")
        .arg(sql)
        .output()
        .context("run floe-node invalid SQL CDC dry-run")?;
    assert!(
        !output.status.success(),
        "expected missing-primary-key CDC table to fail dry-run"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("must declare exactly one primary key column"),
        "expected primary-key validation error in stderr, got:\n{stderr}"
    );
    Ok(())
}

#[test]
fn dry_run_rejects_mv_over_raw_cdc_source() -> Result<()> {
    let sql = r#"
        CREATE SOURCE pg_main WITH (
            connector = 'postgres-cdc',
            connection = 'postgres://postgres:postgres@localhost/postgres',
            slot.name = 'floe_slot'
        );
        CREATE MATERIALIZED VIEW mv_raw AS SELECT * FROM pg_main;
    "#;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--dry-run")
        .arg("--mv-query")
        .arg(sql)
        .output()
        .context("run floe-node raw CDC source dry-run")?;
    assert!(
        !output.status.success(),
        "expected raw CDC source MV to fail dry-run"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("pg_main"),
        "expected raw source name in dry-run error, got:\n{stderr}"
    );
    Ok(())
}
