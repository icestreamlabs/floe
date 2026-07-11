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
fn dry_run_accepts_multiple_materialized_views() -> Result<()> {
    let sql = r#"
        CREATE MATERIALIZED VIEW mv_a AS SELECT 1;
        CREATE MATERIALIZED VIEW mv_b AS SELECT 2;
    "#;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--dry-run")
        .arg("--mv-query")
        .arg(sql)
        .output()
        .context("run floe-node multiple MV dry-run")?;
    if !output.status.success() {
        bail!(
            "expected multiple MV dry-run to succeed, stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn dry_run_accepts_sql_runtime_sources_and_full_sink_options() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let input_path = temp_dir.path().join("events.jsonl");
    let sink_path = temp_dir.path().join("sink.jsonl");
    std::fs::write(
        &input_path,
        r#"{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}"#,
    )
    .context("write input file")?;
    let sql = format!(
        r#"
        CREATE SOURCE file_bid WITH (
            connector = 'file',
            path = '{}',
            default_source = 'nexmark_bid'
        );
        CREATE SOURCE http_bid WITH (
            connector = 'http',
            host = '127.0.0.1',
            port = 18080,
            default_source = 'nexmark_bid'
        );
        CREATE SOURCE kafka_bid WITH (
            connector = 'kafka',
            brokers = 'localhost:9092',
            topics = 'nexmark_bid',
            group_id = 'floe_sql',
            default_source = 'nexmark_bid',
            poll_ms = 5,
            max_messages_per_tick = 512,
            format = 'floe-json'
        );
        CREATE SOURCE object_bid WITH (
            connector = 'object-store',
            url = 'file://{}',
            default_source = 'nexmark_bid'
        );
        CREATE SOURCE gen_sql WITH (
            connector = 'generator',
            events_per_second = 10,
            max_events = 100
        );
        CREATE MATERIALIZED VIEW mv_sql_sources AS
        SELECT auction, bidder, price FROM nexmark_bid;
        CREATE SINK sink_file_sql FROM mv_sql_sources WITH (
            connector = 'file',
            path = '{}',
            append = false,
            with_snapshot = true,
            batch_rows = 10,
            batch_bytes = 65536,
            queue_capacity = 32
        );
        "#,
        input_path.display(),
        input_path.display(),
        sink_path.display()
    );

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--dry-run")
        .arg("--mv-query")
        .arg(sql)
        .output()
        .context("run floe-node SQL source dry-run")?;
    if !output.status.success() {
        bail!(
            "expected dry-run to succeed, stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn dry_run_accepts_sql_source_inline_schema_for_mv() -> Result<()> {
    let sql = r#"
        CREATE SOURCE orders (
            id BIGINT PRIMARY KEY,
            amount BIGINT,
            status TEXT,
            created_at TIMESTAMP
        )
        WITH (
            connector = 'kafka',
            brokers = 'localhost:9092',
            topic = 'orders'
        )
        FORMAT PLAIN ENCODE JSON;

        CREATE MATERIALIZED VIEW mv_orders AS
        SELECT id, amount FROM orders WHERE amount > 10;
    "#;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--dry-run")
        .arg("--mv-query")
        .arg(sql)
        .output()
        .context("run floe-node inline source schema dry-run")?;
    if !output.status.success() {
        bail!(
            "expected dry-run to succeed, stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn dry_run_accepts_duplicate_sql_source_with_if_not_exists() -> Result<()> {
    let sql = r#"
        CREATE SOURCE orders (
            id BIGINT PRIMARY KEY,
            amount BIGINT
        )
        WITH (
            connector = 'kafka',
            brokers = 'localhost:9092',
            topic = 'orders'
        )
        FORMAT PLAIN ENCODE JSON;

        CREATE SOURCE IF NOT EXISTS orders (
            id BIGINT PRIMARY KEY,
            amount BIGINT
        )
        WITH (
            connector = 'kafka',
            properties.bootstrap.server = 'localhost:9092',
            topic = 'orders',
            scan.startup.mode = 'earliest'
        )
        FORMAT PLAIN ENCODE JSON;

        CREATE MATERIALIZED VIEW mv_orders AS
        SELECT id, amount FROM orders;
    "#;

    let output = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--dry-run")
        .arg("--mv-query")
        .arg(sql)
        .output()
        .context("run floe-node duplicate SQL source dry-run")?;
    if !output.status.success() {
        bail!(
            "expected dry-run to succeed, stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
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
