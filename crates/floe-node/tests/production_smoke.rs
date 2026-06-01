use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[path = "support/node_process.rs"]
mod node_process;
#[path = "support/ports.rs"]
mod ports;

use anyhow::{Context, Result, bail};
use node_process::{
    post_bid_with_extra, spawn_node, spawn_node_with_args, stop_child, wait_for_healthz,
};
use ports::find_unused_port;
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::sleep;
use tokio_postgres::NoTls;

const MV_SQL: &str = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_smoke AS \
     SELECT auction, bidder, price FROM nexmark_bid";

#[tokio::test]
async fn smoke_generator_mv_emits_sink_rows() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = 0;
    let data_dir = temp_dir.path().join("data");
    let sink_path = temp_dir.path().join("generator_sink.jsonl");
    let config_path = temp_dir.path().join("generator.toml");
    let config = format!(
        r#"
[[connectors]]
type = "generator"
events_per_second = 100.0

[[sinks]]
type = "file"
path = "{}"
mv = "mv_smoke"
with_snapshot = true
append = true

[storage]
await_durable = false
"#,
        sink_path.to_string_lossy()
    );
    std::fs::write(&config_path, config).context("write generator config")?;

    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;

    let test_result = async {
        wait_for_rows_matching(&sink_path, |value| {
            value.get("auction").and_then(Value::as_i64).is_some()
                && value.get("bidder").and_then(Value::as_i64).is_some()
                && value.get("price").and_then(Value::as_i64).is_some()
        })
        .await?;
        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    test_result
}

#[tokio::test]
async fn smoke_restart_recovers_snapshot_and_new_updates() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = 0;
    let http_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let sink_path = temp_dir.path().join("restart_sink.jsonl");
    let config_path = temp_dir.path().join("restart.toml");
    let config = format!(
        r#"
[[connectors]]
type = "http"
host = "127.0.0.1"
port = {http_port}
default_source = "nexmark_bid"

[[sinks]]
type = "file"
path = "{}"
mv = "mv_smoke"
with_snapshot = true
append = true
"#,
        sink_path.to_string_lossy()
    );
    std::fs::write(&config_path, config).context("write restart config")?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    post_bid(&http_addr, 1, 7, 50).await?;
    wait_for_rows_matching(&sink_path, |value| {
        value.get("auction").and_then(Value::as_i64) == Some(1)
    })
    .await?;
    stop_child(&mut first, "INT").await;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    post_bid(&http_addr, 2, 8, 60).await?;
    wait_for_rows_matching(&sink_path, |value| {
        value.get("auction").and_then(Value::as_i64) == Some(1)
    })
    .await?;
    wait_for_rows_matching(&sink_path, |value| {
        value.get("auction").and_then(Value::as_i64) == Some(2)
    })
    .await?;
    stop_child(&mut restarted, "INT").await;

    Ok(())
}

#[tokio::test]
async fn smoke_crash_restart_recovers_and_processes_new_ticks() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = 0;
    let http_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let sink_path = temp_dir.path().join("crash_sink.jsonl");
    let config_path = temp_dir.path().join("crash.toml");
    let config = format!(
        r#"
[[connectors]]
type = "http"
host = "127.0.0.1"
port = {http_port}
default_source = "nexmark_bid"

[[sinks]]
type = "file"
path = "{}"
mv = "mv_smoke"
with_snapshot = true
append = true
"#,
        sink_path.to_string_lossy()
    );
    std::fs::write(&config_path, config).context("write crash config")?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    post_bid(&http_addr, 10, 70, 150).await?;
    wait_for_rows_matching(&sink_path, |value| {
        value.get("auction").and_then(Value::as_i64) == Some(10)
    })
    .await?;
    stop_child(&mut first, "KILL").await;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    post_bid(&http_addr, 11, 71, 160).await?;
    wait_for_rows_matching(&sink_path, |value| {
        value.get("auction").and_then(Value::as_i64) == Some(10)
    })
    .await?;
    wait_for_rows_matching(&sink_path, |value| {
        value.get("auction").and_then(Value::as_i64) == Some(11)
    })
    .await?;
    stop_child(&mut restarted, "INT").await;

    Ok(())
}

#[tokio::test]
async fn smoke_http_source_journal_is_queryable_as_source_table() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let http_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("http_source_journal.toml");
    let config = format!(
        r#"
[[connectors]]
type = "http"
host = "127.0.0.1"
port = {http_port}
default_source = "nexmark_bid"
"#
    );
    std::fs::write(&config_path, config).context("write http source journal config")?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    post_bid(&http_addr, 17, 117, 170).await?;
    let row = wait_for_source_bid(pg_port, 17).await?;
    stop_child(&mut child, "INT").await;

    assert_eq!(row.bidder, 117);
    assert_eq!(row.price, 170);
    assert_eq!(row.channel, "web");
    assert_eq!(row.url, "http://example.com");
    assert_eq!(row.extra, "smoke");
    Ok(())
}

#[tokio::test]
async fn smoke_sigterm_restart_recovers_and_processes_new_ticks() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let http_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("sigterm_restart.toml");
    let config = format!(
        r#"
[[connectors]]
type = "http"
host = "127.0.0.1"
port = {http_port}
default_source = "nexmark_bid"
"#
    );
    std::fs::write(&config_path, config).context("write sigterm config")?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    post_bid(&http_addr, 14, 74, 174).await?;
    wait_for_auction_count_at_least(pg_port, 14, 1).await?;
    stop_child(&mut first, "TERM").await;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    wait_for_auction_count_at_least(pg_port, 14, 1).await?;
    post_bid(&http_addr, 15, 75, 175).await?;
    wait_for_auction_count_at_least(pg_port, 15, 1).await?;
    stop_child(&mut restarted, "INT").await;

    Ok(())
}

#[tokio::test]
async fn smoke_crash_restart_keeps_mv_queryable() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let http_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("crash_queryable.toml");
    let config = format!(
        r#"
[[connectors]]
type = "http"
host = "127.0.0.1"
port = {http_port}
default_source = "nexmark_bid"
"#
    );
    std::fs::write(&config_path, config).context("write crash config")?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    post_bid(&http_addr, 21, 121, 210).await?;
    wait_for_mv_count_at_least(pg_port, 1).await?;
    stop_child(&mut first, "KILL").await;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    wait_for_mv_count_at_least(pg_port, 1).await?;
    post_bid(&http_addr, 22, 122, 220).await?;
    wait_for_mv_count_at_least(pg_port, 2).await?;
    stop_child(&mut restarted, "INT").await;

    Ok(())
}

#[tokio::test]
async fn smoke_crash_between_ingest_and_tick_commit_loses_uncommitted_tick() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let http_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("crash_pre_commit.toml");
    let config = format!(
        r#"
[[connectors]]
type = "http"
host = "127.0.0.1"
port = {http_port}
default_source = "nexmark_bid"
"#
    );
    std::fs::write(&config_path, config).context("write crash config")?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let mut first = spawn_node_with_args(
        &config_path,
        &data_dir,
        pg_port,
        Some(MV_SQL),
        &["--pre-tick-commit-delay-ms", "3000"],
    )
    .await?;
    wait_for_healthz(&http_addr).await?;
    let precommit_addr = http_addr.clone();
    let precommit_post = tokio::spawn(async move { post_bid(&precommit_addr, 30, 130, 300).await });
    sleep(Duration::from_millis(200)).await;
    assert!(
        !precommit_post.is_finished(),
        "HTTP ingest should wait for tick commit before returning"
    );
    stop_child(&mut first, "KILL").await;
    let _ = precommit_post.await;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(MV_SQL)).await?;
    wait_for_healthz(&http_addr).await?;
    sleep(Duration::from_millis(300)).await;
    let precommit_count = query_auction_count(pg_port, 30).await?;
    assert_eq!(
        precommit_count, 0,
        "rows ingested before tick commit should not survive hard crash in the pre-commit window"
    );

    post_bid(&http_addr, 31, 131, 310).await?;
    wait_for_auction_count_at_least(pg_port, 31, 1).await?;
    stop_child(&mut restarted, "INT").await;

    Ok(())
}

async fn post_bid(addr: &str, auction: i64, bidder: i64, price: i64) -> Result<()> {
    post_bid_with_extra(addr, auction, bidder, price, "smoke").await
}

async fn wait_for_rows_matching(
    path: &Path,
    predicate: impl Fn(&Value) -> bool,
) -> Result<Vec<Value>> {
    node_process::wait_for_jsonl_rows_matching(path, 80, predicate).await
}

async fn wait_for_mv_count_at_least(pg_port: u16, min_count: i64) -> Result<i64> {
    for _ in 0..80 {
        match query_mv_count(pg_port).await {
            Ok(count) if count >= min_count => return Ok(count),
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("timed out waiting for mv_smoke row count >= {min_count}");
}

async fn wait_for_auction_count_at_least(
    pg_port: u16,
    auction: i64,
    min_count: i64,
) -> Result<i64> {
    for _ in 0..80 {
        match query_auction_count(pg_port, auction).await {
            Ok(count) if count >= min_count => return Ok(count),
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("timed out waiting for auction {auction} row count >= {min_count}");
}

async fn query_mv_count(pg_port: u16) -> Result<i64> {
    let dsn = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to pgwire endpoint")?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one("SELECT COUNT(*)::BIGINT FROM mv_smoke", &[])
        .await
        .context("query mv_smoke count")?;
    let count: i64 = row.try_get(0).context("decode row count")?;
    drop(client);
    connection_task.abort();
    let _ = connection_task.await;
    Ok(count)
}

async fn query_auction_count(pg_port: u16, auction: i64) -> Result<i64> {
    let dsn = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to pgwire endpoint")?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM mv_smoke WHERE auction = $1",
            &[&auction],
        )
        .await
        .with_context(|| format!("query mv_smoke count for auction {auction}"))?;
    let count: i64 = row.try_get(0).context("decode auction row count")?;
    drop(client);
    connection_task.abort();
    let _ = connection_task.await;
    Ok(count)
}

struct SourceBidRow {
    bidder: i64,
    price: i64,
    channel: String,
    url: String,
    extra: String,
}

async fn wait_for_source_bid(pg_port: u16, auction: i64) -> Result<SourceBidRow> {
    for _ in 0..80 {
        match query_source_bid(pg_port, auction).await {
            Ok(Some(row)) => return Ok(row),
            Ok(None) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("timed out waiting for source journal row for auction {auction}");
}

async fn query_source_bid(pg_port: u16, auction: i64) -> Result<Option<SourceBidRow>> {
    let dsn = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to pgwire endpoint")?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_opt(
            "SELECT bidder, price, channel, url, extra FROM nexmark_bid WHERE auction = $1",
            &[&auction],
        )
        .await
        .with_context(|| format!("query source journal row for auction {auction}"))?;
    drop(client);
    connection_task.abort();
    let _ = connection_task.await;
    row.map(|row| {
        Ok(SourceBidRow {
            bidder: row.try_get(0).context("decode bidder")?,
            price: row.try_get(1).context("decode price")?,
            channel: row.try_get(2).context("decode channel")?,
            url: row.try_get(3).context("decode url")?,
            extra: row.try_get(4).context("decode extra")?,
        })
    })
    .transpose()
}

#[allow(dead_code)]
fn unique_temp_path(prefix: &str, suffix: &str) -> PathBuf {
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!("{prefix}-{run_id}.{suffix}"));
    path
}
