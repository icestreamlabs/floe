use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tokio::time::sleep;

const MV_SQL: &str = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_smoke AS \
     SELECT auction, bidder, price FROM nexmark_bid";

#[tokio::test]
async fn smoke_generator_mv_emits_tail_rows() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
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
    let pg_port = find_unused_port()?;
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
    let pg_port = find_unused_port()?;
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

async fn spawn_node(
    config_path: &Path,
    data_dir: &Path,
    pg_port: u16,
    mv_sql: Option<&str>,
) -> Result<Child> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe-node"));
    cmd.env("FLOE_PG_ADDR", format!("127.0.0.1:{pg_port}"))
        .env("FLOE_DATA_DIR", data_dir)
        .arg("run")
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(sql) = mv_sql {
        cmd.arg("--mv-query").arg(sql);
    }
    cmd.spawn().context("spawn floe-node")
}

async fn stop_child(child: &mut Child, signal: &str) {
    if let Some(pid) = child.id() {
        let _ = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(pid.to_string())
            .status();
    }
    let _ = child.wait().await;
}

async fn wait_for_healthz(addr: &str) -> Result<()> {
    let client = reqwest::Client::new();
    for attempt in 0..60 {
        match client.get(format!("{addr}/healthz")).send().await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(()),
            Ok(_) | Err(_) if attempt < 59 => sleep(Duration::from_millis(100)).await,
            Ok(response) => bail!("healthz returned {}", response.status()),
            Err(err) => bail!("healthz never became ready: {err}"),
        }
    }
    unreachable!("loop either returns success or bails")
}

async fn post_bid(addr: &str, auction: i64, bidder: i64, price: i64) -> Result<()> {
    let payload = json!({
        "source": "nexmark_bid",
        "data": {
            "auction": auction,
            "bidder": bidder,
            "price": price,
            "channel": "web",
            "url": "http://example.com",
            "date_time": 1_700_000_000_i64 + auction,
            "extra": "smoke"
        }
    });
    let response = reqwest::Client::new()
        .post(format!("{addr}/ingest"))
        .json(&payload)
        .send()
        .await
        .context("post bid payload")?;
    if response.status() != StatusCode::ACCEPTED {
        bail!("ingest returned {}", response.status());
    }
    Ok(())
}

async fn wait_for_rows_matching(
    path: &Path,
    predicate: impl Fn(&Value) -> bool,
) -> Result<Vec<Value>> {
    for _ in 0..80 {
        let rows = read_rows(path).await?;
        if rows.iter().any(|row| predicate(row)) {
            return Ok(rows);
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!("predicate did not match rows in {}", path.to_string_lossy())
}

async fn read_rows(path: &Path) -> Result<Vec<Value>> {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) => {
            let mut rows = Vec::new();
            for line in contents.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                rows.push(serde_json::from_str(trimmed).context("parse sink row json")?);
            }
            Ok(rows)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

fn find_unused_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind to ephemeral port")?;
    Ok(listener.local_addr().context("read ephemeral port")?.port())
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
