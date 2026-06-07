use std::future::Future;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::process::{Child, Command};

use crate::wait::wait_until;

#[path = "http_ready.rs"]
mod http_ready;

pub(crate) use http_ready::wait_for_healthz;

pub(crate) async fn spawn_node(
    config_path: &Path,
    data_dir: &Path,
    pg_port: u16,
    mv_sql: Option<&str>,
) -> Result<Child> {
    spawn_node_with_args(config_path, data_dir, pg_port, mv_sql, &[]).await
}

pub(crate) async fn spawn_node_with_args(
    config_path: &Path,
    data_dir: &Path,
    pg_port: u16,
    mv_sql: Option<&str>,
    extra_args: &[&str],
) -> Result<Child> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe-node"));
    cmd.arg("run");
    if pg_port > 0 {
        cmd.arg("--pgwire-addr").arg(format!("127.0.0.1:{pg_port}"));
    } else {
        cmd.arg("--disable-pgwire");
    }
    cmd.arg("--data-dir")
        .arg(data_dir)
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .stdout(if std::env::var_os("FLOE_TEST_NODE_STDERR").is_some() {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stderr(if std::env::var_os("FLOE_TEST_NODE_STDERR").is_some() {
            Stdio::inherit()
        } else {
            Stdio::null()
        });
    if !extra_args.contains(&"--admin-port") {
        cmd.arg("--admin-port").arg("0");
    }
    for arg in extra_args {
        cmd.arg(*arg);
    }
    if let Some(sql) = mv_sql {
        cmd.arg("--mv-query").arg(sql);
    }
    cmd.spawn().context("spawn floe-node")
}

pub(crate) async fn stop_child(child: &mut Child, signal: &str) {
    if let Some(pid) = child.id() {
        let _ = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(pid.to_string())
            .status();
    }
    let _ = child.wait().await;
}

pub(crate) async fn post_bid_with_extra(
    addr: &str,
    auction: i64,
    bidder: i64,
    price: i64,
    extra: &str,
) -> Result<()> {
    let payload = json!({
        "source": "nexmark_bid",
        "data": {
            "auction": auction,
            "bidder": bidder,
            "price": price,
            "channel": "web",
            "url": "http://example.com",
            "date_time": 1_700_000_000_i64 + auction,
            "extra": extra
        }
    });
    let response = reqwest::Client::new()
        .post(format!("{addr}/ingest"))
        .json(&payload)
        .send()
        .await
        .context("post bid payload")?;
    if response.status() != StatusCode::OK {
        bail!("ingest returned {}", response.status());
    }
    Ok(())
}

pub(crate) async fn wait_for_jsonl_rows_matching(
    path: &Path,
    attempts: usize,
    predicate: impl Fn(&Value) -> bool,
) -> Result<Vec<Value>> {
    wait_until(
        format!("predicate match in {}", path.to_string_lossy()),
        Duration::from_millis(100 * attempts as u64),
        Duration::from_millis(100),
        || async {
            let rows = read_jsonl_rows(path).await?;
            if rows.iter().any(&predicate) {
                Ok(Some(rows))
            } else {
                Ok(None)
            }
        },
    )
    .await
}

pub(crate) async fn wait_for_count_at_least<F, Fut>(
    label: impl AsRef<str>,
    min_count: i64,
    attempts: usize,
    mut query_count: F,
) -> Result<i64>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<i64>>,
{
    let label = label.as_ref();
    wait_until(
        format!("{label} count >= {min_count}"),
        Duration::from_millis(100 * attempts as u64),
        Duration::from_millis(100),
        || {
            let count = query_count();
            async move {
                match count.await {
                    Ok(count) if count >= min_count => Ok(Some(count)),
                    Ok(count) => {
                        bail!("{label} count {count} below {min_count}")
                    }
                    Err(err) => Err(err),
                }
            }
        },
    )
    .await
}

pub(crate) async fn read_jsonl_rows(path: &Path) -> Result<Vec<Value>> {
    let contents = match tokio::fs::read_to_string(path).await {
        Ok(value) => value,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut rows = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        rows.push(serde_json::from_str::<Value>(line).context("parse jsonl row")?);
    }
    Ok(rows)
}
