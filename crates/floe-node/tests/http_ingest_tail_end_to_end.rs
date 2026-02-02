use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::time::sleep;

const MV_SQL: &str = "CREATE MATERIALIZED VIEW mv_http_ingest AS \
     SELECT auction, bidder, price FROM nexmark_bid";

#[tokio::test]
#[ignore = "requires TCP sockets and HTTP; run with cargo test -p floe-node --test http_ingest_tail_end_to_end -- --ignored"]
async fn http_ingest_tail_streams_rows() -> Result<()> {
    let pg_port = find_unused_port()?;
    let http_port = find_unused_port()?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let config_path = temp_path(&format!("floe-http-tail-{run_id}.json"));
    let sink_path = temp_path(&format!("floe-http-tail-{run_id}.jsonl"));

    let config = json!({
        "connectors": [
            {
                "type": "http",
                "host": "127.0.0.1",
                "port": http_port
            }
        ],
        "sinks": [
            {
                "type": "file",
                "path": sink_path,
                "mv": "mv_http_ingest",
                "with_snapshot": true,
                "append": false
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;

    let binary = env!("CARGO_BIN_EXE_floe-node");
    let mut child = Command::new(binary)
        .env("FLOE_PG_ADDR", format!("127.0.0.1:{pg_port}"))
        .arg("run")
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .arg("--mv-query")
        .arg(MV_SQL)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn floe-node binary")?;

    let test_result = async {
        wait_for_healthz(&http_addr).await?;

        let payload = json!({
            "source": "nexmark_bid",
            "data": {
                "auction": 1,
                "bidder": 7,
                "price": 50,
                "channel": "web",
                "url": "http://example.com",
                "date_time": 1_700_000_000_i64,
                "extra": "x"
            }
        });
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{http_addr}/ingest"))
            .json(&payload)
            .send()
            .await
            .context("send http ingest request")?;
        if response.status() != StatusCode::ACCEPTED {
            bail!(
                "expected 202 from ingest endpoint, got {}",
                response.status()
            );
        }

        let rows = wait_for_tail_rows(Path::new(&sink_path)).await?;

        if rows.is_empty() {
            bail!("expected tail to stream at least one row");
        }
        let mut matched = false;
        for row in rows {
            let Some(obj) = row.as_object() else {
                continue;
            };
            let auction = obj.get("auction").and_then(|value| value.as_i64());
            let bidder = obj.get("bidder").and_then(|value| value.as_i64());
            let price = obj.get("price").and_then(|value| value.as_i64());
            if auction == Some(1) && bidder == Some(7) && price == Some(50) {
                matched = true;
                break;
            }
        }
        if !matched {
            bail!("tail rows did not include expected auction/bidder/price payload");
        }

        Ok(())
    }
    .await;

    child.start_kill().ok();
    let _ = child.wait().await;
    let _ = std::fs::remove_file(&config_path);
    let _ = std::fs::remove_file(&sink_path);

    test_result
}

async fn wait_for_healthz(addr: &str) -> Result<()> {
    let client = reqwest::Client::new();
    for attempt in 0..50 {
        match client.get(format!("{addr}/healthz")).send().await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(()),
            Ok(_) | Err(_) if attempt < 49 => sleep(Duration::from_millis(100)).await,
            Ok(response) => bail!("healthz returned {}", response.status()),
            Err(err) => bail!("healthz never became ready: {err}"),
        }
    }
    unreachable!("loop either returns success or bail");
}

async fn wait_for_tail_rows(path: &Path) -> Result<Vec<Value>> {
    for attempt in 0..60 {
        match tokio::fs::read_to_string(path).await {
            Ok(contents) => {
                let mut rows = Vec::new();
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    let value: Value = serde_json::from_str(line).context("parse tail row json")?;
                    rows.push(value);
                }
                if !rows.is_empty() {
                    return Ok(rows);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        if attempt == 30 {
            tracing::warn!("waiting for tail sink output");
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!(
        "tail sink output never appeared in {}",
        path.to_string_lossy()
    )
}

fn find_unused_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind to ephemeral port")?;
    let port = listener.local_addr().context("read ephemeral port")?.port();
    Ok(port)
}

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(name);
    path
}
