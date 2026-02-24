use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::json;
use tokio::process::Command;
use tokio::time::sleep;

#[tokio::test]
async fn http_healthz_reports_ready_during_runtime() -> Result<()> {
    let pg_port = find_unused_port()?;
    let http_port = find_unused_port()?;
    let http_addr = format!("http://127.0.0.1:{http_port}");

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let config_path = temp_path(&format!("floe-healthz-{run_id}.json"));
    let config = json!({
        "connectors": [
            {
                "type": "http",
                "host": "127.0.0.1",
                "port": http_port,
                "default_source": "nexmark_bid"
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .env("FLOE_PG_ADDR", format!("127.0.0.1:{pg_port}"))
        .arg("run")
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn floe-node binary")?;

    let test_result = async {
        let client = reqwest::Client::new();
        for attempt in 0..50 {
            match client.get(format!("{http_addr}/healthz")).send().await {
                Ok(response) if response.status() == StatusCode::OK => {
                    let body: serde_json::Value =
                        response.json().await.context("decode healthz json")?;
                    if body.get("process_alive").and_then(|v| v.as_bool()) != Some(true) {
                        bail!("healthz did not report process_alive=true: {body}");
                    }
                    if body.get("executor_alive").and_then(|v| v.as_bool()) != Some(true) {
                        bail!("healthz did not report executor_alive=true: {body}");
                    }
                    if body.get("storage_reachable").and_then(|v| v.as_bool()) != Some(true) {
                        bail!("healthz did not report storage_reachable=true: {body}");
                    }
                    return Ok(());
                }
                Ok(_) | Err(_) if attempt < 49 => sleep(Duration::from_millis(100)).await,
                Ok(response) => bail!("healthz returned {}", response.status()),
                Err(err) => bail!("healthz never became ready: {err}"),
            }
        }
        unreachable!("loop either returns success or bail");
    }
    .await;

    child.start_kill().ok();
    let _ = child.wait().await;
    let _ = std::fs::remove_file(&config_path);
    test_result
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
