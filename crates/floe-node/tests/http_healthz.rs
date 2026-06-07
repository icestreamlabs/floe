use std::path::PathBuf;
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "support/http_ready.rs"]
mod http_ready;
#[path = "support/ports.rs"]
mod ports;
#[path = "support/wait.rs"]
mod wait;

use anyhow::{Context, Result, bail};
use http_ready::wait_for_healthz;
use ports::find_unused_port;
use serde_json::json;
use tokio::process::Command;

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
        .arg("run")
        .arg("--pgwire-addr")
        .arg(format!("127.0.0.1:{pg_port}"))
        .arg("--admin-port")
        .arg("0")
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn floe-node binary")?;

    let test_result = async {
        wait_for_healthz(&http_addr).await?;
        let client = reqwest::Client::new();
        let body: serde_json::Value = client
            .get(format!("{http_addr}/healthz"))
            .send()
            .await
            .context("request healthz")?
            .json()
            .await
            .context("decode healthz json")?;
        if body.get("process_alive").and_then(|v| v.as_bool()) != Some(true) {
            bail!("healthz did not report process_alive=true: {body}");
        }
        let readyz = client
            .get(format!("{http_addr}/readyz"))
            .send()
            .await
            .context("request readyz")?;
        if !readyz.status().is_success() {
            bail!("readyz returned {}", readyz.status());
        }
        let readyz_body: serde_json::Value = readyz.json().await.context("decode readyz json")?;
        if readyz_body.get("executor_alive").and_then(|v| v.as_bool()) != Some(true) {
            bail!("readyz did not report executor_alive=true: {readyz_body}");
        }
        if readyz_body
            .get("storage_reachable")
            .and_then(|v| v.as_bool())
            != Some(true)
        {
            bail!("readyz did not report storage_reachable=true: {readyz_body}");
        }
        if readyz_body.get("runtime_ready").and_then(|v| v.as_bool()) != Some(true) {
            bail!("readyz did not report runtime_ready=true: {readyz_body}");
        }
        Ok(())
    }
    .await;

    child.start_kill().ok();
    let _ = child.wait().await;
    let _ = std::fs::remove_file(&config_path);
    test_result
}

#[tokio::test]
async fn admin_healthz_is_available_without_http_ingest() -> Result<()> {
    let admin_port = find_unused_port()?;
    let admin_addr = format!("http://127.0.0.1:{admin_port}");
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let config_path = temp_path(&format!("floe-admin-healthz-{run_id}.json"));
    let config = json!({
        "connectors": [
            {
                "type": "generator",
                "events_per_second": 5.0
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_floe-node"))
        .arg("run")
        .arg("--disable-pgwire")
        .arg("--admin-port")
        .arg(admin_port.to_string())
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn floe-node binary")?;

    let test_result = async {
        wait_for_healthz(&admin_addr).await?;
        let client = reqwest::Client::new();
        let body: serde_json::Value = client
            .get(format!("{admin_addr}/healthz"))
            .send()
            .await
            .context("request admin healthz")?
            .json()
            .await
            .context("decode admin healthz json")?;
        if body.get("process_alive").and_then(|v| v.as_bool()) != Some(true) {
            bail!("admin healthz did not report process_alive=true: {body}");
        }
        let metrics = client
            .get(format!("{admin_addr}/metrics"))
            .send()
            .await
            .context("request admin metrics")?;
        if !metrics.status().is_success() {
            bail!("admin metrics returned {}", metrics.status());
        }
        let metrics_body = metrics.text().await.context("decode metrics body")?;
        if !metrics_body.contains("floe_ingest_queue_depth") {
            bail!("admin metrics did not include floe_ingest_queue_depth");
        }
        Ok(())
    }
    .await;

    child.start_kill().ok();
    let _ = child.wait().await;
    let _ = std::fs::remove_file(&config_path);
    test_result
}

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(name);
    path
}
