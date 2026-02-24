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
