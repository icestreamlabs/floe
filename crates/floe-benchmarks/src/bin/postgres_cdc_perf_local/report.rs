use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::json;

use super::{Config, DatasetPlan, LoadPlan, TargetKind};
use crate::harness_common::*;

#[derive(Debug, Clone, Default)]
pub(super) struct CounterMetrics {
    pub(super) values: BTreeMap<String, String>,
}

impl CounterMetrics {
    pub(super) fn from_file(path: &Path) -> Self {
        let mut values = BTreeMap::new();
        if let Ok(content) = fs::read_to_string(path) {
            for line in content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    values.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }
        Self { values }
    }

    pub(super) fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }
}

#[derive(Debug, Clone)]
pub(super) struct RunSummary {
    pub(super) initial_rows: u64,
    pub(super) live_insert_rows: u64,
    pub(super) live_update_rows: u64,
    pub(super) source_rows: u64,
    pub(super) table_row_counts: Vec<u64>,
    pub(super) expected_kafka_messages: Option<u64>,
    pub(super) observed_kafka_messages: Option<String>,
    pub(super) expected_sink_rows: Option<u64>,
    pub(super) observed_sink_rows: Option<u64>,
    pub(super) expected_postgres_updated_rows: u64,
    pub(super) observed_postgres_updated_rows: Option<u64>,
    pub(super) postgres_load_seconds: f64,
    pub(super) live_write_seconds: f64,
    pub(super) end_to_end_seconds: f64,
    pub(super) sink_wait_seconds: Option<f64>,
    pub(super) counter_metrics: CounterMetrics,
    pub(super) artifact_paths: ArtifactPaths,
}

#[derive(Debug, Clone)]
pub(super) struct ArtifactPaths {
    pub(super) node_stdout: PathBuf,
    pub(super) node_stderr: PathBuf,
    pub(super) node_resource_log: PathBuf,
    pub(super) counter_log: PathBuf,
    pub(super) reproduce_log: PathBuf,
    pub(super) system_log: PathBuf,
    pub(super) postgres_settings_log: PathBuf,
    pub(super) postgres_slot_log: PathBuf,
    pub(super) kafka_topic_log: PathBuf,
    pub(super) docker_stats_log: PathBuf,
    pub(super) floe_metrics_log: PathBuf,
    pub(super) cdc_replication_debug_json: PathBuf,
}

impl ArtifactPaths {
    pub(super) fn new(config: &Config) -> Self {
        let artifact_dir = &config.artifact_dir;
        Self {
            node_stdout: artifact_dir.join("floe-node.stdout.log"),
            node_stderr: artifact_dir.join("floe-node.stderr.log"),
            node_resource_log: artifact_dir.join("floe-node.resources.log"),
            counter_log: artifact_dir.join("kafka-counter.log"),
            reproduce_log: artifact_dir.join("reproduce.sh"),
            system_log: artifact_dir.join("system.txt"),
            postgres_settings_log: artifact_dir.join("postgres-settings.txt"),
            postgres_slot_log: artifact_dir.join("postgres-slot.log"),
            kafka_topic_log: artifact_dir.join("kafka-topic.txt"),
            docker_stats_log: artifact_dir.join("docker-stats.log"),
            floe_metrics_log: artifact_dir.join("floe-metrics.prom"),
            cdc_replication_debug_json: artifact_dir.join("cdc-replication-debug.json"),
        }
    }
}

pub(super) fn write_reproduce_command(config: &Config, artifacts: &ArtifactPaths) -> Result<()> {
    let lines = [
        "#!/usr/bin/env bash".to_string(),
        "set -euo pipefail".to_string(),
        format!(
            "ARTIFACT_DIR={} \\",
            shell_quote(&config.artifact_dir.display().to_string())
        ),
        format!("ROWS={} \\", shell_quote(&config.rows.to_string())),
        format!("DATASET={} \\", shell_quote(config.dataset.as_str())),
        format!(
            "TPCH_SCALE_FACTOR={} \\",
            shell_quote(&config.tpch_scale_factor)
        ),
        format!("BENCH_MODE={} \\", shell_quote(config.bench_mode.as_str())),
        format!("TARGET={} \\", shell_quote(config.target.as_str())),
        format!("TOPIC={} \\", shell_quote(&config.topic)),
        format!(
            "PIPELINE_FORMAT={} \\",
            shell_quote(&config.pipeline_format)
        ),
        format!(
            "DURABLE_REPLICATION_BUFFER={} \\",
            shell_quote(&config.durable_replication_buffer.to_string())
        ),
        format!(
            "ARROW_IPC_ROWS_PER_RECORD={} \\",
            shell_quote(&config.arrow_ipc_rows_per_record.to_string())
        ),
        format!(
            "KAFKA_METADATA_HEADERS={} \\",
            shell_quote(&config.kafka_metadata_headers.to_string())
        ),
        "scripts/postgres_cdc_perf_local.sh".to_string(),
    ];
    write_file(&artifacts.reproduce_log, lines.join("\n") + "\n")?;
    let _ = std::process::Command::new("chmod")
        .args(["+x", artifacts.reproduce_log.to_str().unwrap_or_default()])
        .status();
    Ok(())
}

pub(super) fn write_summary_files(
    config: &Config,
    plan: &DatasetPlan,
    load: &LoadPlan,
    summary: &RunSummary,
) -> Result<()> {
    let row_counts = plan
        .upstream_tables
        .iter()
        .zip(&summary.table_row_counts)
        .map(|(table, rows)| json!({"table": table, "rows": rows}))
        .collect::<Vec<_>>();
    write_file(
        &config.summary_json,
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "run": {
                "id": config.run_id,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "git_commit": run_capture("git", ["rev-parse", "HEAD"], Some(&config.repo_root)).unwrap_or_default().trim(),
                "git_branch": run_capture("git", ["rev-parse", "--abbrev-ref", "HEAD"], Some(&config.repo_root)).unwrap_or_default().trim(),
                "build_profile": config.profile(),
                "artifact_dir": config.artifact_dir.display().to_string()
            },
            "scenario": {
                "dataset": config.dataset.as_str(),
                "tpch_scale_factor": config.tpch_scale_factor,
                "requested_rows": config.rows,
                "mode": config.bench_mode.as_str(),
                "source_table": plan.source_table,
                "upstream_table": plan.upstream_table,
                "upstream_tables": plan.upstream_tables,
                "target": {
                    "kind": config.target.as_str(),
                    "kafka_topics": plan.topics,
                    "postgres_tables": plan.target_tables
                },
                "pipeline_format": config.pipeline_format,
                "durable_replication_buffer": config.durable_replication_buffer,
                "encoding": {
                    "arrow_ipc_rows_per_record": config.arrow_ipc_rows_per_record,
                    "arrow_ipc_compression": config.arrow_ipc_compression,
                    "kafka_metadata_headers": config.kafka_metadata_headers
                },
                "postgres_snapshot": {
                    "rows_per_batch": config.snapshot_rows_per_batch,
                    "max_workers": config.snapshot_max_workers,
                    "intra_table_chunks": config.snapshot_intra_table_chunks
                }
            },
            "counts": {
                "table_rows": row_counts,
                "initial_rows": summary.initial_rows,
                "live_insert_rows": summary.live_insert_rows,
                "live_update_rows": summary.live_update_rows,
                "source_rows": summary.source_rows,
                "expected_kafka_messages": summary.expected_kafka_messages,
                "observed_kafka_messages": summary.observed_kafka_messages,
                "expected_sink_rows": summary.expected_sink_rows,
                "observed_sink_rows": summary.observed_sink_rows,
                "expected_postgres_updated_rows": summary.expected_postgres_updated_rows,
                "observed_postgres_updated_rows": summary.observed_postgres_updated_rows
            },
            "timings_seconds": {
                "postgres_load": summary.postgres_load_seconds,
                "postgres_live_write": summary.live_write_seconds,
                "end_to_end": summary.end_to_end_seconds,
                "kafka_counter_wall": summary.counter_metrics.get("cdc_counter.wall_seconds"),
                "kafka_stream": summary.counter_metrics.get("cdc_counter.stream_seconds"),
                "sink_wait": summary.sink_wait_seconds
            },
            "rates": {
                "end_to_end_source_rows_per_second": rate(load.source_rows(), summary.end_to_end_seconds),
                "postgres_load_rows_per_second": rate(summary.initial_rows, summary.postgres_load_seconds),
                "postgres_live_write_rows_per_second": rate(summary.live_insert_rows + summary.live_update_rows, summary.live_write_seconds),
                "kafka_stream_rows_per_second": summary.counter_metrics.get("cdc_counter.stream_rows_per_second"),
                "kafka_stream_mb_per_second": summary.counter_metrics.get("cdc_counter.stream_mb_per_second")
            },
            "artifacts": {
                "summary_env": config.summary_env.display().to_string(),
                "summary_json": config.summary_json.display().to_string(),
                "summary_md": config.summary_md.display().to_string(),
                "node_stdout": summary.artifact_paths.node_stdout.display().to_string(),
                "node_stderr": summary.artifact_paths.node_stderr.display().to_string(),
                "node_resource_log": summary.artifact_paths.node_resource_log.display().to_string(),
                "counter_log": summary.artifact_paths.counter_log.display().to_string(),
                "reproduce_log": summary.artifact_paths.reproduce_log.display().to_string(),
                "system_log": summary.artifact_paths.system_log.display().to_string(),
                "postgres_settings_log": summary.artifact_paths.postgres_settings_log.display().to_string(),
                "postgres_slot_log": summary.artifact_paths.postgres_slot_log.display().to_string(),
                "kafka_topic_log": summary.artifact_paths.kafka_topic_log.display().to_string(),
                "docker_stats_log": summary.artifact_paths.docker_stats_log.display().to_string(),
                "floe_metrics_log": summary.artifact_paths.floe_metrics_log.display().to_string(),
                "cdc_replication_debug_json": summary.artifact_paths.cdc_replication_debug_json.display().to_string()
            }
        }))?,
    )?;
    write_summary_markdown(config, summary)
}

fn write_summary_markdown(config: &Config, summary: &RunSummary) -> Result<()> {
    let target_observed_records_per_second = match config.target {
        TargetKind::Kafka => summary
            .counter_metrics
            .get("cdc_counter.stream_rows_per_second")
            .unwrap_or(""),
        TargetKind::Postgres => "",
    };
    write_file(
        &config.summary_md,
        format!(
            "# Postgres CDC Benchmark\n\nRun: `{}`\n\nDataset: `{}`\n\nMode: `{}`\n\nTarget: `{}`\n\nFormat: `{}`\n\nDurable replication buffer: `{}`\n\nArtifact directory: `{}`\n\n| Metric | Value |\n| --- | ---: |\n| Source rows | {} |\n| Expected Kafka messages | {} |\n| Observed Kafka messages | {} |\n| Expected Postgres sink rows | {} |\n| Observed Postgres sink rows | {} |\n| End-to-end seconds | {:.3} |\n| End-to-end source rows/s | {} |\n| Target observed records/s | {} |\n| Postgres load seconds | {:.3} |\n| Postgres live write seconds | {:.3} |\n\nMachine-readable report: `{}`\n",
            config.run_id,
            config.dataset.as_str(),
            config.bench_mode.as_str(),
            config.target.as_str(),
            config.pipeline_format,
            config.durable_replication_buffer,
            config.artifact_dir.display(),
            summary.source_rows,
            opt_display(summary.expected_kafka_messages),
            summary.observed_kafka_messages.as_deref().unwrap_or(""),
            opt_display(summary.expected_sink_rows),
            opt_display(summary.observed_sink_rows),
            summary.end_to_end_seconds,
            rate(summary.source_rows, summary.end_to_end_seconds),
            target_observed_records_per_second,
            summary.postgres_load_seconds,
            summary.live_write_seconds,
            config.summary_json.display()
        ),
    )
}

pub(super) fn write_summary_env(
    config: &Config,
    plan: &DatasetPlan,
    summary: &RunSummary,
) -> Result<()> {
    let mut content = String::new();
    for (key, value) in [
        ("benchmark.dataset", config.dataset.as_str().to_string()),
        ("benchmark.rows", config.rows.to_string()),
        ("benchmark.source_table", plan.source_table.clone()),
        ("benchmark.upstream_table", plan.upstream_table.clone()),
        ("benchmark.target", config.target.as_str().to_string()),
        ("benchmark.kafka_topics", plan.topic_list()),
        ("benchmark.postgres_sink_tables", plan.target_table_list()),
        ("benchmark.mode", config.bench_mode.as_str().to_string()),
        ("benchmark.source_rows", summary.source_rows.to_string()),
        ("benchmark.pipeline_format", config.pipeline_format.clone()),
        (
            "benchmark.end_to_end_seconds",
            format!("{:.3}", summary.end_to_end_seconds),
        ),
        (
            "benchmark.artifact_dir",
            config.artifact_dir.display().to_string(),
        ),
        (
            "benchmark.summary_json",
            config.summary_json.display().to_string(),
        ),
        (
            "benchmark.summary_md",
            config.summary_md.display().to_string(),
        ),
    ] {
        content.push_str(key);
        content.push('=');
        content.push_str(&value);
        content.push('\n');
    }
    write_file(&config.summary_env, content)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn rate(rows: u64, seconds: f64) -> u64 {
    if rows == 0 {
        0
    } else {
        (rows as f64 / seconds.max(0.001)).round() as u64
    }
}

fn opt_display<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}
