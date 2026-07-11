use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Value, json};

use super::{Config, DatasetPlan, LoadPlan, TargetKind};
use floe_benchmarks::harness_common::*;

mod metrics;

use metrics::{CdcMetrics, cdc_summary_keys};

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
    pub(super) counter_seconds: Option<f64>,
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
    let values = summary_values(config, plan, summary);
    let cdc_debug = read_json_or_null(&summary.artifact_paths.cdc_replication_debug_json);
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
                "buffer": {
                    "max_pending_bytes": config.buffer_max_pending_bytes,
                    "max_pending_records": config.buffer_max_pending_records,
                    "max_pending_objects": config.buffer_max_pending_objects,
                    "max_pending_age_ms": config.buffer_max_pending_age_ms
                },
                "encoding": {
                    "arrow_ipc_rows_per_record": config.arrow_ipc_rows_per_record,
                    "arrow_ipc_compression": config.arrow_ipc_compression,
                    "kafka_metadata_headers": config.kafka_metadata_headers
                },
                "postgres_snapshot": {
                    "rows_per_batch": config.snapshot_rows_per_batch,
                    "max_workers": config.snapshot_max_workers,
                    "intra_table_chunks": config.snapshot_intra_table_chunks
                },
                "floe_ports": {
                    "pgwire": config.floe_pg_port,
                    "admin": config.floe_admin_port
                },
                "redpanda": {
                    "kafka_batch_max_bytes": config.redpanda_kafka_batch_max_bytes,
                    "topic_max_message_bytes": config.redpanda_topic_max_message_bytes
                },
                "live_write": {
                    "chunk_rows": config.live_write_chunk_rows,
                    "sleep_ms": config.live_write_sleep_ms
                },
                "slatedb": {
                    "flush_interval_ms": config.slatedb_flush_interval_ms
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
                "observed_postgres_updated_rows": summary.observed_postgres_updated_rows,
                "message_multiplier": json_number(values.get("benchmark.message_multiplier"))
            },
            "timings_seconds": {
                "postgres_load": summary.postgres_load_seconds,
                "postgres_live_write": summary.live_write_seconds,
                "end_to_end": summary.end_to_end_seconds,
                "kafka_counter_wall": summary.counter_metrics.get("cdc_counter.wall_seconds"),
                "kafka_counter_process": summary.counter_seconds,
                "kafka_pre_stream_wait": json_number(values.get("benchmark.kafka_pre_stream_wait_seconds")),
                "kafka_stream": summary.counter_metrics.get("cdc_counter.stream_seconds"),
                "kafka_post_stream_wait": json_number(values.get("benchmark.kafka_post_stream_wait_seconds")),
                "sink_wait": summary.sink_wait_seconds,
                "target_observation": json_number(values.get("benchmark.target_observation_seconds")),
                "harness_overhead": json_number(values.get("benchmark.harness_overhead_seconds"))
            },
            "rates": {
                "end_to_end_source_rows_per_second": rate(load.source_rows(), summary.end_to_end_seconds),
                "kafka_stream_messages_per_second": json_number(values.get("benchmark.kafka_stream_rows_per_second")),
                "kafka_stream_source_rows_per_second": json_number(values.get("benchmark.kafka_stream_source_rows_per_second")),
                "consumer_wall_source_rows_per_second": json_number(values.get("benchmark.consumer_wall_source_rows_per_second")),
                "postgres_load_rows_per_second": rate(summary.initial_rows, summary.postgres_load_seconds),
                "postgres_live_write_rows_per_second": rate(summary.live_insert_rows + summary.live_update_rows, summary.live_write_seconds),
                "sink_rows_per_second": json_number(values.get("benchmark.postgres_sink_rows_per_second")),
                "target_observed_records_per_second": json_number(values.get("benchmark.target_observed_records_per_second")),
                "kafka_stream_rows_per_second": summary.counter_metrics.get("cdc_counter.stream_rows_per_second"),
                "kafka_stream_mb_per_second": summary.counter_metrics.get("cdc_counter.stream_mb_per_second"),
                "kafka_wall_mb_per_second": json_number(values.get("benchmark.kafka_wall_mb_per_second")),
                "harness_overhead_percent": json_number(values.get("benchmark.harness_overhead_percent"))
            },
            "bytes": {
                "kafka_key_bytes": json_number(values.get("benchmark.kafka_key_bytes")),
                "kafka_value_bytes": json_number(values.get("benchmark.kafka_value_bytes")),
                "kafka_total_bytes": json_number(values.get("benchmark.kafka_total_bytes"))
            },
            "cdc_replication": {
                "durable_buffer": {
                    "pending_records": json_number(values.get("benchmark.cdc_buffer_pending_records")),
                    "pending_bytes": json_number(values.get("benchmark.cdc_buffer_pending_bytes")),
                    "appended_records": json_number(values.get("benchmark.cdc_buffer_appended_records")),
                    "appended_bytes": json_number(values.get("benchmark.cdc_buffer_appended_bytes")),
                    "append_latency_count": json_number(values.get("benchmark.cdc_buffer_append_latency_count")),
                    "append_latency_sum_ms": json_number(values.get("benchmark.cdc_buffer_append_latency_sum_ms")),
                    "forced_flushes": json_number(values.get("benchmark.cdc_buffer_forced_flushes")),
                    "flush_latency_count": json_number(values.get("benchmark.cdc_buffer_flush_latency_count")),
                    "flush_latency_sum_ms": json_number(values.get("benchmark.cdc_buffer_flush_latency_sum_ms")),
                    "replayed_records": json_number(values.get("benchmark.cdc_buffer_replayed_records")),
                    "replay_latency_count": json_number(values.get("benchmark.cdc_buffer_replay_latency_count")),
                    "replay_latency_sum_ms": json_number(values.get("benchmark.cdc_buffer_replay_latency_sum_ms")),
                    "replay_delivery_latency_count": json_number(values.get("benchmark.cdc_buffer_replay_delivery_latency_count")),
                    "replay_delivery_latency_sum_ms": json_number(values.get("benchmark.cdc_buffer_replay_delivery_latency_sum_ms")),
                    "replay_payload_load_latency_count": json_number(values.get("benchmark.cdc_buffer_replay_payload_load_latency_count")),
                    "replay_payload_load_latency_sum_ms": json_number(values.get("benchmark.cdc_buffer_replay_payload_load_latency_sum_ms")),
                    "replay_encode_latency_count": json_number(values.get("benchmark.cdc_buffer_replay_encode_latency_count")),
                    "replay_encode_latency_sum_ms": json_number(values.get("benchmark.cdc_buffer_replay_encode_latency_sum_ms")),
                    "object_create_count": json_number(values.get("benchmark.cdc_buffer_object_create_count")),
                    "object_get_count": json_number(values.get("benchmark.cdc_buffer_object_get_count")),
                    "object_delete_count": json_number(values.get("benchmark.cdc_buffer_object_delete_count")),
                    "drain_attempts": json_number(values.get("benchmark.cdc_buffer_drain_attempts"))
                },
                "target_write": {
                    "success_records": json_number(values.get("benchmark.cdc_target_write_success_records")),
                    "failure_records": json_number(values.get("benchmark.cdc_target_write_failure_records")),
                    "latency_count": json_number(values.get("benchmark.cdc_target_write_latency_count")),
                    "latency_sum_ms": json_number(values.get("benchmark.cdc_target_write_latency_sum_ms")),
                    "latency_avg_ms": json_number(values.get("benchmark.cdc_target_write_latency_avg_ms")),
                    "batch_records_sum": json_number(values.get("benchmark.cdc_target_write_batch_records_sum"))
                },
                "debug": cdc_debug
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
    write_summary_markdown(config, plan, summary)
}

fn write_summary_markdown(config: &Config, plan: &DatasetPlan, summary: &RunSummary) -> Result<()> {
    let values = summary_values(config, plan, summary);
    write_file(
        &config.summary_md,
        format!(
            "# Postgres CDC Benchmark\n\nRun: `{}`\n\nDataset: `{}`\n\nMode: `{}`\n\nTarget: `{}`\n\nFormat: `{}`\n\nDurable replication buffer: `{}`\n\nArtifact directory: `{}`\n\n| Metric | Value |\n| --- | ---: |\n| Source rows | {} |\n| Expected Kafka messages | {} |\n| Observed Kafka messages | {} |\n| Expected Postgres sink rows | {} |\n| Observed Postgres sink rows | {} |\n| Observed Postgres updated rows | {} |\n| End-to-end seconds | {} |\n| End-to-end source rows/s | {} |\n| Target observation seconds | {} |\n| Target observed records/s | {} |\n| Kafka stream seconds | {} |\n| Kafka stream messages/s | {} |\n| Kafka stream source rows/s | {} |\n| Consumer wall source rows/s | {} |\n| Harness overhead seconds | {} |\n| Harness overhead percent | {} |\n| Postgres load seconds | {} |\n| Postgres load rows/s | {} |\n| Postgres live write seconds | {} |\n| Postgres live write rows/s | {} |\n| Postgres sink wait seconds | {} |\n| Postgres sink rows/s | {} |\n| Kafka total bytes | {} |\n| Kafka stream MB/s | {} |\n| CDC buffer appended records | {} |\n| CDC buffer appended bytes | {} |\n| CDC buffer append latency sum ms | {} |\n| CDC buffer forced flushes | {} |\n| CDC buffer flush latency sum ms | {} |\n| CDC buffer replayed records | {} |\n| CDC buffer replay latency sum ms | {} |\n| CDC target write success records | {} |\n| CDC target write failure records | {} |\n| CDC target write latency count | {} |\n| CDC target write latency sum ms | {} |\n| CDC target write latency avg ms | {} |\n| CDC target write batch records sum | {} |\n\nMachine-readable report: `{}`\n",
            config.run_id,
            config.dataset.as_str(),
            config.bench_mode.as_str(),
            config.target.as_str(),
            config.pipeline_format,
            config.durable_replication_buffer,
            config.artifact_dir.display(),
            env_value(&values, "benchmark.source_rows"),
            env_value(&values, "benchmark.expected_kafka_messages"),
            env_value(&values, "benchmark.observed_kafka_messages"),
            env_value(&values, "benchmark.expected_postgres_sink_rows"),
            env_value(&values, "benchmark.observed_postgres_sink_rows"),
            env_value(&values, "benchmark.observed_postgres_sink_updated_rows"),
            env_value(&values, "benchmark.end_to_end_seconds"),
            env_value(&values, "benchmark.end_to_end_rows_per_second"),
            env_value(&values, "benchmark.target_observation_seconds"),
            env_value(&values, "benchmark.target_observed_records_per_second"),
            env_value(&values, "benchmark.kafka_stream_seconds"),
            env_value(&values, "benchmark.kafka_stream_rows_per_second"),
            env_value(&values, "benchmark.kafka_stream_source_rows_per_second"),
            env_value(&values, "benchmark.consumer_wall_source_rows_per_second"),
            env_value(&values, "benchmark.harness_overhead_seconds"),
            env_value(&values, "benchmark.harness_overhead_percent"),
            env_value(&values, "benchmark.postgres_load_seconds"),
            env_value(&values, "benchmark.postgres_load_rows_per_second"),
            env_value(&values, "benchmark.postgres_live_write_seconds"),
            env_value(&values, "benchmark.postgres_live_write_rows_per_second"),
            env_value(&values, "benchmark.postgres_sink_wait_seconds"),
            env_value(&values, "benchmark.postgres_sink_rows_per_second"),
            env_value(&values, "benchmark.kafka_total_bytes"),
            env_value(&values, "benchmark.kafka_stream_mb_per_second"),
            env_value(&values, "benchmark.cdc_buffer_appended_records"),
            env_value(&values, "benchmark.cdc_buffer_appended_bytes"),
            env_value(&values, "benchmark.cdc_buffer_append_latency_sum_ms"),
            env_value(&values, "benchmark.cdc_buffer_forced_flushes"),
            env_value(&values, "benchmark.cdc_buffer_flush_latency_sum_ms"),
            env_value(&values, "benchmark.cdc_buffer_replayed_records"),
            env_value(&values, "benchmark.cdc_buffer_replay_latency_sum_ms"),
            env_value(&values, "benchmark.cdc_target_write_success_records"),
            env_value(&values, "benchmark.cdc_target_write_failure_records"),
            env_value(&values, "benchmark.cdc_target_write_latency_count"),
            env_value(&values, "benchmark.cdc_target_write_latency_sum_ms"),
            env_value(&values, "benchmark.cdc_target_write_latency_avg_ms"),
            env_value(&values, "benchmark.cdc_target_write_batch_records_sum"),
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
    for (key, value) in summary_values(config, plan, summary) {
        content.push_str(&key);
        content.push('=');
        content.push_str(&value);
        content.push('\n');
    }
    write_file(&config.summary_env, content)
}

fn summary_values(
    config: &Config,
    plan: &DatasetPlan,
    summary: &RunSummary,
) -> BTreeMap<String, String> {
    let cdc = CdcMetrics::from_file(&summary.artifact_paths.floe_metrics_log);
    let mut values = BTreeMap::new();
    insert(&mut values, "benchmark.dataset", config.dataset.as_str());
    insert(
        &mut values,
        "benchmark.tpch_scale_factor",
        &config.tpch_scale_factor,
    );
    insert(&mut values, "benchmark.rows", config.rows);
    insert(&mut values, "benchmark.source_table", &plan.source_table);
    insert(
        &mut values,
        "benchmark.upstream_table",
        &plan.upstream_table,
    );
    insert(&mut values, "benchmark.target", config.target.as_str());
    insert(&mut values, "benchmark.kafka_topics", plan.topic_list());
    insert(
        &mut values,
        "benchmark.postgres_sink_tables",
        plan.target_table_list(),
    );
    insert(&mut values, "benchmark.mode", config.bench_mode.as_str());
    insert(&mut values, "benchmark.initial_rows", summary.initial_rows);
    insert(
        &mut values,
        "benchmark.live_insert_rows",
        summary.live_insert_rows,
    );
    insert(
        &mut values,
        "benchmark.live_update_rows",
        summary.live_update_rows,
    );
    insert(&mut values, "benchmark.source_rows", summary.source_rows);
    insert(
        &mut values,
        "benchmark.pipeline_format",
        &config.pipeline_format,
    );
    insert(
        &mut values,
        "benchmark.durable_replication_buffer",
        config.durable_replication_buffer,
    );
    insert_optional(
        &mut values,
        "benchmark.buffer_max_pending_bytes",
        config.buffer_max_pending_bytes,
    );
    insert_optional(
        &mut values,
        "benchmark.buffer_max_pending_records",
        config.buffer_max_pending_records,
    );
    insert_optional(
        &mut values,
        "benchmark.buffer_max_pending_objects",
        config.buffer_max_pending_objects,
    );
    insert_optional(
        &mut values,
        "benchmark.buffer_max_pending_age_ms",
        config.buffer_max_pending_age_ms,
    );
    insert(
        &mut values,
        "benchmark.arrow_ipc_rows_per_record",
        config.arrow_ipc_rows_per_record,
    );
    insert(
        &mut values,
        "benchmark.arrow_ipc_compression",
        config.arrow_ipc_compression.as_deref().unwrap_or("none"),
    );
    insert(
        &mut values,
        "benchmark.kafka_metadata_headers",
        config.kafka_metadata_headers,
    );
    insert(
        &mut values,
        "benchmark.postgres_snapshot_rows_per_batch",
        config.snapshot_rows_per_batch,
    );
    insert(
        &mut values,
        "benchmark.postgres_snapshot_max_workers",
        config.snapshot_max_workers,
    );
    insert(
        &mut values,
        "benchmark.postgres_snapshot_intra_table_chunks",
        config.snapshot_intra_table_chunks,
    );
    insert(&mut values, "benchmark.floe_pg_port", config.floe_pg_port);
    insert(
        &mut values,
        "benchmark.floe_admin_port",
        config.floe_admin_port,
    );
    insert(
        &mut values,
        "benchmark.redpanda_kafka_batch_max_bytes",
        config.redpanda_kafka_batch_max_bytes,
    );
    insert(
        &mut values,
        "benchmark.redpanda_topic_max_message_bytes",
        config.redpanda_topic_max_message_bytes,
    );
    insert(
        &mut values,
        "benchmark.live_write_chunk_rows",
        config.live_write_chunk_rows,
    );
    insert(
        &mut values,
        "benchmark.live_write_sleep_ms",
        config.live_write_sleep_ms,
    );
    insert(
        &mut values,
        "benchmark.slatedb_flush_interval_ms",
        config.slatedb_flush_interval_ms,
    );
    insert_optional(
        &mut values,
        "benchmark.expected_kafka_messages",
        summary.expected_kafka_messages,
    );
    insert_optional_str(
        &mut values,
        "benchmark.observed_kafka_messages",
        summary.observed_kafka_messages.as_deref(),
    );
    insert_optional(
        &mut values,
        "benchmark.expected_postgres_sink_rows",
        summary.expected_sink_rows,
    );
    insert_optional(
        &mut values,
        "benchmark.observed_postgres_sink_rows",
        summary.observed_sink_rows,
    );
    insert(
        &mut values,
        "benchmark.expected_postgres_sink_updated_rows",
        summary.expected_postgres_updated_rows,
    );
    insert_optional(
        &mut values,
        "benchmark.observed_postgres_sink_updated_rows",
        summary.observed_postgres_updated_rows,
    );
    insert(
        &mut values,
        "benchmark.postgres_load_seconds",
        seconds(summary.postgres_load_seconds),
    );
    insert(
        &mut values,
        "benchmark.postgres_live_write_seconds",
        seconds(summary.live_write_seconds),
    );
    insert(
        &mut values,
        "benchmark.end_to_end_seconds",
        seconds(summary.end_to_end_seconds),
    );
    insert_optional(
        &mut values,
        "benchmark.counter_seconds",
        summary.counter_seconds.map(seconds),
    );
    insert(
        &mut values,
        "benchmark.end_to_end_rows_per_second",
        rate(summary.source_rows, summary.end_to_end_seconds),
    );

    for (source, target) in [
        (
            "cdc_counter.wall_seconds",
            "benchmark.kafka_counter_wall_seconds",
        ),
        (
            "cdc_counter.pre_stream_wait_seconds",
            "benchmark.kafka_pre_stream_wait_seconds",
        ),
        (
            "cdc_counter.stream_seconds",
            "benchmark.kafka_stream_seconds",
        ),
        (
            "cdc_counter.post_stream_wait_seconds",
            "benchmark.kafka_post_stream_wait_seconds",
        ),
        (
            "cdc_counter.stream_rows_per_second",
            "benchmark.kafka_stream_rows_per_second",
        ),
        (
            "cdc_counter.stream_mb_per_second",
            "benchmark.kafka_stream_mb_per_second",
        ),
        ("cdc_counter.key_bytes", "benchmark.kafka_key_bytes"),
        ("cdc_counter.value_bytes", "benchmark.kafka_value_bytes"),
        ("cdc_counter.total_bytes", "benchmark.kafka_total_bytes"),
        (
            "cdc_counter.wall_mb_per_second",
            "benchmark.kafka_wall_mb_per_second",
        ),
    ] {
        insert_optional_str(&mut values, target, summary.counter_metrics.get(source));
    }

    if let Some(stream_seconds) = summary
        .counter_metrics
        .get("cdc_counter.stream_seconds")
        .and_then(parse_f64)
    {
        insert(
            &mut values,
            "benchmark.kafka_stream_source_rows_per_second",
            rate(summary.source_rows, stream_seconds),
        );
        let overhead = (summary.end_to_end_seconds - stream_seconds).max(0.0);
        insert(
            &mut values,
            "benchmark.harness_overhead_seconds",
            seconds(overhead),
        );
        insert(
            &mut values,
            "benchmark.harness_overhead_percent",
            format!(
                "{:.1}",
                overhead / summary.end_to_end_seconds.max(0.001) * 100.0
            ),
        );
    } else {
        insert_optional_str(
            &mut values,
            "benchmark.kafka_stream_source_rows_per_second",
            None,
        );
        insert_optional_str(&mut values, "benchmark.harness_overhead_seconds", None);
        insert_optional_str(&mut values, "benchmark.harness_overhead_percent", None);
    }

    if let Some(wall_seconds) = summary
        .counter_metrics
        .get("cdc_counter.wall_seconds")
        .and_then(parse_f64)
    {
        insert(
            &mut values,
            "benchmark.consumer_wall_source_rows_per_second",
            rate(summary.source_rows, wall_seconds),
        );
    } else {
        insert_optional_str(
            &mut values,
            "benchmark.consumer_wall_source_rows_per_second",
            None,
        );
    }

    if let Some(expected) = summary.expected_kafka_messages {
        insert(
            &mut values,
            "benchmark.message_multiplier",
            format!(
                "{:.3}",
                expected as f64 / (summary.source_rows as f64).max(1.0)
            ),
        );
    } else {
        insert_optional_str(&mut values, "benchmark.message_multiplier", None);
    }

    insert(
        &mut values,
        "benchmark.postgres_load_rows_per_second",
        rate(summary.initial_rows, summary.postgres_load_seconds),
    );
    let live_rows = summary.live_insert_rows + summary.live_update_rows;
    if live_rows > 0 {
        insert(
            &mut values,
            "benchmark.postgres_live_write_rows_per_second",
            rate(live_rows, summary.live_write_seconds),
        );
    } else {
        insert_optional_str(
            &mut values,
            "benchmark.postgres_live_write_rows_per_second",
            None,
        );
    }
    if let Some(sink_wait_seconds) = summary.sink_wait_seconds {
        insert(
            &mut values,
            "benchmark.postgres_sink_wait_seconds",
            seconds(sink_wait_seconds),
        );
        insert(
            &mut values,
            "benchmark.postgres_sink_rows_per_second",
            rate(summary.source_rows, sink_wait_seconds),
        );
    } else {
        insert_optional_str(&mut values, "benchmark.postgres_sink_wait_seconds", None);
        insert_optional_str(&mut values, "benchmark.postgres_sink_rows_per_second", None);
    }

    match config.target {
        TargetKind::Kafka => {
            let target_observation = values
                .get("benchmark.kafka_counter_wall_seconds")
                .cloned()
                .unwrap_or_default();
            let target_rate = values
                .get("benchmark.consumer_wall_source_rows_per_second")
                .cloned()
                .unwrap_or_default();
            insert(
                &mut values,
                "benchmark.target_observation_seconds",
                target_observation,
            );
            insert(
                &mut values,
                "benchmark.target_observed_records_per_second",
                target_rate,
            );
        }
        TargetKind::Postgres => {
            let target_observation = values
                .get("benchmark.postgres_sink_wait_seconds")
                .cloned()
                .unwrap_or_default();
            let target_rate = values
                .get("benchmark.postgres_sink_rows_per_second")
                .cloned()
                .unwrap_or_default();
            insert(
                &mut values,
                "benchmark.target_observation_seconds",
                target_observation,
            );
            insert(
                &mut values,
                "benchmark.target_observed_records_per_second",
                target_rate,
            );
        }
    }

    for key in cdc_summary_keys() {
        insert_optional_str(
            &mut values,
            format!("benchmark.{key}"),
            cdc.get(key).filter(|value| !value.is_empty()),
        );
    }
    insert_optional_str(
        &mut values,
        "benchmark.cdc_target_write_latency_avg_ms",
        cdc.get("cdc_target_write_latency_avg_ms"),
    );

    insert(
        &mut values,
        "benchmark.artifact_dir",
        config.artifact_dir.display(),
    );
    insert(
        &mut values,
        "benchmark.node_stdout",
        summary.artifact_paths.node_stdout.display(),
    );
    insert(
        &mut values,
        "benchmark.node_stderr",
        summary.artifact_paths.node_stderr.display(),
    );
    insert(
        &mut values,
        "benchmark.node_resource_log",
        summary.artifact_paths.node_resource_log.display(),
    );
    insert(
        &mut values,
        "benchmark.counter_log",
        summary.artifact_paths.counter_log.display(),
    );
    insert(
        &mut values,
        "benchmark.reproduce_log",
        summary.artifact_paths.reproduce_log.display(),
    );
    insert(
        &mut values,
        "benchmark.system_log",
        summary.artifact_paths.system_log.display(),
    );
    insert(
        &mut values,
        "benchmark.postgres_settings_log",
        summary.artifact_paths.postgres_settings_log.display(),
    );
    insert(
        &mut values,
        "benchmark.postgres_slot_log",
        summary.artifact_paths.postgres_slot_log.display(),
    );
    insert(
        &mut values,
        "benchmark.kafka_topic_log",
        summary.artifact_paths.kafka_topic_log.display(),
    );
    insert(
        &mut values,
        "benchmark.docker_stats_log",
        summary.artifact_paths.docker_stats_log.display(),
    );
    insert(
        &mut values,
        "benchmark.floe_metrics_log",
        summary.artifact_paths.floe_metrics_log.display(),
    );
    insert(
        &mut values,
        "benchmark.cdc_replication_debug_json",
        summary.artifact_paths.cdc_replication_debug_json.display(),
    );
    insert(
        &mut values,
        "benchmark.summary_json",
        config.summary_json.display(),
    );
    insert(
        &mut values,
        "benchmark.summary_md",
        config.summary_md.display(),
    );
    values
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn insert<T: std::fmt::Display>(
    values: &mut BTreeMap<String, String>,
    key: impl Into<String>,
    value: T,
) {
    values.insert(key.into(), value.to_string());
}

fn insert_optional<T: std::fmt::Display>(
    values: &mut BTreeMap<String, String>,
    key: impl Into<String>,
    value: Option<T>,
) {
    values.insert(
        key.into(),
        value.map(|value| value.to_string()).unwrap_or_default(),
    );
}

fn insert_optional_str(
    values: &mut BTreeMap<String, String>,
    key: impl Into<String>,
    value: Option<&str>,
) {
    values.insert(key.into(), value.unwrap_or_default().to_string());
}

fn env_value<'a>(values: &'a BTreeMap<String, String>, key: &str) -> &'a str {
    values.get(key).map(String::as_str).unwrap_or("")
}

fn json_number(value: Option<&String>) -> Value {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Value::Null;
    };
    if let Ok(number) = value.parse::<i64>() {
        return json!(number);
    }
    if let Ok(number) = value.parse::<u64>() {
        return json!(number);
    }
    if let Ok(number) = value.parse::<f64>()
        && number.is_finite()
    {
        return json!(number);
    }
    json!(value)
}

fn read_json_or_null(path: &Path) -> Value {
    fs::read_to_string(path)
        .ok()
        .and_then(|content| serde_json::from_str(&content).ok())
        .unwrap_or(Value::Null)
}

fn parse_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

fn seconds(value: f64) -> String {
    format!("{value:.3}")
}

fn rate(rows: u64, seconds: f64) -> u64 {
    if rows == 0 {
        0
    } else {
        (rows as f64 / seconds.max(0.001)).round() as u64
    }
}
