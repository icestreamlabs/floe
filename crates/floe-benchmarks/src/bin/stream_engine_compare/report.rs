use std::path::Path;

use anyhow::Result;
use serde_json::json;

use super::{Config, Engine};
use crate::harness_common::*;

pub(super) struct EngineResult<'a> {
    pub(super) engine: Engine,
    pub(super) artifact_dir: &'a Path,
    pub(super) total_ms: u128,
    pub(super) produce_ms: u128,
    pub(super) post_ms: u128,
    pub(super) rows_per_sec: u64,
    pub(super) completion_signal: &'a str,
}

pub(super) fn write_summary_header(config: &Config) -> Result<()> {
    write_file(
        config.results_file(),
        format!(
            "# Stream Engine Benchmark Summary\n\nQuery: `{}` ({})\nTotal input rows: `{}`\nExpected output rows: `{}`\n\n| Engine | Ingest Complete (s) | Produce (s) | Post-Produce Wait (s) | Input Rows/s |\n| --- | ---: | ---: | ---: | ---: |\n",
            config.bench_query.as_str(),
            config.bench_query.description(),
            config.input_rows_total,
            config.expected_rows
        ),
    )
}

pub(super) fn write_run_context(config: &Config) -> Result<()> {
    write_file(
        config.run_dir.join("run_context.json"),
        serde_json::to_vec_pretty(&json!({
            "run_id": config.run_id,
            "benchmark_query": config.bench_query.as_str(),
            "benchmark_query_description": config.bench_query.description(),
            "rows": config.input_rows_total,
            "primary_rows": config.rows,
            "join_auction_rows": config.join_auction_rows,
            "expected_rows": config.expected_rows,
            "polling": {
                "interval_ms": config.poll_interval.as_millis(),
                "timeout_ms": config.poll_timeout.as_millis()
            },
            "kafka": {
                "broker_addr": config.broker_addr,
                "broker_addr_from_container": config.broker_addr_from_container
            },
            "images": {
                "redpanda": config.redpanda_image,
                "materialize": config.materialize_image,
                "risingwave": config.risingwave_image,
                "feldera": config.feldera_image
            },
            "floe": {
                "git_commit": run_capture("git", ["rev-parse", "HEAD"], Some(&config.repo_root)).unwrap_or_default().trim(),
                "git_branch": run_capture("git", ["branch", "--show-current"], Some(&config.repo_root)).unwrap_or_default().trim(),
                "rustc_version": run_capture("rustc", ["-V"], Some(&config.repo_root)).unwrap_or_default().trim()
            }
        }))?,
    )
}

pub(super) fn write_result(config: &Config, result: EngineResult<'_>) -> Result<()> {
    write_file(
        result.artifact_dir.join("summary.json"),
        serde_json::to_vec_pretty(&json!({
            "engine": result.engine.as_str(),
            "benchmark_query": config.bench_query.as_str(),
            "benchmark_query_description": config.bench_query.description(),
            "rows": config.input_rows_total,
            "primary_rows": config.rows,
            "join_auction_rows": config.join_auction_rows,
            "expected_rows": config.expected_rows,
            "timing": {
                "total_ms": result.total_ms,
                "produce_ms": result.produce_ms,
                "post_produce_wait_ms": result.post_ms
            },
            "throughput": {"input_rows_per_sec": result.rows_per_sec},
            "measurement": {"completion_signal": result.completion_signal},
            "artifact_dir": result.artifact_dir.display().to_string()
        }))?,
    )?;
    append_file(
        config.results_file(),
        format!(
            "| {} | {} | {} | {} | {} |\n",
            result.engine.as_str(),
            seconds(result.total_ms),
            seconds(result.produce_ms),
            seconds(result.post_ms),
            result.rows_per_sec
        ),
    )
}

pub(super) fn capture_image_metadata(config: &Config, image_ref: &str, output_path: &Path) {
    let Ok(raw) = run_capture(
        "docker",
        ["image", "inspect", image_ref],
        Some(&config.repo_root),
    ) else {
        return;
    };
    let metadata = serde_json::from_str::<serde_json::Value>(&raw)
        .ok()
        .and_then(|value| value.as_array().and_then(|items| items.first().cloned()))
        .map(|image| {
            json!({
                "id": image["Id"],
                "repo_tags": image["RepoTags"],
                "repo_digests": image["RepoDigests"],
                "created": image["Created"],
                "architecture": image["Architecture"],
                "os": image["Os"]
            })
        })
        .unwrap_or_else(|| json!({}));
    let _ = write_file(
        output_path,
        serde_json::to_vec_pretty(&metadata).unwrap_or_default(),
    );
}

pub(super) fn capture_floe_metadata(config: &Config, output_path: &Path) -> Result<()> {
    write_file(
        output_path,
        serde_json::to_vec_pretty(&json!({
            "binary": config.release_binary("floe-node").display().to_string(),
            "git_commit": run_capture("git", ["rev-parse", "HEAD"], Some(&config.repo_root)).unwrap_or_default().trim(),
            "git_branch": run_capture("git", ["branch", "--show-current"], Some(&config.repo_root)).unwrap_or_default().trim(),
            "rustc_version": run_capture("rustc", ["-V"], Some(&config.repo_root)).unwrap_or_default().trim(),
            "pg_port": config.floe_pg_port,
            "kafka": {
                "poll_ms": config.floe_kafka_poll_ms,
                "max_messages_per_tick": config.floe_kafka_max_messages_per_tick
            },
            "runtime": {
                "ingest_queue_capacity": config.floe_ingest_queue_capacity,
                "ingest_batch_size": config.floe_ingest_batch_size,
                "ingest_batch_per_source": config.floe_ingest_batch_per_source,
                "ingest_batch_per_connector": config.floe_ingest_batch_per_connector,
                "mv_retain_last": config.floe_mv_retain_last
            },
            "storage": {
                "slatedb_l0_sst_bytes": config.floe_l0_sst_bytes,
                "slatedb_max_unflushed_bytes": config.floe_max_unflushed_bytes
            }
        }))?,
    )
}
