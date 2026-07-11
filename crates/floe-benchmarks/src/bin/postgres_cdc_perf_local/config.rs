use std::path::PathBuf;

use anyhow::{Result, bail};
use serde_json::json;

use floe_benchmarks::harness_common::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum TargetKind {
    Kafka,
    Postgres,
}

impl TargetKind {
    fn parse(raw: &str) -> Result<Self> {
        match normalize_token(raw).as_str() {
            "kafka" => Ok(Self::Kafka),
            "postgres" => Ok(Self::Postgres),
            other => bail!("unsupported TARGET={other} (expected kafka|postgres)"),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Kafka => "kafka",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum BenchMode {
    Snapshot,
    LiveInsert,
    SnapshotLiveUpdate,
}

impl BenchMode {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "snapshot" => Ok(Self::Snapshot),
            "live_insert" => Ok(Self::LiveInsert),
            "snapshot_live_update" => Ok(Self::SnapshotLiveUpdate),
            other => {
                bail!(
                    "unsupported BENCH_MODE={other} (expected snapshot|live_insert|snapshot_live_update)"
                )
            }
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::LiveInsert => "live_insert",
            Self::SnapshotLiveUpdate => "snapshot_live_update",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum Dataset {
    SyntheticOrders,
    TpchLineitemFlat,
    TpchLineitem,
    TpchTop2,
    TpchAll,
}

impl Dataset {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "synthetic-orders" => Ok(Self::SyntheticOrders),
            "tpch-lineitem-flat" => Ok(Self::TpchLineitemFlat),
            "tpch-lineitem" => Ok(Self::TpchLineitem),
            "tpch-top2" => Ok(Self::TpchTop2),
            "tpch-all" => Ok(Self::TpchAll),
            other => bail!("unsupported DATASET={other}"),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::SyntheticOrders => "synthetic-orders",
            Self::TpchLineitemFlat => "tpch-lineitem-flat",
            Self::TpchLineitem => "tpch-lineitem",
            Self::TpchTop2 => "tpch-top2",
            Self::TpchAll => "tpch-all",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Config {
    pub(super) repo_root: PathBuf,
    pub(super) postgres_container: String,
    pub(super) postgres_image: String,
    pub(super) postgres_port: u16,
    pub(super) postgres_user: String,
    pub(super) postgres_password: String,
    pub(super) postgres_db: String,
    pub(super) redpanda_container: String,
    pub(super) redpanda_image: String,
    pub(super) redpanda_port: u16,
    pub(super) redpanda_kafka_batch_max_bytes: u64,
    pub(super) redpanda_topic_max_message_bytes: u64,
    pub(super) brokers: String,
    pub(super) rows: u64,
    pub(super) dataset: Dataset,
    pub(super) bench_mode: BenchMode,
    pub(super) target: TargetKind,
    pub(super) topic: String,
    pub(super) slot: String,
    pub(super) publication: String,
    pub(super) pipeline_format: String,
    pub(super) durable_replication_buffer: bool,
    pub(super) buffer_max_pending_bytes: Option<u64>,
    pub(super) buffer_max_pending_records: Option<u64>,
    pub(super) buffer_max_pending_objects: Option<u64>,
    pub(super) buffer_max_pending_age_ms: Option<u64>,
    pub(super) arrow_ipc_rows_per_record: u64,
    pub(super) arrow_ipc_compression: Option<String>,
    pub(super) kafka_metadata_headers: bool,
    pub(super) live_write_chunk_rows: u64,
    pub(super) live_write_sleep_ms: u64,
    pub(super) snapshot_rows_per_batch: u64,
    pub(super) snapshot_max_workers: u64,
    pub(super) snapshot_intra_table_chunks: u64,
    pub(super) floe_pg_port: u16,
    pub(super) floe_admin_port: u16,
    pub(super) timeout_secs: u64,
    pub(super) build_release: bool,
    pub(super) keep_containers: bool,
    pub(super) tpch_scale_factor: String,
    pub(super) tpchgen_bin: String,
    pub(super) slatedb_flush_interval_ms: u64,
    pub(super) run_id: String,
    pub(super) artifact_dir: PathBuf,
    pub(super) tpch_data_dir: PathBuf,
    pub(super) config_path: PathBuf,
    pub(super) sql_path: PathBuf,
    pub(super) summary_env: PathBuf,
    pub(super) summary_json: PathBuf,
    pub(super) summary_md: PathBuf,
}

impl Config {
    pub(super) fn from_env() -> Result<Self> {
        let repo_root = repo_root()?;
        let run_id = chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string();
        let artifact_dir = env_path("ARTIFACT_DIR")
            .unwrap_or_else(|| repo_root.join("target/cdc_bench").join(&run_id));
        let tpch_data_dir = env_path("TPCH_DATA_DIR").unwrap_or_else(|| artifact_dir.join("tpch"));
        let redpanda_port = env_parse("REDPANDA_PORT", 19092)?;
        let brokers = env_string("BROKERS", &format!("127.0.0.1:{redpanda_port}"));
        let target = TargetKind::parse(&env_string("TARGET", "kafka"))?;
        let pipeline_format = env_string("PIPELINE_FORMAT", "floe-json");
        validate_target_format(target, &pipeline_format)?;
        Ok(Self {
            repo_root,
            postgres_container: env_string("POSTGRES_CONTAINER", "floe-cdc-bench-postgres"),
            postgres_image: env_string("POSTGRES_IMAGE", "postgres:16"),
            postgres_port: env_parse("POSTGRES_PORT", 55434)?,
            postgres_user: env_string("POSTGRES_USER", "postgres"),
            postgres_password: env_string("POSTGRES_PASSWORD", "postgres"),
            postgres_db: env_string("POSTGRES_DB", "postgres"),
            redpanda_container: env_string("REDPANDA_CONTAINER", "floe-cdc-bench-redpanda"),
            redpanda_image: env_string(
                "REDPANDA_IMAGE",
                "docker.redpanda.com/redpandadata/redpanda:latest",
            ),
            redpanda_port,
            redpanda_kafka_batch_max_bytes: env_parse(
                "REDPANDA_KAFKA_BATCH_MAX_BYTES",
                10_485_760,
            )?,
            redpanda_topic_max_message_bytes: env_parse(
                "REDPANDA_TOPIC_MAX_MESSAGE_BYTES",
                10_485_760,
            )?,
            brokers,
            rows: env_parse("ROWS", 100_000)?,
            dataset: Dataset::parse(&env_string("DATASET", "synthetic-orders"))?,
            bench_mode: BenchMode::parse(&env_string("BENCH_MODE", "snapshot"))?,
            target,
            topic: env_string("TOPIC", "floe_cdc_bench_orders"),
            slot: env_string("SLOT", "floe_cdc_bench_slot"),
            publication: env_string("PUBLICATION", "floe_cdc_bench_pub"),
            pipeline_format,
            durable_replication_buffer: env_bool("DURABLE_REPLICATION_BUFFER", true),
            buffer_max_pending_bytes: env_optional_parse("BUFFER_MAX_PENDING_BYTES")?,
            buffer_max_pending_records: env_optional_parse("BUFFER_MAX_PENDING_RECORDS")?,
            buffer_max_pending_objects: env_optional_parse("BUFFER_MAX_PENDING_OBJECTS")?,
            buffer_max_pending_age_ms: env_optional_parse("BUFFER_MAX_PENDING_AGE_MS")?,
            arrow_ipc_rows_per_record: env_parse("ARROW_IPC_ROWS_PER_RECORD", 16_384)?,
            arrow_ipc_compression: arrow_ipc_compression(&env_string(
                "ARROW_IPC_COMPRESSION",
                "none",
            ))?,
            kafka_metadata_headers: env_bool("KAFKA_METADATA_HEADERS", false),
            live_write_chunk_rows: env_parse("LIVE_WRITE_CHUNK_ROWS", 0)?,
            live_write_sleep_ms: env_parse("LIVE_WRITE_SLEEP_MS", 0)?,
            snapshot_rows_per_batch: env_parse(
                "FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH",
                16_384,
            )?,
            snapshot_max_workers: env_parse("FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS", 1)?,
            snapshot_intra_table_chunks: env_parse(
                "FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS",
                1,
            )?,
            floe_pg_port: env_parse("FLOE_PG_PORT", 16432)?,
            floe_admin_port: env_parse("FLOE_ADMIN_PORT", 18080)?,
            timeout_secs: env_parse("TIMEOUT_SECS", 900)?,
            build_release: env_bool("BUILD_RELEASE", true),
            keep_containers: env_bool("KEEP_CONTAINERS", false),
            tpch_scale_factor: env_string("TPCH_SCALE_FACTOR", "0.01"),
            tpchgen_bin: env_string("TPCHGEN_BIN", "tpchgen-cli"),
            slatedb_flush_interval_ms: env_parse("FLOE_SLATEDB_FLUSH_INTERVAL_MS", 500)?,
            run_id,
            artifact_dir: artifact_dir.clone(),
            tpch_data_dir,
            config_path: artifact_dir.join("empty_config.json"),
            sql_path: artifact_dir.join("program.sql"),
            summary_env: artifact_dir.join("summary.env"),
            summary_json: artifact_dir.join("summary.json"),
            summary_md: artifact_dir.join("summary.md"),
        })
    }

    pub(super) fn pg_dsn(&self) -> String {
        format!(
            "postgres://{}:{}@127.0.0.1:{}/{}",
            self.postgres_user, self.postgres_password, self.postgres_port, self.postgres_db
        )
    }

    pub(super) fn floe_config_json(&self) -> serde_json::Value {
        json!({
            "runtime": {
                "pgwire_addr": format!("127.0.0.1:{}", self.floe_pg_port),
                "admin_port": self.floe_admin_port
            },
            "storage": {"data_dir": self.artifact_dir.join("floe-data").display().to_string()},
            "replication": {
                "encoding": {
                    "arrow_ipc_rows_per_record": self.arrow_ipc_rows_per_record,
                    "arrow_ipc_compression": self.arrow_ipc_compression,
                    "kafka_metadata_headers": self.kafka_metadata_headers
                }
            },
            "postgres_cdc": {
                "snapshot": {
                    "rows_per_batch": self.snapshot_rows_per_batch,
                    "max_workers": self.snapshot_max_workers,
                    "intra_table_chunks": self.snapshot_intra_table_chunks
                }
            }
        })
    }

    pub(super) fn profile(&self) -> &'static str {
        if self.build_release {
            "release"
        } else {
            "debug"
        }
    }

    pub(super) fn target_binary(&self, name: &str) -> PathBuf {
        self.repo_root
            .join("target")
            .join(self.profile())
            .join(name)
    }
}

fn env_optional_parse<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env_nonempty(name)
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|err| anyhow::anyhow!("parse {name}={value}: {err}"))
        })
        .transpose()
}

fn normalize_token(value: &str) -> String {
    value.to_ascii_lowercase().replace('-', "_")
}

fn validate_target_format(target: TargetKind, format: &str) -> Result<()> {
    if target == TargetKind::Postgres {
        match normalize_token(format).as_str() {
            "floe_json" | "compact_json" => {}
            _ => bail!("TARGET=postgres currently requires PIPELINE_FORMAT=floe-json"),
        }
    }
    Ok(())
}

fn arrow_ipc_compression(raw: &str) -> Result<Option<String>> {
    match raw {
        "" | "none" | "off" | "false" | "0" => Ok(None),
        "lz4" | "lz4_frame" | "lz4-frame" => Ok(Some("lz4_frame".to_string())),
        other => bail!("unsupported ARROW_IPC_COMPRESSION={other}; expected none or lz4_frame"),
    }
}
