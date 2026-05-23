use clap::{Args, Parser, Subcommand, ValueEnum};

use floe_node_core::tail_client::{TailConfig, build_subscribe_sql, build_tail_sql};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Floe node entrypoint",
    long_about = "Run and tail a single-node streaming SQL runtime.",
    after_long_help = "Examples:\n  floe-node run --mv-query \"CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid\"\n  floe-node run --config ./floe.toml\n  floe-node tail --mv mv\n  floe-node subscribe --mv mv --with-snapshot\n  floe-node tail --sql \"TAIL mv WITH SNAPSHOT\""
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    Run(RunArgs),
    Tail(TailArgs),
    Subscribe(SubscribeArgs),
}

#[derive(Debug, Args)]
#[command(
    after_long_help = "Examples:\n  # Generator + MV\n  floe-node run --mv-query \"CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid\"\n\n  # File ingest + MV\n  floe-node run --input-file ./events.jsonl --input-source nexmark_bid --mv-query \"CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid\"\n\n  # Kafka ingest + MV\n  floe-node run --kafka-brokers localhost:9092 --kafka-topics nexmark_bid --mv-query \"CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid\"\n\n  # Config-first startup\n  floe-node run --config ./floe.toml\n\n  # Validate only (no connectors/server)\n  floe-node run --config ./floe.toml --dry-run"
)]
pub struct RunArgs {
    /// Number of events to emit every second from the Nexmark generator.
    #[arg(
        long = "events-per-second",
        default_value_t = 10.0,
        value_parser = parse_positive_rate
    )]
    pub events_per_second: f64,

    /// Maximum number of events to emit before exiting the generator task.
    #[arg(long = "max-events")]
    pub max_events: Option<u64>,

    /// SQL program text (supports CREATE MATERIALIZED VIEW and CREATE SINK statements).
    #[arg(long = "mv-query", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    pub mv_query: Option<String>,

    /// Connector configuration file (TOML/YAML/JSON).
    #[arg(long = "config")]
    pub config: Option<String>,

    /// Validate config + SQL planning and exit without running connectors/server.
    #[arg(long = "dry-run")]
    pub dry_run: bool,

    /// Persist SlateDB state under this filesystem directory instead of using in-memory storage.
    #[arg(long = "data-dir")]
    pub data_dir: Option<String>,

    /// Initialise SlateDB storage from object-store environment variables.
    #[arg(long = "object-store-from-env")]
    pub object_store_from_env: bool,

    /// Dotenv-style file to load before reading object-store environment variables.
    #[arg(long = "object-store-env-file")]
    pub object_store_env_file: Option<String>,

    /// SlateDB database name when using object-store storage.
    #[arg(long = "slatedb-name")]
    pub slatedb_name: Option<String>,

    /// Address for the pgwire endpoint.
    #[arg(long = "pgwire-addr")]
    pub pgwire_addr: Option<String>,

    /// Do not start the pgwire endpoint.
    #[arg(long = "disable-pgwire")]
    pub disable_pgwire: bool,

    /// Port for the admin HTTP endpoint.
    #[arg(long = "admin-port")]
    pub admin_port: Option<u16>,

    /// Test/debug delay before committing each runtime tick.
    #[arg(long = "pre-tick-commit-delay-ms", value_parser = parse_nonnegative_u64)]
    pub pre_tick_commit_delay_ms: Option<u64>,

    /// Duration after which idle sources stop holding back the global watermark.
    #[arg(long = "watermark-idle-source-ms", value_parser = parse_positive_u64)]
    pub watermark_idle_source_ms: Option<u64>,

    /// Channel capacity for pgwire TAIL streams.
    #[arg(long = "tail-channel-capacity", value_parser = parse_positive_usize)]
    pub tail_channel_capacity: Option<usize>,

    /// Maximum materialized-view versions a TAIL stream catches up per scheduler pass.
    #[arg(long = "tail-max-catchup-versions", value_parser = parse_positive_i64)]
    pub tail_max_catchup_versions: Option<i64>,

    /// Maximum number of transient operators folded into one materialization segment.
    #[arg(long = "transient-segment-max-nodes", value_parser = parse_positive_usize)]
    pub transient_segment_max_nodes: Option<usize>,

    /// Minimum score required before the transient segment optimization is used.
    #[arg(long = "transient-segment-min-score", value_parser = parse_nonnegative_i32)]
    pub transient_segment_min_score: Option<i32>,

    /// SlateDB settings file (TOML/YAML/JSON).
    #[arg(long = "slatedb-config")]
    pub slatedb_config: Option<String>,

    /// Environment variable prefix to read SlateDB settings from when explicitly set.
    #[arg(long = "slatedb-env-prefix")]
    pub slatedb_env_prefix: Option<String>,

    /// SlateDB flush interval in milliseconds (0 disables automatic flushing).
    #[arg(long = "slatedb-flush-interval-ms", value_parser = parse_nonnegative_u64)]
    pub slatedb_flush_interval_ms: Option<u64>,

    /// SlateDB L0 SST size in bytes.
    #[arg(long = "slatedb-l0-sst-bytes", value_parser = parse_positive_usize)]
    pub slatedb_l0_sst_size_bytes: Option<usize>,

    /// Max unflushed bytes before SlateDB applies backpressure.
    #[arg(long = "slatedb-max-unflushed-bytes", value_parser = parse_positive_usize)]
    pub slatedb_max_unflushed_bytes: Option<usize>,

    /// SlateDB compactor max SST size in bytes.
    #[arg(long = "slatedb-compaction-max-sst-bytes", value_parser = parse_positive_usize)]
    pub slatedb_compaction_max_sst_bytes: Option<usize>,

    /// SlateDB compactor max concurrent compactions.
    #[arg(long = "slatedb-compaction-max-concurrent", value_parser = parse_positive_usize)]
    pub slatedb_compaction_max_concurrent: Option<usize>,

    /// Wait for SlateDB writes to be durable before acknowledging writes.
    /// Defaults to true when not set (GA-safe default).
    #[arg(
        long = "slatedb-await-durable",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::value_parser!(bool)
    )]
    pub slatedb_await_durable: Option<bool>,

    /// Enable SlateDB object-store cache at this local directory.
    #[arg(long = "slatedb-cache-dir")]
    pub slatedb_cache_dir: Option<String>,

    /// Max SlateDB object-store cache size in bytes.
    #[arg(long = "slatedb-cache-max-bytes", value_parser = parse_positive_usize)]
    pub slatedb_cache_max_bytes: Option<usize>,

    /// SlateDB object-store cache part size in bytes.
    #[arg(long = "slatedb-cache-part-bytes", value_parser = parse_positive_usize)]
    pub slatedb_cache_part_bytes: Option<usize>,

    /// Cache SlateDB PUT operations to disk (requires --slatedb-cache-dir).
    #[arg(long = "slatedb-cache-puts")]
    pub slatedb_cache_puts: bool,

    /// Timeout for closing SlateDB during shutdown.
    #[arg(long = "slatedb-close-timeout-ms", value_parser = parse_positive_u64)]
    pub slatedb_close_timeout_ms: Option<u64>,

    /// Number of materialized view versions to retain (0 keeps all versions).
    #[arg(long = "mv-retain-last", default_value_t = 1, value_parser = parse_nonnegative_usize)]
    pub mv_retain_last: usize,

    /// Max version-chain length before runtime compaction is triggered.
    #[arg(
        long = "zset-compaction-max-chain-len",
        default_value_t = 512,
        value_parser = parse_positive_usize
    )]
    pub zset_compaction_max_chain_len: usize,

    /// Max versioned segment count before runtime compaction is triggered.
    #[arg(
        long = "zset-compaction-max-segments",
        default_value_t = 4096,
        value_parser = parse_positive_usize
    )]
    pub zset_compaction_max_segments: usize,

    /// Tick backoff after compaction failures before retrying.
    #[arg(
        long = "zset-compaction-backoff-ticks",
        default_value_t = 1,
        value_parser = parse_nonnegative_u64
    )]
    pub zset_compaction_backoff_ticks: u64,

    /// Max number of runtime compaction jobs allowed in flight per stream.
    #[arg(
        long = "zset-compaction-max-concurrent-jobs",
        default_value_t = 1,
        value_parser = parse_positive_usize
    )]
    pub zset_compaction_max_concurrent_jobs: usize,

    /// Grace period before unreachable manifest/segment artifacts are reclaimed.
    #[arg(
        long = "zset-gc-grace-period-ms",
        default_value_t = 30_000,
        value_parser = parse_nonnegative_u64
    )]
    pub zset_gc_grace_period_ms: u64,

    /// Start with maintenance operations paused.
    #[arg(long = "maintenance-paused")]
    pub maintenance_paused: bool,

    /// Inspect manifest/GC reachability state for one or more namespaces on startup.
    #[arg(long = "maintenance-inspect-namespace")]
    pub maintenance_inspect_namespace: Vec<String>,

    /// Trigger a one-shot compaction for one or more namespaces on startup.
    #[arg(long = "maintenance-compact-namespace")]
    pub maintenance_compact_namespace: Vec<String>,

    /// Trigger a one-shot GC sweep for one or more namespaces on startup.
    #[arg(long = "maintenance-gc-namespace")]
    pub maintenance_gc_namespace: Vec<String>,

    /// Output delta consolidation mode for materialized view writes.
    #[arg(
        long = "output-consolidation-mode",
        value_enum,
        default_value_t = OutputConsolidationMode::AllColumns
    )]
    pub output_consolidation_mode: OutputConsolidationMode,

    /// Read newline-delimited JSON events from a file (use "-" for stdin).
    #[arg(long = "input-file")]
    pub input_file: Option<String>,

    /// Default source name to apply when input lines omit "source" and "data".
    #[arg(long = "input-source")]
    pub input_source: Option<String>,

    /// Kafka bootstrap servers (comma-separated host:port list).
    #[arg(long = "kafka-brokers")]
    pub kafka_brokers: Option<String>,

    /// Kafka topics to subscribe to (comma-separated).
    #[arg(long = "kafka-topics", value_delimiter = ',')]
    pub kafka_topics: Vec<String>,

    /// Kafka consumer group id.
    #[arg(long = "kafka-group-id", default_value = "floe")]
    pub kafka_group_id: String,

    /// Default source name for Kafka payloads that omit "source" and "data".
    #[arg(long = "kafka-default-source")]
    pub kafka_default_source: Option<String>,

    /// Kafka poll timeout in milliseconds.
    #[arg(long = "kafka-poll-ms", default_value_t = 100)]
    pub kafka_poll_ms: u64,

    /// Max Kafka messages processed per tick.
    #[arg(long = "kafka-max-messages", default_value_t = 256, value_parser = parse_positive_usize)]
    pub kafka_max_messages: usize,

    /// Max number of events buffered between connectors and the executor.
    #[arg(
        long = "ingest-queue-capacity",
        default_value_t = 1024,
        value_parser = parse_positive_usize
    )]
    pub ingest_queue_capacity: usize,

    /// Max number of events processed per ingestion batch.
    #[arg(
        long = "ingest-batch-size",
        default_value_t = 256,
        value_parser = parse_positive_usize
    )]
    pub ingest_batch_size: usize,

    /// Max number of events per source per ingestion batch.
    #[arg(
        long = "ingest-batch-per-source",
        default_value_t = 64,
        value_parser = parse_positive_usize
    )]
    pub ingest_batch_per_source: usize,

    /// Max number of events per connector per ingestion batch.
    #[arg(
        long = "ingest-batch-per-connector",
        default_value_t = 64,
        value_parser = parse_positive_usize
    )]
    pub ingest_batch_per_connector: usize,

    /// Host for the HTTP ingest endpoint (requires --http-port).
    #[arg(long = "http-host", default_value = "127.0.0.1")]
    pub http_host: String,

    /// Port for the HTTP ingest endpoint (when set, the server starts).
    #[arg(long = "http-port")]
    pub http_port: Option<u16>,

    /// Default source name for HTTP ingest payloads that omit "source" and "data".
    #[arg(long = "http-source")]
    pub http_source: Option<String>,
}

#[derive(Debug, Args)]
#[command(
    after_long_help = "Examples:\n  # Tail by materialized view name\n  floe-node tail --mv mv_bid\n\n  # Tail by explicit SQL\n  floe-node tail --sql \"TAIL mv_bid WITH (SNAPSHOT)\"\n\n  # Tail from a specific version\n  floe-node tail --mv mv_bid --as-of 42 --with-snapshot"
)]
pub struct TailArgs {
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,
    #[arg(long, default_value_t = 6432)]
    pub port: u16,
    #[arg(long, default_value = "postgres")]
    pub user: String,
    #[arg(long, default_value = "postgres")]
    pub database: String,
    #[arg(long, required_unless_present = "sql", conflicts_with = "sql")]
    pub mv: Option<String>,
    #[arg(long, required_unless_present = "mv", conflicts_with = "mv")]
    pub sql: Option<String>,
    #[arg(long)]
    pub with_snapshot: bool,
    #[arg(long)]
    pub as_of: Option<i64>,
    #[arg(long)]
    pub max_rows: Option<usize>,
    #[arg(long)]
    pub no_header: bool,
}

#[derive(Debug, Args)]
#[command(
    after_long_help = "Examples:\n  # Subscribe by materialized view name\n  floe-node subscribe --mv mv_bid\n\n  # Subscribe with an initial snapshot\n  floe-node subscribe --mv mv_bid --with-snapshot\n\n  # Subscribe by explicit SQL\n  floe-node subscribe --sql \"SUBSCRIBE mv_bid WITH SNAPSHOT\""
)]
pub struct SubscribeArgs {
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,
    #[arg(long, default_value_t = 6432)]
    pub port: u16,
    #[arg(long, default_value = "postgres")]
    pub user: String,
    #[arg(long, default_value = "postgres")]
    pub database: String,
    #[arg(long, required_unless_present = "sql", conflicts_with = "sql")]
    pub mv: Option<String>,
    #[arg(long, required_unless_present = "mv", conflicts_with = "mv")]
    pub sql: Option<String>,
    #[arg(long)]
    pub with_snapshot: bool,
    #[arg(long)]
    pub as_of: Option<i64>,
    #[arg(long)]
    pub max_rows: Option<usize>,
    #[arg(long)]
    pub no_header: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum, Eq, PartialEq)]
pub enum OutputConsolidationMode {
    AllColumns,
    Key,
}

impl TailArgs {
    pub fn to_config(&self) -> anyhow::Result<TailConfig> {
        let sql = match self.sql.as_ref() {
            Some(sql) => sql.to_string(),
            None => {
                let mv = self
                    .mv
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("--mv is required when --sql is not set"))?;
                build_tail_sql(mv, self.with_snapshot, self.as_of)
            }
        };
        Ok(TailConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            database: self.database.clone(),
            sql,
            max_rows: self.max_rows,
            no_header: self.no_header,
        })
    }
}

impl SubscribeArgs {
    pub fn to_config(&self) -> anyhow::Result<TailConfig> {
        let sql = match self.sql.as_ref() {
            Some(sql) => sql.to_string(),
            None => {
                let mv = self
                    .mv
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("--mv is required when --sql is not set"))?;
                build_subscribe_sql(mv, self.with_snapshot, self.as_of)
            }
        };
        Ok(TailConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            database: self.database.clone(),
            sql,
            max_rows: self.max_rows,
            no_header: self.no_header,
        })
    }
}

fn parse_positive_rate(value: &str) -> Result<f64, String> {
    let parsed: f64 = value
        .parse()
        .map_err(|_| "events-per-second must be a positive number".to_string())?;
    if parsed <= 0.0 {
        Err("events-per-second must be greater than 0".to_string())
    } else {
        Ok(parsed)
    }
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed: usize = value
        .parse()
        .map_err(|_| "value must be a positive integer".to_string())?;
    if parsed == 0 {
        Err("value must be greater than 0".to_string())
    } else {
        Ok(parsed)
    }
}

fn parse_nonnegative_usize(value: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| "value must be a non-negative integer".to_string())
}

fn parse_nonnegative_u64(value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| "value must be a non-negative integer".to_string())
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let parsed = parse_nonnegative_u64(value)?;
    if parsed == 0 {
        Err("value must be greater than 0".to_string())
    } else {
        Ok(parsed)
    }
}

fn parse_positive_i64(value: &str) -> Result<i64, String> {
    let parsed: i64 = value
        .parse()
        .map_err(|_| "value must be a positive integer".to_string())?;
    if parsed <= 0 {
        Err("value must be greater than 0".to_string())
    } else {
        Ok(parsed)
    }
}

fn parse_nonnegative_i32(value: &str) -> Result<i32, String> {
    let parsed: i32 = value
        .parse()
        .map_err(|_| "value must be a non-negative integer".to_string())?;
    if parsed < 0 {
        Err("value must be greater than or equal to 0".to_string())
    } else {
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn run_help_includes_examples() {
        let mut cmd = Cli::command();
        let run = cmd
            .find_subcommand_mut("run")
            .expect("run subcommand should exist");
        let help = run.render_long_help().to_string();
        assert!(help.contains("Generator + MV"));
        assert!(help.contains("File ingest + MV"));
        assert!(help.contains("Kafka ingest + MV"));
        assert!(help.contains("Config-first startup"));
    }

    #[test]
    fn tail_help_includes_examples() {
        let mut cmd = Cli::command();
        let tail = cmd
            .find_subcommand_mut("tail")
            .expect("tail subcommand should exist");
        let help = tail.render_long_help().to_string();
        assert!(help.contains("Tail by materialized view name"));
        assert!(help.contains("Tail by explicit SQL"));
        assert!(help.contains("--as-of 42 --with-snapshot"));
    }

    #[test]
    fn subscribe_help_includes_examples() {
        let mut cmd = Cli::command();
        let subscribe = cmd
            .find_subcommand_mut("subscribe")
            .expect("subscribe subcommand should exist");
        let help = subscribe.render_long_help().to_string();
        assert!(help.contains("Subscribe by materialized view name"));
        assert!(help.contains("SUBSCRIBE mv_bid WITH SNAPSHOT"));
    }
}
