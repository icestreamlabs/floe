use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Floe node entrypoint",
    long_about = "Run a single-node streaming SQL runtime.",
    after_long_help = "Examples:\n  floe-node run --mv-query \"CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid\"\n  floe-node run --config ./floe.toml\n  psql -h 127.0.0.1 -p 6432 -U postgres -c \"COPY (SUBSCRIBE mv WITH SNAPSHOT) TO STDOUT\""
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    Run(RunArgs),
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

    /// Duration after which idle sources stop holding back the global watermark.
    #[arg(long = "watermark-idle-source-ms", value_parser = parse_positive_u64)]
    pub watermark_idle_source_ms: Option<u64>,

    /// Channel capacity for pgwire SUBSCRIBE streams.
    #[arg(long = "subscribe-channel-capacity", value_parser = parse_positive_usize)]
    pub subscribe_channel_capacity: Option<usize>,

    /// Maximum materialized-view versions a SUBSCRIBE stream catches up per scheduler pass.
    #[arg(long = "subscribe-max-catchup-versions", value_parser = parse_positive_i64)]
    pub subscribe_max_catchup_versions: Option<i64>,

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

    /// Max WAL flushes before forcing the active memtable to L0.
    #[arg(
        long = "slatedb-max-wal-flushes-before-l0-flush",
        value_parser = parse_positive_u64
    )]
    pub slatedb_max_wal_flushes_before_l0_flush: Option<u64>,

    /// Max total SlateDB L0 SSTs before write backpressure.
    #[arg(long = "slatedb-l0-max-ssts", value_parser = parse_positive_usize)]
    pub slatedb_l0_max_ssts: Option<usize>,

    /// Max SlateDB L0 SSTs covering any single key before write backpressure.
    #[arg(
        long = "slatedb-l0-max-ssts-per-key",
        value_parser = parse_positive_usize
    )]
    pub slatedb_l0_max_ssts_per_key: Option<usize>,

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

    /// Max open file handles retained by the SlateDB disk-cache file-handle cache.
    #[arg(
        long = "slatedb-cache-max-open-file-handles",
        value_parser = parse_positive_usize
    )]
    pub slatedb_cache_max_open_file_handles: Option<usize>,

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
    fn root_help_includes_psql_subscribe_example() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("COPY (SUBSCRIBE mv WITH SNAPSHOT) TO STDOUT"));
    }
}
