use clap::{Args, Parser, Subcommand};

use floe_node_core::tail_client::{TailConfig, build_tail_sql};

#[derive(Debug, Parser)]
#[command(author, version, about = "Floe node entrypoint", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Run(RunArgs),
    Tail(TailArgs),
}

#[derive(Debug, Args)]
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

    /// SQL text for a single materialized view definition (specify at most once).
    #[arg(long = "mv-query", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    pub mv_query: Option<String>,

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
