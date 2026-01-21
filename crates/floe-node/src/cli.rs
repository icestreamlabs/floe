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
