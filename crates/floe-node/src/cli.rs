use clap::Parser;

#[derive(Debug, Parser)]
#[command(author, version, about = "Floe node entrypoint", long_about = None)]
pub struct Cli {
    /// Number of events to emit every second from the Nexmark generator.
    #[arg(long = "events-per-second", default_value_t = 10.0)]
    pub events_per_second: f64,

    /// Maximum number of events to emit before exiting the generator task.
    #[arg(long = "max-events")]
    pub max_events: Option<u64>,

    /// SQL text for a single materialized view definition (specify at most once).
    #[arg(long = "mv-query", action = clap::ArgAction::Append)]
    pub mv_query: Vec<String>,
}
