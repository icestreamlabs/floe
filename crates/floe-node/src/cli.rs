use clap::Parser;

#[derive(Debug, Parser)]
#[command(author, version, about = "Floe node entrypoint", long_about = None)]
pub struct Cli {
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
