use clap::{Parser, ValueEnum};

#[derive(Debug, Parser)]
#[command(author, version, about = "Floe node entrypoint", long_about = None)]
pub struct Cli {
    /// Select the execution mode.
    #[arg(long, value_enum, default_value_t = Mode::Generator)]
    pub mode: Mode,

    /// Number of events to emit every second when running the generator.
    #[arg(long = "events-per-second", default_value_t = 10.0)]
    pub events_per_second: f64,

    /// Maximum number of events to emit before exiting.
    #[arg(long = "max-events")]
    pub max_events: Option<u64>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum Mode {
    Generator,
    Server,
}
