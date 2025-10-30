mod cli;
mod generator;
mod server;
mod sql;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();

    match cli.mode {
        cli::Mode::Generator => {
            let config = generator::Config {
                events_per_second: cli.events_per_second,
                max_events: cli.max_events,
            };
            generator::run(config).await
        }
        cli::Mode::Server => server::run().await,
    }
}
