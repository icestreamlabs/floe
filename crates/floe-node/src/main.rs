mod cli;
mod generator;
mod server;
mod sql;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();

    let generator_config = generator::Config {
        events_per_second: cli.events_per_second,
        max_events: cli.max_events,
    };

    tokio::spawn(async move {
        if let Err(err) = generator::run(generator_config).await {
            eprintln!("Nexmark generator failed: {err}");
        }
    });

    server::run().await
}
