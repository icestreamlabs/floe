mod cli;
mod config;
mod http_ingest;
mod metrics;
mod node_runtime;
mod sinks;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    node_runtime::run().await
}
