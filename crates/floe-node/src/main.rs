mod server;
mod sql;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server::run().await
}
