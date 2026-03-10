mod cli;
mod config;
mod http_ingest;
mod metrics;
mod node_runtime;
mod sinks;

#[cfg(all(feature = "allocator-mimalloc", feature = "allocator-jemalloc"))]
compile_error!("enable only one allocator feature at a time");

#[cfg(feature = "allocator-mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "allocator-jemalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    node_runtime::run().await
}
