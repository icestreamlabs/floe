#![allow(
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::manual_is_multiple_of,
    clippy::needless_borrow,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_update,
    clippy::too_many_arguments,
    clippy::while_let_loop
)]

mod cli;
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
