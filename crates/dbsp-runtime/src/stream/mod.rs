//! Operational runtime streams and execution helpers.
//!
//! This module is intentionally not the denotational DBSP paper model.
//! `Stream<T>` exposes runtime-facing notions such as current time,
//! committed frontier, semantic horizon, and default-tail behavior.

pub mod core;
mod groups;
mod roles;
pub mod util;
mod zset_stream;

#[cfg(test)]
pub mod tests;

pub use core::Stream;
pub use roles::{DeltaHandleStream, SnapshotHandleStream};
pub use zset_stream::{CompactionSchedulerConfig, StreamRetention, ZSetStream};
