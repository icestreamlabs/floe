//! Operational runtime streams and execution helpers.
//!
//! This module is intentionally not the denotational DBSP paper model.
//! `Stream<T>` exposes runtime-facing notions such as current time,
//! committed frontier, semantic horizon, and default-tail behavior.

mod addition;
pub mod core;
mod cursor;
mod groups;
pub mod operations;
mod roles;
pub mod runtime;
pub mod util;
mod zset_stream;

#[cfg(test)]
pub mod tests;

pub use addition::StreamAddition;
pub use core::Stream;
pub use cursor::StreamCursor;
pub use operations::*;
pub use roles::{DeltaHandleStream, SnapshotHandleStream};
pub use zset_stream::{CompactionSchedulerConfig, StreamRetention, ZSetStream};
