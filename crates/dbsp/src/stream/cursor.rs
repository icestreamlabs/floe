use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::watch;

use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::Stream;

/// Cursor that observes committed frontier changes on a [`Stream<T>`] and surfaces
/// `(timestamp, value)` pairs whenever the underlying stream commits.
pub struct StreamCursor<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    stream: Stream<T>,
    frontier_rx: watch::Receiver<i64>,
    observed_ts: i64,
}

impl<T> StreamCursor<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    /// Creates a new cursor pinned to the provided stream. The cursor starts
    /// at the stream’s committed frontier and will only return entries for
    /// versions strictly greater than the observed timestamp.
    pub fn new(stream: Stream<T>) -> Self {
        let frontier_rx = stream.subscribe_frontier();
        let observed_ts = *frontier_rx.borrow();
        Self {
            stream,
            frontier_rx,
            observed_ts,
        }
    }

    /// Returns the last timestamp that has been consumed by the cursor.
    pub fn observed(&self) -> i64 {
        self.observed_ts
    }

    /// Ensures the cursor is caught up with the committed frontier and returns
    /// the latest `(timestamp, value)` without waiting for a new advancement.
    pub async fn snapshot(&mut self) -> Result<(i64, T)> {
        let ts = *self.frontier_rx.borrow();
        let value = self.stream.get(ts).await?;
        self.observed_ts = ts;
        Ok((ts, value))
    }

    /// Waits until the committed frontier advances beyond the last observed
    /// timestamp and returns the newly committed `(timestamp, value)` pair.
    pub async fn next(&mut self) -> Result<(i64, T)> {
        loop {
            let frontier = *self.frontier_rx.borrow();
            if frontier > self.observed_ts {
                let next_ts = self.observed_ts + 1;
                let value = self
                    .stream
                    .get(next_ts)
                    .await
                    .with_context(|| format!("load stream value at {next_ts}"))?;
                self.observed_ts = next_ts;
                return Ok((next_ts, value));
            }
            self.frontier_rx
                .changed()
                .await
                .context("committed frontier closed unexpectedly")?;
        }
    }
}
