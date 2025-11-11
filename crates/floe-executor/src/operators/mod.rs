mod filter;
mod join;
mod map;
mod materialize;
mod scan;

pub use filter::{FilterDerivedState, FilterOperator};
pub use join::JoinOperator;
pub use map::{MapDerivedState, MapOperator};
pub use materialize::MaterializeOperator;
pub use scan::ScanOperator;

use std::any::Any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::stream_types::{Diff, InputPort, Row, Timestamp};

#[derive(Debug)]
pub struct DispatchEvent {
    pub target_op_index: usize,
    pub input_port: InputPort,
    pub row: Row,
    pub diff: Diff,
    pub ts: Timestamp,
}

pub type EventQueue = Arc<Mutex<VecDeque<DispatchEvent>>>;

/// Output sink used by operators to forward rows downstream.
pub trait RowSink: Send + 'static {
    fn push(&mut self, row: Row, diff: Diff, timestamp: Timestamp) -> Result<()>;

    fn watermark(&mut self, _watermark: Timestamp) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Sink that forwards rows to downstream operators by enqueuing dispatch events.
pub struct DispatchSink {
    targets: Vec<(usize, InputPort)>,
    queue: EventQueue,
}

impl DispatchSink {
    pub fn new(targets: Vec<(usize, InputPort)>, queue: EventQueue) -> Self {
        Self { targets, queue }
    }
}

impl RowSink for DispatchSink {
    fn push(&mut self, row: Row, diff: Diff, timestamp: Timestamp) -> Result<()> {
        let mut queue = self.queue.lock().expect("dispatch queue lock");
        for (operator_idx, port) in &self.targets {
            queue.push_back(DispatchEvent {
                target_op_index: *operator_idx,
                input_port: *port,
                row: row.clone(),
                diff,
                ts: timestamp,
            });
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Sink that drops all incoming data.
#[derive(Default)]
pub struct NullSink;

impl RowSink for NullSink {
    fn push(&mut self, _row: Row, _diff: Diff, _timestamp: Timestamp) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;

    #[derive(Default)]
    pub struct TestSink {
        pub rows: Vec<(Row, Diff, Timestamp)>,
        pub watermarks: Vec<Timestamp>,
    }

    impl RowSink for TestSink {
        fn push(&mut self, row: Row, diff: Diff, timestamp: Timestamp) -> Result<()> {
            self.rows.push((row, diff, timestamp));
            Ok(())
        }

        fn watermark(&mut self, watermark: Timestamp) -> Result<()> {
            self.watermarks.push(watermark);
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }
}
