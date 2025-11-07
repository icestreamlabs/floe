mod filter;
mod join;
mod map;
mod materialize;
mod scan;

pub use filter::FilterOperator;
pub use join::JoinOperator;
pub use map::MapOperator;
pub use materialize::MaterializeOperator;
pub use scan::ScanOperator;

use anyhow::Result;

use crate::stream_types::{Diff, Row, Timestamp};

/// Output sink used by operators to forward rows downstream.
pub trait RowSink: Send {
    fn push(&mut self, row: Row, diff: Diff, timestamp: Timestamp) -> Result<()>;

    fn watermark(&mut self, _watermark: Timestamp) -> Result<()> {
        Ok(())
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
    }
}
