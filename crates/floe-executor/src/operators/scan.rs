use std::any::Any;

use anyhow::{Result, bail};

use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

/// Scan operators serve as sources; they do not consume upstream input.
pub struct ScanOperator {
    source_name: String,
    sink: Box<dyn RowSink>,
}

impl ScanOperator {
    pub fn new(source_name: impl Into<String>, sink: impl RowSink) -> Self {
        Self {
            source_name: source_name.into(),
            sink: Box::new(sink),
        }
    }

    pub fn source(&self) -> &str {
        &self.source_name
    }

    pub fn ingest(&mut self, row: Row, diff: Diff, timestamp: Timestamp) -> Result<()> {
        self.sink.push(row, diff, timestamp)
    }

    #[cfg(test)]
    pub fn sink(&self) -> &dyn RowSink {
        self.sink.as_ref()
    }
}

impl StreamOperator for ScanOperator {
    fn on_input(
        &mut self,
        _input: InputPort,
        _row: Row,
        _diff: Diff,
        _timestamp: Timestamp,
    ) -> Result<()> {
        bail!(
            "scan operator '{}' does not accept upstream input",
            self.source_name
        );
    }

    fn on_watermark(&mut self, watermark: Timestamp) -> Result<()> {
        self.sink.watermark(watermark)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::*;
    use crate::operators::test_support::TestSink;

    #[test]
    fn ingests_rows() {
        let sink = TestSink::default();
        let mut op = ScanOperator::new("bid", sink);
        let row = vec![ScalarValue::Int64(Some(1))];
        op.ingest(row.clone(), 1, 1).expect("ingest");
        let sink = op.sink().as_any().downcast_ref::<TestSink>().unwrap();
        assert_eq!(sink.rows.len(), 1);
        assert_eq!(sink.rows[0].0, row);
    }
}
