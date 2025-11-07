use anyhow::{Result, bail};

use crate::operators::RowSink;
use crate::stream_types::{Diff, InputPort, Row, StreamOperator, Timestamp};

/// Scan operators serve as sources; they do not consume upstream input.
pub struct ScanOperator<S: RowSink> {
    source_name: String,
    sink: S,
}

impl<S: RowSink> ScanOperator<S> {
    pub fn new(source_name: impl Into<String>, sink: S) -> Self {
        Self {
            source_name: source_name.into(),
            sink,
        }
    }

    pub fn source(&self) -> &str {
        &self.source_name
    }

    pub fn ingest(&mut self, row: Row, diff: Diff, timestamp: Timestamp) -> Result<()> {
        self.sink.push(row, diff, timestamp)
    }

    #[cfg(test)]
    pub fn sink(&self) -> &S {
        &self.sink
    }
}

impl<S: RowSink> StreamOperator for ScanOperator<S> {
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
        assert_eq!(op.sink().rows.len(), 1);
        assert_eq!(op.sink().rows[0].0, row);
    }
}
