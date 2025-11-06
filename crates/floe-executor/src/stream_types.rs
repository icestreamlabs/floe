use anyhow::Result;
use datafusion::scalar::ScalarValue;

/// Logical timestamp used to order stream events.
pub type Timestamp = u64;

/// Signed multiplicity for ZSet-style updates (+1 insert, -1 delete).
pub type Diff = i64;

/// Row representation backed by DataFusion's `ScalarValue` type.
pub type Row = Vec<ScalarValue>;

/// Identifier for a logical operator within a dataflow plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperatorId(pub usize);

/// Addressable input port on a specific operator node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InputPort {
    pub operator: OperatorId,
    pub port_index: usize,
}

impl InputPort {
    pub fn new(operator: OperatorId, port_index: usize) -> Self {
        Self {
            operator,
            port_index,
        }
    }
}

/// Addressable output port on a specific operator node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OutputPort {
    pub operator: OperatorId,
    pub port_index: usize,
}

impl OutputPort {
    pub fn new(operator: OperatorId, port_index: usize) -> Self {
        Self {
            operator,
            port_index,
        }
    }
}

/// Common interface implemented by runtime stream operators.
pub trait StreamOperator {
    fn on_input(
        &mut self,
        input: InputPort,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()>;

    fn on_watermark(&mut self, watermark: Timestamp) -> Result<()>;
}
