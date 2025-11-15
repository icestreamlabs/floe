use datafusion::scalar::ScalarValue;

/// Logical timestamp used to order stream events.
pub type Timestamp = u64;

/// Signed multiplicity for ZSet-style updates (+1 insert, -1 delete).
pub type Diff = i64;

/// Row representation backed by DataFusion's `ScalarValue` type.
pub type Row = Vec<ScalarValue>;
