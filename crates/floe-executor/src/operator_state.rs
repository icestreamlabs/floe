/// Handle that identifies a persisted operator state table snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct OperatorStateHandle {
    pub table: String,
    pub namespace: String,
    pub version: u64,
}

impl OperatorStateHandle {
    pub fn new(table: impl Into<String>, namespace: impl Into<String>, version: u64) -> Self {
        Self {
            table: table.into(),
            namespace: namespace.into(),
            version,
        }
    }
}
