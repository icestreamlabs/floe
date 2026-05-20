use floe_cdc_core::{CdcCheckpoint, CdcColumnarRowBatch, CdcRow, CdcTableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcRowDelta {
    row: CdcRow,
    diff: i64,
}

impl CdcRowDelta {
    pub fn insert(row: CdcRow) -> Self {
        Self { row, diff: 1 }
    }

    pub fn delete(row: CdcRow) -> Self {
        Self { row, diff: -1 }
    }

    pub fn row(&self) -> &CdcRow {
        &self.row
    }

    pub fn diff(&self) -> i64 {
        self.diff
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcTableDeltas {
    table_id: CdcTableId,
    payload: CdcTableDeltaPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CdcTableDeltaPayload {
    RowDeltas(Vec<CdcRowDelta>),
    SnapshotInserts(CdcColumnarRowBatch),
}

impl CdcTableDeltas {
    pub fn new(table_id: CdcTableId, deltas: Vec<CdcRowDelta>) -> Self {
        Self {
            table_id,
            payload: CdcTableDeltaPayload::RowDeltas(deltas),
        }
    }

    pub fn snapshot_insert(table_id: CdcTableId, rows: CdcColumnarRowBatch) -> Self {
        Self {
            table_id,
            payload: CdcTableDeltaPayload::SnapshotInserts(rows),
        }
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn deltas(&self) -> &[CdcRowDelta] {
        match &self.payload {
            CdcTableDeltaPayload::RowDeltas(deltas) => deltas,
            CdcTableDeltaPayload::SnapshotInserts(_) => &[],
        }
    }

    pub fn snapshot_insert_rows(&self) -> Option<&CdcColumnarRowBatch> {
        match &self.payload {
            CdcTableDeltaPayload::RowDeltas(_) => None,
            CdcTableDeltaPayload::SnapshotInserts(rows) => Some(rows),
        }
    }

    pub fn row_count(&self) -> usize {
        match &self.payload {
            CdcTableDeltaPayload::RowDeltas(deltas) => deltas.len(),
            CdcTableDeltaPayload::SnapshotInserts(rows) => rows.row_count(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcApplyResult {
    pub(crate) checkpoint: CdcCheckpoint,
    pub(crate) table_deltas: Vec<CdcTableDeltas>,
    pub(crate) already_committed: bool,
}

impl CdcApplyResult {
    pub fn checkpoint(&self) -> &CdcCheckpoint {
        &self.checkpoint
    }

    pub fn table_deltas(&self) -> &[CdcTableDeltas] {
        &self.table_deltas
    }

    pub fn already_committed(&self) -> bool {
        self.already_committed
    }
}
