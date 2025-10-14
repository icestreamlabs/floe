use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use floe_core::catalog::TableDefinition;
use floe_storage::SlateCatalog;

pub struct SlateTableProvider {
    storage: Arc<SlateCatalog>,
    table: TableDefinition,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl fmt::Debug for SlateTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlateTableProvider")
            .field("table", &self.table.name())
            .finish()
    }
}

impl SlateTableProvider {
    pub fn new(storage: Arc<SlateCatalog>, table: TableDefinition) -> Self {
        let schema = table.to_arrow_schema();
        Self {
            storage,
            table,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for SlateTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut rows = self
            .storage
            .read_rows(&self.table)
            .await
            .map_err(to_datafusion_error)?;

        if let Some(limit) = limit {
            if rows.len() > limit {
                rows.truncate(limit);
            }
        }

        let batches = build_batches(rows, self.schema.clone()).map_err(to_datafusion_error)?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

fn build_batches(
    rows: Vec<Vec<i64>>,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<i64>> = vec![Vec::with_capacity(rows.len()); column_count];

    for row in rows {
        for (idx, value) in row.into_iter().enumerate() {
            if let Some(column) = columns.get_mut(idx) {
                column.push(value);
            } else {
                return Err(anyhow!("row contains unexpected column index {idx}"));
            }
        }
    }

    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(anyhow::Error::from)?;
    Ok(vec![batch])
}

fn to_datafusion_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}
