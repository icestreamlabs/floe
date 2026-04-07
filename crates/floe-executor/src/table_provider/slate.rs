use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::anyhow;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use floe_core::RowValue;
use floe_core::catalog::TableDefinition;
use floe_storage::SlateCatalog;

use super::filters::parse_mv_version_expr;
use super::helpers::to_datafusion_error;
use crate::scalar_array_builder::ScalarColumnBuilder;

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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if parse_mv_version_expr(expr).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
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

        if let Some(limit) = limit
            && rows.len() > limit
        {
            rows.truncate(limit);
        }

        let batches =
            build_row_value_batches(rows, self.schema.clone()).map_err(to_datafusion_error)?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

fn build_row_value_batches(
    rows: Vec<Vec<RowValue>>,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> anyhow::Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let column_count = schema.fields().len();
    let mut builders = schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), rows.len()))
        .collect::<anyhow::Result<Vec<_>>>()?;

    for row in &rows {
        if row.len() != column_count {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                column_count
            ));
        }
    }

    for (idx, builder) in builders.iter_mut().enumerate() {
        builder.append_row_values_column(&rows, idx)?;
    }

    let arrays = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<ArrayRef>>();
    let batch = RecordBatch::try_new(schema, arrays).map_err(anyhow::Error::from)?;
    Ok(vec![batch])
}
