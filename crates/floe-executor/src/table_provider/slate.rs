use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use floe_core::RowValue;
use floe_core::catalog::TableDefinition;
use floe_storage::SlateCatalog;

use super::filters::parse_mv_version_expr;
use super::helpers::{build_scalar_batches, to_datafusion_error};

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

        let scalar_rows = rows
            .into_iter()
            .map(row_values_to_scalar_row)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(to_datafusion_error)?;

        let batches =
            build_scalar_batches(scalar_rows, self.schema.clone()).map_err(to_datafusion_error)?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

fn row_values_to_scalar_row(values: Vec<RowValue>) -> anyhow::Result<crate::stream_types::Row> {
    let mut row = Vec::with_capacity(values.len());
    for value in values {
        let scalar = match value {
            RowValue::Int64(v) => datafusion::scalar::ScalarValue::Int64(Some(v)),
            RowValue::Bool(flag) => datafusion::scalar::ScalarValue::Boolean(Some(flag)),
            RowValue::Utf8(text) => datafusion::scalar::ScalarValue::Utf8(Some(text)),
            RowValue::TimestampMillis(value) => {
                datafusion::scalar::ScalarValue::TimestampMillisecond(Some(value), None)
            }
        };
        row.push(scalar);
    }
    Ok(row)
}
