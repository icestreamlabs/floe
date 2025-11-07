use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use floe_core::catalog::TableDefinition;
use floe_storage::SlateCatalog;

use crate::materialized_view::MaterializedViewRegistry;
use crate::stream_types::{Diff, Row};

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

        let batches = build_i64_batches(rows, self.schema.clone()).map_err(to_datafusion_error)?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

fn build_i64_batches(
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

pub struct MaterializedViewTableProvider {
    registry: Arc<MaterializedViewRegistry>,
    view_name: String,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl MaterializedViewTableProvider {
    pub fn new(
        registry: Arc<MaterializedViewRegistry>,
        view_name: impl Into<String>,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        Self {
            registry,
            view_name: view_name.into(),
            schema,
        }
    }

    fn snapshot_rows(&self) -> DFResult<Vec<Row>> {
        let view = self.registry.get(&self.view_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "materialized view '{}' is not registered",
                self.view_name
            ))
        })?;

        let snapshot = view.snapshot();
        let mut rows = Vec::new();
        for (row, diff) in snapshot {
            append_row_with_diff(&mut rows, row, diff)?;
        }
        Ok(rows)
    }

    fn build_batches(&self) -> DFResult<Vec<RecordBatch>> {
        let rows = self.snapshot_rows()?;
        build_scalar_batches(rows, self.schema.clone())
            .map_err(|err| DataFusionError::Execution(err.to_string()))
    }
}

impl fmt::Debug for MaterializedViewTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedViewTableProvider")
            .field("view", &self.view_name)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for MaterializedViewTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let batches = self.build_batches()?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

fn append_row_with_diff(rows: &mut Vec<Row>, row: Row, diff: Diff) -> DFResult<()> {
    if diff < 0 {
        return Err(DataFusionError::Execution(format!(
            "materialized view snapshot contains negative diff {diff}"
        )));
    }
    for _ in 0..diff {
        rows.push(row.clone());
    }
    Ok(())
}

fn build_scalar_batches(
    rows: Vec<Row>,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(rows.len()); column_count];

    for row in rows {
        if row.len() != column_count {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                column_count
            ));
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_count);
    for column in columns {
        let array = ScalarValue::iter_to_array(column.into_iter())
            .map_err(|err| anyhow!(err.to_string()))?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(schema, arrays).map_err(anyhow::Error::from)?;
    Ok(vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::scalar::ScalarValue;

    #[test]
    fn materialized_view_provider_emits_rows() {
        let registry = Arc::new(MaterializedViewRegistry::new());
        let view = registry.register("mv_test");
        view.apply(
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("one".into())),
            ],
            2,
        );
        view.apply(
            vec![
                ScalarValue::Int64(Some(2)),
                ScalarValue::Utf8(Some("two".into())),
            ],
            1,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]));

        let provider = MaterializedViewTableProvider::new(registry.clone(), "mv_test", schema);
        let batches = provider.build_batches().expect("build batches");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);
    }
}
