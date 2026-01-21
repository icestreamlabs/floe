use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use dbsp::handles::ZSetHandleView;

use crate::encoding::decode_projected_row_key;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::stream_types::Row;

use super::MV_VERSION_COLUMN;
use super::filters::{extract_mv_version_filter, parse_mv_version_expr};
use super::helpers::{append_mv_version_field, append_row_with_diff, build_scalar_batches};

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
        let include_mv_version = !schema
            .fields()
            .iter()
            .any(|field| field.name() == MV_VERSION_COLUMN);
        let schema_with_meta = if include_mv_version {
            append_mv_version_field(&schema)
        } else {
            Arc::clone(&schema)
        };
        Self {
            registry,
            view_name: view_name.into(),
            schema: schema_with_meta,
        }
    }

    async fn build_batches(&self, as_of_version: Option<u64>) -> DFResult<Vec<RecordBatch>> {
        let (rows, version) = self.load_rows(as_of_version).await?;
        let rows = self.attach_version_column(rows, version);
        build_scalar_batches(rows, self.schema.clone())
            .map_err(|err| DataFusionError::Execution(err.to_string()))
    }

    #[cfg(test)]
    pub async fn build_batches_for_test(&self) -> DFResult<Vec<RecordBatch>> {
        self.build_batches(None).await
    }

    #[cfg(test)]
    pub async fn build_batches_at_version(&self, version: u64) -> DFResult<Vec<RecordBatch>> {
        self.build_batches(Some(version)).await
    }

    async fn load_rows(&self, as_of_version: Option<u64>) -> DFResult<(Vec<Row>, u64)> {
        let view = self.registry.get(&self.view_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "materialized view '{}' is not registered",
                self.view_name
            ))
        })?;

        let Some(state) = view.dbsp_state() else {
            tracing::warn!(
                view = %self.view_name,
                "materialized view has no DBSP state when loading rows"
            );
            return Ok((Vec::new(), 0));
        };
        let target_version = as_of_version.unwrap_or(state.version());
        let rows = self
            .materialize_dbsp_rows(state, Some(target_version))
            .await?;
        tracing::info!(
            view = %self.view_name,
            version = target_version,
            rows = rows.len(),
            "materialized view loaded rows"
        );
        Ok((rows, target_version))
    }

    async fn materialize_dbsp_rows(
        &self,
        state: DbspPersistedState,
        as_of_version: Option<u64>,
    ) -> DFResult<Vec<Row>> {
        let target_version = as_of_version.unwrap_or(state.version());
        let handle_view = ZSetHandleView::new(
            state.dictionary(),
            state.table(),
            state.namespace().to_string(),
            target_version,
        );
        let snapshot = handle_view
            .materialize()
            .await
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        #[cfg(test)]
        tracing::debug!(
            view = %self.view_name,
            version = target_version,
            snapshot_len = snapshot.len(),
            "materialize dbsp rows"
        );
        let mut rows = Vec::new();
        for (key, diff) in snapshot {
            let decoded = decode_projected_row_key(&key)
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
            append_row_with_diff(&mut rows, decoded, diff)?;
        }
        Ok(rows)
    }

    fn attach_version_column(&self, rows: Vec<Row>, version: u64) -> Vec<Row> {
        let schema_has_mv_version = self
            .schema
            .fields()
            .iter()
            .any(|field| field.name() == MV_VERSION_COLUMN);
        if !schema_has_mv_version {
            return rows;
        }
        rows.into_iter()
            .map(|mut row| {
                row.push(ScalarValue::UInt64(Some(version)));
                row
            })
            .collect()
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
        let (as_of_version, passthrough_filters) = extract_mv_version_filter(filters);
        let batches = self.build_batches(as_of_version).await?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        // Always expose full schema (including __mv_version) regardless of projection pushdown.
        mem_table
            .scan(state, projection, &passthrough_filters, limit)
            .await
    }
}
