use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
#[cfg(test)]
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::Mutex;

use crate::dbsp_bridge::DbspBridge;
use crate::encoding::decode_projected_row_key;
use crate::namespaces;
use crate::stream_types::Row;

use super::helpers::{append_row_with_diff, build_scalar_batches, to_datafusion_error};

pub struct SourceTableProvider {
    bridge: Arc<Mutex<DbspBridge>>,
    table_name: String,
    namespace: String,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl SourceTableProvider {
    pub fn new(
        bridge: Arc<Mutex<DbspBridge>>,
        table_name: impl Into<String>,
        source_name: &str,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<Self> {
        let namespace = namespaces::source(source_name)?;
        Ok(Self {
            bridge,
            table_name: table_name.into(),
            namespace,
            schema,
        })
    }

    async fn load_rows(&self) -> DFResult<Vec<Row>> {
        let mut bridge = self.bridge.lock().await;
        let handle = bridge
            .latest_view_handle(&self.namespace)
            .await
            .map_err(to_datafusion_error)?;
        let view = bridge
            .handle_view_for(&self.namespace, handle.version)
            .await
            .map_err(to_datafusion_error)?;
        let snapshot = view
            .materialize()
            .await
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;

        let mut rows = Vec::new();
        for (key, diff) in snapshot {
            let decoded = decode_projected_row_key(&key)
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
            append_row_with_diff(&mut rows, decoded, diff)?;
        }
        Ok(rows)
    }

    #[cfg(test)]
    pub(super) async fn build_batches_for_test(&self) -> DFResult<Vec<RecordBatch>> {
        let rows = self.load_rows().await?;
        build_scalar_batches(rows, self.schema.clone())
            .map_err(|err| DataFusionError::Execution(err.to_string()))
    }
}

impl fmt::Debug for SourceTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceTableProvider")
            .field("table", &self.table_name)
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for SourceTableProvider {
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
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut rows = self.load_rows().await?;
        if let Some(limit) = limit
            && rows.len() > limit
        {
            rows.truncate(limit);
        }
        let batches = build_scalar_batches(rows, self.schema.clone())
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}
