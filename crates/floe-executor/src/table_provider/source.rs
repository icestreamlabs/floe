use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
#[cfg(test)]
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::Mutex;

use crate::dbsp_bridge::DbspBridge;
use crate::namespaces;
use crate::table_provider::SnapshotScanExec;

use super::filters::{PrimaryKeyFilter, extract_primary_key_filter, parse_primary_key_expr};
use super::helpers::{build_batches_from_encoded_snapshot, to_datafusion_error};

pub struct SourceTableProvider {
    bridge: Arc<Mutex<DbspBridge>>,
    table_name: String,
    namespace: String,
    schema: datafusion::arrow::datatypes::SchemaRef,
    primary_key_column: Option<String>,
    primary_key_index: Option<usize>,
}

impl SourceTableProvider {
    pub fn new(
        bridge: Arc<Mutex<DbspBridge>>,
        table_name: impl Into<String>,
        source_name: &str,
        schema: datafusion::arrow::datatypes::SchemaRef,
        primary_key_column: Option<&str>,
    ) -> Result<Self> {
        let namespace = namespaces::source(source_name)?;
        let primary_key_column = primary_key_column.map(|name| name.to_string());
        let primary_key_index = primary_key_column
            .as_deref()
            .map(|name| schema.index_of(name).map_err(anyhow::Error::from))
            .transpose()?;
        Ok(Self {
            bridge,
            table_name: table_name.into(),
            namespace,
            schema,
            primary_key_column,
            primary_key_index,
        })
    }

    async fn load_snapshot(&self) -> DFResult<HashMap<Vec<u8>, i64>> {
        let mut bridge = self.bridge.lock().await;
        let handle = bridge
            .latest_view_handle(&self.namespace)
            .await
            .map_err(to_datafusion_error)?;
        let view = bridge
            .handle_view_for(&self.namespace, handle.version)
            .await
            .map_err(to_datafusion_error)?;
        view.materialize()
            .await
            .map_err(|err| DataFusionError::Execution(err.to_string()))
    }

    async fn build_batches(
        &self,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        primary_key_filter: Option<&PrimaryKeyFilter>,
    ) -> DFResult<(
        datafusion::arrow::datatypes::SchemaRef,
        Vec<datafusion::arrow::record_batch::RecordBatch>,
    )> {
        let snapshot = self.load_snapshot().await?;
        let primary_key_index = self.primary_key_index;
        build_batches_from_encoded_snapshot(
            snapshot,
            self.schema.clone(),
            projection,
            limit,
            None,
            Some(move |row: &crate::stream_types::Row| {
                if let (Some(filter), Some(index)) = (primary_key_filter, primary_key_index) {
                    row.get(index).is_some_and(|value| filter.matches(value))
                } else {
                    true
                }
            }),
        )
    }

    #[cfg(test)]
    pub(super) async fn build_batches_for_test(&self) -> DFResult<Vec<RecordBatch>> {
        let (_, batches) = self.build_batches(None, None, None).await?;
        Ok(batches)
    }
}

impl fmt::Debug for SourceTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceTableProvider")
            .field("table", &self.table_name)
            .field("namespace", &self.namespace)
            .field("primary_key_column", &self.primary_key_column)
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
        let primary_key_column = self.primary_key_column.as_deref();
        Ok(filters
            .iter()
            .map(|expr| {
                if let Some(column) = primary_key_column
                    && parse_primary_key_expr(expr, column).is_some()
                {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (primary_key_filter, _retained) =
            extract_primary_key_filter(filters, self.primary_key_column.as_deref());
        let (projected_schema, batches) = self
            .build_batches(projection, limit, primary_key_filter.as_ref())
            .await?;
        Ok(Arc::new(SnapshotScanExec::new(projected_schema, batches)))
    }
}
