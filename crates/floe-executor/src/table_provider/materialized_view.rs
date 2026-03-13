use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use dbsp::handles::ZSetHandleView;

use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::table_provider::SnapshotScanExec;

use super::MV_VERSION_COLUMN;
use super::filters::{extract_mv_version_filter, parse_mv_version_expr};
use super::helpers::{append_mv_version_field, build_batches_from_encoded_snapshot};

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

    async fn build_batches(
        &self,
        as_of_version: Option<u64>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<(
        datafusion::arrow::datatypes::SchemaRef,
        Vec<datafusion::arrow::record_batch::RecordBatch>,
    )> {
        let (snapshot, version) = self.load_snapshot(as_of_version).await?;
        build_batches_from_encoded_snapshot(
            snapshot,
            self.schema.clone(),
            projection,
            limit,
            Some(version),
            |_| true,
        )
    }

    #[cfg(test)]
    pub async fn build_batches_for_test(&self) -> DFResult<Vec<RecordBatch>> {
        let (_, batches) = self.build_batches(None, None, None).await?;
        Ok(batches)
    }

    #[cfg(test)]
    pub async fn build_batches_at_version(&self, version: u64) -> DFResult<Vec<RecordBatch>> {
        let (_, batches) = self.build_batches(Some(version), None, None).await?;
        Ok(batches)
    }

    async fn load_snapshot(
        &self,
        as_of_version: Option<u64>,
    ) -> DFResult<(HashMap<Vec<u8>, i64>, u64)> {
        let total_start = Instant::now();
        let view = self.registry.get(&self.view_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "materialized view '{}' is not registered",
                self.view_name
            ))
        })?;

        if let Some((base_version, target_version, overlay)) =
            view.encoded_overlay_batches(as_of_version)
        {
            let mut snapshot = if let Some(state) = view.dbsp_state() {
                if target_version <= state.version() {
                    self.materialize_dbsp_rows(state, Some(target_version))
                        .await?
                } else {
                    self.materialize_dbsp_rows(state, Some(base_version))
                        .await?
                }
            } else {
                HashMap::new()
            };
            for (key, diff) in overlay {
                if diff == 0 {
                    continue;
                }
                let previous = snapshot.get(&key).copied().unwrap_or(0);
                let next = previous.saturating_add(diff);
                if next <= 0 {
                    snapshot.remove(&key);
                } else {
                    snapshot.insert(key, next);
                }
            }
            tracing::info!(
                view = %self.view_name,
                version = target_version,
                rows = snapshot.len(),
                storage = "overlay",
                total_ms = total_start.elapsed().as_millis() as u64,
                "materialized view loaded rows"
            );
            return Ok((snapshot, target_version));
        }

        let Some(state) = view.dbsp_state() else {
            tracing::warn!(
                view = %self.view_name,
                "materialized view has no DBSP state when loading rows"
            );
            return Ok((HashMap::new(), 0));
        };
        let target_version = as_of_version.unwrap_or(state.version());
        let snapshot = self
            .materialize_dbsp_rows(state, Some(target_version))
            .await?;
        tracing::info!(
            view = %self.view_name,
            version = target_version,
            rows = snapshot.len(),
            storage = "slatedb",
            total_ms = total_start.elapsed().as_millis() as u64,
            "materialized view loaded rows"
        );
        Ok((snapshot, target_version))
    }

    async fn materialize_dbsp_rows(
        &self,
        state: DbspPersistedState,
        as_of_version: Option<u64>,
    ) -> DFResult<HashMap<Vec<u8>, i64>> {
        let total_start = Instant::now();
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
        tracing::debug!(
            view = %self.view_name,
            version = target_version,
            snapshot_len = snapshot.len(),
            total_ms = total_start.elapsed().as_millis() as u64,
            "materialize dbsp rows"
        );
        Ok(snapshot)
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (as_of_version, _passthrough_filters) = extract_mv_version_filter(filters);
        let (projected_schema, batches) =
            self.build_batches(as_of_version, projection, limit).await?;
        Ok(Arc::new(SnapshotScanExec::new(projected_schema, batches)))
    }
}
