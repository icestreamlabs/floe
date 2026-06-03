use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::mv::registry::MaterializedViewRegistry;

use super::MV_VERSION_COLUMN;
use super::SnapshotScanExec;
use super::filters::{extract_mv_version_filter, parse_mv_version_expr};
use super::helpers::{
    append_mv_version_field, build_batches_from_arrow_snapshot,
    build_constant_u64_projection_batches,
};

#[derive(Clone)]
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
        let (projected_schema, projected_indices) =
            super::helpers::project_schema(&self.schema, projection)?;
        let mv_version_index = self
            .schema
            .fields()
            .iter()
            .position(|field| field.name() == MV_VERSION_COLUMN);
        let fast_count_eligible = projected_indices.is_empty()
            || mv_version_index.is_some_and(|index| {
                !projected_indices.is_empty() && projected_indices.iter().all(|idx| *idx == index)
            });
        if fast_count_eligible
            && let Some((row_count, version)) = self.fast_count_batches(as_of_version, limit)?
        {
            if projected_indices.is_empty() {
                let options = datafusion::arrow::record_batch::RecordBatchOptions::new()
                    .with_row_count(Some(row_count));
                let batch = datafusion::arrow::record_batch::RecordBatch::try_new_with_options(
                    Arc::clone(&projected_schema),
                    vec![],
                    &options,
                )
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
                return Ok((projected_schema, vec![batch]));
            }
            let batches = build_constant_u64_projection_batches(
                Arc::clone(&projected_schema),
                version,
                row_count,
            )?;
            return Ok((projected_schema, batches));
        }
        if let Some((snapshot, version)) = self.load_arrow_snapshot(as_of_version)? {
            return build_batches_from_arrow_snapshot(
                snapshot,
                Arc::clone(&self.schema),
                projection,
                limit,
                version,
            );
        }
        build_batches_from_arrow_snapshot(
            Arc::new(Vec::new()),
            Arc::clone(&self.schema),
            projection,
            limit,
            as_of_version.unwrap_or(0),
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

    fn load_arrow_snapshot(
        &self,
        as_of_version: Option<u64>,
    ) -> DFResult<Option<(Arc<Vec<datafusion::arrow::record_batch::RecordBatch>>, u64)>> {
        let Some(view) = self.registry.get(&self.view_name) else {
            return Ok(None);
        };
        let latest_visible_version = view
            .latest_version()
            .and_then(|version| u64::try_from(version).ok());
        let target_version = as_of_version.or(latest_visible_version).unwrap_or(0);
        let Some(snapshot) = i64::try_from(target_version)
            .ok()
            .and_then(|version| view.arrow_snapshot_for(version))
        else {
            return Ok(None);
        };
        tracing::info!(
            view = %self.view_name,
            version = target_version,
            rows = snapshot.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            storage = "arrow_snapshot",
            "materialized view loaded rows"
        );
        Ok(Some((snapshot, target_version)))
    }

    fn fast_count_batches(
        &self,
        as_of_version: Option<u64>,
        limit: Option<usize>,
    ) -> DFResult<Option<(usize, u64)>> {
        let Some(view) = self.registry.get(&self.view_name) else {
            return Ok(Some((0, as_of_version.unwrap_or(0))));
        };
        let latest_version = view
            .latest_version()
            .and_then(|version| u64::try_from(version).ok());
        let target_version = as_of_version.or(latest_version).unwrap_or(0);
        if as_of_version.is_some() && latest_version != Some(target_version) {
            return Ok(None);
        }
        if let Some(row_count) = i64::try_from(target_version)
            .ok()
            .and_then(|version| view.arrow_row_count_for(version))
        {
            let row_count = limit.map(|limit| row_count.min(limit)).unwrap_or(row_count);
            tracing::info!(
                view = %self.view_name,
                version = target_version,
                rows = row_count,
                storage = "arrow_snapshot_cached_count",
                "materialized view loaded rows"
            );
            return Ok(Some((row_count, target_version)));
        }
        Ok(Some((0, target_version)))
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
        let mut pushed_version = None;
        let mut pushdown = Vec::with_capacity(filters.len());
        for expr in filters {
            let filter_pushdown = match parse_mv_version_expr(expr) {
                Some(version)
                    if pushed_version
                        .map(|pushed| pushed == version)
                        .unwrap_or(true) =>
                {
                    pushed_version = Some(version);
                    TableProviderFilterPushDown::Exact
                }
                Some(_) | None => TableProviderFilterPushDown::Unsupported,
            };
            pushdown.push(filter_pushdown);
        }
        Ok(pushdown)
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
