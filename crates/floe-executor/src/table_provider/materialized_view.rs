use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::Context;
use datafusion::arrow::array::{Array, Int64Array};
#[cfg(test)]
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};

use crate::encoded_batch::{encoded_snapshot_row_count, encoded_snapshot_to_arrow_batches};
use crate::mv::registry::MaterializedViewRegistry;
use crate::mv::runtime::MaterializedView;
use crate::scalar_array_builder::ScalarColumnBuilder;

use super::MV_VERSION_COLUMN;
use super::SnapshotScanExec;
use super::filters::{extract_mv_version_filter, parse_mv_version_expr};
use super::helpers::{
    append_mv_version_field, build_batches_from_arrow_snapshot,
    build_constant_u64_projection_batches,
};

const COLUMNAR_SCAN_BATCH_ROW_LIMIT: usize = 4096;

#[derive(Clone)]
pub struct MaterializedViewTableProvider {
    registry: Arc<MaterializedViewRegistry>,
    view_name: String,
    schema: datafusion::arrow::datatypes::SchemaRef,
    data_schema: datafusion::arrow::datatypes::SchemaRef,
    has_virtual_mv_version: bool,
}

impl MaterializedViewTableProvider {
    pub fn new(
        registry: Arc<MaterializedViewRegistry>,
        view_name: impl Into<String>,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        let has_virtual_mv_version = !schema
            .fields()
            .iter()
            .any(|field| field.name() == MV_VERSION_COLUMN);
        let schema_with_meta = if has_virtual_mv_version {
            append_mv_version_field(&schema)
        } else {
            Arc::clone(&schema)
        };
        Self {
            registry,
            view_name: view_name.into(),
            schema: schema_with_meta,
            data_schema: schema,
            has_virtual_mv_version,
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
            .has_virtual_mv_version
            .then(|| {
                self.schema
                    .fields()
                    .iter()
                    .position(|field| field.name() == MV_VERSION_COLUMN)
            })
            .flatten();
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
                mv_version_index,
            );
        }
        if let Some((snapshot, version)) = self.load_columnar_snapshot(as_of_version, limit).await?
        {
            return build_batches_from_arrow_snapshot(
                snapshot,
                Arc::clone(&self.schema),
                projection,
                limit,
                version,
                mv_version_index,
            );
        }
        if let Some((snapshot, version)) = self.load_encoded_snapshot(as_of_version, limit).await? {
            return build_batches_from_arrow_snapshot(
                snapshot,
                Arc::clone(&self.schema),
                projection,
                limit,
                version,
                mv_version_index,
            );
        }
        build_batches_from_arrow_snapshot(
            Arc::new(Vec::new()),
            Arc::clone(&self.schema),
            projection,
            limit,
            as_of_version.unwrap_or(0),
            mv_version_index,
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

    async fn load_columnar_snapshot(
        &self,
        as_of_version: Option<u64>,
        limit: Option<usize>,
    ) -> DFResult<Option<(Arc<Vec<datafusion::arrow::record_batch::RecordBatch>>, u64)>> {
        let Some(view) = self.registry.get(&self.view_name) else {
            return Ok(None);
        };
        let latest_visible_version = view
            .latest_version()
            .and_then(|version| u64::try_from(version).ok());
        let Some(target_version) = as_of_version.or(latest_visible_version) else {
            return Ok(None);
        };
        let target_version_i64 = i64::try_from(target_version)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let Some(handle) = view.handle_for_version(target_version_i64) else {
            return Ok(None);
        };
        let Some(storage) = view.columnar_storage() else {
            return Ok(None);
        };
        if storage.schema().as_ref() != self.data_schema.as_ref() {
            return Err(DataFusionError::Execution(format!(
                "columnar materialized view schema for '{}' does not match table provider schema",
                self.view_name
            )));
        }

        let zset = SlateBackedColumnarZSet::new(
            storage.table(),
            handle.ns.clone(),
            Arc::clone(&self.data_schema),
        )
        .await
        .map_err(super::helpers::to_datafusion_error)?;
        let materialized = zset
            .materialize_columnar_version(handle.version)
            .await
            .map_err(super::helpers::to_datafusion_error)?;
        let row_count = columnar_zset_positive_row_count(&materialized)
            .map_err(super::helpers::to_datafusion_error)?;
        view.seed_authoritative_row_count_if_latest(target_version, row_count);
        let batches =
            columnar_zset_to_arrow_snapshot(&materialized, Arc::clone(&self.data_schema), limit)
                .map_err(super::helpers::to_datafusion_error)?;
        tracing::info!(
            view = %self.view_name,
            version = target_version,
            rows = row_count.min(limit.unwrap_or(usize::MAX)),
            storage = "columnar_zset",
            "materialized view loaded rows"
        );
        Ok(Some((Arc::new(batches), target_version)))
    }

    async fn load_encoded_snapshot(
        &self,
        as_of_version: Option<u64>,
        limit: Option<usize>,
    ) -> DFResult<Option<(Arc<Vec<datafusion::arrow::record_batch::RecordBatch>>, u64)>> {
        let Some(view) = self.registry.get(&self.view_name) else {
            return Ok(None);
        };
        let latest_visible_version = view
            .latest_version()
            .and_then(|version| u64::try_from(version).ok());
        let Some(target_version) = as_of_version.or(latest_visible_version) else {
            return Ok(None);
        };
        let target_version_i64 = i64::try_from(target_version)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let snapshot = view
            .snapshot_for(target_version_i64)
            .await
            .map_err(super::helpers::to_datafusion_error)?;
        let row_count = encoded_snapshot_row_count(&snapshot);
        let batches = encoded_snapshot_to_arrow_batches(&snapshot, self.data_schema(), limit)
            .map_err(super::helpers::to_datafusion_error)?;
        view.seed_authoritative_row_count_if_latest(target_version, row_count);
        tracing::info!(
            view = %self.view_name,
            version = target_version,
            rows = row_count.min(limit.unwrap_or(usize::MAX)),
            storage = "encoded_snapshot",
            "materialized view loaded rows"
        );
        Ok(Some((Arc::new(batches), target_version)))
    }

    fn data_schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        Arc::clone(&self.data_schema)
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
        if let Some(row_count) = view.authoritative_row_count_for(target_version) {
            let row_count = limit.map(|limit| row_count.min(limit)).unwrap_or(row_count);
            tracing::info!(
                view = %self.view_name,
                version = target_version,
                rows = row_count,
                storage = "authoritative_row_count",
                "materialized view loaded rows"
            );
            return Ok(Some((row_count, target_version)));
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
        Ok(None)
    }
}

fn columnar_zset_positive_row_count(zset: &ColumnarZSet) -> anyhow::Result<usize> {
    let mut row_count = 0usize;
    for batch in zset.batches() {
        let weights = batch
            .column(zset.value_column_count())
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..weights.len() {
            let weight = weights.value(row_idx);
            if weight < 0 {
                anyhow::bail!("columnar zset materialized snapshot contains negative weight");
            }
            row_count = row_count.saturating_add(
                usize::try_from(weight).context("columnar zset row weight exceeds usize")?,
            );
        }
    }
    Ok(row_count)
}

fn columnar_zset_to_arrow_snapshot(
    zset: &ColumnarZSet,
    schema: datafusion::arrow::datatypes::SchemaRef,
    limit: Option<usize>,
) -> anyhow::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    let mut output = Vec::new();
    let mut builders = snapshot_output_builders(&schema)?;
    let mut buffered_rows = 0usize;
    let mut emitted_rows = 0usize;
    let max_rows = limit.unwrap_or(usize::MAX);

    'batches: for batch in zset.batches() {
        if batch.num_rows() == 0 {
            continue;
        }
        let weights = batch
            .column(zset.value_column_count())
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            let weight = weights.value(row_idx);
            if weight < 0 {
                anyhow::bail!("columnar zset materialized snapshot contains negative weight");
            }
            let repeat =
                usize::try_from(weight).context("columnar zset row weight exceeds usize")?;
            for _ in 0..repeat {
                if emitted_rows == max_rows {
                    break 'batches;
                }
                append_snapshot_row(&mut builders, batch, row_idx, zset.value_column_count())?;
                buffered_rows = buffered_rows.saturating_add(1);
                emitted_rows = emitted_rows.saturating_add(1);
                if buffered_rows == COLUMNAR_SCAN_BATCH_ROW_LIMIT {
                    output.push(finish_snapshot_batch(&schema, &mut builders)?);
                    buffered_rows = 0;
                }
            }
        }
    }

    if buffered_rows > 0 {
        output.push(finish_snapshot_batch(&schema, &mut builders)?);
    }
    if output.is_empty() {
        output.push(datafusion::arrow::record_batch::RecordBatch::new_empty(
            schema,
        ));
    }
    Ok(output)
}

fn snapshot_output_builders(
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> anyhow::Result<Vec<ScalarColumnBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), COLUMNAR_SCAN_BATCH_ROW_LIMIT))
        .collect()
}

fn append_snapshot_row(
    builders: &mut [ScalarColumnBuilder],
    batch: &datafusion::arrow::record_batch::RecordBatch,
    row_idx: usize,
    value_column_count: usize,
) -> anyhow::Result<()> {
    for column_idx in 0..value_column_count {
        builders[column_idx].append_array_value(batch.column(column_idx).as_ref(), row_idx)?;
    }
    Ok(())
}

fn finish_snapshot_batch(
    schema: &datafusion::arrow::datatypes::SchemaRef,
    builders: &mut [ScalarColumnBuilder],
) -> anyhow::Result<datafusion::arrow::record_batch::RecordBatch> {
    let arrays = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    Ok(datafusion::arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        arrays,
    )?)
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
        if !self.has_virtual_mv_version {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }
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
        let (as_of_version, _passthrough_filters) = if self.has_virtual_mv_version {
            extract_mv_version_filter(filters)
        } else {
            (None, Vec::new())
        };
        let (projected_schema, batches) =
            self.build_batches(as_of_version, projection, limit).await?;
        Ok(Arc::new(SnapshotScanExec::new(projected_schema, batches)))
    }
}
