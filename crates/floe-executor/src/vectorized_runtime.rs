use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, Int64Array};
use datafusion::arrow::compute::{concat_batches, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use floe_core::source::{SourceDefinition, SourceRegistry};

use crate::delta_consolidation::{add_weight_column_to_batches, diff_snapshot_batches};
use crate::metrics;
use crate::mv::registry::MaterializedViewRegistry;
use crate::table_provider::DynamicStateTableProvider;

const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";
const INCREMENTAL_SNAPSHOT_MAX_BATCHES: usize = 256;
const INCREMENTAL_SNAPSHOT_COMPACT_TARGET_ROWS: usize = 65_536;

#[derive(Debug, Clone)]
pub struct VectorizedMaterializedViewPlan {
    view_name: String,
    query: String,
    output_schema: SchemaRef,
}

impl VectorizedMaterializedViewPlan {
    pub fn new(
        view_name: impl Into<String>,
        query: impl Into<String>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            view_name: view_name.into(),
            query: query.into(),
            output_schema,
        }
    }
}

#[derive(Clone)]
struct VectorizedSourceState {
    schema: SchemaRef,
    provider: Arc<DynamicStateTableProvider>,
    query_provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    query_alias_provider: Option<Arc<DynamicStateTableProvider>>,
    primary_key_columns: Vec<String>,
}

struct VectorizedMaterializedViewState {
    view_name: String,
    output_schema: SchemaRef,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    previous_snapshot: Vec<RecordBatch>,
    incremental: Option<IncrementalMaterializedViewState>,
}

struct IncrementalMaterializedViewState {
    source_name: String,
    ctx: SessionContext,
    source_provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
}

impl IncrementalMaterializedViewState {
    fn set_delta_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        self.source_provider.set_batches(batches.to_vec());
        if let (Some(alias_schema), Some(alias_provider)) =
            (self.alias_schema.as_ref(), self.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?);
        }
        Ok(())
    }

    fn clear_delta_batches(&self) {
        self.source_provider.set_batches(Vec::new());
        if let Some(alias_provider) = self.alias_provider.as_ref() {
            alias_provider.set_batches(Vec::new());
        }
    }
}

pub struct VectorizedExecutionRuntime {
    ctx: SessionContext,
    sources: HashMap<String, VectorizedSourceState>,
    materialized_views: Vec<VectorizedMaterializedViewState>,
    registry: Arc<MaterializedViewRegistry>,
    current_insert_batches: HashMap<String, Vec<RecordBatch>>,
    current_general_delta_sources: HashSet<String>,
}

impl VectorizedExecutionRuntime {
    pub async fn new(
        sources: &SourceRegistry,
        materialized_views: Vec<VectorizedMaterializedViewPlan>,
        registry: Arc<MaterializedViewRegistry>,
    ) -> Result<Self> {
        Self::new_with_udfs(sources, materialized_views, registry, Vec::new()).await
    }

    pub async fn new_with_udfs(
        sources: &SourceRegistry,
        materialized_views: Vec<VectorizedMaterializedViewPlan>,
        registry: Arc<MaterializedViewRegistry>,
        udfs: Vec<ScalarUDF>,
    ) -> Result<Self> {
        let ctx = SessionContext::new();
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let mut source_states = HashMap::new();

        for definition in sources.definitions() {
            let schema = definition.to_arrow_schema();
            let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
            let query_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
            ctx.register_table(
                definition.name(),
                Arc::clone(&provider) as Arc<dyn TableProvider>,
            )
            .with_context(|| format!("register vectorized source {}", definition.name()))?;

            let (alias_schema, alias_provider) =
                if let Some(alias) = definition.name().strip_prefix("nexmark_") {
                    let schema = camel_case_schema(definition);
                    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
                    ctx.register_table(alias, Arc::clone(&provider) as Arc<dyn TableProvider>)
                        .with_context(|| {
                            format!(
                                "register vectorized source alias {alias} for {}",
                                definition.name()
                            )
                        })?;
                    (Some(schema), Some(provider))
                } else {
                    (None, None)
                };
            let query_alias_provider = alias_schema
                .as_ref()
                .map(|schema| Arc::new(DynamicStateTableProvider::new(Arc::clone(schema))));

            source_states.insert(
                definition.name().to_string(),
                VectorizedSourceState {
                    schema,
                    provider,
                    query_provider,
                    alias_schema,
                    alias_provider,
                    query_alias_provider,
                    primary_key_columns: source_primary_key_columns(definition),
                },
            );
        }

        let mut mv_states = Vec::with_capacity(materialized_views.len());
        for mv in materialized_views {
            registry.set_schema(mv.view_name.clone(), Arc::clone(&mv.output_schema));
            let df = ctx
                .sql(&mv.query)
                .await
                .with_context(|| format!("plan vectorized SQL for {}", mv.view_name))?;
            let incremental_source = incremental_source_for_plan(df.logical_plan(), &source_states);
            let incremental = match incremental_source {
                Some(source_name) => Some(
                    build_incremental_materialized_view_state(
                        &mv.query,
                        &source_name,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build isolated incremental vectorized plan for {}",
                            mv.view_name
                        )
                    })?,
                ),
                None => None,
            };
            let plan = df
                .create_physical_plan()
                .await
                .with_context(|| format!("create vectorized physical plan for {}", mv.view_name))?;
            mv_states.push(VectorizedMaterializedViewState {
                view_name: mv.view_name,
                output_schema: mv.output_schema,
                plan,
                previous_snapshot: Vec::new(),
                incremental,
            });
        }

        Ok(Self {
            ctx,
            sources: source_states,
            materialized_views: mv_states,
            registry,
            current_insert_batches: HashMap::new(),
            current_general_delta_sources: HashSet::new(),
        })
    }

    pub fn table_providers(&self) -> Vec<(String, Arc<dyn TableProvider>)> {
        let mut providers = Vec::new();
        for (source_name, source) in &self.sources {
            providers.push((
                source_name.clone(),
                Arc::clone(&source.query_provider) as Arc<dyn TableProvider>,
            ));
            if let Some(alias) = source_name.strip_prefix("nexmark_")
                && let Some(alias_provider) = source.query_alias_provider.as_ref()
            {
                providers.push((
                    alias.to_string(),
                    Arc::clone(alias_provider) as Arc<dyn TableProvider>,
                ));
            }
        }
        providers
    }

    pub async fn append_source_batch(
        &mut self,
        source_name: &str,
        batch: RecordBatch,
    ) -> Result<()> {
        self.append_source_batches(source_name, vec![batch]).await
    }

    pub async fn append_source_batches(
        &mut self,
        source_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.append_source_batches_for_execution_and_query(source_name, batches.clone(), batches)
            .await
    }

    pub async fn append_source_batches_for_execution_and_query(
        &mut self,
        source_name: &str,
        execution_batches: Vec<RecordBatch>,
        query_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if execution_batches.is_empty() && query_batches.is_empty() {
            return Ok(());
        }
        let state = self
            .sources
            .get(source_name)
            .ok_or_else(|| anyhow!("unknown vectorized source '{source_name}'"))?
            .clone();
        for batch in execution_batches.iter().chain(query_batches.iter()) {
            if batch.schema().as_ref() != state.schema.as_ref() {
                bail!("source batch schema does not match source '{source_name}'");
            }
        }
        self.apply_insert_source_batches(source_name, &state, execution_batches, query_batches)
    }

    pub async fn apply_weighted_source_delta(
        &mut self,
        source_name: &str,
        delta: RecordBatch,
    ) -> Result<()> {
        let state = self
            .sources
            .get(source_name)
            .ok_or_else(|| anyhow!("unknown vectorized source '{source_name}'"))?
            .clone();
        if let Some(insert_batch) = insert_only_source_delta_batch(&state, &delta)? {
            return self.apply_insert_source_batches(
                source_name,
                &state,
                vec![insert_batch.clone()],
                vec![insert_batch],
            );
        }
        self.current_insert_batches.remove(source_name);
        self.current_general_delta_sources
            .insert(source_name.to_string());
        let next = apply_source_delta(&state, &state.provider, &delta)
            .await
            .with_context(|| format!("apply vectorized source delta for '{source_name}'"))?;
        let query_next = apply_source_delta(&state, &state.query_provider, &delta)
            .await
            .with_context(|| format!("apply query-visible source delta for '{source_name}'"))?;
        state.provider.set_batches(next.clone());
        state.query_provider.set_batches(query_next.clone());
        if let (Some(alias_schema), Some(alias_provider)) =
            (state.alias_schema.as_ref(), state.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(&next, alias_schema)?);
        }
        if let (Some(alias_schema), Some(alias_provider)) = (
            state.alias_schema.as_ref(),
            state.query_alias_provider.as_ref(),
        ) {
            alias_provider.set_batches(rename_batches(&query_next, alias_schema)?);
        }
        Ok(())
    }

    pub async fn run_tick(&mut self, version: i64) -> Result<()> {
        let ctx = &self.ctx;
        let registry = &self.registry;
        let insert_batches = &self.current_insert_batches;
        let general_delta_sources = &self.current_general_delta_sources;
        for mv in &mut self.materialized_views {
            if run_incremental_materialized_view_tick(
                registry,
                insert_batches,
                general_delta_sources,
                mv,
                version,
            )
            .await?
            {
                continue;
            }
            run_full_materialized_view_tick(ctx, registry, mv, version).await?;
        }
        self.current_insert_batches.clear();
        self.current_general_delta_sources.clear();
        Ok(())
    }

    fn apply_insert_source_batches(
        &mut self,
        source_name: &str,
        state: &VectorizedSourceState,
        execution_batches: Vec<RecordBatch>,
        query_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if execution_batches.is_empty() && query_batches.is_empty() {
            return Ok(());
        }
        state.provider.append_batches(execution_batches.clone());
        state.query_provider.append_batches(query_batches.clone());
        if let (Some(alias_schema), Some(alias_provider)) =
            (state.alias_schema.as_ref(), state.alias_provider.as_ref())
        {
            alias_provider.append_batches(rename_batches(&execution_batches, alias_schema)?);
        }
        if let (Some(alias_schema), Some(alias_provider)) = (
            state.alias_schema.as_ref(),
            state.query_alias_provider.as_ref(),
        ) {
            alias_provider.append_batches(rename_batches(&query_batches, alias_schema)?);
        }
        if !self.current_general_delta_sources.contains(source_name) {
            self.current_insert_batches
                .entry(source_name.to_string())
                .or_default()
                .extend(execution_batches);
        }
        Ok(())
    }
}

async fn build_incremental_materialized_view_state(
    query: &str,
    source_name: &str,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<IncrementalMaterializedViewState> {
    let source = sources
        .get(source_name)
        .ok_or_else(|| anyhow!("unknown vectorized source '{source_name}'"))?;
    let ctx = SessionContext::new();
    for udf in udfs.iter().cloned() {
        ctx.register_udf(udf);
    }

    let source_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&source.schema)));
    ctx.register_table(
        source_name,
        Arc::clone(&source_provider) as Arc<dyn TableProvider>,
    )
    .with_context(|| format!("register isolated incremental source {source_name}"))?;

    let (alias_schema, alias_provider) = if let (Some(alias), Some(alias_schema)) = (
        source_name.strip_prefix("nexmark_"),
        source.alias_schema.as_ref(),
    ) {
        let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(alias_schema)));
        ctx.register_table(alias, Arc::clone(&provider) as Arc<dyn TableProvider>)
            .with_context(|| {
                format!("register isolated incremental source alias {alias} for {source_name}")
            })?;
        (Some(Arc::clone(alias_schema)), Some(provider))
    } else {
        (None, None)
    };

    let df = ctx.sql(query).await?;
    let plan = df.create_physical_plan().await?;
    Ok(IncrementalMaterializedViewState {
        source_name: source_name.to_string(),
        ctx,
        source_provider,
        alias_schema,
        alias_provider,
        plan,
    })
}

async fn run_full_materialized_view_tick(
    ctx: &SessionContext,
    registry: &MaterializedViewRegistry,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<()> {
    let plan_start = Instant::now();
    let mut next_snapshot = collect(Arc::clone(&mv.plan), ctx.task_ctx())
        .await
        .with_context(|| format!("execute vectorized materialized view '{}'", mv.view_name))?;
    next_snapshot = normalize_batches(next_snapshot, &mv.output_schema)?;
    if next_snapshot.is_empty() {
        next_snapshot.push(RecordBatch::new_empty(Arc::clone(&mv.output_schema)));
    }

    let diff_start = Instant::now();
    let diff = diff_snapshot_batches(
        Arc::clone(&mv.output_schema),
        &mv.previous_snapshot,
        &next_snapshot,
    )
    .await
    .with_context(|| format!("diff vectorized snapshot for '{}'", mv.view_name))?;
    metrics::observe_delta_consolidation(diff.stats, diff_start.elapsed().as_millis() as u64);

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), diff.batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "full",
        "vectorized materialized view tick completed"
    );
    Ok(())
}

async fn run_incremental_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    general_delta_sources: &HashSet<String>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(incremental) = mv.incremental.as_ref() else {
        return Ok(false);
    };
    let source_name = incremental.source_name.as_str();
    if general_delta_sources.contains(source_name) {
        return Ok(false);
    }

    let plan_start = Instant::now();
    let delta = if let Some(source_batches) = insert_batches.get(source_name) {
        incremental.set_delta_batches(source_batches)?;
        let collected = collect(Arc::clone(&incremental.plan), incremental.ctx.task_ctx()).await;
        incremental.clear_delta_batches();
        normalize_batches(
            collected.with_context(|| {
                format!(
                    "execute incremental vectorized materialized view '{}'",
                    mv.view_name
                )
            })?,
            &mv.output_schema,
        )?
    } else {
        Vec::new()
    };

    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&mv.output_schema)?;
    let delta_batches = add_weight_column_to_batches(&delta, &weighted_schema, 1)?;
    let mut next_snapshot = mv
        .previous_snapshot
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .cloned()
        .collect::<Vec<_>>();
    next_snapshot.extend(delta.iter().filter(|batch| batch.num_rows() > 0).cloned());
    compact_incremental_snapshot_batches(&mv.output_schema, &mut next_snapshot)?;
    if next_snapshot.is_empty() {
        next_snapshot.push(RecordBatch::new_empty(Arc::clone(&mv.output_schema)));
    }

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "incremental_filter_project",
        "vectorized materialized view tick completed"
    );
    Ok(true)
}

fn compact_incremental_snapshot_batches(
    schema: &SchemaRef,
    batches: &mut Vec<RecordBatch>,
) -> Result<()> {
    if batches.len() <= INCREMENTAL_SNAPSHOT_MAX_BATCHES {
        return Ok(());
    }

    let mut compacted = Vec::new();
    let mut chunk = Vec::new();
    let mut chunk_rows = 0usize;
    for batch in batches.drain(..) {
        if batch.num_rows() == 0 {
            continue;
        }
        chunk_rows = chunk_rows.saturating_add(batch.num_rows());
        chunk.push(batch);
        if chunk_rows >= INCREMENTAL_SNAPSHOT_COMPACT_TARGET_ROWS {
            compacted.push(concat_snapshot_chunk(schema, &chunk)?);
            chunk.clear();
            chunk_rows = 0;
        }
    }
    if !chunk.is_empty() {
        compacted.push(concat_snapshot_chunk(schema, &chunk)?);
    }
    *batches = compacted;
    Ok(())
}

fn concat_snapshot_chunk(schema: &SchemaRef, chunk: &[RecordBatch]) -> Result<RecordBatch> {
    let refs = chunk.iter().collect::<Vec<_>>();
    concat_batches(schema, refs).context("compact incremental materialized view snapshot batches")
}

async fn apply_source_delta(
    state: &VectorizedSourceState,
    provider: &DynamicStateTableProvider,
    delta: &RecordBatch,
) -> Result<Vec<RecordBatch>> {
    let weight_idx = delta.schema().index_of(WEIGHT_COLUMN_NAME)?;
    if delta.schema().field(weight_idx).data_type() != &DataType::Int64 {
        bail!("source delta {} column must be Int64", WEIGHT_COLUMN_NAME);
    }
    let expected_delta_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&state.schema)?;
    if delta.schema().as_ref() != expected_delta_schema.as_ref() {
        bail!("source delta schema does not match source schema");
    }

    let old_snapshot = provider.snapshot();
    let delete_key_indices = delete_key_indices(state)?;
    let delete_keys = deleted_delta_keys(&state.schema, &delete_key_indices, delta, weight_idx)?;
    let mut next = if delete_keys.is_empty() {
        old_snapshot.iter().cloned().collect::<Vec<_>>()
    } else {
        filter_deleted_source_rows(
            &state.schema,
            &delete_key_indices,
            &delete_keys,
            &old_snapshot,
        )?
    };
    if let Some(positive_batch) = positive_delta_batch(&state.schema, delta, weight_idx)? {
        next.push(positive_batch);
    }
    Ok(next)
}

fn delete_key_indices(state: &VectorizedSourceState) -> Result<Vec<usize>> {
    if state.primary_key_columns.is_empty() {
        return Ok((0..state.schema.fields().len()).collect());
    }
    state
        .primary_key_columns
        .iter()
        .map(|column| {
            state.schema.index_of(column).with_context(|| {
                format!("source primary key column '{column}' missing from schema")
            })
        })
        .collect()
}

fn deleted_delta_keys(
    schema: &SchemaRef,
    key_indices: &[usize],
    delta: &RecordBatch,
    weight_idx: usize,
) -> Result<HashSet<OwnedRow>> {
    let weights = delta
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("source delta weight column must be Int64"))?;
    let converter = key_row_converter(schema, key_indices)?;
    let rows = converter
        .convert_columns(&project_columns(delta, key_indices))
        .context("encode source delete keys")?;
    let mut keys = HashSet::new();
    for row_idx in 0..weights.len() {
        if !weights.is_null(row_idx) && weights.value(row_idx) < 0 {
            keys.insert(rows.row(row_idx).owned());
        }
    }
    Ok(keys)
}

fn filter_deleted_source_rows(
    schema: &SchemaRef,
    key_indices: &[usize],
    delete_keys: &HashSet<OwnedRow>,
    snapshot: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    let converter = key_row_converter(schema, key_indices)?;
    let mut next = Vec::with_capacity(snapshot.len());
    for batch in snapshot {
        if batch.num_rows() == 0 {
            continue;
        }
        let rows = converter
            .convert_columns(&project_columns(batch, key_indices))
            .context("encode source state keys")?;
        let mut keep = BooleanBuilder::with_capacity(batch.num_rows());
        let mut kept_rows = 0usize;
        for row_idx in 0..batch.num_rows() {
            let keep_row = !delete_keys.contains(&rows.row(row_idx).owned());
            if keep_row {
                kept_rows = kept_rows.saturating_add(1);
            }
            keep.append_value(keep_row);
        }
        if kept_rows == batch.num_rows() {
            next.push(batch.clone());
        } else if kept_rows > 0 {
            next.push(filter_record_batch(batch, &keep.finish())?);
        }
    }
    Ok(next)
}

fn positive_delta_batch(
    schema: &SchemaRef,
    delta: &RecordBatch,
    weight_idx: usize,
) -> Result<Option<RecordBatch>> {
    let weights = delta
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("source delta weight column must be Int64"))?;
    let mut keep = BooleanBuilder::with_capacity(weights.len());
    let mut kept_rows = 0usize;
    for row_idx in 0..weights.len() {
        let keep_row = !weights.is_null(row_idx) && weights.value(row_idx) > 0;
        if keep_row {
            kept_rows = kept_rows.saturating_add(1);
        }
        keep.append_value(keep_row);
    }
    if kept_rows == 0 {
        return Ok(None);
    }
    let filtered = filter_record_batch(delta, &keep.finish())?;
    let columns = filtered
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| (idx != weight_idx).then_some(Arc::clone(column)))
        .collect::<Vec<_>>();
    Ok(Some(RecordBatch::try_new(Arc::clone(schema), columns)?))
}

fn key_row_converter(schema: &SchemaRef, key_indices: &[usize]) -> Result<RowConverter> {
    let fields = key_indices
        .iter()
        .map(|idx| SortField::new(schema.field(*idx).data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build Arrow row converter for source keys")
}

fn project_columns(batch: &RecordBatch, indices: &[usize]) -> Vec<ArrayRef> {
    indices
        .iter()
        .map(|idx| Arc::clone(batch.column(*idx)))
        .collect()
}

fn insert_only_source_delta_batch(
    state: &VectorizedSourceState,
    delta: &RecordBatch,
) -> Result<Option<RecordBatch>> {
    let weight_idx = delta.schema().index_of(WEIGHT_COLUMN_NAME)?;
    if delta.schema().field(weight_idx).data_type() != &DataType::Int64 {
        bail!("source delta {} column must be Int64", WEIGHT_COLUMN_NAME);
    }
    let expected_delta_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&state.schema)?;
    if delta.schema().as_ref() != expected_delta_schema.as_ref() {
        bail!("source delta schema does not match source schema");
    }

    let weights = delta
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("source delta weight column must be Int64"))?;
    for row_idx in 0..weights.len() {
        if weights.is_null(row_idx) || weights.value(row_idx) <= 0 {
            return Ok(None);
        }
    }

    let columns = delta
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| (idx != weight_idx).then_some(Arc::clone(column)))
        .collect::<Vec<_>>();
    Ok(Some(RecordBatch::try_new(
        Arc::clone(&state.schema),
        columns,
    )?))
}

fn normalize_batches(batches: Vec<RecordBatch>, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    batches
        .into_iter()
        .map(|batch| {
            if batch.schema().as_ref() == schema.as_ref() {
                return Ok(batch);
            }
            if batch.num_columns() != schema.fields().len() {
                bail!("RecordBatch column count does not match target schema");
            }
            let batch_schema = batch.schema();
            for (idx, field) in schema.fields().iter().enumerate() {
                let actual = batch_schema.field(idx);
                if actual.name() != field.name() || actual.data_type() != field.data_type() {
                    bail!("RecordBatch schema does not match target schema");
                }
            }
            Ok(RecordBatch::try_new(
                Arc::clone(schema),
                batch.columns().to_vec(),
            )?)
        })
        .collect()
}

fn rename_batches(batches: &[RecordBatch], schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            if batch.num_columns() != schema.fields().len() {
                bail!("alias schema column count does not match source batch");
            }
            Ok(RecordBatch::try_new(
                Arc::clone(schema),
                batch.columns().to_vec(),
            )?)
        })
        .collect()
}

pub fn weighted_batch_from_diffs(
    batch: &RecordBatch,
    weighted_schema: &SchemaRef,
    diffs: &[i64],
) -> Result<RecordBatch> {
    if batch.num_rows() != diffs.len() {
        bail!(
            "weighted source batch row count {} does not match diff count {}",
            batch.num_rows(),
            diffs.len()
        );
    }
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(diffs.to_vec())) as ArrayRef);
    Ok(RecordBatch::try_new(Arc::clone(weighted_schema), columns)?)
}

fn incremental_source_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    match plan {
        LogicalPlan::Projection(projection) => {
            incremental_source_for_plan(projection.input.as_ref(), sources)
        }
        LogicalPlan::Filter(filter) => incremental_source_for_plan(filter.input.as_ref(), sources),
        LogicalPlan::SubqueryAlias(alias) => {
            incremental_source_for_plan(alias.input.as_ref(), sources)
        }
        LogicalPlan::TableScan(scan) => resolve_source_table(scan.table_name.to_string(), sources),
        _ => None,
    }
}

fn resolve_source_table(
    table_name: String,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    if sources.contains_key(&table_name) {
        return Some(table_name);
    }
    sources
        .keys()
        .find(|source_name| source_name.strip_prefix("nexmark_") == Some(table_name.as_str()))
        .cloned()
}

fn source_primary_key_columns(definition: &SourceDefinition) -> Vec<String> {
    definition
        .property(SOURCE_PRIMARY_KEY_PROPERTY)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn camel_case_schema(definition: &SourceDefinition) -> SchemaRef {
    let fields = definition
        .columns()
        .iter()
        .map(|column| {
            Field::new(
                to_camel_case(column.name()),
                column.data_type().arrow_type(),
                true,
            )
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

fn to_camel_case(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut uppercase_next = false;
    for ch in input.chars() {
        if ch == '_' {
            uppercase_next = true;
            continue;
        }
        if uppercase_next {
            for upper in ch.to_uppercase() {
                out.push(upper);
            }
            uppercase_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;

    fn source_state(schema: SchemaRef) -> VectorizedSourceState {
        let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
        let query_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
        VectorizedSourceState {
            provider,
            query_provider,
            schema,
            alias_schema: None,
            alias_provider: None,
            query_alias_provider: None,
            primary_key_columns: Vec::new(),
        }
    }

    #[test]
    fn insert_only_delta_strips_weight_without_rebuilding_source_state() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
        let delta = RecordBatch::try_new(
            weighted_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .expect("weighted delta");

        let batch = insert_only_source_delta_batch(&source_state(Arc::clone(&schema)), &delta)
            .expect("detect insert-only")
            .expect("insert batch");

        assert_eq!(batch.schema().as_ref(), schema.as_ref());
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn delete_delta_uses_general_source_state_path() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
        let delta = RecordBatch::try_new(
            weighted_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![-1])),
            ],
        )
        .expect("weighted delta");

        assert!(
            insert_only_source_delta_batch(&source_state(schema), &delta)
                .expect("inspect delta")
                .is_none()
        );
    }
}
