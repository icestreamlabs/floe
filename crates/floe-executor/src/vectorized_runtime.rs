use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use datafusion::physical_plan::collect;
use floe_core::source::{SourceDefinition, SourceRegistry};

use crate::delta_consolidation::{
    DeltaConsolidator, add_weight_column_to_batches, diff_snapshot_batches,
};
use crate::metrics;
use crate::mv::registry::MaterializedViewRegistry;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_source_delta::{
    apply_source_delta, apply_weighted_snapshot_delta, insert_only_source_delta_batch,
    prepare_source_delta, unit_source_delta_batches, validate_unit_source_delta,
};
use source_state::{
    camel_case_schema, dynamic_state_provider, incremental_source_for_plan, rename_batches,
    source_key_indices, source_primary_key_columns,
};

const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";
const INCREMENTAL_SNAPSHOT_MAX_BATCHES: usize = 256;
const INCREMENTAL_SNAPSHOT_COMPACT_TARGET_ROWS: usize = 65_536;

mod source_state;

#[derive(Debug, Clone)]
pub struct VectorizedMaterializedViewPlan {
    view_name: String,
    query: String,
    output_schema: SchemaRef,
    execution_policy: VectorizedMaterializedViewExecutionPolicy,
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
            execution_policy: VectorizedMaterializedViewExecutionPolicy::IncrementalOnly,
        }
    }

    pub fn allow_full_refresh(mut self) -> Self {
        self.execution_policy = VectorizedMaterializedViewExecutionPolicy::AllowFullRefresh;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorizedMaterializedViewExecutionPolicy {
    IncrementalOnly,
    AllowFullRefresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VectorizedExecutionRuntimeOptions {
    pub maintain_source_query_tables: bool,
}

impl VectorizedExecutionRuntimeOptions {
    pub fn with_source_query_tables(mut self) -> Self {
        self.maintain_source_query_tables = true;
        self
    }
}

#[derive(Clone)]
struct VectorizedSourceState {
    schema: SchemaRef,
    provider: Arc<DynamicStateTableProvider>,
    query_provider: Option<Arc<DynamicStateTableProvider>>,
    maintain_execution_state: bool,
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
    execution_mode: MaterializedViewExecutionMode,
}

struct IncrementalMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    ctx: SessionContext,
    source_provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializedViewExecutionMode {
    IncrementalFilterProject,
    FullRefresh,
}

impl IncrementalMaterializedViewState {
    fn set_delta_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        self.source_provider.set_batches(batches.to_vec())?;
        if let (Some(alias_schema), Some(alias_provider)) =
            (self.alias_schema.as_ref(), self.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
        }
        Ok(())
    }

    fn clear_delta_batches(&self) -> Result<()> {
        self.source_provider.set_batches(Vec::new())?;
        if let Some(alias_provider) = self.alias_provider.as_ref() {
            alias_provider.set_batches(Vec::new())?;
        }
        Ok(())
    }
}

pub struct VectorizedExecutionRuntime {
    ctx: SessionContext,
    sources: HashMap<String, VectorizedSourceState>,
    materialized_views: Vec<VectorizedMaterializedViewState>,
    registry: Arc<MaterializedViewRegistry>,
    current_insert_batches: HashMap<String, Vec<RecordBatch>>,
    current_weighted_delta_batches: HashMap<String, Vec<RecordBatch>>,
}

impl VectorizedExecutionRuntime {
    pub async fn new(
        sources: &SourceRegistry,
        materialized_views: Vec<VectorizedMaterializedViewPlan>,
        registry: Arc<MaterializedViewRegistry>,
    ) -> Result<Self> {
        Self::new_with_options(
            sources,
            materialized_views,
            registry,
            VectorizedExecutionRuntimeOptions::default(),
        )
        .await
    }

    pub async fn new_with_options(
        sources: &SourceRegistry,
        materialized_views: Vec<VectorizedMaterializedViewPlan>,
        registry: Arc<MaterializedViewRegistry>,
        options: VectorizedExecutionRuntimeOptions,
    ) -> Result<Self> {
        Self::new_with_udfs_and_options(sources, materialized_views, registry, Vec::new(), options)
            .await
    }

    pub async fn new_with_udfs(
        sources: &SourceRegistry,
        materialized_views: Vec<VectorizedMaterializedViewPlan>,
        registry: Arc<MaterializedViewRegistry>,
        udfs: Vec<ScalarUDF>,
    ) -> Result<Self> {
        Self::new_with_udfs_and_options(
            sources,
            materialized_views,
            registry,
            udfs,
            VectorizedExecutionRuntimeOptions::default(),
        )
        .await
    }

    pub async fn new_with_udfs_and_options(
        sources: &SourceRegistry,
        materialized_views: Vec<VectorizedMaterializedViewPlan>,
        registry: Arc<MaterializedViewRegistry>,
        udfs: Vec<ScalarUDF>,
        options: VectorizedExecutionRuntimeOptions,
    ) -> Result<Self> {
        let ctx = SessionContext::new();
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let mut source_states = HashMap::new();

        for definition in sources.definitions() {
            let schema = definition.to_arrow_schema();
            let primary_key_columns = source_primary_key_columns(definition);
            let key_indices = source_key_indices(&schema, &primary_key_columns)?;
            let provider = Arc::new(dynamic_state_provider(
                Arc::clone(&schema),
                key_indices.as_deref(),
            )?);
            let query_provider = options
                .maintain_source_query_tables
                .then(|| dynamic_state_provider(Arc::clone(&schema), key_indices.as_deref()))
                .transpose()?
                .map(Arc::new);
            ctx.register_table(
                definition.name(),
                Arc::clone(&provider) as Arc<dyn TableProvider>,
            )
            .with_context(|| format!("register vectorized source {}", definition.name()))?;

            let (alias_schema, alias_provider) =
                if let Some(alias) = definition.name().strip_prefix("nexmark_") {
                    let schema = camel_case_schema(definition);
                    let provider = Arc::new(dynamic_state_provider(
                        Arc::clone(&schema),
                        key_indices.as_deref(),
                    )?);
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
            let query_alias_provider = if options.maintain_source_query_tables {
                alias_schema
                    .as_ref()
                    .map(|schema| {
                        dynamic_state_provider(Arc::clone(schema), key_indices.as_deref())
                            .map(Arc::new)
                    })
                    .transpose()?
            } else {
                None
            };

            source_states.insert(
                definition.name().to_string(),
                VectorizedSourceState {
                    schema,
                    provider,
                    query_provider,
                    maintain_execution_state: false,
                    alias_schema,
                    alias_provider,
                    query_alias_provider,
                    primary_key_columns,
                },
            );
        }

        let mut mv_states = Vec::with_capacity(materialized_views.len());
        let mut requires_full_refresh_execution_state = false;
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
            if incremental.is_none()
                && mv.execution_policy == VectorizedMaterializedViewExecutionPolicy::IncrementalOnly
            {
                bail!(
                    "materialized view '{}' requires full-refresh vectorized execution; only filter/project source plans are currently incremental",
                    mv.view_name
                );
            }
            let execution_mode = if incremental.is_some() {
                MaterializedViewExecutionMode::IncrementalFilterProject
            } else {
                requires_full_refresh_execution_state = true;
                MaterializedViewExecutionMode::FullRefresh
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
                execution_mode,
            });
        }
        if requires_full_refresh_execution_state {
            for source in source_states.values_mut() {
                source.maintain_execution_state = true;
            }
        }

        Ok(Self {
            ctx,
            sources: source_states,
            materialized_views: mv_states,
            registry,
            current_insert_batches: HashMap::new(),
            current_weighted_delta_batches: HashMap::new(),
        })
    }

    pub fn table_providers(&self) -> Vec<(String, Arc<dyn TableProvider>)> {
        let mut providers = Vec::new();
        for (source_name, source) in &self.sources {
            if let Some(query_provider) = source.query_provider.as_ref() {
                providers.push((
                    source_name.clone(),
                    Arc::clone(query_provider) as Arc<dyn TableProvider>,
                ));
            }
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
        validate_unit_source_delta(&state.schema, &delta)
            .with_context(|| format!("validate weighted source delta for '{source_name}'"))?;
        if let Some(insert_batch) = insert_only_source_delta_batch(&state.schema, &delta)? {
            return self.apply_insert_source_batches(
                source_name,
                &state,
                vec![insert_batch.clone()],
                vec![insert_batch],
            );
        }
        let weighted_batches = self
            .current_weighted_delta_batches
            .entry(source_name.to_string())
            .or_default();
        if let Some(pending_inserts) = self.current_insert_batches.remove(source_name) {
            let weighted_schema =
                crate::delta_consolidation::weighted_snapshot_schema(&state.schema)?;
            weighted_batches.extend(add_weight_column_to_batches(
                &pending_inserts,
                &weighted_schema,
                1,
            )?);
        }
        weighted_batches.push(delta.clone());

        if state.primary_key_columns.is_empty() {
            if let Some(query_provider) = state.query_provider.as_ref() {
                let query_next = apply_source_delta(
                    &state.schema,
                    &state.primary_key_columns,
                    query_provider,
                    &delta,
                )
                .await
                .with_context(|| format!("apply query-visible source delta for '{source_name}'"))?;
                query_provider.set_batches(query_next.clone())?;
                if let (Some(alias_schema), Some(alias_provider)) = (
                    state.alias_schema.as_ref(),
                    state.query_alias_provider.as_ref(),
                ) {
                    alias_provider.set_batches(rename_batches(&query_next, alias_schema)?)?;
                }
            }
            if state.maintain_execution_state {
                let next = apply_source_delta(
                    &state.schema,
                    &state.primary_key_columns,
                    &state.provider,
                    &delta,
                )
                .await
                .with_context(|| format!("apply vectorized source delta for '{source_name}'"))?;
                state.provider.set_batches(next.clone())?;
                if let (Some(alias_schema), Some(alias_provider)) =
                    (state.alias_schema.as_ref(), state.alias_provider.as_ref())
                {
                    alias_provider.set_batches(rename_batches(&next, alias_schema)?)?;
                }
            }
            return Ok(());
        }

        let update = prepare_source_delta(&state.schema, &state.primary_key_columns, &delta)
            .with_context(|| format!("prepare keyed source delta for '{source_name}'"))?;
        let positive_batches = update
            .final_positive_batch
            .as_ref()
            .map(|batch| vec![batch.clone()])
            .unwrap_or_default();
        if let Some(query_provider) = state.query_provider.as_ref() {
            query_provider.apply_keyed_delta(&update.touched_keys, positive_batches.clone())?;
        }
        if state.maintain_execution_state {
            state
                .provider
                .apply_keyed_delta(&update.touched_keys, positive_batches.clone())?;
            if let (Some(alias_schema), Some(alias_provider)) =
                (state.alias_schema.as_ref(), state.alias_provider.as_ref())
            {
                alias_provider.apply_keyed_delta(
                    &update.touched_keys,
                    rename_batches(&positive_batches, alias_schema)?,
                )?;
            }
        }
        if let (Some(alias_schema), Some(alias_provider)) = (
            state.alias_schema.as_ref(),
            state.query_alias_provider.as_ref(),
        ) {
            alias_provider.apply_keyed_delta(
                &update.touched_keys,
                rename_batches(&positive_batches, alias_schema)?,
            )?;
        }
        Ok(())
    }

    pub async fn run_tick(&mut self, version: i64) -> Result<()> {
        let ctx = &self.ctx;
        let registry = &self.registry;
        let insert_batches = &self.current_insert_batches;
        let weighted_delta_batches = &self.current_weighted_delta_batches;
        for mv in &mut self.materialized_views {
            if run_incremental_materialized_view_tick(
                registry,
                insert_batches,
                weighted_delta_batches,
                mv,
                version,
            )
            .await?
            {
                continue;
            }
            run_full_refresh_materialized_view_tick(ctx, registry, mv, version).await?;
        }
        self.current_insert_batches.clear();
        self.current_weighted_delta_batches.clear();
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
        if state.maintain_execution_state {
            state.provider.append_batches(execution_batches.clone())?;
        }
        if let Some(query_provider) = state.query_provider.as_ref() {
            query_provider.append_batches(query_batches.clone())?;
        }
        if state.maintain_execution_state
            && let (Some(alias_schema), Some(alias_provider)) =
                (state.alias_schema.as_ref(), state.alias_provider.as_ref())
        {
            alias_provider.append_batches(rename_batches(&execution_batches, alias_schema)?)?;
        }
        if let (Some(alias_schema), Some(alias_provider)) = (
            state.alias_schema.as_ref(),
            state.query_alias_provider.as_ref(),
        ) {
            alias_provider.append_batches(rename_batches(&query_batches, alias_schema)?)?;
        }
        if let Some(weighted_batches) = self.current_weighted_delta_batches.get_mut(source_name) {
            let weighted_schema =
                crate::delta_consolidation::weighted_snapshot_schema(&state.schema)?;
            weighted_batches.extend(add_weight_column_to_batches(
                &execution_batches,
                &weighted_schema,
                1,
            )?);
        } else {
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
        source_schema: Arc::clone(&source.schema),
        ctx,
        source_provider,
        alias_schema,
        alias_provider,
        plan,
    })
}

async fn run_full_refresh_materialized_view_tick(
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
    let snapshot_rows = next_snapshot
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();

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
    let total_ms = plan_start.elapsed().as_millis() as u64;
    metrics::observe_full_mv_refresh_tick(snapshot_rows, total_ms);
    tracing::warn!(
        view = %mv.view_name,
        version,
        rows = snapshot_rows,
        total_ms,
        mode = ?mv.execution_mode,
        "vectorized materialized view full-refresh tick completed"
    );
    Ok(())
}

async fn run_incremental_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(incremental) = mv.incremental.as_ref() else {
        return Ok(false);
    };
    let source_name = incremental.source_name.as_str();
    if let Some(weighted_source_batches) = weighted_delta_batches.get(source_name) {
        return run_signed_incremental_materialized_view_tick(
            registry,
            weighted_source_batches,
            mv,
            version,
        )
        .await;
    }

    let plan_start = Instant::now();
    let delta = if let Some(source_batches) = insert_batches.get(source_name) {
        incremental.set_delta_batches(source_batches)?;
        let collected = collect(Arc::clone(&incremental.plan), incremental.ctx.task_ctx()).await;
        incremental.clear_delta_batches()?;
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

async fn run_signed_incremental_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    weighted_source_batches: &[RecordBatch],
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(incremental) = mv.incremental.as_ref() else {
        return Ok(false);
    };
    let plan_start = Instant::now();
    let mut positive_source_batches = Vec::new();
    let mut negative_source_batches = Vec::new();
    for batch in weighted_source_batches {
        let unit_delta = unit_source_delta_batches(&incremental.source_schema, batch)?
            .with_context(|| {
                format!(
                    "incremental vectorized materialized view '{}' received non-unit weighted source deltas",
                    mv.view_name
                )
            })?;
        positive_source_batches.extend(unit_delta.positive);
        negative_source_batches.extend(unit_delta.negative);
    }

    let mut output_delta_batches = Vec::new();
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&mv.output_schema)?;
    let positive_output =
        collect_incremental_output(incremental, &positive_source_batches, &mv.output_schema)
            .await?;
    output_delta_batches.extend(add_weight_column_to_batches(
        &positive_output,
        &weighted_schema,
        1,
    )?);
    let negative_output =
        collect_incremental_output(incremental, &negative_source_batches, &mv.output_schema)
            .await?;
    output_delta_batches.extend(add_weight_column_to_batches(
        &negative_output,
        &weighted_schema,
        -1,
    )?);

    let diff_start = Instant::now();
    let consolidated = DeltaConsolidator::new(weighted_schema.clone())?
        .consolidate_with_stats(output_delta_batches)
        .await?;
    metrics::observe_delta_consolidation(
        consolidated.stats,
        diff_start.elapsed().as_millis() as u64,
    );
    let delta_batches = consolidated.batches;
    let next_snapshot = apply_weighted_snapshot_delta(
        &mv.output_schema,
        &mv.previous_snapshot,
        delta_batches.clone(),
    )
    .await
    .with_context(|| {
        format!(
            "apply signed incremental snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "incremental_filter_project_signed",
        "vectorized materialized view tick completed"
    );
    Ok(true)
}

async fn collect_incremental_output(
    incremental: &IncrementalMaterializedViewState,
    source_batches: &[RecordBatch],
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() {
        return Ok(Vec::new());
    }
    incremental.set_delta_batches(source_batches)?;
    let collected = collect(Arc::clone(&incremental.plan), incremental.ctx.task_ctx()).await;
    incremental.clear_delta_batches()?;
    normalize_batches(
        collected.context("execute signed incremental vectorized materialized view")?,
        output_schema,
    )
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
    for diff in diffs {
        match *diff {
            -1..=1 => {}
            other => bail!("weighted source batch diff must be -1, 0, or 1, got {other}"),
        }
    }
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(diffs.to_vec())) as ArrayRef);
    Ok(RecordBatch::try_new(Arc::clone(weighted_schema), columns)?)
}

#[cfg(test)]
mod tests;
