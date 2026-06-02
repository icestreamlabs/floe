use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use floe_core::source::{SourceDefinition, SourceRegistry};

use crate::delta_consolidation::{add_weight_column, diff_snapshot_batches};
use crate::metrics;
use crate::mv::registry::MaterializedViewRegistry;
use crate::table_provider::DynamicStateTableProvider;

const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";

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
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    primary_key_columns: Vec<String>,
}

struct VectorizedMaterializedViewState {
    view_name: String,
    output_schema: SchemaRef,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    previous_snapshot: Vec<RecordBatch>,
}

pub struct VectorizedExecutionRuntime {
    ctx: SessionContext,
    sources: HashMap<String, VectorizedSourceState>,
    materialized_views: Vec<VectorizedMaterializedViewState>,
    registry: Arc<MaterializedViewRegistry>,
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
        for udf in udfs {
            ctx.register_udf(udf);
        }
        let mut source_states = HashMap::new();

        for definition in sources.definitions() {
            let schema = definition.to_arrow_schema();
            let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
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

            source_states.insert(
                definition.name().to_string(),
                VectorizedSourceState {
                    schema,
                    provider,
                    alias_schema,
                    alias_provider,
                    primary_key_columns: source_primary_key_columns(definition),
                },
            );
        }

        let mut mv_states = Vec::with_capacity(materialized_views.len());
        for mv in materialized_views {
            registry.set_schema(mv.view_name.clone(), Arc::clone(&mv.output_schema));
            let plan = ctx
                .sql(&mv.query)
                .await
                .with_context(|| format!("plan vectorized SQL for {}", mv.view_name))?
                .create_physical_plan()
                .await
                .with_context(|| format!("create vectorized physical plan for {}", mv.view_name))?;
            mv_states.push(VectorizedMaterializedViewState {
                view_name: mv.view_name,
                output_schema: mv.output_schema,
                plan,
                previous_snapshot: Vec::new(),
            });
        }

        Ok(Self {
            ctx,
            sources: source_states,
            materialized_views: mv_states,
            registry,
        })
    }

    pub fn table_providers(&self) -> Vec<(String, Arc<dyn TableProvider>)> {
        let mut providers = Vec::new();
        for (source_name, source) in &self.sources {
            providers.push((
                source_name.clone(),
                Arc::clone(&source.provider) as Arc<dyn TableProvider>,
            ));
            if let Some(alias) = source_name.strip_prefix("nexmark_")
                && let Some(alias_provider) = source.alias_provider.as_ref()
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
        let state = self
            .sources
            .get(source_name)
            .ok_or_else(|| anyhow!("unknown vectorized source '{source_name}'"))?
            .clone();
        if batch.schema().as_ref() != state.schema.as_ref() {
            bail!("source batch schema does not match source '{source_name}'");
        }
        let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&state.schema)?;
        let weighted = add_weight_column(&batch, &weighted_schema, 1)?;
        self.apply_weighted_source_delta(source_name, weighted)
            .await
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
        let next = apply_source_delta_with_datafusion(&state, delta)
            .await
            .with_context(|| format!("apply vectorized source delta for '{source_name}'"))?;
        state.provider.set_batches(next.clone());
        if let (Some(alias_schema), Some(alias_provider)) =
            (state.alias_schema.as_ref(), state.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(&next, alias_schema)?);
        }
        Ok(())
    }

    pub async fn run_tick(&mut self, version: i64) -> Result<()> {
        for mv in &mut self.materialized_views {
            let plan_start = Instant::now();
            let mut next_snapshot = collect(Arc::clone(&mv.plan), self.ctx.task_ctx())
                .await
                .with_context(|| {
                    format!("execute vectorized materialized view '{}'", mv.view_name)
                })?;
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
            metrics::observe_delta_consolidation(
                diff.stats,
                diff_start.elapsed().as_millis() as u64,
            );

            let handle = self.registry.register(mv.view_name.clone());
            handle.publish_arrow_version(version, next_snapshot.clone(), diff.batches);
            mv.previous_snapshot = next_snapshot;
            tracing::debug!(
                view = %mv.view_name,
                version,
                total_ms = plan_start.elapsed().as_millis() as u64,
                "vectorized materialized view tick completed"
            );
        }
        Ok(())
    }
}

async fn apply_source_delta_with_datafusion(
    state: &VectorizedSourceState,
    delta: RecordBatch,
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

    let old_snapshot = state.provider.snapshot();
    let ctx = SessionContext::new();
    ctx.register_table(
        "state",
        Arc::new(MemTable::try_new(
            Arc::clone(&state.schema),
            vec![old_snapshot.iter().cloned().collect()],
        )?),
    )?;
    ctx.register_table(
        "delta",
        Arc::new(MemTable::try_new(delta.schema(), vec![vec![delta]])?),
    )?;

    let select_state = select_list("s", &state.schema);
    let select_delta = select_list("d", &state.schema);
    let delete_key_columns = if state.primary_key_columns.is_empty() {
        state
            .schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>()
    } else {
        state.primary_key_columns.clone()
    };
    let delete_keys = delete_key_columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let delete_join = delete_key_columns
        .iter()
        .map(|column| {
            let quoted = quote_ident(column);
            format!("s.{quoted} = deleted.{quoted}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT {select_state} \
         FROM state s \
         LEFT ANTI JOIN (SELECT DISTINCT {delete_keys} FROM delta WHERE {weight} < 0) deleted \
         ON {delete_join} \
         UNION ALL \
         SELECT {select_delta} FROM delta d WHERE d.{weight} > 0",
        weight = quote_ident(WEIGHT_COLUMN_NAME),
    );

    let batches = ctx.sql(&sql).await?.collect().await?;
    normalize_batches(batches, &state.schema)
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

fn select_list(alias: &str, schema: &SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|field| {
            let ident = quote_ident(field.name());
            format!("{alias}.{ident} AS {ident}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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
