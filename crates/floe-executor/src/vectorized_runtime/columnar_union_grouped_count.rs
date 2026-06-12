use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::DFSchemaRef;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use dbsp::storage::KeyValueTable;

use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;

use super::columnar_grouped_count::{
    ColumnarGroupedCountMaterializedViewState, ColumnarGroupedCountPlan,
    build_columnar_grouped_count_materialized_view_state_in_namespace,
    columnar_grouped_count_plan_for_plan, run_columnar_grouped_count_state_tick,
};
use super::columnar_union::{
    ColumnarUnionMaterializedViewState, ColumnarUnionPlan,
    build_columnar_union_materialized_view_state_in_namespace, columnar_union_plan_for_plan,
    run_columnar_union_delta_tick,
};
use super::{VectorizedMaterializedViewState, VectorizedSourceState, profile};

const UNION_GROUPED_COUNT_SOURCE: &str = "__floe_union_grouped_count_input";

pub(super) struct ColumnarUnionGroupedCountPlan {
    union_plan: ColumnarUnionPlan,
    grouped_count_plan: ColumnarGroupedCountPlan,
    union_source_name: String,
    union_schema: SchemaRef,
}

pub(super) struct ColumnarUnionGroupedCountMaterializedViewState {
    union: ColumnarUnionMaterializedViewState,
    grouped_count: ColumnarGroupedCountMaterializedViewState,
    union_source_name: String,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarUnionGroupedCountMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

pub(super) fn columnar_union_grouped_count_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    output_schema: &SchemaRef,
) -> Result<Option<ColumnarUnionGroupedCountPlan>> {
    let Some((union_input, rewritten_plan, union_source_name, union_schema)) =
        rewrite_aggregate_over_union_count(plan, sources)?
    else {
        return Ok(None);
    };
    let Some(union_plan) = columnar_union_plan_for_plan(&union_input, sources)? else {
        return Ok(None);
    };
    let derived_sources = derived_source_map(&union_source_name, &union_schema);
    let Some(grouped_count_plan) =
        columnar_grouped_count_plan_for_plan(&rewritten_plan, &derived_sources, output_schema)?
    else {
        return Ok(None);
    };
    Ok(Some(ColumnarUnionGroupedCountPlan {
        union_plan,
        grouped_count_plan,
        union_source_name,
        union_schema,
    }))
}

pub(super) async fn build_columnar_union_grouped_count_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarUnionGroupedCountPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarUnionGroupedCountMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let union_namespace = format!("{mv_namespace}/columnar/union_grouped_count/union");
    let grouped_count_namespace =
        format!("{mv_namespace}/columnar/union_grouped_count/grouped_count");
    let union = build_columnar_union_materialized_view_state_in_namespace(
        Arc::clone(&table),
        union_namespace,
        &plan.union_schema,
        plan.union_plan,
        sources,
        udfs,
    )
    .await
    .context("initialize union child for union grouped-count operator")?;
    let derived_sources = derived_source_map(&plan.union_source_name, &plan.union_schema);
    let grouped_count = build_columnar_grouped_count_materialized_view_state_in_namespace(
        table,
        grouped_count_namespace,
        output_schema,
        plan.grouped_count_plan,
        &derived_sources,
        udfs,
    )
    .await
    .context("initialize grouped-count child for union grouped-count operator")?;
    let initial_snapshot = grouped_count.initial_snapshot();
    Ok(ColumnarUnionGroupedCountMaterializedViewState {
        union,
        grouped_count,
        union_source_name: plan.union_source_name,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_union_grouped_count_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_union_grouped_count.as_mut() else {
        return Ok(false);
    };

    let plan_start = Instant::now();
    let total_start = profile::start();
    let phase_start = profile::start();
    let union_start = Instant::now();
    let union_delta =
        run_columnar_union_delta_tick(&mut columnar.union, insert_batches, weighted_delta_batches)
            .await
            .context("evaluate union child for union grouped-count operator")?;
    let union_ms = union_start.elapsed().as_millis() as u64;
    let union_delta_rows = union_delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    profile::record_since("union_grouped_count.union", phase_start);

    let phase_start = profile::start();
    let grouped_count_start = Instant::now();
    let mut union_deltas = HashMap::new();
    union_deltas.insert(
        columnar.union_source_name.clone(),
        union_delta.batches().to_vec(),
    );
    let grouped_tick = run_columnar_grouped_count_state_tick(
        &mut columnar.grouped_count,
        &HashMap::new(),
        &union_deltas,
        &mv.output_schema,
        &mv.previous_snapshot,
    )
    .await
    .context("evaluate grouped-count child for union grouped-count operator")?;
    let grouped_count_ms = grouped_count_start.elapsed().as_millis() as u64;
    let grouped_delta_rows = grouped_tick
        .delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    profile::record_since("union_grouped_count.grouped_count", phase_start);
    profile::record_since("union_grouped_count.total", total_start);

    let delta_batches = grouped_tick.delta.batches().to_vec();
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, grouped_tick.next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = grouped_tick.next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        union_ms,
        union_delta_rows,
        grouped_count_ms,
        grouped_delta_rows,
        mode = "columnar_union_grouped_count",
        "SlateDB-backed union grouped-count columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

fn rewrite_aggregate_over_union_count(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<(LogicalPlan, LogicalPlan, String, SchemaRef)>> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => {
            let union_input = aggregate.input.as_ref();
            if columnar_union_plan_for_plan(union_input, sources)?.is_none() {
                return Ok(None);
            }
            let union_schema = arrow_schema_for_df(union_input.schema());
            let union_source_name = derived_source_name(union_input);
            let rewritten_input = scan_plan_for_schema(&union_source_name, &union_schema)?;
            let mut rewritten = aggregate.clone();
            rewritten.input = Arc::new(rewritten_input);
            Ok(Some((
                union_input.clone(),
                LogicalPlan::Aggregate(rewritten),
                union_source_name,
                union_schema,
            )))
        }
        LogicalPlan::Projection(projection) => {
            let Some((union_input, rewritten_input, union_source_name, union_schema)) =
                rewrite_aggregate_over_union_count(projection.input.as_ref(), sources)?
            else {
                return Ok(None);
            };
            let mut rewritten = projection.clone();
            rewritten.input = Arc::new(rewritten_input);
            Ok(Some((
                union_input,
                LogicalPlan::Projection(rewritten),
                union_source_name,
                union_schema,
            )))
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            let Some((union_input, rewritten_input, union_source_name, union_schema)) =
                rewrite_aggregate_over_union_count(sort.input.as_ref(), sources)?
            else {
                return Ok(None);
            };
            let mut rewritten = sort.clone();
            rewritten.input = Arc::new(rewritten_input);
            Ok(Some((
                union_input,
                LogicalPlan::Sort(rewritten),
                union_source_name,
                union_schema,
            )))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let Some((union_input, rewritten_input, union_source_name, union_schema)) =
                rewrite_aggregate_over_union_count(alias.input.as_ref(), sources)?
            else {
                return Ok(None);
            };
            let mut rewritten = alias.clone();
            rewritten.input = Arc::new(rewritten_input);
            Ok(Some((
                union_input,
                LogicalPlan::SubqueryAlias(rewritten),
                union_source_name,
                union_schema,
            )))
        }
        _ => Ok(None),
    }
}

fn derived_source_name(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => alias.alias.table().to_string(),
        _ => UNION_GROUPED_COUNT_SOURCE.to_string(),
    }
}

fn scan_plan_for_schema(source_name: &str, schema: &SchemaRef) -> Result<LogicalPlan> {
    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(schema)));
    LogicalPlanBuilder::scan(
        source_name,
        provider_as_source(provider as Arc<dyn TableProvider>),
        None,
    )?
    .build()
    .map_err(Into::into)
}

fn derived_source_map(
    source_name: &str,
    schema: &SchemaRef,
) -> HashMap<String, VectorizedSourceState> {
    HashMap::from([(
        source_name.to_string(),
        VectorizedSourceState {
            schema: Arc::clone(schema),
            provider: Arc::new(DynamicStateTableProvider::new(Arc::clone(schema))),
            query_provider: None,
            maintain_execution_state: false,
            append_only: false,
            alias_schema: None,
            alias_provider: None,
            query_alias_provider: None,
            primary_key_columns: Vec::new(),
        },
    )])
}

fn arrow_schema_for_df(schema: &DFSchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}
