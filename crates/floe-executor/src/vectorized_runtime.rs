use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};
use datafusion::physical_plan::collect;
use dbsp::storage::KeyValueTable;
#[cfg(test)]
use dbsp::{FloeAsofJoinNode, create_logical_plan_with_asof_preplanner};
use floe_core::source::{SourceDefinition, SourceRegistry};

use crate::delta_consolidation::add_weight_column_to_batches;
use crate::mv::registry::MaterializedViewRegistry;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_source_delta::{
    apply_keyed_source_snapshot_delta, apply_source_delta, apply_weighted_snapshot_delta,
    insert_only_source_delta_batch, prepare_source_delta, validate_unit_source_delta,
};
use source_state::{
    camel_case_schema, dynamic_state_provider, rename_batches, source_key_indices,
    source_primary_key_columns,
};

const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";
const SOURCE_APPEND_ONLY_PROPERTY: &str = "append_only";
const SOURCE_QUERY_ALIAS_PROPERTY: &str = "query_alias";

mod columnar_constant;
mod columnar_count;
mod columnar_grouped_count;
mod columnar_grouped_max;
mod columnar_grouped_stats;
mod columnar_join;
mod columnar_join_topn;
mod columnar_stateless;
mod columnar_topn;
mod columnar_union;
mod columnar_union_grouped_count;
mod profile;
mod source_state;

pub use profile::{print_columnar_phase_profile, reset_columnar_phase_profile};

use columnar_constant::{
    ColumnarConstantMaterializedViewState, build_columnar_constant_materialized_view_state,
    columnar_constant_plan_for_plan, run_columnar_constant_materialized_view_tick,
};
use columnar_count::{
    ColumnarCountMaterializedViewState, build_columnar_count_materialized_view_state,
    columnar_count_plan_for_plan, run_columnar_count_materialized_view_tick,
};
use columnar_grouped_count::{
    ColumnarGroupedCountMaterializedViewState,
    build_columnar_grouped_count_materialized_view_state, columnar_grouped_count_plan_for_plan,
    run_columnar_grouped_count_materialized_view_tick,
};
use columnar_grouped_max::{
    ColumnarGroupedMaxMaterializedViewState, build_columnar_grouped_max_materialized_view_state,
    columnar_grouped_max_plan_for_plan, run_columnar_grouped_max_materialized_view_tick,
};
use columnar_grouped_stats::{
    ColumnarGroupedStatsMaterializedViewState,
    build_columnar_grouped_stats_materialized_view_state, columnar_grouped_stats_plan_for_plan,
    run_columnar_grouped_stats_materialized_view_tick,
};
use columnar_join::{
    ColumnarJoinMaterializedViewState, build_columnar_join_materialized_view_state,
    columnar_join_plan_for_plan, run_columnar_join_materialized_view_tick,
};
use columnar_join_topn::{
    ColumnarJoinTopNMaterializedViewState, build_columnar_join_topn_materialized_view_state,
    columnar_join_topn_plan_for_plan, run_columnar_join_topn_materialized_view_tick,
};
use columnar_stateless::{
    ColumnarStatelessMaterializedViewState, build_columnar_stateless_materialized_view_state,
    columnar_stateless_plan_for_plan, run_columnar_stateless_materialized_view_tick,
};
use columnar_topn::{
    ColumnarTopNMaterializedViewState, build_columnar_topn_materialized_view_state,
    columnar_topn_plan_for_plan, run_columnar_topn_materialized_view_tick,
};
use columnar_union::{
    ColumnarUnionMaterializedViewState, build_columnar_union_materialized_view_state,
    columnar_union_plan_for_plan, run_columnar_union_materialized_view_tick,
};
use columnar_union_grouped_count::{
    ColumnarUnionGroupedCountMaterializedViewState,
    build_columnar_union_grouped_count_materialized_view_state,
    columnar_union_grouped_count_plan_for_plan,
    run_columnar_union_grouped_count_materialized_view_tick,
};

#[derive(Debug, Clone)]
pub struct VectorizedMaterializedViewPlan {
    view_name: String,
    #[cfg(not(test))]
    logical_plan: LogicalPlan,
    #[cfg(test)]
    logical_plan: Option<LogicalPlan>,
    #[cfg(test)]
    query: Option<String>,
    output_schema: SchemaRef,
}

impl VectorizedMaterializedViewPlan {
    pub fn new(
        view_name: impl Into<String>,
        logical_plan: LogicalPlan,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            view_name: view_name.into(),
            #[cfg(not(test))]
            logical_plan,
            #[cfg(test)]
            logical_plan: Some(logical_plan),
            #[cfg(test)]
            query: None,
            output_schema,
        }
    }

    #[cfg(test)]
    fn from_sql(
        view_name: impl Into<String>,
        query: impl Into<String>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            view_name: view_name.into(),
            logical_plan: None,
            query: Some(query.into()),
            output_schema,
        }
    }
}

#[derive(Clone)]
pub struct VectorizedExecutionRuntimeOptions {
    pub maintain_source_query_tables: bool,
    pub source_query_table_names: Option<BTreeSet<String>>,
    pub operator_state_table: Option<Arc<dyn KeyValueTable>>,
    pub publish_grouped_stats_arrow_snapshots: bool,
}

impl Default for VectorizedExecutionRuntimeOptions {
    fn default() -> Self {
        Self {
            maintain_source_query_tables: false,
            source_query_table_names: None,
            operator_state_table: None,
            publish_grouped_stats_arrow_snapshots: true,
        }
    }
}

impl VectorizedExecutionRuntimeOptions {
    pub fn with_source_query_tables(mut self) -> Self {
        self.maintain_source_query_tables = true;
        self.source_query_table_names = None;
        self
    }

    pub fn with_source_query_tables_for<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.maintain_source_query_tables = true;
        self.source_query_table_names = Some(names.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_operator_state_table(mut self, table: Arc<dyn KeyValueTable>) -> Self {
        self.operator_state_table = Some(table);
        self
    }

    pub fn without_grouped_stats_arrow_snapshots(mut self) -> Self {
        self.publish_grouped_stats_arrow_snapshots = false;
        self
    }

    fn maintains_query_table_for(&self, source: &SourceDefinition) -> bool {
        if !self.maintain_source_query_tables {
            return false;
        }
        let Some(names) = self.source_query_table_names.as_ref() else {
            return true;
        };
        names.contains(source.name())
            || source
                .property(SOURCE_QUERY_ALIAS_PROPERTY)
                .is_some_and(|alias| names.contains(alias))
    }

    fn maintains_query_alias_for(&self, source: &SourceDefinition) -> bool {
        if !self.maintain_source_query_tables {
            return false;
        }
        let Some(alias) = source.property(SOURCE_QUERY_ALIAS_PROPERTY) else {
            return false;
        };
        let Some(names) = self.source_query_table_names.as_ref() else {
            return true;
        };
        names.contains(alias)
    }
}

impl std::fmt::Debug for VectorizedExecutionRuntimeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorizedExecutionRuntimeOptions")
            .field(
                "maintain_source_query_tables",
                &self.maintain_source_query_tables,
            )
            .field("source_query_table_names", &self.source_query_table_names)
            .field(
                "operator_state_table",
                &self.operator_state_table.as_ref().map(|_| "SlateDB"),
            )
            .field(
                "publish_grouped_stats_arrow_snapshots",
                &self.publish_grouped_stats_arrow_snapshots,
            )
            .finish()
    }
}

#[derive(Clone)]
struct VectorizedSourceState {
    schema: SchemaRef,
    provider: Arc<DynamicStateTableProvider>,
    query_provider: Option<Arc<DynamicStateTableProvider>>,
    maintain_execution_state: bool,
    append_only: bool,
    alias_name: Option<String>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    query_alias_provider: Option<Arc<DynamicStateTableProvider>>,
    primary_key_columns: Vec<String>,
}

struct VectorizedMaterializedViewState {
    view_name: String,
    output_schema: SchemaRef,
    previous_snapshot: Vec<RecordBatch>,
    operator: MaterializedViewOperator,
}

enum MaterializedViewOperator {
    Constant(Box<ColumnarConstantMaterializedViewState>),
    Stateless(Box<ColumnarStatelessMaterializedViewState>),
    GroupedCount(Box<ColumnarGroupedCountMaterializedViewState>),
    GroupedMax(Box<ColumnarGroupedMaxMaterializedViewState>),
    GroupedStats(Box<ColumnarGroupedStatsMaterializedViewState>),
    Join(Box<ColumnarJoinMaterializedViewState>),
    JoinTopN(Box<ColumnarJoinTopNMaterializedViewState>),
    UnionGroupedCount(Box<ColumnarUnionGroupedCountMaterializedViewState>),
    TopN(Box<ColumnarTopNMaterializedViewState>),
    Union(Box<ColumnarUnionMaterializedViewState>),
    CountByKey(Box<ColumnarCountMaterializedViewState>),
}

struct IncrementalMaterializedViewState {
    ctx: SessionContext,
    source_provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
}

type IncrementalContextProviders = (
    Arc<DynamicStateTableProvider>,
    Option<SchemaRef>,
    Option<Arc<DynamicStateTableProvider>>,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializedViewExecutionMode {
    ColumnarConstant,
    ColumnarStateless,
    ColumnarGroupedCount,
    ColumnarGroupedMax,
    ColumnarGroupedStats,
    ColumnarJoin,
    ColumnarJoinTopN,
    ColumnarUnionGroupedCount,
    ColumnarTopN,
    ColumnarUnion,
    ColumnarCountByKey,
}

impl MaterializedViewExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::ColumnarConstant => "columnar_constant",
            Self::ColumnarStateless => "columnar_stateless",
            Self::ColumnarGroupedCount => "columnar_grouped_count",
            Self::ColumnarGroupedMax => "columnar_grouped_max",
            Self::ColumnarGroupedStats => "columnar_grouped_stats",
            Self::ColumnarJoin => "columnar_join",
            Self::ColumnarJoinTopN => "columnar_join_topn",
            Self::ColumnarUnionGroupedCount => "columnar_union_grouped_count",
            Self::ColumnarTopN => "columnar_topn",
            Self::ColumnarUnion => "columnar_union",
            Self::ColumnarCountByKey => "columnar_count_by_key",
        }
    }
}

impl MaterializedViewOperator {
    fn mode(&self) -> MaterializedViewExecutionMode {
        match self {
            Self::Constant(_) => MaterializedViewExecutionMode::ColumnarConstant,
            Self::Stateless(_) => MaterializedViewExecutionMode::ColumnarStateless,
            Self::GroupedCount(_) => MaterializedViewExecutionMode::ColumnarGroupedCount,
            Self::GroupedMax(_) => MaterializedViewExecutionMode::ColumnarGroupedMax,
            Self::GroupedStats(_) => MaterializedViewExecutionMode::ColumnarGroupedStats,
            Self::Join(_) => MaterializedViewExecutionMode::ColumnarJoin,
            Self::JoinTopN(_) => MaterializedViewExecutionMode::ColumnarJoinTopN,
            Self::UnionGroupedCount(_) => MaterializedViewExecutionMode::ColumnarUnionGroupedCount,
            Self::TopN(_) => MaterializedViewExecutionMode::ColumnarTopN,
            Self::Union(_) => MaterializedViewExecutionMode::ColumnarUnion,
            Self::CountByKey(_) => MaterializedViewExecutionMode::ColumnarCountByKey,
        }
    }

    fn initial_snapshot(&self) -> Vec<RecordBatch> {
        match self {
            Self::Constant(state) => state.initial_snapshot(),
            Self::Stateless(state) => state.initial_snapshot(),
            Self::GroupedCount(state) => state.initial_snapshot(),
            Self::GroupedMax(state) => state.initial_snapshot(),
            Self::GroupedStats(state) => state.initial_snapshot(),
            Self::Join(state) => state.initial_snapshot(),
            Self::JoinTopN(state) => state.initial_snapshot(),
            Self::UnionGroupedCount(state) => state.initial_snapshot(),
            Self::TopN(state) => state.initial_snapshot(),
            Self::Union(state) => state.initial_snapshot(),
            Self::CountByKey(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
fn plan_contains_asof_extension(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<FloeAsofJoinNode>()
            .is_some(),
        LogicalPlan::Projection(projection) => {
            plan_contains_asof_extension(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => plan_contains_asof_extension(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => plan_contains_asof_extension(alias.input.as_ref()),
        LogicalPlan::Subquery(subquery) => plan_contains_asof_extension(subquery.subquery.as_ref()),
        LogicalPlan::Aggregate(aggregate) => plan_contains_asof_extension(aggregate.input.as_ref()),
        LogicalPlan::Sort(sort) => plan_contains_asof_extension(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => plan_contains_asof_extension(limit.input.as_ref()),
        LogicalPlan::Window(window) => plan_contains_asof_extension(window.input.as_ref()),
        LogicalPlan::Repartition(repartition) => {
            plan_contains_asof_extension(repartition.input.as_ref())
        }
        LogicalPlan::Distinct(distinct) => plan_contains_asof_extension(distinct.input()),
        LogicalPlan::Join(join) => {
            plan_contains_asof_extension(join.left.as_ref())
                || plan_contains_asof_extension(join.right.as_ref())
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| plan_contains_asof_extension(input.as_ref())),
        _ => false,
    }
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
    sources: HashMap<String, VectorizedSourceState>,
    materialized_views: Vec<VectorizedMaterializedViewState>,
    registry: Arc<MaterializedViewRegistry>,
    current_insert_batches: HashMap<String, Vec<RecordBatch>>,
    current_weighted_delta_batches: HashMap<String, Vec<RecordBatch>>,
}

impl VectorizedExecutionRuntime {
    #[cfg(test)]
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

    #[cfg(test)]
    pub async fn new_with_options(
        sources: &SourceRegistry,
        materialized_views: Vec<VectorizedMaterializedViewPlan>,
        registry: Arc<MaterializedViewRegistry>,
        options: VectorizedExecutionRuntimeOptions,
    ) -> Result<Self> {
        Self::new_with_udfs_and_options(sources, materialized_views, registry, Vec::new(), options)
            .await
    }

    #[cfg(test)]
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
            let append_only = definition
                .property(SOURCE_APPEND_ONLY_PROPERTY)
                .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1");
            let key_indices = source_key_indices(&schema, &primary_key_columns)?;
            let provider = Arc::new(dynamic_state_provider(
                Arc::clone(&schema),
                key_indices.as_deref(),
            )?);
            let maintain_query_table = options.maintains_query_table_for(definition);
            let query_provider = maintain_query_table
                .then(|| dynamic_state_provider(Arc::clone(&schema), key_indices.as_deref()))
                .transpose()?
                .map(Arc::new);
            ctx.register_table(
                definition.name(),
                Arc::clone(&provider) as Arc<dyn TableProvider>,
            )
            .with_context(|| format!("register vectorized source {}", definition.name()))?;

            let alias_name = definition
                .property(SOURCE_QUERY_ALIAS_PROPERTY)
                .map(str::to_string);
            let (alias_schema, alias_provider) = if let Some(alias) = alias_name.as_deref() {
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
            let query_alias_provider = if options.maintains_query_alias_for(definition) {
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
                    append_only,
                    alias_name,
                    alias_schema,
                    alias_provider,
                    query_alias_provider,
                    primary_key_columns,
                },
            );
        }

        let mut mv_states = Vec::with_capacity(materialized_views.len());
        for mv in materialized_views {
            #[cfg(not(test))]
            let logical_plan = mv.logical_plan.clone();
            #[cfg(test)]
            let logical_plan = if let Some(logical_plan) = mv.logical_plan.clone() {
                logical_plan
            } else {
                let query = mv.query.as_deref().ok_or_else(|| {
                    anyhow!("materialized view '{}' has no logical plan", mv.view_name)
                })?;
                let state = ctx.state();
                let asof_preplanned = create_logical_plan_with_asof_preplanner(&state, query)
                    .await
                    .with_context(|| format!("plan vectorized SQL for {}", mv.view_name))?;
                if plan_contains_asof_extension(&asof_preplanned) {
                    ctx.execute_logical_plan(asof_preplanned)
                        .await
                        .with_context(|| {
                            format!("build vectorized ASOF DataFrame for {}", mv.view_name)
                        })?
                        .logical_plan()
                        .clone()
                } else {
                    ctx.sql(query)
                        .await
                        .with_context(|| format!("plan vectorized SQL for {}", mv.view_name))?
                        .logical_plan()
                        .clone()
                }
            };
            let columnar_constant_plan = columnar_constant_plan_for_plan(&logical_plan);
            let columnar_constant = match (
                columnar_constant_plan,
                options.operator_state_table.as_ref(),
            ) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_constant_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &ctx,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar constant operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_count_plan = if columnar_constant.is_none() {
                columnar_count_plan_for_plan(&logical_plan, &source_states, &mv.output_schema)?
            } else {
                None
            };
            let columnar_count = match (columnar_count_plan, options.operator_state_table.as_ref())
            {
                (Some(plan), Some(table)) => Some(
                    build_columnar_count_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        plan,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar count operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_grouped_count_plan = if columnar_count.is_none() {
                columnar_grouped_count_plan_for_plan(
                    &logical_plan,
                    &source_states,
                    &mv.output_schema,
                )?
            } else {
                None
            };
            let columnar_grouped_count = match (
                columnar_grouped_count_plan,
                options.operator_state_table.as_ref(),
            ) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_grouped_count_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar grouped count operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_grouped_max_plan =
                if columnar_count.is_none() && columnar_grouped_count.is_none() {
                    columnar_grouped_max_plan_for_plan(
                        &logical_plan,
                        &source_states,
                        &mv.output_schema,
                    )?
                } else {
                    None
                };
            let columnar_grouped_max = match (
                columnar_grouped_max_plan,
                options.operator_state_table.as_ref(),
            ) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_grouped_max_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar grouped max operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_grouped_stats_plan = if columnar_count.is_none()
                && columnar_grouped_count.is_none()
                && columnar_grouped_max.is_none()
            {
                columnar_grouped_stats_plan_for_plan(
                    &logical_plan,
                    &source_states,
                    &mv.output_schema,
                )?
            } else {
                None
            };
            let columnar_grouped_stats = match (
                columnar_grouped_stats_plan,
                options.operator_state_table.as_ref(),
            ) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_grouped_stats_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                        options.publish_grouped_stats_arrow_snapshots,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar grouped stats operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_join_plan = if columnar_count.is_none()
                && columnar_grouped_count.is_none()
                && columnar_grouped_max.is_none()
                && columnar_grouped_stats.is_none()
            {
                columnar_join_plan_for_plan(&logical_plan, &source_states)?
            } else {
                None
            };
            let columnar_join = match (columnar_join_plan, options.operator_state_table.as_ref()) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_join_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar join operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_topn_plan = if columnar_count.is_none()
                && columnar_grouped_count.is_none()
                && columnar_grouped_max.is_none()
                && columnar_grouped_stats.is_none()
                && columnar_join.is_none()
            {
                columnar_topn_plan_for_plan(&logical_plan, &source_states)?
            } else {
                None
            };
            let columnar_topn = match (columnar_topn_plan, options.operator_state_table.as_ref()) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_topn_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar topn operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_join_topn_plan = if columnar_count.is_none()
                && columnar_grouped_count.is_none()
                && columnar_grouped_max.is_none()
                && columnar_grouped_stats.is_none()
                && columnar_join.is_none()
                && columnar_topn.is_none()
            {
                columnar_join_topn_plan_for_plan(&logical_plan, &source_states)?
            } else {
                None
            };
            let columnar_join_topn = match (
                columnar_join_topn_plan,
                options.operator_state_table.as_ref(),
            ) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_join_topn_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar join-topn operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_union_plan = if columnar_count.is_none()
                && columnar_grouped_count.is_none()
                && columnar_grouped_max.is_none()
                && columnar_grouped_stats.is_none()
                && columnar_join.is_none()
                && columnar_topn.is_none()
                && columnar_join_topn.is_none()
            {
                columnar_union_plan_for_plan(&logical_plan, &source_states)?
            } else {
                None
            };
            let columnar_union = match (columnar_union_plan, options.operator_state_table.as_ref())
            {
                (Some(plan), Some(table)) => Some(
                    build_columnar_union_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar union operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_stateless_plan = if columnar_count.is_none()
                && columnar_grouped_count.is_none()
                && columnar_grouped_max.is_none()
                && columnar_grouped_stats.is_none()
                && columnar_join.is_none()
                && columnar_topn.is_none()
                && columnar_join_topn.is_none()
                && columnar_union.is_none()
            {
                columnar_stateless_plan_for_plan(&logical_plan, &source_states)
            } else {
                None
            };
            let columnar_stateless = match (
                columnar_stateless_plan,
                options.operator_state_table.as_ref(),
            ) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_stateless_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &logical_plan,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar stateless operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let columnar_union_grouped_count_plan = if columnar_count.is_none()
                && columnar_grouped_count.is_none()
                && columnar_grouped_max.is_none()
                && columnar_grouped_stats.is_none()
                && columnar_join.is_none()
                && columnar_topn.is_none()
                && columnar_join_topn.is_none()
                && columnar_union.is_none()
                && columnar_stateless.is_none()
            {
                columnar_union_grouped_count_plan_for_plan(
                    &logical_plan,
                    &source_states,
                    &mv.output_schema,
                )?
            } else {
                None
            };
            let columnar_union_grouped_count = match (
                columnar_union_grouped_count_plan,
                options.operator_state_table.as_ref(),
            ) {
                (Some(plan), Some(table)) => Some(
                    build_columnar_union_grouped_count_materialized_view_state(
                        Arc::clone(table),
                        &mv.view_name,
                        &mv.output_schema,
                        plan,
                        &source_states,
                        &udfs,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed columnar union grouped-count operator for {}",
                            mv.view_name
                        )
                    })?,
                ),
                (Some(_), None) => {
                    bail!(
                        "materialized view '{}' requires SlateDB-backed operator state for columnar DBSP execution",
                        mv.view_name
                    );
                }
                _ => None,
            };
            let operator = if let Some(state) = columnar_constant {
                MaterializedViewOperator::Constant(Box::new(state))
            } else if let Some(state) = columnar_stateless {
                MaterializedViewOperator::Stateless(Box::new(state))
            } else if let Some(state) = columnar_grouped_count {
                MaterializedViewOperator::GroupedCount(Box::new(state))
            } else if let Some(state) = columnar_grouped_max {
                MaterializedViewOperator::GroupedMax(Box::new(state))
            } else if let Some(state) = columnar_grouped_stats {
                MaterializedViewOperator::GroupedStats(Box::new(state))
            } else if let Some(state) = columnar_join {
                MaterializedViewOperator::Join(Box::new(state))
            } else if let Some(state) = columnar_join_topn {
                MaterializedViewOperator::JoinTopN(Box::new(state))
            } else if let Some(state) = columnar_union_grouped_count {
                MaterializedViewOperator::UnionGroupedCount(Box::new(state))
            } else if let Some(state) = columnar_topn {
                MaterializedViewOperator::TopN(Box::new(state))
            } else if let Some(state) = columnar_union {
                MaterializedViewOperator::Union(Box::new(state))
            } else if let Some(state) = columnar_count {
                MaterializedViewOperator::CountByKey(Box::new(state))
            } else {
                let source_schemas = source_states
                    .iter()
                    .map(|(name, state)| format!("{name}: {:?}", state.schema.fields()))
                    .collect::<Vec<_>>();
                tracing::debug!(
                    view = %mv.view_name,
                    output_schema = ?mv.output_schema.fields(),
                    sources = ?source_schemas,
                    plan = %&logical_plan.display_indent(),
                    "materialized view did not match a SlateDB-backed columnar DBSP operator"
                );
                bail!(
                    "materialized view '{}' requires a supported SlateDB-backed columnar DBSP operator",
                    mv.view_name
                );
            };
            let previous_snapshot = operator.initial_snapshot();
            registry.set_schema(mv.view_name.clone(), Arc::clone(&mv.output_schema));
            registry.register(mv.view_name.clone());
            mv_states.push(VectorizedMaterializedViewState {
                view_name: mv.view_name,
                output_schema: mv.output_schema,
                previous_snapshot,
                operator,
            });
        }
        Ok(Self {
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
            if let Some(alias) = source.alias_name.as_deref()
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

    pub fn materialized_view_execution_modes(&self) -> Vec<(&str, &'static str)> {
        self.materialized_views
            .iter()
            .map(|mv| (mv.view_name.as_str(), mv.operator.mode().as_str()))
            .collect()
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
        let total_start = profile::start();
        let state = self
            .sources
            .get(source_name)
            .ok_or_else(|| anyhow!("unknown vectorized source '{source_name}'"))?
            .clone();
        let phase_start = profile::start();
        if let Some(insert_batch) = insert_only_source_delta_batch(&state.schema, &delta)? {
            profile::record_since("source_delta.insert_only_check", phase_start);
            let phase_start = profile::start();
            let result = self.apply_insert_source_batches(
                source_name,
                &state,
                vec![insert_batch.clone()],
                vec![insert_batch],
            );
            profile::record_since("source_delta.apply_insert_only", phase_start);
            profile::record_since("source_delta.total", total_start);
            return result;
        }
        profile::record_since("source_delta.insert_only_check", phase_start);
        let phase_start = profile::start();
        validate_unit_source_delta(&state.schema, &delta)
            .with_context(|| format!("validate weighted source delta for '{source_name}'"))?;
        profile::record_since("source_delta.validate", phase_start);
        let phase_start = profile::start();
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
        profile::record_since("source_delta.stage_weighted_delta", phase_start);

        let result = if state.primary_key_columns.is_empty() {
            let phase_start = profile::start();
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
            profile::record_since("source_delta.apply_unkeyed_query_state", phase_start);
            let phase_start = profile::start();
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
            profile::record_since("source_delta.apply_unkeyed_execution_state", phase_start);
            Ok(())
        } else {
            let phase_start = profile::start();
            let update = prepare_source_delta(&state.schema, &state.primary_key_columns, &delta)
                .with_context(|| format!("prepare keyed source delta for '{source_name}'"))?;
            profile::record_since("source_delta.prepare_keyed_delta", phase_start);
            let phase_start = profile::start();
            let positive_batches = update
                .final_positive_batch
                .as_ref()
                .map(|batch| vec![batch.clone()])
                .unwrap_or_default();
            if let Some(query_provider) = state.query_provider.as_ref() {
                query_provider.apply_keyed_delta(&update.touched_keys, positive_batches.clone())?;
            }
            profile::record_since("source_delta.apply_keyed_query_state", phase_start);
            let phase_start = profile::start();
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
            profile::record_since("source_delta.apply_keyed_execution_state", phase_start);
            let phase_start = profile::start();
            if let (Some(alias_schema), Some(alias_provider)) = (
                state.alias_schema.as_ref(),
                state.query_alias_provider.as_ref(),
            ) {
                alias_provider.apply_keyed_delta(
                    &update.touched_keys,
                    rename_batches(&positive_batches, alias_schema)?,
                )?;
            }
            profile::record_since("source_delta.apply_keyed_query_alias_state", phase_start);
            Ok(())
        };
        profile::record_since("source_delta.total", total_start);
        result
    }

    pub async fn run_tick(&mut self, version: i64) -> Result<()> {
        let registry = &self.registry;
        let insert_batches = &self.current_insert_batches;
        let weighted_delta_batches = &self.current_weighted_delta_batches;
        for mv in &mut self.materialized_views {
            match mv.operator.mode() {
                MaterializedViewExecutionMode::ColumnarConstant => {
                    run_columnar_constant_materialized_view_tick(registry, mv, version).await?
                }
                MaterializedViewExecutionMode::ColumnarStateless => {
                    run_columnar_stateless_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarCountByKey => {
                    run_columnar_count_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarGroupedCount => {
                    run_columnar_grouped_count_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarGroupedMax => {
                    run_columnar_grouped_max_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarGroupedStats => {
                    run_columnar_grouped_stats_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarJoin => {
                    run_columnar_join_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarJoinTopN => {
                    run_columnar_join_topn_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarTopN => {
                    run_columnar_topn_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarUnion => {
                    run_columnar_union_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
                MaterializedViewExecutionMode::ColumnarUnionGroupedCount => {
                    run_columnar_union_grouped_count_materialized_view_tick(
                        registry,
                        insert_batches,
                        weighted_delta_batches,
                        mv,
                        version,
                    )
                    .await?
                }
            }
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

async fn build_incremental_materialized_view_state_from_logical_plan(
    source_name: &str,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
    logical_plan: &LogicalPlan,
) -> Result<IncrementalMaterializedViewState> {
    let ctx = incremental_context_with_udfs(udfs);
    let (source_provider, alias_schema, alias_provider) =
        incremental_context_providers(&ctx, source_name, sources)?;
    let source_alias = sources
        .get(source_name)
        .and_then(|source| source.alias_name.as_deref());
    let logical_plan = rebind_incremental_logical_plan(
        logical_plan.clone(),
        source_name,
        source_alias,
        &source_provider,
        alias_provider.as_ref(),
    )?;
    let plan = ctx.state().create_physical_plan(&logical_plan).await?;
    Ok(IncrementalMaterializedViewState {
        ctx,
        source_provider,
        alias_schema,
        alias_provider,
        plan,
    })
}

fn incremental_context_with_udfs(udfs: &[ScalarUDF]) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for udf in udfs.iter().cloned() {
        ctx.register_udf(udf);
    }
    ctx
}

fn incremental_context_providers(
    ctx: &SessionContext,
    source_name: &str,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<IncrementalContextProviders> {
    let source = sources
        .get(source_name)
        .ok_or_else(|| anyhow!("unknown vectorized source '{source_name}'"))?;
    let source_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&source.schema)));
    ctx.register_table(
        source_name,
        Arc::clone(&source_provider) as Arc<dyn TableProvider>,
    )
    .with_context(|| format!("register isolated incremental source {source_name}"))?;

    let (alias_schema, alias_provider) = if let (Some(alias), Some(alias_schema)) =
        (source.alias_name.as_deref(), source.alias_schema.as_ref())
    {
        let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(alias_schema)));
        ctx.register_table(alias, Arc::clone(&provider) as Arc<dyn TableProvider>)
            .with_context(|| {
                format!("register isolated incremental source alias {alias} for {source_name}")
            })?;
        (Some(Arc::clone(alias_schema)), Some(provider))
    } else {
        (None, None)
    };
    Ok((source_provider, alias_schema, alias_provider))
}

fn rebind_incremental_logical_plan(
    logical_plan: LogicalPlan,
    source_name: &str,
    source_alias: Option<&str>,
    source_provider: &Arc<DynamicStateTableProvider>,
    alias_provider: Option<&Arc<DynamicStateTableProvider>>,
) -> Result<LogicalPlan> {
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) if scan.table_name.table() == source_name => {
            scan.source = provider_as_source(Arc::clone(source_provider) as Arc<dyn TableProvider>);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        LogicalPlan::TableScan(mut scan) if Some(scan.table_name.table()) == source_alias => {
            let Some(alias_provider) = alias_provider else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            scan.source = provider_as_source(Arc::clone(alias_provider) as Arc<dyn TableProvider>);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(transformed.data)
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

fn direct_projection_indices(
    logical_plan: &LogicalPlan,
    input_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let LogicalPlan::Projection(projection) = logical_plan else {
        return None;
    };
    projection
        .expr
        .iter()
        .map(|expr| match strip_projection_alias(expr) {
            Expr::Column(column) => input_schema.index_of(&column.name).ok(),
            _ => None,
        })
        .collect()
}

fn direct_project_record_batches(
    batches: &[RecordBatch],
    output_schema: &SchemaRef,
    indices: &[usize],
    label: &str,
) -> Result<Vec<RecordBatch>> {
    if indices.len() != output_schema.fields().len() {
        bail!(
            "{label} direct projection width {} does not match output width {}",
            indices.len(),
            output_schema.fields().len()
        );
    }
    let mut output = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let mut columns = Vec::with_capacity(indices.len());
        for (output_idx, input_idx) in indices.iter().copied().enumerate() {
            let column = batch.column(input_idx);
            let expected_type = output_schema.field(output_idx).data_type();
            if column.data_type() != expected_type {
                bail!(
                    "{label} direct projection column {} type {:?} does not match expected {:?}",
                    output_idx,
                    column.data_type(),
                    expected_type
                );
            }
            columns.push(Arc::clone(column));
        }
        output.push(RecordBatch::try_new(Arc::clone(output_schema), columns)?);
    }
    Ok(output)
}

fn strip_projection_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_projection_alias(alias.expr.as_ref()),
        _ => expr,
    }
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
